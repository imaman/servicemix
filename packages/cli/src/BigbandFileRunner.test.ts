import * as chai from 'chai';
import chaiSubset = require('chai-subset');
import * as tmp from 'tmp'
import * as path from 'path'
import * as fs from 'fs'
import * as child_process from 'child_process'

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';

import { LambdaInstrument, Section, BigbandSpec, Bigband, wire, BigbandInit } from 'bigband-core';
import { BigbandFileRunner } from './BigbandFileRunner';
import { BigbandModel } from './models/BigbandModel';
import { DeployMode } from './Packager';
import { S3Ref } from './S3Ref';
import { InstrumentModel } from './models/InstrumentModel';
import { CloudProvider } from './CloudProvider';


interface LambdaInput {
    event: any
    context: any
}

describe('BigbandFileRunner', () => {
    const bigbandInit: BigbandInit = {
        name: "b",
        profileName: CloudProvider.UNIT_TESTING_PROFILE_NAME,
        s3Prefix: "my_prefix",
        s3BucketGuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    }
    const b = new Bigband(bigbandInit)

    describe("compilation", () => {

        async function compileAndRun(bigbandSpec, pathToInstrument, accountId: string, content: string, input: LambdaInput) {

            const bigbandModel = new BigbandModel(bigbandSpec, "somedir", accountId)
            const instrument = bigbandModel.getInstrument(pathToInstrument)
            
            const bigbandFileRunner = new BigbandFileRunner(bigbandModel, 
                bigbandModel.findSectionModel(instrument.section.path), true, DeployMode.IF_CHANGED)            
            const dir = tmp.dirSync().name

            const nodeModules = path.resolve(dir, 'node_modules')
            fs.mkdirSync(nodeModules)
            fs.symlinkSync(path.resolve(__dirname, '../node_modules/@types'), path.resolve(nodeModules, '@types'), 
                'dir')            
            fs.symlinkSync(path.resolve(__dirname, '../../lambda/lib'), path.resolve(nodeModules, 'bigband-lambda'), 
                'dir')
            const srcFile = path.resolve(dir, (instrument.instrument as LambdaInstrument).getEntryPointFile() + '.ts')

            fs.writeFileSync(srcFile, content)

            const npmPackageDir = path.resolve(__dirname, '..')
            const temp = await bigbandFileRunner.compileInstrument(dir, npmPackageDir, instrument)

            const outDir = tmp.dirSync().name
            temp.zb.unzip(outDir)

            const outNodeModules = path.resolve(outDir, 'node_modules')
            if (!fs.existsSync(outNodeModules)) {
                fs.mkdirSync(outNodeModules)
            }
            fs.symlinkSync(path.resolve(__dirname, '../../lambda/node_modules/aws-sdk'),
                path.resolve(outNodeModules, 'aws-sdk'), 'dir')

            const stubFile = path.resolve(outDir, "stub.js")

            const stubFileContent = `
                const handler = require('./build/autogenerated/${instrument.instrument.fullyQualifiedName()}/handler.js')

                async function run() {
                    return new Promise((resolve, reject) => {
                        const cb = (err, done) => {
                            if (err) return reject(err) 
                            resolve(JSON.stringify(done))
                        }   
                        handler.handle(${JSON.stringify(input.event)}, ${JSON.stringify(input.context)}, cb)
                    })
                }

                run().then(v => console.log(v)).catch(e => console.error('failure', e))
            `
            fs.writeFileSync(stubFile, stubFileContent)

            // TODO(imaman): use the cp wrapper used elsewhere.
            const cp = child_process.fork(stubFile, [], {stdio: "pipe"})

            const stdout: string[] = []
            await new Promise((resolve, reject) => {
                cp.stdout.on('data', data => {
                    stdout.push(data.toString())
                })
    
                cp.stderr.on('data', data => {
                    reject(new Error(`Output emitted to stderr. First line: "${data.toString()}"`))
                })
    
                cp.on('exit', (code, signal) => {
                    resolve({code, signal})
                })    
            })

            return stdout.join('\n').trim()
        }

        it("compiles", async () => {
            const f1 = new LambdaInstrument("p1", "f1", "file_1")
            
            const spec: BigbandSpec = {
                bigband: b,
                sections: [{
                    section: new Section("r1", "s1"), 
                    instruments: [f1],
                    wiring: []
                }]
            }

            const content = `
                export default class Controller {
                    executeScheduledEvent(): void {}
                    
                    async runLambda(event: any, context: any): Promise<any> {                
                        return "context.a=" + context.a + ", event.b=" + event.b
                    }

                    initialize(x, y) {}
                }
                `
            const output = await compileAndRun(spec, "r1/s1/p1/f1", "", content, {context: {a: 1}, event: {b: 2}})
            expect(JSON.parse(output)).to.eql("context.a=1, event.b=2")
        })
        it("compiles and injects wires", async () => {
            const f1 = new LambdaInstrument("p1", "f1", "file_1")
            const f2 = new LambdaInstrument("p1", "f2", "file_2")
            
            const spec: BigbandSpec = {
                bigband: b,
                sections: [{
                    section: new Section("r1", "s1"), 
                    instruments: [f1, f2],
                    wiring: [wire(f1, "w1", f2)]
                }]
            }

            const content = `
                import {LambdaClient} from 'bigband-lambda'
                interface D {
                    w1: LambdaClient
                }

                export default class MyController {
                    private d: D
                    executeScheduledEvent(): void {}
                    
                    async runLambda(event: any, context: any): Promise<any> {                
                        return "w1=" + this.d.w1 
                    }

                    initialize(d: D, y) {
                        this.d = d;
                    }
                }
                `
            const output = await compileAndRun(spec, "r1/s1/p1/f1", 'a-id-600', content, 
                {context: {a: 1}, event: {b: 2}})
            expect(JSON.parse(output)).to.eql('w1=(LambdaClient region "r1", ARN "arn:aws:lambda:r1:a-id-600:function:b-s1-p1-f2")')
        })
    })

    describe("cloudformation template generation", () => {

        function computePushedInstruments(bigbandModel: BigbandModel, names) {
            return names.map(curr => bigbandModel.getInstrument(curr))
                .map((im: InstrumentModel) => ({
                        physicalName: im.physicalName,
                        wasPushed: true,
                        model: im,
                        s3Ref: new S3Ref("my_bucket", `my_prefix/${im.physicalName}.zip`)
                    }))
        }
        it("places the definition inside template", () => {
            const f1 = new LambdaInstrument("p1", "f1", "src/file_1")

            const spec: BigbandSpec = {
                bigband: b,
                sections: [{
                    section: new Section("r1", "s1"), 
                    instruments: [f1],
                    wiring: []
                }]
            }

            const bigbandModel = new BigbandModel(spec, "somedir")
            const bigbandFileRunner = new BigbandFileRunner(bigbandModel, bigbandModel.findSectionModel("r1/s1"), true,
                    DeployMode.IF_CHANGED)            

            const templateBody = bigbandFileRunner.buildCloudFormationTemplate(
                computePushedInstruments(bigbandModel, ["r1/s1/p1/f1"]))

            expect(templateBody).to.eql({
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "An unspecified bigband description",
                "Resources": {
                    "P1F1": {
                        "Properties": {
                            "CodeUri": "s3://my_bucket/my_prefix/b-s1-p1-f1.zip",
                            "Events": {},
                            "FunctionName": "b-s1-p1-f1",
                            "Handler": "build/autogenerated/p1-f1/handler.handle",
                            "Policies": [],
                            "Runtime": "nodejs8.10",
                        },
                        "Type": "AWS::Serverless::Function",
                    }
                },
                "Transform": "AWS::Serverless-2016-10-31"
            })
        })
        it("generates a logicl ID by pascal-casing the fully-qualified name", () => {
            const f1 = new LambdaInstrument(["abc", "def"], "this-is-the-name", "src/file_1")

            const spec: BigbandSpec = {
                bigband: b,
                sections: [{
                    section: new Section("r1", "s1"), 
                    instruments: [f1],
                    wiring: []
                }]
            }

            const bigbandModel = new BigbandModel(spec, "somedir")
            const bigbandFileRunner = new BigbandFileRunner(bigbandModel, bigbandModel.findSectionModel("r1/s1"), true,
                    DeployMode.IF_CHANGED)            

            const templateBody = bigbandFileRunner.buildCloudFormationTemplate(computePushedInstruments(bigbandModel, 
                ["r1/s1/abc/def/this-is-the-name"]))

            expect(templateBody).to.eql({
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "An unspecified bigband description",
                "Resources": {
                    "AbcDefThisIsTheName": {
                        "Properties": {
                            "CodeUri": "s3://my_bucket/my_prefix/b-s1-abc-def-this-is-the-name.zip",
                            "Events": {},
                            "FunctionName": "b-s1-abc-def-this-is-the-name",
                            "Handler": "build/autogenerated/abc-def-this-is-the-name/handler.handle",
                            "Policies": [],
                            "Runtime": "nodejs8.10",
                        },
                        "Type": "AWS::Serverless::Function",
                    }
                },
                "Transform": "AWS::Serverless-2016-10-31"
            })
        })
        describe("wiring", () => {
            it("allows wiring within the same section", () => {
                const f1 = new LambdaInstrument(["p1"], "f1", "src/file_1")
                const f2 = new LambdaInstrument(["p2"], "f2", "src/file_1")

                const spec: BigbandSpec = {
                    bigband: b,
                    sections: [{
                        section: new Section("r1", "s1"), 
                        instruments: [f1, f2],
                        wiring: [wire(f1, "w1", f2)]
                    }]
                }

                const bigbandModel = new BigbandModel(spec, "somedir")
                const bigbandFileRunner = new BigbandFileRunner(bigbandModel, bigbandModel.findSectionModel("r1/s1"), true,
                        DeployMode.IF_CHANGED)            

                const templateBody = bigbandFileRunner.buildCloudFormationTemplate(
                    computePushedInstruments(bigbandModel, ["r1/s1/p1/f1", "r1/s1/p2/f2"]))

                expect(templateBody).to.eql({
                    "AWSTemplateFormatVersion": "2010-09-09",
                    "Transform": "AWS::Serverless-2016-10-31",
                    "Description": "An unspecified bigband description",
                    "Resources": {
                        "P1F1": {
                            "Type": "AWS::Serverless::Function",
                            "Properties": {
                                "Runtime": "nodejs8.10",
                                "Policies": [
                                {
                                    "Version": "2012-10-17",
                                    "Statement": [{
                                        "Effect": "Allow",
                                        "Action": ["lambda:InvokeFunction" ],
                                        "Resource": "arn:aws:lambda:r1:<unspecfieid>:function:b-s1-p2-f2"
                                    }]
                                }
                                ],
                                "Events": {},
                                "Handler": "build/autogenerated/p1-f1/handler.handle",
                                "FunctionName": "b-s1-p1-f1",
                                "CodeUri": "s3://my_bucket/my_prefix/b-s1-p1-f1.zip"
                            }
                        },
                        "P2F2": {
                            "Type": "AWS::Serverless::Function",
                            "Properties": {
                                "Runtime": "nodejs8.10",
                                "Policies": [],
                                "Events": {},
                                "Handler": "build/autogenerated/p2-f2/handler.handle",
                                "FunctionName": "b-s1-p2-f2",
                                "CodeUri": "s3://my_bucket/my_prefix/b-s1-p2-f2.zip"
                            }
                        }
                    }
                })
            })
        })
        it("allows cross-section wiring", () => {
            const f1 = new LambdaInstrument(["p1"], "f1", "src/file_1")
            const f2 = new LambdaInstrument(["p2"], "f2", "src/file_1")

            const s1 = new Section("r1", "s1")
            const s2 = new Section("r2", "s2")

            const spec: BigbandSpec = {
                bigband: b,
                sections: [
                    {
                        section: s1,
                        instruments: [f1],
                        wiring: [wire(f1, "w1", f2, s2)]
                    },
                    {
                        section: s2,
                        instruments: [f2],
                        wiring: []
                    }
                ]
            }

            const bigbandModel = new BigbandModel(spec, "somedir")
            const bigbandFileRunner = new BigbandFileRunner(bigbandModel, bigbandModel.findSectionModel("r1/s1"), true,
                    DeployMode.IF_CHANGED)            

            const templateBody = bigbandFileRunner.buildCloudFormationTemplate(
                computePushedInstruments(bigbandModel, ["r1/s1/p1/f1"]))

                
            expect(templateBody).to.eql({
                "AWSTemplateFormatVersion": "2010-09-09",
                "Transform": "AWS::Serverless-2016-10-31",
                "Description": "An unspecified bigband description",
                "Resources": {
                    "P1F1": {
                        "Type": "AWS::Serverless::Function",
                        "Properties": {
                            "Runtime": "nodejs8.10",
                            "Policies": [
                            {
                                "Version": "2012-10-17",
                                "Statement": [{
                                    "Effect": "Allow",
                                    "Action": ["lambda:InvokeFunction" ],
                                    "Resource": "arn:aws:lambda:r2:<unspecfieid>:function:b-s2-p2-f2"
                                }]
                            }
                            ],
                            "Events": {},
                            "Handler": "build/autogenerated/p1-f1/handler.handle",
                            "FunctionName": "b-s1-p1-f1",
                            "CodeUri": "s3://my_bucket/my_prefix/b-s1-p1-f1.zip"
                        }
                    }
                }
            })
        })
    })
});

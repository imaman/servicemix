import * as path from 'path';
import * as fs from 'fs';
import * as hash from 'hash.js'
require('ts-node').register({})

import { AwsFactory, DeployableFragment } from 'bigband-core'
import { BigbandSpec, NameStyle, Instrument, LambdaInstrument } from 'bigband-core';
import { Packager, PushResult, DeployMode } from './Packager'
import { DeployableAtom } from 'bigband-core'
import { ZipBuilder } from './ZipBuilder'
import { S3Ref } from './S3Ref'
import { CloudFormationPusher } from './CloudFormationPusher';
import { UpdateFunctionCodeRequest } from 'aws-sdk/clients/lambda';
import { logger } from './logger';
import { S3BlobPool } from './Teleporter';
import { Misc } from './Misc';
import { CONTRIVED_NPM_PACAKGE_NAME, CONTRIVED_IN_FILE_NAME } from './scotty';
import { BigbandModel } from './models/BigbandModel';
import { SectionModel } from './models/SectionModel';
import { InstrumentModel } from './models/InstrumentModel';
import { Namer } from './Namer';
import { CloudProvider } from './CloudProvider';
import { CreateBucketRequest } from 'aws-sdk/clients/s3';

const DEPLOYABLES_FOLDER = 'deployables';

export { DeployMode } from './Packager'

function grantPermission(instrument: Instrument, action: string, arn: string) {
    instrument.getDefinition().mutate(o => o.Properties.Policies.push({
        Version: '2012-10-17',
        Statement: [{ 
            Effect: "Allow",
            Action: [
              action,
            ],
            Resource: arn
        }]
    }));
}


export interface PushedInstrument {
    s3Ref: S3Ref
    wasPushed: boolean
    existed: boolean
    physicalName: string
    model: InstrumentModel
}


// TODO(imaman): coverage. can be hard.
export class BigbandFileRunner {
    private readonly awsFactory: AwsFactory
    private readonly poolPrefix: string
    private readonly blobPool: S3BlobPool
    private readonly teleportInstrument: Instrument
    private readonly namer: Namer

    constructor(private readonly bigbandModel: BigbandModel, 
        private readonly sectionModel: SectionModel, 
        private readonly teleportingEnabled: boolean, 
        private readonly deployMode: DeployMode) {
            this.awsFactory = CloudProvider.get(this.sectionModel)
            this.poolPrefix = `${this.ttlPrefix()}/fragments`;
            this.blobPool = new S3BlobPool(this.awsFactory, this.s3Bucket, this.poolPrefix);
            this.teleportInstrument = new LambdaInstrument(['bigband', 'system'], 'teleport', CONTRIVED_IN_FILE_NAME, {
                Description: 'Rematerializes a deployable at the deployment site',
                MemorySize: 2560,
                Timeout: 30
            }).fromNpmPackage(CONTRIVED_NPM_PACAKGE_NAME);
        
            this.namer = new Namer(bigbandModel.bigband, sectionModel.section, bigbandModel.accountId)
    
        }


    private get s3Bucket(): string {
        if (this.sectionModel.section.s3Bucket) {
            return this.sectionModel.section.s3Bucket
        }

        return `${this.bigbandModel.bigband.s3BucketPrefix}-${this.bigbandModel.bigband.s3BucketGuid}`;
    }

    // TODO(imaman): rename to ship()
    static async runBigbandFile(bigbandFile: string, pathToSection: string, teleportingEnabled: boolean, deployMode: DeployMode) {
        const t0 = Date.now();
        if (Number(process.versions.node.split('.')[0]) < 8) {
            throw new Error('You must use node version >= 8 to run this program');
        }
        const bigbandModel = await BigbandFileRunner.loadModel(bigbandFile);
        const sectionModel = bigbandModel.findSectionModel(pathToSection)
        
        const flow = new BigbandFileRunner(bigbandModel, sectionModel, teleportingEnabled, deployMode)

        await flow.createBucketIfNeeded()
        
        await Promise.all([flow.runSpec(), flow.configureBucket()]);
        const dt = (Date.now() - t0) / 1000;
        return `Section "${sectionModel.section.name}" shipped in ${dt.toFixed(1)}s`;        
    }

    private async runSpec() {
        const cfp = new CloudFormationPusher(this.awsFactory);
        cfp.peekAtExistingStack();
            
        grantPermission(this.teleportInstrument, 's3:GetObject', 
            `arn:aws:s3:::${this.s3Bucket}/${this.poolPrefix}/*`);
    
        const deployablesLocation = `${this.s3Bucket}/${this.bigbandModel.bigband.s3Prefix}/${DEPLOYABLES_FOLDER}/*`
        grantPermission(this.teleportInstrument, 's3:PutObject',
            `arn:aws:s3:::${deployablesLocation}`);
        
        const section = this.sectionModel.section
        logger.info(`Shipping section "${section.name}" to ${section.region}`);
    
        const dir = this.bigbandModel.dir
        if (!dir) {
            throw new Error('Found a fasly dir') 
        }
    
        const ps = this.sectionModel.instruments.map(im => this.pushCode(dir, dir, im));
    
        const teleportModel = new InstrumentModel(this.bigbandModel.bigband, this.sectionModel,
            this.teleportInstrument, [], true)
        // teleporter needs slightly different parameters so we pushCode() it separately. 
        ps.push(this.pushCode(Misc.bigbandPackageDir(), dir, teleportModel));
        
        const pushedInstruments = await Promise.all(ps);
    
        const templateBody = this.buildCloudFormationTemplate(pushedInstruments)
        await cfp.deploy(templateBody)
    
        const promises = pushedInstruments
            .filter(curr => curr.s3Ref.isOk() && curr.wasPushed && curr.existed)
            .map(async curr => {
                const req: UpdateFunctionCodeRequest = {
                    FunctionName: curr.physicalName,
                    S3Bucket: curr.s3Ref.s3Bucket,
                    S3Key: curr.s3Ref.s3Key
                };    
                try {
                    return await this.awsFactory.newLambda().updateFunctionCode(req).promise();
                } catch (e) {
                    logger.error(`lambda.updateFunctionCode failure (${JSON.stringify(req)})`, e);
                    throw new Error(`Failed to update code of ${curr.physicalName}`);
                }
            })
        await Promise.all(promises)
    }        

    buildCloudFormationTemplate(pushedInstruments: PushedInstrument[]) {
        const ret = {
            AWSTemplateFormatVersion: '2010-09-09',
            Transform: 'AWS::Serverless-2016-10-31',
            Description: this.bigbandModel.bigband.description,
            Resources: {}
        };
    
        for (const curr of pushedInstruments) {
            const def = this.namer.getPhysicalDefinition(curr.model.instrument)

            for (const wireModel of curr.model.wirings) {
                const arn = wireModel.supplier.arn
                wireModel.supplier.instrument.contributeToConsumerDefinition(wireModel.consumer.section.section, def,
                    arn);
            }
    
            if (curr.s3Ref.isOk()) {
                def.mutate(o => o.Properties.CodeUri = curr.s3Ref.toUri());
            }

            const nameInStack = curr.model.instrument.fullyQualifiedName(NameStyle.PASCAL_CASE)
            ret.Resources[nameInStack] = def.get();
        }

        return ret
    }

    private async pushCode(dir: string, npmPackageDir: string, instrumentModel: InstrumentModel)
            : Promise<PushedInstrument> {
        if (!fs.existsSync(dir) || !fs.statSync(dir).isDirectory()) {
            throw new Error(`Bad value. ${dir} is not a directory.`);
        }
        
        const instrument = instrumentModel.instrument
        const physicalName = this.namer.physicalName(instrument);
        const def = this.namer.getPhysicalDefinition(instrument)
        if (!instrument.getEntryPointFile()) {
            return {
                s3Ref: S3Ref.EMPTY,
                wasPushed: false,
                existed: false,
                physicalName,
                model: instrumentModel
            }
        }
    
        const {zb, packager} = await this.compileInstrument(dir, npmPackageDir, instrumentModel);

        const s3Ref = new S3Ref(this.s3Bucket, 
            `${this.bigbandModel.bigband.s3Prefix}/${DEPLOYABLES_FOLDER}/${physicalName}.zip`)

        const pushResult: PushResult = await packager.pushToS3(this.namer.resolve(instrument), s3Ref, zb, 
            this.namer.physicalName(this.teleportInstrument), this.teleportingEnabled, this.deployMode,
            !instrumentModel.isSystemInstrument);
        const resource = def.get();
        resource.Properties.CodeUri = pushResult.deployableLocation.toUri();
    
        return {
            s3Ref: pushResult.deployableLocation,
            wasPushed: pushResult.wasPushed,
            existed: pushResult.existed,
            physicalName,
            model: instrumentModel
        }
    }
    
    async compileInstrument(d: string, npmPackageDir: string, instrumentModel: InstrumentModel) {
        const model: SectionModel = this.sectionModel
        const section = model.section
        const instrument = instrumentModel.instrument
        try {
            const pathPrefix = 'build';
            logger.info(`Compiling ${instrument.fullyQualifiedName()}`);
            const handlerFragment = instrument.createFragment(`../..`);
            const packager = new Packager(d, npmPackageDir, this.awsFactory, this.blobPool, handlerFragment);
    
            const zb: ZipBuilder = await packager.run(instrument.getEntryPointFile(), pathPrefix,
                    (instrument as LambdaInstrument).getNpmPackage(),
                    instrumentModel.instrument.fullyQualifiedName());
    
            const sha256 = hash.sha256();
            const fingerprintCalculator = (a: DeployableAtom) => {
                sha256.update(a.path);
                sha256.update(a.content);
            };
            zb.forEach(fingerprintCalculator);

            const bigbandFolderFragment = new DeployableFragment()

            const mapping = {};
            // TODO(imaman): coverage
            for (const wireModel of instrumentModel.wirings) {
                const reg = wireModel.supplier.section.section.region
                const arn = wireModel.supplier.arn
                const src = wireModel.supplier.instrument.contributeToConsumerCode(reg, arn)
                if (src) {
                    // TODO(imaman): wiremodel.name should be legal file name
                    bigbandFolderFragment.add(new DeployableAtom(`bigband/wires/${wireModel.name}.ts`, src))
                }
                mapping[wireModel.name] = {name: wireModel.supplier.physicalName, region: section.region};
            }

            bigbandFolderFragment.add(new DeployableAtom('bigband/deps.js', 
                `module.exports = ${JSON.stringify(mapping)}`));
        
            bigbandFolderFragment.forEach(fingerprintCalculator);
    
            const fp = Buffer.from(sha256.digest()).toString('base64');
            bigbandFolderFragment.add(new DeployableAtom('bigband/build_manifest.js',
                `module.exports = "${fp}";`));
    
            zb.importFragment(bigbandFolderFragment);
    
            return {zb, packager}
        } catch (e) {
            e.message = `(instrument: "${instrumentModel.path}", rootDir: "${d}", npmPackageDir: ` + 
                `"${npmPackageDir}") ${e.message}`;
            throw e;
        }
    }
    
    
    private ttlPrefix() {
        return `${this.bigbandModel.bigband.s3Prefix}/TTL/7d`;
    }     
    
    private async createBucketIfNeeded() {
        const regionA = await S3Ref.getRegion(this.awsFactory, this.s3Bucket)
        logger.silly(`region of ${this.s3Bucket} is "${regionA}"`)

        const sectionRegion = this.sectionModel.section.region
        if (regionA === sectionRegion) {
            return
        }
        
        const s3 = this.awsFactory.newS3();
        const cbr: CreateBucketRequest = {
            Bucket: this.s3Bucket,
            CreateBucketConfiguration: {
                LocationConstraint: sectionRegion
            }
        }
        try {
            logger.silly("creating a new bucket: " + JSON.stringify(cbr))
            await s3.createBucket(cbr).promise()
        } catch (e) {
            logger.silly("createBucket() failed", e)
            // check existence (again) in case the bucket was just created by someone else
            const regionB = await S3Ref.getRegion(this.awsFactory, this.s3Bucket)
            logger.silly("regionNow=" + regionB)
            if (regionB !== sectionRegion) {
                throw new Error(`Failed to create an S3 Bucket ("${this.s3Bucket}" at "${sectionRegion}"`)
            }
        }
    }

    private async configureBucket() {
        // You can check the content of the TTL folder via:
        // $ aws s3 ls s3://<isolation_scope_name>/root/TTL/7d/fragments/
    
        const prefix = `${this.ttlPrefix()}/`;
        const req: AWS.S3.PutBucketLifecycleConfigurationRequest = {
            Bucket: this.s3Bucket,
            LifecycleConfiguration: {
                Rules: [
                    {
                        Expiration: {
                            Days: 7
                        },
                        Filter: {
                            Prefix: prefix
                        },
                        ID: "BigbandStandardTTL7D",
                        Status: "Enabled"
                    }
                ]
            }
        };
    
        try {
            const s3 = this.awsFactory.newS3();
            await s3.putBucketLifecycleConfiguration(req).promise();
            logger.silly(`expiration policy set via ${JSON.stringify(req)}`);
        } catch (e) {
            console.error(`s3.putBucketLifecycleConfiguration failure (request: ${JSON.stringify(req)})`, e);
            throw new Error(`Failed to set lifecycle policies (bucket: ${req.Bucket}, prefix: ${prefix})`);
        }
    }    

    static async loadModel(bigbandFile: string): Promise<BigbandModel> {
        if (!bigbandFile) {
            throw new Error('bigbandFile cannot be falsy');
        }

        const pathToRequire = path.resolve(bigbandFile)
    
        // const uninstall = installCustomRequire();
        let bigbandSpec: BigbandSpec
        try {
            logger.silly(`Loading bigbandfile from ${pathToRequire} using protocolversion`);
            const f = require(path.resolve(bigbandFile))
            bigbandSpec = f.run()
        } finally {
            // uninstall();
        }

        const accountId = await CloudProvider.getAccountId(bigbandSpec.bigband.profileName)
        return new BigbandModel(bigbandSpec, path.dirname(pathToRequire), accountId)
    }    
}






// TODO list:
// + rig -> section (in the public API)
// + rename scotty to bigand-system-teleport
// + add a special version indication at dataplatform
// - introduce a metric-monster npm with intruments + source code for superclasses
// - migrate metric-machine to reuse metric-monster
// - deploy the tagging ui to github.testim.io
// - new CLI language
// - show telport in the CLI (logs, list)
// - migrate dataplatform lambdas to the new stubs. get rid of the rogue abstratcontroller there
// - injected stubs
//
// - hard to understand error message when tsc compilation fails (for instance, error in the bigband file)
// - improve parsing of the output of npm ls. Sepcficially, it looks like the devDependencies just mentions the names of deps.
//     These deps are listed under "dependencies" so cross filtering is needed.
// - add checking of names. in particular, "bigband" and "bigband-system" should be reserved.
//     We probably want to disallow the use of hyphens inside logical names as they are used as separators.
// - consider changing "package" to something different (to avoid collision with "NPM packages")
// - bigbandfilerunner should be a class
// - rig -> section (in the impl)
// - unit tests for as much of /cli/src/ as possible


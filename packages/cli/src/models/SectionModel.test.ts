import * as chai from 'chai';
import chaiSubset = require('chai-subset');

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';

import { LambdaInstrument, Section, Bigband, SectionSpec, BigbandSpec } from 'bigband-core';
import { SectionModel } from './SectionModel'
import { BigbandModel } from './BigbandModel';


describe('SectionModel', () => {
    const b = new Bigband({
        awsAccount: "a",
        name: "b",
        profileName: "p",
        s3Bucket: "my_bucket",
        s3Prefix: "my_prefix"
    })

    describe('instruments', () => {
        function createSectionModel(s: SectionSpec): SectionModel {
            const bigbandSpec: BigbandSpec = {
                bigband: b,
                sections: [s]
            }

            const bm = new BigbandModel(bigbandSpec, "_")
            return bm.findSectionModel(s.section.path)
        }

        it('returns all instruments', () => {
            const f1 = new LambdaInstrument("p1", "f1", "src/file_1")
            const f2 = new LambdaInstrument("p1", "f2", "src/file_2")
            const spec: SectionSpec = {
                section: new Section("r1", "s1"), 
                instruments: [f1, f2],
                wiring: []
            }

            const model = createSectionModel(spec)
            expect(model.instruments.map(i => i.instrument)).to.eql([f1, f2])
        });
    })
    describe("validation", () => {
        // TODO(imaman): check validity of region (small-caps, digits, dash)
        describe("name", () => {
            it("allows dash-separated sequences of lower-case letters and digits", () => {
                const spec: SectionSpec = {
                    section: new Section("r1", "abc-def58-xyz"), 
                    instruments: [],
                    wiring: []
                }
    
                const model = new SectionModel(b, spec)
                expect(() => model.validate()).not.to.throw()
            });
            it("rejects upper-case letters", () => {
                const spec: SectionSpec = {
                    section: new Section("r1", "aBc"), 
                    instruments: [],
                    wiring: []
                }
    
                const model = new SectionModel(b, spec)
                expect(() => model.validate()).to.throw()
            });
        })
    })
});

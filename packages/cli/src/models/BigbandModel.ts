import { BigbandSpec, Instrument, Section, WireSpec, SectionSpec, Bigband } from "bigband-core";
import { Misc } from "../Misc";
import { SectionModel } from "./SectionModel";
import { InstrumentModel } from "./InstrumentModel";
import { Namer } from "../Namer";


export interface AssignedInstrument {
    instrument: Instrument
    section: Section
}


export interface LookupResult {
    section: Section
    instrument: Instrument
    // TODO(imaman): rename to physical name
    name: string
}

export class BigbandModel {

    public readonly dir: string
    constructor(private readonly spec: BigbandSpec, defaultDir: string) {
        if (!defaultDir) {
            throw new Error('defaultDir cannot be falsy')
        }

        this.dir = spec.dir || defaultDir
        this.validate()
    }

    get bigband(): Bigband {
        return this.sections[0].section.bigband
    }

    searchInstrument(instrumentName: string): LookupResult {
        const matches: LookupResult[] = [];
        const names: string[] = [];    
        const exactMatches: LookupResult[] = []
    

       this.sections.forEach(sectionSpec => {
            sectionSpec.instruments.forEach(curr => {
                const name = new Namer(this.bigband, sectionSpec.section).physicalName(curr.instrument)
                const lookupResult = {section: sectionSpec.section, instrument: curr.instrument, name};

                if (curr.instrument.name == instrumentName) {
                    exactMatches.push(lookupResult)
                } 
                names.push(name);
                if (name.indexOf(instrumentName) >= 0) {
                    matches.push(lookupResult);
                }
            });
        });

        if (exactMatches.length === 1) {
            return exactMatches[0]
        }
    
        if (!matches.length) {
            throw new Error(`Instrument "${instrumentName}" not found in ${JSON.stringify(names)}`);
        }
    
        if (matches.length > 1) {
            throw new Error(`Multiple matches on "${instrumentName}": ${JSON.stringify(matches.map(x => x.name))}`);
        }
    
        return matches[0];    
    }

    findSectionModel(sectionName: string): SectionModel {
        const sections = this.sections
        const ret = sections.length === 1 && !sectionName ? sections[0] : sections.find(curr => curr.section.name === sectionName);
        if (!ret) {
            throw new Error(`Failed to find a section named ${sectionName} in ${sections.map(curr => curr.section.name).join(', ')}`);
        }    

        return ret
    }

    private get sections(): SectionModel[] {
        return this.spec.sections.map(s => new SectionModel(s))
    }

    computeList() {
        const ret = {};
        const bigbandObject = {}
        ret[this.bigband.name] = bigbandObject;
        this.sections.forEach(sectionModel => {
            const secObject = {};
            bigbandObject[sectionModel.section.name] = secObject;
            const namer = new Namer(this.bigband, sectionModel.section)
            sectionModel.instruments.forEach(curr => 
                secObject[namer.physicalName(curr.instrument)] = curr.instrument.arnService());
        });
    
        return ret;    
    }

    validate() {
        this.sections.forEach(curr => curr.validate())

        const numBigbanbds = new Set(this.sections.map(s => s.section.bigband)).size
        if (numBigbanbds != 1) {
            throw new Error("multiple bigband instances")
        }

        let dupes = Misc.checkDuplicates(this.sections.map(s => s.section.name));
        if (dupes.length) {
            throw new Error(`Section name collision. The following names were used by two (or more) sections: ${JSON.stringify(dupes)}`);
        }

        const instruments: InstrumentModel[] = Misc.flatten(this.sections.map(s => s.instruments))
        dupes = Misc.checkDuplicates(instruments.map(curr => curr.physicalName))
        if (dupes.length) {
            throw new Error('Instrument name collision. The following names were used by two (or more) instruments: ' +
                    JSON.stringify(dupes));
        }
        
        // TODO(imaman): validate name length + characters
    }
}

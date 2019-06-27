import { BigbandSpec, Instrument, Section, WireSpec, SectionSpec } from "bigband-core";
import { Misc } from "../Misc";
import { SectionModel } from "./SectionModel";


export interface AssignedInstrument {
    instrument: Instrument
    section: Section
}


export interface LookupResult {
    section: Section
    instrument: Instrument
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

    searchInstrument(instrumentName: string): LookupResult {
        const matches: LookupResult[] = [];
        const names: string[] = [];    
        const exactMatches: LookupResult[] = []
    

       this.sectionModels.forEach(sectionSpec => {
            sectionSpec.instruments.forEach(curr => {
                const name = curr.instrument.physicalName(sectionSpec.section);
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
        const models = this.sectionModels
        const ret = models.length === 1 && !sectionName ? models[0] : models.find(curr => curr.section.name === sectionName);
        if (!ret) {
            throw new Error(`Failed to find a section named ${sectionName} in ${models.map(curr => curr.section.name).join(', ')}`);
        }    

        return ret
    }

    get instruments(): Instrument[] {
        return Misc.flatten(this.spec.sections.map(s => s.instruments))
    }

    get assignedInstruments(): AssignedInstrument[] {
        return Misc.flatten(this.spec.sections.map(s => s.instruments.map(i => ({instrument: i, section: s.section}))))
    }

    get sections(): Section[] {
        return this.spec.sections.map(s => s.section)
    }

    get sectionModels(): SectionModel[] {
        return this.spec.sections.map(s => new SectionModel(s))
    }

    validate() {
        this.sectionModels.forEach(curr => curr.validate())

        // TODO(imaman): all instruments mentioned in wiring are also defined in the "instruments" field of the section mentioned in the wiring
        // TODO(imaman): validate there is only one bigband
        let dupes = Misc.checkDuplicates(this.sections.map(s => s.name));
        if (dupes.length) {
            throw new Error(`Section name collision. The following names were used by two (or more) sections: ${JSON.stringify(dupes)}`);
        }
    
        dupes = Misc.checkDuplicates(this.assignedInstruments.map(curr => curr.instrument.physicalName(curr.section)))
        if (dupes.length) {
            throw new Error('Instrument name collision. The following names were used by two (or more) instruments: ' +
                    JSON.stringify(dupes));
        }
        
        // TODO(imaman): validate name length + characters
    }
}
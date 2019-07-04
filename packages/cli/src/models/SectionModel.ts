import { SectionSpec, Instrument, Bigband } from "bigband-core";
import { InstrumentModel } from "./InstrumentModel";
import { NameValidator } from "../NameValidator";

export class SectionModel {
    constructor(readonly bigband: Bigband, private readonly spec: SectionSpec,
            private readonly instruments_: InstrumentModel[] = []) {}

    get section() {
        return this.spec.section
    }

    get path(): string {
        return this.section.path
    }

    get instruments(): InstrumentModel[] {
        return [...this.instruments_]
    }

    get physicalName(): string {
        return `${this.bigband.name}-${this.section.name}`;
    }

    getInstrumentModel(instrument: Instrument): InstrumentModel {
        const subPath = instrument.path
        const ret = this.instruments.find(curr => curr.instrument.path === subPath)
        if (!ret) {
            throw new Error(`Section ${this.path} does not contain an instrument at sub path ("${subPath}")`)
        }

        return ret
    }

    validate() {
        const name = this.section.name
        if(!NameValidator.isOk(name)) {
            throw new Error(`Bad section name: "${name}"`)
        }
        this.instruments.forEach(curr => curr.validate())
    }
}

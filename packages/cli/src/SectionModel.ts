import { SectionSpec } from "bigband-core";
import { InstrumentModel } from "./InstrumentModel";

export class SectionModel {
    constructor(private readonly spec: SectionSpec) {}

    get section() {
        return this.spec.section
    }

    get instruments(): InstrumentModel[] {
        return this.spec.instruments.map(i => new InstrumentModel(this.spec.section, i, this.spec.wiring.filter(w => w.consumer === i)))
    }


    validate() {
        this.instruments.forEach(curr => curr.validate())
    }
}

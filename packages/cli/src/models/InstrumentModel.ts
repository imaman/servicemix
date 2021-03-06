import { Instrument, Section, Bigband, CompositeName } from "bigband-core";
import { Misc } from "../Misc";
import { Namer } from "../Namer";
import { NameValidator } from "../NameValidator";
import { WireModel } from "./WireModel";
import { NavigationNode } from "../NavigationNode";
import { Role, NavigationItem } from "bigband-core";
import { CloudProvider } from "../CloudProvider";
import { SectionModel } from "./SectionModel";

export class InstrumentModel {
    constructor(private readonly bigband: Bigband, public readonly section: SectionModel,
        public readonly instrument: Instrument,
        // TODO(imaman): make wirings private
        // TODO(imaman): rename to wires
        public readonly wirings: WireModel[], readonly isSystemInstrument) {}

    private get namer(): Namer {
        return new Namer(this.bigband, this.section.section, this.section.bigband.accountId)
    }
    
    get physicalName(): string {
        return this.namer.physicalName(this.instrument)
    }

    get path(): string {
        return this.namer.path(this.instrument)
    }

    get arn(): string {
        return this.namer.resolve(this.instrument).arn
    }

    validate() {
        if (!NameValidator.isCompositeNameOk(this.instrument.cname)) {
            throw new Error(`Bad instrument name: "${this.instrument.fullyQualifiedName()}"`)
        }
        // Reserve the "bigband" top-level package for system instruments.
        if (!this.isSystemInstrument) {
            const topLevel = this.instrument.topLevelPackageName
            const fqn = this.instrument.fullyQualifiedName()
            if (topLevel.toLowerCase() == 'bigband') {
                throw new Error(`Instrument "${fqn}" has a bad name: the fully qualified name of ` +
                    'an instrument is not allowed to start with "bigband"');
            }    
        }

        const dups = Misc.checkDuplicates(this.wirings.map(w => w.name))
        if (dups.length) {
            throw new Error(`Name collision(s) in wiring of "${this.physicalName}": ${JSON.stringify(dups)}`)
        }
    }

    generateNavigationNodes(root: NavigationNode, generateSynthetic = true) {
        let node = root.navigate(this.section.path)
        if (!node) {
            throw new Error(`Path ${this.section.path} leads to nowhere`)
        }

        let path = CompositeName.fromString(this.section.path)

        for (const curr of this.instrument.cname.butLast().all) {
            path = path.append(curr)
            let item: NavigationItem  = {
                path: path.toString(),
                role: Role.PATH,
            }
            node = node.addChild(curr, item)
        }

        const last = this.instrument.cname.last("")
        path = path.append(last)
        const item = {
            path: path.toString(),
            role: Role.INSTRUMENT,
            type: this.instrument.arnService()
        }

        const instrumentNode = node.addChild(last, item)


        const awsFactory = CloudProvider.get(this.section)
        
        if (generateSynthetic) {
            const items: Map<string, NavigationItem> = this.instrument.getNavigationItems(
                CompositeName.fromString(this.path), this.arn, this.physicalName, awsFactory)

            for (const token of items.keys()) {
                const item = items.get(token)
                if (!item) {
                    throw new Error('No item found at ' + token)
                }
                instrumentNode.addChild(token, item)
            }
        }
    }
}

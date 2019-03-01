import {DeployableFragment} from './DeployableFragment';
import {Section} from './Section';
import {Definition} from './Definition';

export enum NameStyle {
    DASH,
    CAMEL_CASE
}


class Dependency {
    constructor(readonly consumer: Instrument, readonly supplier: Instrument, readonly name: string) {}
}

export abstract class Instrument {

    protected readonly definition = new Definition();
    public readonly dependencies: Dependency[] = [];

    constructor(
        private readonly packageName: string,
        private readonly _name: string) {}

    uses(supplier: Instrument, name: string) {
        const existingDep = this.dependencies.find(d => d.name === name);
        if (existingDep) {
            throw new Error(`Name conflict. This instrument (${this.fullyQualifiedName()}) already has a dependency named ${name} (on ${existingDep.supplier.fullyQualifiedName()})`);
        }
        this.dependencies.push(new Dependency(this, supplier, name));
    }

    canDo(action: string, arn: string) {
        this.definition.mutate(o => o.Properties.Policies.push({
            Version: '2012-10-17',
            Statement: [{ 
                Effect: "Allow",
                Action: [
                  action,
                ],
                Resource: arn
            }]
        }));      
        return this;
    }

    abstract createFragment(pathPrefix: string): DeployableFragment
    abstract contributeToConsumerDefinition(rig: Section, consumerDef: Definition): void
    abstract arnService(): string
    abstract arnType(): string
    abstract nameProperty(): string
    abstract getEntryPointFile(): string

    name(): string {
        return this._name;
    }

    fullyQualifiedName(style: NameStyle = NameStyle.DASH) {
        if (style == NameStyle.DASH) {
            return `${this.packageName}-${this.name()}`;
        }

        return camelCase(this.packageName, this.name());
    }

    physicalName(rig: Section, style: NameStyle = NameStyle.DASH) {
        if (style == NameStyle.DASH) {
            return `${rig.isolationScope.name}-${rig.name}-${this.fullyQualifiedName()}`;
        }

        return camelCase(rig.isolationScope.name, rig.name, this.packageName, this.name());
    }
    
    arn(rig: Section): string {
        return `arn:aws:${this.arnService()}:${rig.region}:${rig.isolationScope.awsAccount}:${this.arnType()}${this.physicalName(rig)}`;
    }

    getDefinition() : Definition {
        return this.definition;
    }

    getPhysicalDefinition(rig: Section) : Definition {
        const copy = JSON.parse(JSON.stringify(this.definition.get()));
        copy.Properties[this.nameProperty()] = this.physicalName(rig);
        return new Definition(copy);
    }

    // contributeToConsumerCode(deployable: Deployable) {
    //     throw new Error('Not implemented yet.');
    // }
}

function camelCase(...args) {
    function capitalize(s: string) {
        if (!s) {
            throw new Error('Cannot capitalize an empty string');
        }
        return s[0].toUpperCase() + s.substr(1);
    }

    return args.map((curr, i) => i === 0 ? curr : capitalize(curr)).join('');
}

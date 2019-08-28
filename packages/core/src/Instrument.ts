import {DeployableFragment} from './DeployableFragment';
import {Section} from './Section';
import {Definition} from './Definition';
import { CompositeName } from './CompositeName';
import { NavigationItem } from './NavigationItem';
import { AwsFactory } from './AwsFactory';

export enum NameStyle {
    DASH,
    CAMEL_CASE,
    PASCAL_CASE,
    SLASH
}


/** 
 * Bigband's basic building block. Usually corresponds to an AWS resources such as: a Lambda function, a DynamoDB
 * table, a Kinesis stream, etc.
 * 
 * Naming. Inside every [[Section]], Instruments are arranged in a tree-like hierarchy (similar to the way files are
 * arranged in directories). This enables logical grouping of related instruments. The packageName (denoting a "path
 * in the tree") is specified as an array of string: ["p1", "p2", "p3"] denotes a package nested inside the
 * ["p1", "p2"] package. For brevity, a packageName can also be specified as a plain string: "p1" is equivalent to
 * ["p1"]. The instrument's plain name must be unique within its package. In other words: two instruments can have
 * the same simple name if they belond to two different packages.
 */
export abstract class Instrument {

    protected readonly definition = new Definition();
    readonly cname: CompositeName

    /**
     * Initializes an Instrument.
     * 
     * @param {(string|string[])} packageName the package name of the instrument. See "Naming" above.
     * @param {string} plainName the instrument's simple name (must be unique within its package). See "Naming" above.
     * @memberof Instrument
     */
    constructor(packageName: string|string[], simpleName: string) {
        const arr = (Array.isArray(packageName) ? packageName : [packageName])
        const temp = CompositeName.fromArray(arr)
        this.cname = temp.append(simpleName)
        if (!arr.join('').trim().length) {
            throw new Error('pacakge name cannot be empty');
        }

        const withUpperCase = arr.find(curr => curr.search(/[A-Z]/) >= 0)
        if (withUpperCase) {
            throw new Error(`Upper-case symbols are not allowed in package names. Found: "${withUpperCase}"`);
        }
        if (!simpleName.trim().length) {
            throw new Error('name cannot be empty');
        }
    }

    /**
     * Returns a [[DeployableFragment]] to be added to the bundle of shipped code
     * @param pathPrefix path to the directory where the compiled sources file will reside
     */
    abstract createFragment(pathPrefix: string): DeployableFragment

    /**
     * Called on supplier instruments (as per the [[uses]] method). This allows supplier instruments to affect the
     * cloudformation template of their consumer insturments. A supplier would typically add an IAM permission to its
     * consumer. 
     * @param section 
     * @param consumerDef 
     */
    abstract contributeToConsumerDefinition(section: Section, consumerDef: Definition, myArn: string): void

    contributeToConsumerCode(region: string, myArn: string, physicalName: string): string {
        return ""
    }

    /**
     * Returns the AWS service namespace to be used when constructing the ARN of this instrument. For instance, in a
     * an ARN of a lambda function, "arn:aws:lambda:eu-west-2:111111111111:function:my-function", this is the "lamda"
     * token.
     */
    abstract arnService(): string
    /**
     * Returns the AWS resource name to be used when constructing the ARN of this instrument. For instance, in a
     * an ARN of a lambda function, "arn:aws:lambda:eu-west-2:111111111111:function:my-function", this is the
     * "function" token.
     */
    abstract arnType(): string
    /**
     * Returns the name of the cloudformation property whose value specifies the exact name of the resource. You need
     * to check Cloudformation's "Resource and Property Types Reference" 
     * (https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-template-resource-type-ref.html) to find
     * the exact value. For instance, for a lambda function this would be "FunctionName", wheras for a DynamoDB table
     * this would be "TableName"
     */
    abstract nameProperty(): string
    
    /**
     * Returns the path to a file that needs to be compiled with this instrument. This is needed for instruments
     * that need user-supplied code to be shipped, such as Lambda instruments. An empty string denotes that no node
     * needs to be shipped.
     */
    abstract getEntryPointFile(): string

    /**
     * Computes the full name of this instrument. The full name is a composition of the "last name" (as specified by the
     * package name) with the "first name" (this instrument's name)
     *
     * @param {NameStyle} [style=NameStyle.DASH] determines how to concatenate the different pieces of the name
     * @returns the full qualified name
     * @memberof Instrument
     */
    fullyQualifiedName(style: NameStyle = NameStyle.DASH) {

        // TODO(imaman): move everything into compositename
        const tokens = this.cname.all
        if (style == NameStyle.CAMEL_CASE) {
            return toCamelCase(tokens)
        } 
        
        if (style == NameStyle.PASCAL_CASE) {
            return toPascalCase(tokens)        
        }

        if (style == NameStyle.SLASH) {
            return tokens.join("/")
        }

        return tokens.join('-');
    }

    get topLevelPackageName(): string {
        return this.cname.first("")
    }

    getDefinition() : Definition {
        return this.definition;
    }

    // TODO(imaman): rename this. "path" now denotes the full path to an instrument not just it "directories"path()
    get sectionRelativeName(): string {
        return this.fullyQualifiedName(NameStyle.SLASH)
    }

    getNavigationItems(path: CompositeName, arn: string, physicalName: string, awsFactory: AwsFactory): Map<string, NavigationItem> {
        return new Map<string, NavigationItem>()
    }
}

function toCamelCase(...args) {
    function capitalize(s: string) {
        if (!s) {
            throw new Error('Cannot capitalize an empty string');
        }
        return s[0].toUpperCase() + s.substr(1);
    }

    return [].concat(...args).map((curr, i) => i === 0 ? curr : capitalize(curr)).join('');
}

function toPascalCase(tokens: string[]) {
    const violation = tokens.find(curr => curr.indexOf("--") >= 0)
    if (violation) {
        throw new Error(`One of the tokens ("${violation}") contains multiple consecutive dash signs`)
    }
    
    return flatten(tokens.map(curr => curr.split("-")))
        .filter(s => Boolean(s))
        .map(curr => curr.substr(0, 1).toUpperCase() + curr.substr(1)).join('')
}

function flatten(input: string[][]): string[] {
    const ret: string[] = [];
    for (const a  of input) {
        for (const b of a) {
            ret.push(b);
        }
    }

    return ret
}

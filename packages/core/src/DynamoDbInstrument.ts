import {Instrument} from './Instrument'
import {Section} from './Section'
import {Definition} from './Definition'
import {DeployableFragment} from './DeployableFragment'

export enum DynamoDbAttributeType {
    STRING = 'S',
    NUMBER = 'N',
    BINARY = 'B'
}

interface DynamoDbAttribute {
    name: string
    type: DynamoDbAttributeType
}


export interface DynamoDbInstrumentOptions {
    provisioned?: {
        readCapacityUnits: number
        writeCapacityUnits: number
    }
    cloudFormationProperties?: any
}

export class DynamoDbInstrument extends Instrument {

    private static readonly BASE_DEF = {
        Type: "AWS::DynamoDB::Table",
        Properties: {}
    }

    constructor(packageName: string|string[], name: string, partitionKey: DynamoDbAttribute, sortKey?: DynamoDbAttribute,
            options: DynamoDbInstrumentOptions = {}) {
        super(packageName, name);

        function toAttributeDefinition(a: DynamoDbAttribute) {
            if (!a.type) {
                throw new Error(`Missing type in attribute ${a.name}`);
            }
            return {
                AttributeName: a.name,
                AttributeType: a.type
            }
        }

        const atts: any[] = [];
        atts.push(toAttributeDefinition(partitionKey));
        sortKey && atts.push(toAttributeDefinition(sortKey));

        const keySchema: any[] = [];
        keySchema.push({AttributeName: partitionKey.name, KeyType: 'HASH'});
        sortKey && keySchema.push({AttributeName: sortKey.name, KeyType: 'RANGE'});

        this.definition.overwrite(DynamoDbInstrument.BASE_DEF);


        let provisioned: any = {
            BillingMode: 'PAY_PER_REQUEST'
        };
        if (options.provisioned) {
            provisioned = {
                BillingMode: 'PROVISIONED',
                ProvisionedThroughput: {
                    ReadCapacityUnits: options.provisioned.readCapacityUnits,
                    WriteCapacityUnits: options.provisioned.writeCapacityUnits
                }    
            }
        };

        this.definition.mutate(o => Object.assign(o.Properties, provisioned, {
            AttributeDefinitions: atts,
            KeySchema: keySchema,
        }));
        this.definition.mutate(o => Object.assign(o.Properties, options.cloudFormationProperties));
    }

    createFragment(pathPrefix: string): DeployableFragment {
        return new DeployableFragment();
    }

    contributeToConsumerDefinition(section: Section, consumerDef: Definition, myArn: string): void {
        consumerDef.mutate(o => o.Properties.Policies.push({
            Version: '2012-10-17',
            Statement: [{ 
                Effect: "Allow",
                Action: [
                  'dynamodb:*'
                ],
                Resource: myArn
            }]
        }));
    }

    contributeToConsumerCode(region: string, myArn: string): string {
        return `
            import * as AWS from 'aws-sdk'

            export function(region: string, arn: string) {
                    
            } 
        `
    }

    arnService(): string {
        return 'dynamodb';
    }

    arnType(): string {
        return 'table/'
    }

    nameProperty(): string {
        return 'TableName'
    }

    getEntryPointFile(): string {
        return "";
    }
}


import chai = require('chai');
import chaiSubset = require('chai-subset');

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';


import {IsolationScope, Rig, DynamoDbInstrument, LambdaInstrument, DynamoDbAttributeType} from '../src'

function newLambda(packageName: string, name: string, controllerPath: string, cloudFormationProperties?) {
    return new LambdaInstrument(packageName, name, controllerPath, cloudFormationProperties);
}


describe('Instruments', () => {
    describe('cando', () => {
        it ('adds an IAM policy', () => {
            const instrument = newLambda('p1-p2-p3', 'abc', '');

            instrument.canDo('uvw:xyz', 'arn:aws:something:foo:bar')
            expect(instrument.getDefinition().get()).to.containSubset({Properties: {
                Policies: [{
                    Statement: [{
                        Action: ['uvw:xyz'],
                        Effect: 'Allow',
                        Resource: 'arn:aws:something:foo:bar'
                    }]
                }]
            }});
        });

    });

    describe('Lambda', () => {
        it('produces cloudformation', () => {
            const instrument = newLambda('p1-p2-p3', 'abc', '');
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.getPhysicalDefinition(rig).get()).to.deep.equal({
                Type: "AWS::Serverless::Function",
                Properties: {
                    FunctionName: "scope_1-prod-main-p1-p2-p3-abc",
                    Handler: "p1-p2-p3-abc_Handler.handle",
                    Runtime: "nodejs8.10",
                    Policies: [],
                    Events: {}
                }
            });
        });
        it('has FQN', () => {
            const instrument = newLambda('p1-p2-p3', 'abc', '');
            expect(instrument.fullyQualifiedName()).to.equal('p1-p2-p3-abc');
        });
        it('has ARN', () => {
            const instrument = newLambda('p1-p2-p3', 'abc', '');
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.arn(rig)).to.equal('arn:aws:lambda:eu-central-1:acc_100:function:scope_1-prod-main-p1-p2-p3-abc');
        });
        it('contributes to consumers', () => {
            const consumer = newLambda('p1-p2-p3', 'c1', '');
            const supplier = newLambda('p4-p5-p6', 'c2', '');

            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_2");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            supplier.contributeToConsumerDefinition(rig, consumer.getDefinition());
            expect(consumer.getDefinition().get()).to.containSubset({Properties: {
                Policies: [{
                    Statement: [{
                        Action: ['lambda:InvokeFunction'],
                        Effect: 'Allow',
                        Resource: 'arn:aws:lambda:eu-central-1:acc_100:function:scope_1-prod-main-p4-p5-p6-c2'
                    }]
                }]
            }});
        });
    });
    describe('Dynamo', () => {
        it('produces yml', () => {
            const instrument = new DynamoDbInstrument('p1-p2-p3', 'table_1', {name: 'id', type: DynamoDbAttributeType.STRING});
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.getPhysicalDefinition(rig).get()).to.deep.equal({
                Type: "AWS::DynamoDB::Table",
                Properties: {
                    TableName: "scope_1-prod-main-p1-p2-p3-table_1",
                    AttributeDefinitions: [{
                        AttributeName: "id",
                        AttributeType: "S"
                    }],
                    BillingMode: "PAY_PER_REQUEST",
                    KeySchema: [{
                        AttributeName: "id",
                        KeyType: "HASH"
                    }]
                }
            });
        });
        it('supports sort key', () => {
            const instrument = new DynamoDbInstrument('p1-p2-p3', 'table_1', 
                {name: 'id', type: DynamoDbAttributeType.STRING},
                {name: 'n', type: DynamoDbAttributeType.NUMBER});
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.getPhysicalDefinition(rig).get()).to.containSubset({
                Properties: {
                    AttributeDefinitions: [
                        { AttributeName: "id", AttributeType: "S" }, 
                        { AttributeName: "n", AttributeType: "N" }
                    ],
                    KeySchema: [
                        { AttributeName: "id", KeyType: "HASH" },
                        { AttributeName: "n", KeyType: "RANGE" }
                    ]
                }
            });
        });
        it('uses pay-per-request, by default', () => {
            const instrument = new DynamoDbInstrument('p1-p2-p3', 'table_1', 
                {name: 'id', type: DynamoDbAttributeType.STRING});
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.getPhysicalDefinition(rig).get()).to.containSubset({
                Properties: {
                    BillingMode: 'PAY_PER_REQUEST'
                }
            });
        });
        it('supports provisioned throughput', () => {
            const instrument = new DynamoDbInstrument('p1-p2-p3', 'table_1', 
                {name: 'id', type: DynamoDbAttributeType.STRING}, 
                undefined,
                {
                  provisioned: {
                      readCapacityUnits: 625,
                      writeCapacityUnits: 576
                  }
                });
            const scope = new IsolationScope("acc_100", "scope_1", "b_1", "s_1", "p_1");
            const rig = new Rig(scope, "eu-central-1", "prod-main");
            expect(instrument.getPhysicalDefinition(rig).get()).to.containSubset({
                Properties: {
                    BillingMode: 'PROVISIONED',
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 625,
                        WriteCapacityUnits: 576
                    }
                }
            });
        });
    });
})

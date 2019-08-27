import * as AWS from 'aws-sdk'
import { PutItemOutput, GetItemInput } from 'aws-sdk/clients/dynamodb';

// const client = new AWS.DynamoDB.DocumentClient({region: this.mapping.distanceTable.region});
// const q = event.query;
// const answers = lookup(q);

// const req = {
//     TableName: this.mapping.distanceTable.name,
//     Item: {
//         dist: answers[0].score,
//         query: q,
//         answers: answers,
//         numAnswers: answers.length
//     }
// };
// await client.put(req).promise();

export class DynamoDbClient {
    private readonly docClient: AWS.DynamoDB.DocumentClient

    constructor(private readonly region: string, private readonly arn: string) {
        this.docClient = new AWS.DynamoDB.DocumentClient({region})
    }

    async put(item: any): Promise<any> {
        const req = {
            TableName: this.arn,
            Item: item
        }

        try {
            await this.docClient.put(req).promise()
        } catch(e) {
            throw new Error(`Put operation failed on ${this} `)
        }
    }

    async get(key: any, ...attributeNames: string[]): Promise<any> {
        const req: GetItemInput = {
            TableName: this.arn,
            Key: key,
        }

        if (attributeNames.length) {
            req.AttributesToGet = attributeNames
        }

        try {
            const resp = await this.docClient.get(req).promise()
            return resp.Item
        } catch(e) {
            throw new Error(`Get operation failed on ${this}: ${e.message}`)
        }
    }

    toString(): string {
        return `(DynamoDbClient region "${this.region}" ARN "${this.arn}")`
    }
}

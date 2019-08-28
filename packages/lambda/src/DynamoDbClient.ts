import * as AWS from 'aws-sdk'
import { GetItemInput, QueryInput, QueryOutput } from 'aws-sdk/clients/dynamodb';


(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

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

    constructor(private readonly region: string, private readonly tableName: string) {
        this.docClient = new AWS.DynamoDB.DocumentClient({region})
    }

    async put(item: any): Promise<any> {
        const req = {
            TableName: this.tableName,
            Item: item
        }

        try {
            await this.docClient.put(req).promise()
        } catch(e) {
            throw new Error(`Put operation failed on ${this}: ${e.message}`)
        }
    }

    async get(key: any, ...attributesToGet: string[]): Promise<any> {
        const req: GetItemInput = {
            TableName: this.tableName,
            Key: key,
        }

        if (attributesToGet.length) {
            req.AttributesToGet = attributesToGet
        }

        try {
            const resp = await this.docClient.get(req).promise()
            return resp.Item
        } catch(e) {
            throw new Error(`Get operation failed on ${this}: ${e.message}`)
        }
    }

    /**
     * 
     * @param expressionAttributeValues example: {':s': 2, ':e': 9, ':topic': 'PHRASE'}
     * @param keyConditionExpression example: 'Season = :s and Episode > :e'
     * @param filterExpression example: 'contains (Subtitle, :topic)'
     * @param attributeNames 
     */
    async* query(expressionAttributeValues: any, keyConditionExpression: string, limit: number, 
            expressionAttributeNames?: any, reqBase: any = {}) {
        if (limit <= 0) {
            throw new Error(`limit (${limit}) cannot be negative`)
        }
        for (const k in expressionAttributeValues) {
            if (k.startsWith(':')) {
                continue
            }

            const v = expressionAttributeValues[k]
            expressionAttributeValues[':' + k] = v
            delete expressionAttributeValues[k]
        }

        const req: QueryInput = {
            TableName: this.tableName,
            ExpressionAttributeValues: expressionAttributeValues,
            KeyConditionExpression: keyConditionExpression,
            ExpressionAttributeNames: expressionAttributeNames,
        };

        Object.assign(req, reqBase)

        try {
            while (limit > 0) {
                req.Limit = limit
                const resp: QueryOutput = await this.docClient.query(req).promise()
                const items = resp.Items || []
                yield* items
                limit -= items.length

                if (!resp.LastEvaluatedKey) {
                    return
                }

                req.ExclusiveStartKey = resp.LastEvaluatedKey
            }
        } catch(e) {
            throw new Error(`Get operation failed on ${this}: ${e.message}`)
        }
    }

    toString(): string {
        return `(DynamoDbClient region "${this.region}" table "${this.tableName}")`
    }
}

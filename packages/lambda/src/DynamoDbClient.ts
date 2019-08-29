import * as AWS from 'aws-sdk'
import { GetItemInput, QueryInput, QueryOutput, ConsistentRead, ProjectionExpression, ConditionExpression, ExpressionAttributeNameMap, PositiveIntegerObject, ScanOutput, ScanInput } from 'aws-sdk/clients/dynamodb';


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


export interface QueryOptions {
    Limit?: PositiveIntegerObject
    /**
     * Determines the read consistency model: If set to true, then the operation uses strongly consistent reads.
     */
    ConsistentRead?: ConsistentRead
    /**
     * Specifies the order for index traversal: ascending (true) or descending (false). Defaults to true.
     */
    ScanIndexForward?: boolean
    /**
     * The primary key to start evaluation from (exclusive).
     */
    ExclusiveStartKey?: any
    /**
     * A comma-separated string that identifies one or more attributes to retrieve from the table.
     */
    ProjectionExpression?: ProjectionExpression
}

export interface ScanOptions {

}

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
     * Fetches (possibly) multiples items that match the given key condition and filter expression. The key condition
     * expression is a condition on the item's primary key (partition key + range key if there is one). The filter
     * expression is an optional condition on the item's remaining attributes.
     * 
     * Example:
     * ```
     * client.query('id = :v1 and timeMs > :v2', '', {v1: 'foo', v2: 1564617600000}, 20)`
     * ```
     * 
     * Returns an AsyncIterableIterator so call sites can use "for await" loops to iterate over the fetched items:
     * ```
     * for await (const item of client.query('id = :v1 and timeMs > :v2', '', {v1: 'foo', v2: 1564617600000}, 20))
     *     console.log(item.id)
     * }
     * ```
     * 
     * Samples for common usage scenarios:
     * 
     * (1) condition on the range key
     * ```
     * client.query('id = :v1 and timeMs between :v2 and :v3', '', {v1: userId, v2: startTimeMs, v3: endTimeMs}, 10)
     * ```
     * 
     * (2) filtering
     * ```
     * client.query('id = :v1', 'bookName = :v2', {v1: 'foo', v2: 'bar'}, 10)
     * ```
     *
     * (3) projection
     * ```
     * client.query('id = :v1', '', {v1: 'foo'}, 10, [], {ProjectionExpression: 'id,name'}))
     * ```
     * 
     * (4) attributes with reserved names 
     * ```
     * client.query('#name = :v1', '', {v1: 'qux'}, 10, ['name'])
     * ```
     * 
     * @param keyConditionExpression Conditions the primary key. E.g., `'id = :v1 and timeMs > :v2'`. Fails at runtime
     *      if refers to attributes that are not part of the primary key. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param filterExpression Conditions the remaining attributes (attributes that are not part of primary key). E.g.,
     *      `'begins_with(bookName, :v3)'`. Can be empty. Fails at runtime if refers to attributes that are part of the
     *      primary key. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param expressionAttributeValues values for the placeholders specified in the expression strings 
     *      (keyConditionExpression, filterExpression). E.g., `{v1: 'foo', v2: 1564617600000, v3: 'Dublin'}`. The 
     *      placeholders in the expression strings are colon-prefixed tokens, so the given example populates the
     *      following placeholders: `':v1', ':v2', ':v3'`
     * @param atMost an upper cap on the number of items to return. Actual number can be lower than that, in case the 
     *      table does not contain enough matching items.
     * @param expressionAttributeNames an array of strings for attribute name aliases specified in the expression
     *      strings (keyConditionExpression, filterExpression). E.g., `['query', 'name']` will define the following
     *      aliases `'#query', '#name'`. Aliases are needed in cases where an attrbitue name happen to also be a
     *      DynamoDB reserved word: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.
     *      Can be empty.
     */
    async* query(keyConditionExpression: string, filterExpression: string, expressionAttributeValues: any, 
            atMost: number, expressionAttributeNames: string[] = [], options: QueryOptions = {}): AsyncIterableIterator<any> {
        if (atMost <= 0) {
            throw new Error(`atMost (${atMost}) must be positive`)
        }
        const req: QueryInput = {
            TableName: this.tableName,
            ExpressionAttributeValues: createValuesObject(expressionAttributeValues),
            KeyConditionExpression: keyConditionExpression,
            FilterExpression: filterExpression || undefined,
            ExpressionAttributeNames: createNamesObject(expressionAttributeNames)
        };

        Object.assign(req, options)

        yield* execute(atMost, 'Query', this.toString(), req, () => this.docClient.query(req).promise())
    }

    async* scan(filterExpression: string, expressionAttributeValues: any, atMost: number,
            expressionAttributeNames: string[] = [], options: ScanOptions = {}): AsyncIterableIterator<any> {
        if (atMost <= 0) {
            throw new Error(`atMost (${atMost}) must be positive`)
        }

        const req: ScanInput = {
            TableName: this.tableName,
            ExpressionAttributeValues: createValuesObject(expressionAttributeValues),
            FilterExpression: filterExpression || undefined,
            ExpressionAttributeNames: createNamesObject(expressionAttributeNames)
        };

        Object.assign(req, options)

        yield* execute(atMost, 'Scan', this.toString(), req, () => this.docClient.scan(req).promise())
    }

    toString(): string {
        return `(DynamoDbClient region "${this.region}" table "${this.tableName}")`
    }
}


function createValuesObject(a: any) {
    let ret: any|undefined = undefined
    for (let k in a) {
        ret = ret || {}
        const v = a[k]

        if (!k.startsWith(':')) {
            k = `:${k}`
        }

        ret[k] = v
    }

    return ret
}

function createNamesObject(names: string[]) {
    let ret: any|undefined = undefined
    for (const n of names) {
        ret = ret || {}
        ret[`#${n}`] = n
    }

    return ret
}


interface A {
    Items?: any[]
    LastEvaluatedKey?: any
}

interface B {
    ExclusiveStartKey?: any
}


async function* execute<R extends B, T extends A>(atMost: number, operation: string, desc: string, req: R, f: (r: R) => Promise<T>) {
    try {
        let count = 0
        while (count < atMost) {
            const resp: T = await f(req)
            let items = resp.Items || []
            const numLeft = atMost - count
            if (items.length > numLeft) {
                items = items.slice(0, numLeft)
            }
            yield* items
            count += items.length

            if (!resp.LastEvaluatedKey) {
                break
            }

            req.ExclusiveStartKey = resp.LastEvaluatedKey
        }
    } catch(e) {
        throw new Error(`${operation} operation failed on ${desc}: ${e.message}`)
    }
}

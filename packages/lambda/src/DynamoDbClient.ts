import * as AWS from 'aws-sdk'
import { GetItemInput, QueryInput, ConsistentRead, ProjectionExpression, ExpressionAttributeNameMap, 
        PositiveIntegerObject, ScanInput, DeleteItemInput, UpdateItemInput, ScanSegment, ScanTotalSegments, 
        AttributeName } from 'aws-sdk/clients/dynamodb';


(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

/**
 * Definition of attribute name aliases that can be used in expression strings (updateExpression, 
 * conditionExpression, filterExpression, keyConditionExpression, etc.). A typical use case for name aliases, is that of
 * accessing an attribute whose name conflicts with a DynamoDB reserved word.
 *
 * For instance, if your items have an attribute called `timestamp` then you may want to write a filter expresion
 * that checks that the timestamp is greater than some value. If you try to naively write `timestamp > :v1` DynamoDB
 * will reject this expression because `timestamp` is a DynamoDB reserved word. To overcome this, you need to use name
 * aliases. Specifically, you need to use `#timestamp > :v1` as your expression and then define `#timestamp` as an 
 * alias to `timestamp`. The `#` prefix is mandatory and is the syntactic indication for name aliases in expressions.
 * 
 * The `ExpressionAttributeNames` type provides you with two ways to dedine name aliases
 * 
 * (i) an array of strings: This allows you to define aliases which are auto-derived from attribute names by adding a
 * `#`-prefix. For instance, `["timestamp", "query"]` aliases `#timestamp` to `timestamp` and `#query` to `query`.
 * 
 * (ii) a string-to-string mapping: This allows you set the alias string and the attribute name it refers to. For
 * instance, `{t: "timestamp", q: "query"} aliases `#t` to `timestamp` and `#q` to `query`.
 * 
 * Note that (i) is terser and is more intuitive to readers of your code. You may still want to use (ii) in situations
 * where your expression repeatedly refers to long attribure names. 
 *
 * Further details are explained under "ExpressionAttributeNames" in 
 * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeysAndAttributes.html.
 */
export type ExpressionAttributeNames = string[] |  {[key: string]: string}

export interface QueryOptions {
    Limit?: PositiveIntegerObject
    /**
     * Whether the query operation uses strongly consistent reads.
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
     * A comma-separated string that identifies the specific attributes to retrieve.
     */
    ProjectionExpression?: ProjectionExpression
}

export interface ScanOptions {
    /**
     * Number of segments into that a parallel scan will be divided into. See 
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-TotalSegments
     */
    TotalSegments?: ScanTotalSegments
    /**
     * identifies an individual segment to be scanned. See
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-Segment
     */
    Segment?: ScanSegment
    /**
     * Whether all write operations that completed before the Scan began are guaranteed to be contained in the response.
     * Defaults to false.
     */
    ConsistentRead?: ConsistentRead
}

export interface GetOptions {
    /**
     * Whether the get operation uses strongly consistent reads.
     */
    ConsistentRead?: ConsistentRead

    /**
     * A comma-separated string that identifies the specific attributes to retrieve.
     */
    ProjectionExpression?: ProjectionExpression

    // TODO(imaman): elaborate
    /**
     * One or more substitution tokens for attribute names in an expression.
     */
    ExpressionAttributeNames?: ExpressionAttributeNameMap;
}

export class DynamoDbClient {
    private readonly docClient: AWS.DynamoDB.DocumentClient

    constructor(private readonly region: string, private readonly tableName: string) {
        this.docClient = new AWS.DynamoDB.DocumentClient({region})
    }

    /**
     * Creates a new item, or updates an existing one.
     * 
     * @param item the item to create/update.
     */
    async put(item: any): Promise<void> {
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

    /**
     * Removes an item from the table if it exists, otherwise this is a no-op.
     * 
     * @param key the primary key of the item to be removed.
     */
    async delete(key: any): Promise<void> {
        const req: DeleteItemInput = {
            TableName: this.tableName,
            Key: key
        }

        try {
            await this.docClient.delete(req).promise()
        } catch(e) {
            throw new Error(`Delete operation failed on ${this}: ${e.message}`)
        }
    }

    /**
     * Modifies an existing item, or creates one. 
     * 
     * Example:
     * ```
     * client.update({id: 'foo', timeMs: 1564617600000}, 'SET bookName = :v1', '', {v1: 'bar'})
     * ```
     * 
     * @param key the primary key of the item to be updated. 
     * @param updateExpression an expression that describes the attributes to be modfied. E.g.,
     *      `'SET ProductCategory = :c, Price = :p'. The update will fail at runtime if this expression modifies
     *      attributes that are part of the primary key. Update expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
     * @param conditionExpression a condition that must be satisfied in order for the update to take place. E.g.,
     *      `'begins_with(bookName, :v3)'`. Can be empty. The update will fail at runtime if this condition is not
     *      satisfied. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param expressionAttributeValues values for the placeholders specified in the expression strings 
     *      (updateExpression, conditionExpression). E.g., `{v1: 'foo', v2: 1564617600000, v3: 'Dublin'}`. The 
     *      placeholders in the expression strings are colon-prefixed tokens, so the given example populates the
     *      following placeholders: `:v1`, `:v2`, `:v3`
     * @param expressionAttributeNames an array of strings for attribute name aliases specified in the expression
     *      strings (updateExpression, conditionExpression). E.g., `['query', 'name']` will define the following
     *      aliases `#query`, `#name`. Aliases are needed in cases where an attrbitue name happens to also be a
     *      DynamoDB reserved word: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.
     *      Can be empty.
     */
    async update(key: any, updateExpression: string, conditionExpression: string, expressionAttributeValues: any,
            expressionAttributeNames: ExpressionAttributeNames = []): Promise<void> {
        const req: UpdateItemInput = {
            TableName: this.tableName,
            Key: key,
            ExpressionAttributeValues: createValuesObject(expressionAttributeValues),
            UpdateExpression: updateExpression,
            ConditionExpression: conditionExpression || undefined,
            ExpressionAttributeNames: createNamesObject(expressionAttributeNames)
        };

        try {
            await this.docClient.update(req).promise()
        } catch(e) {
            throw new Error(`Update operation failed on ${this}: ${e.message}`)
        }
    }

    /**
     * Retrieves the attributes of an item. Returns `undefined` if no item with the given key exists.
     * 
     * Samples for common usage scenarios:
     * 
     * (1) Projection (retrieve just two attributes)
     * ```
     * client.get({id: 'd', t: 100}, {ProjectionExpression: 'bookName,isbn'})
     * ```
     * (2) Projection where the attribute name is a DynamoDB reserved word
     * ```
     * client.get({id: 'd', t: 100}, {ProjectionExpression: '#query', ExpressionAttributeNames: {"#query": "query"}})))
     * ```
     * 
     * @param key the primary key of the item to retrieve.
     * @param projectionExpression
     */
    async get(key: any, options: GetOptions = {}): Promise<any> {
        const req: GetItemInput = {
            TableName: this.tableName,
            Key: key,
            // ProjectionExpression: projectionExpression || undefined
        }

        Object.assign(req, options)

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
     * client.query(10, 'id = :v1 and timeMs between :v2 and :v3', '', {v1: userId, v2: startTimeMs, v3: endTimeMs})
     * ```
     * 
     * (2) filtering
     * ```
     * client.query(10, 'id = :v1', 'bookName = :v2', {v1: 'foo', v2: 'bar'})
     * ```
     *
     * (3) projection
     * ```
     * client.query(10, 'id = :v1', '', {v1: 'foo'}, [], {ProjectionExpression: 'id,name'}))
     * ```
     * 
     * (4) attributes with reserved names 
     * ```
     * client.query(10, '#name = :v1', '', {v1: 'qux'}, ['name'])
     * ```
     * (5) Fetch at most one item
     * ```
     * client.query(1, '#name = :v1', '', {v1: 'qux'}, ['name'])
     * ```
     * 
     * @param atMost a cap on the number of items to return. Actual number can be lower than that, in case the 
     *      table does not contain enough matching items.
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
     *      following placeholders: `:v1`, `:v2`, `:v3`
     * @param expressionAttributeNames an array of strings for attribute name aliases specified in the expression
     *      strings (keyConditionExpression, filterExpression). E.g., `['query', 'name']` will define the following
     *      aliases `#query`, `#name`. Aliases are needed in cases where an attrbitue name happens to also be a
     *      DynamoDB reserved word: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.
     *      Can be empty.
     */
    async* query(atMost: number, keyConditionExpression: string, filterExpression: string,
            expressionAttributeValues: any, expressionAttributeNames: ExpressionAttributeNames = [],
            options: QueryOptions = {}): AsyncIterableIterator<any> {
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

    /**
     * Fetches up to `atMost` items that match the given filter expression.
     * 
     * Example:
     * ```
     * client.scan('timeMs > :v1', {v1: 1564617600000}, 20)`
     * ```
     * 
     * Returns an AsyncIterableIterator so call sites can use "for await" loops to iterate over the fetched items:
     * ```
     * for await (const item of client.scan('timeMs >: v1', {v1: 1564617600000}, 20))
     *     console.log(item.id)
     * }
     * ```
     * 
     * @param atMost a cap on the number of items to return. Actual number can be lower than that, in case the 
     *      table does not contain enough matching items.
     * @param filterExpression Conditions on the fetched items E.g., `'begins_with(bookName, :v3)'`. An empty string
     *      means "fetch all".Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param expressionAttributeValues values for the placeholders specified in `filterExpression`.E.g., 
     *      `{v1: 'foo', v2: 1564617600000}`. The placeholders in the expression strings are colon-prefixed tokens,
     *      so the given example populates the following placeholders: `:v1`, `:v2`
     * @param expressionAttributeNames an array of strings for attribute name aliases specified in `filterExpression`.
     *      E.g., `['query', 'name']` will define the following aliases `#query`, `#name`. Aliases are needed in cases
     *      where an attrbitue name happens to also be a DynamoDB reserved word:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html. Can be empty.
     * @param options 
     */
    async* scan(atMost: number, filterExpression: string, expressionAttributeValues: any,
            expressionAttributeNames: ExpressionAttributeNames = [], options: ScanOptions = {})
            : AsyncIterableIterator<any> {
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
    return transformKeys(a, ':')
}

function createNamesObject(names: ExpressionAttributeNames) {
    if (!(names instanceof Array)) {
        return transformKeys(names, '#')
    }

    let ret: any|undefined = undefined
    for (const n of names) {
        ret = ret || {}
        ret[`#${n}`] = n
    }

    return ret    
}


interface Req {
    ExclusiveStartKey?: any
}

interface Resp {
    Items?: any[]
    LastEvaluatedKey?: any
}

async function* execute<R extends Req, T extends Resp>(atMost: number, operation: string, desc: string, req: R,
        f: (r: R) => Promise<T>) {
    if (atMost <= 0) {
        throw new Error(`atMost (${atMost}) must be positive`)
    }
    
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



function transformKeys(a: any, prefix: string) {
    let ret: any|undefined = undefined
    for (let k in a) {
        ret = ret || {}
        const v = a[k]

        if (!k.startsWith(prefix)) {
            k = `${prefix}${k}`
        }

        ret[k] = v
    }

    return ret
}

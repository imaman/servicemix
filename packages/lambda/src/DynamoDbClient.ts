import * as AWS from 'aws-sdk'
import { GetItemInput, QueryInput, ConsistentRead, ProjectionExpression, ExpressionAttributeNameMap, 
        PositiveIntegerObject, ScanInput, DeleteItemInput, UpdateItemInput, ScanSegment, ScanTotalSegments, 
        StringAttributeValue, NumberAttributeValue, BinaryAttributeValue, StringSetAttributeValue, 
        NumberSetAttributeValue, BinarySetAttributeValue, MapAttributeValue, ListAttributeValue, NullAttributeValue, 
        BooleanAttributeValue} from 'aws-sdk/clients/dynamodb';


    

(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

export type ExpressionAttributeNames = string[] |  {[key: string]: string}



type AttributeValue =
    StringAttributeValue |
    NumberAttributeValue |
    BinaryAttributeValue |
    StringSetAttributeValue |
    NumberSetAttributeValue |
    BinarySetAttributeValue |
    MapAttributeValue |
    ListAttributeValue |
    NullAttributeValue |
    BooleanAttributeValue

/**
 * Defines value placeholders and name aliases that are used in expression strings (updateExpression, 
 * conditionExpression, filterExpression, keyConditionExpression, etc.).
 */
interface ExpressionConfig {
    /**
     * values for the placeholders specified in the expression strings (updateExpression, conditionExpression, etc.). 
     * E.g., `{v1: 'foo', v2: 1564617600000, v3: 'Dublin'}`. The placeholders in the expression strings are 
     * colon-prefixed tokens, so the given example populates the following placeholders: `:v1`, `:v2`, `:v3`
     */
    values: {[key: string]: AttributeValue} 
    /**
     * Definition of attribute name aliases that can be used in expression strings (updateExpression, 
     * conditionExpression, filterExpression, keyConditionExpression, etc.). A typical use case for name aliases, is
     * that of accessing an attribute whose name conflicts with a DynamoDB reserved word.
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
    aliases?: ExpressionAttributeNames
}

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


export interface ChangerOptions {
    /**
     * How long to wait for the response
     */
    timeoutInMs: number
}

export interface Changer {
    exec(options: ChangerOptions): Promise<void>
}

class ChangerImpl implements Changer {
    //         return new ChangerImpl(req, opts => this.newDocumentClient(opts), this.toString(), (c, r) => c.put(req))

    constructor(private readonly op: string, private readonly clientFactory: (o: any) => AWS.DynamoDB.DocumentClient,
        private readonly str: string, private readonly f: (c: AWS.DynamoDB.DocumentClient) => Promise<any>) {}

    async exec(): Promise<void> {
        try {
            const c = this.clientFactory({})
            this.f(c)
        } catch(e) {
            throw new Error(`Put operation failed on ${this.str}. ${e.message}`)
        }
    }
}

export interface FetchOptions {
    timeoutMs: number
    numItems: number
    stronglyConsistent?: boolean
}

export interface Fetcher {
    fetch(options: FetchOptions): AsyncIterableIterator<any>
}



class QueryFetcher implements Fetcher {

    constructor(private readonly req: QueryInput, 
            private readonly docClientFactory: (o: any) => AWS.DynamoDB.DocumentClient, private readonly description: string) {}

    async* fetch(options: FetchOptions): AsyncIterableIterator<any> {
        const c = this.docClientFactory({maxRetries: 0, httpOptions: {timeout: options.timeoutMs}});
        this.req.ConsistentRead = options.stronglyConsistent || false
        yield* execute(options.numItems, 'Query', this.description, this.req, (r) => c.query(r).promise())
    }

}

class ScanFetcher implements Fetcher {

    constructor(private readonly req: ScanInput,
        private readonly docClientFactory: (o: any) => AWS.DynamoDB.DocumentClient, private readonly description: string) {}
        
    async* fetch(options: FetchOptions): AsyncIterableIterator<any> {
        const c = this.docClientFactory({maxRetries: 0, httpOptions: {timeout: options.timeoutMs}});
        this.req.ConsistentRead = options.stronglyConsistent || false
        yield* execute(options.numItems, 'Scan', this.description, this.req, (r) => c.scan(r).promise())
    }

}

export class DynamoDbClient {
    private readonly docClient: AWS.DynamoDB.DocumentClient

    constructor(private readonly region: string, private readonly tableName: string) {
        this.docClient = this.newDocumentClient({})
    }

    private newDocumentClient(options) {
        const combinedOptions = Object.assign({}, options, {region: this.region})
        return new AWS.DynamoDB.DocumentClient(combinedOptions)
    }

    /**
     * Creates a new item, or updates an existing one.
     * 
     * @param item the item to create/update.
     */
    put(item: any): Changer {
        const req = {
            TableName: this.tableName,
            Item: item
        }

        return new ChangerImpl("Put", opts => this.newDocumentClient(opts), this.toString(), c => c.put(req).promise())
    }

    /**
     * Removes an item from the table if it exists. Silently returns otherwise.
     * 
     * @param key the primary key of the item to be removed.
     */
    delete(key: any): Changer {
        const req: DeleteItemInput = {
            TableName: this.tableName,
            Key: key
        }

        return new ChangerImpl("Delete", opts => this.newDocumentClient(opts), this.toString(), c => c.delete(req).promise())
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
     *      `'SET ProductCategory = :c, Price = :p'`. The update will fail at runtime if this expression modifies
     *      attributes that are part of the primary key. Update expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html.
     * @param conditionExpression a condition that must be satisfied in order for the update to take place. E.g.,
     *      `'begins_with(bookName, :v3)'`. Can be empty. The update will fail at runtime if this condition is not
     *      satisfied. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html.
     * @param ec
     */
    update(key: any, updateExpression: string, conditionExpression: string, ec: ExpressionConfig): Changer {
        const req: UpdateItemInput = {
            TableName: this.tableName,
            Key: key,
            ExpressionAttributeValues: createValuesObject(ec.values),
            UpdateExpression: updateExpression,
            ConditionExpression: conditionExpression || undefined,
            ExpressionAttributeNames: createNamesObject(ec.aliases)
        };

        return new ChangerImpl("Update", opts => this.newDocumentClient(opts), this.toString(), c => c.update(req).promise())
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
     * @param keyConditionExpression Conditions the primary key. E.g., `'id = :v1 and timeMs > :v2'`. Fails at runtime
     *      if refers to attributes that are not part of the primary key. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param filterExpression Conditions the remaining attributes (attributes that are not part of primary key). E.g.,
     *      `'begins_with(bookName, :v3)'`. Can be empty. Fails at runtime if refers to attributes that are part of the
     *      primary key. Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param ec
     * @param options
     */
    query(keyConditionExpression: string, filterExpression: string, ec: ExpressionConfig, options: QueryOptions = {})
            : Fetcher {
        const req: QueryInput = {
            TableName: this.tableName,
            ExpressionAttributeValues: createValuesObject(ec.values),
            KeyConditionExpression: keyConditionExpression,
            FilterExpression: filterExpression || undefined,
            ExpressionAttributeNames: createNamesObject(ec.aliases)
        };

        Object.assign(req, options)

        return new QueryFetcher(req, opts => this.newDocumentClient(opts), this.toString())
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
     * @param filterExpression Conditions on the fetched items E.g., `'begins_with(bookName, :v3)'`. An empty string
     *      means "fetch all".Condition expression reference:
     *      https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
     * @param ec 
     * @param options 
     */
    scan(filterExpression: string, ec: ExpressionConfig, options: ScanOptions = {}): Fetcher {
        const req: ScanInput = {
            TableName: this.tableName,
            ExpressionAttributeValues: createValuesObject(ec.values),
            FilterExpression: filterExpression || undefined,
            ExpressionAttributeNames: createNamesObject(ec.aliases)
        };

        Object.assign(req, options)

        return new ScanFetcher(req, opts => this.newDocumentClient(opts), this.toString())
    }

    toString(): string {
        return `(DynamoDbClient region "${this.region}" table "${this.tableName}")`
    }
}

function createValuesObject(a: any) {
    return transformKeys(a, ':')
}

function createNamesObject(names?: ExpressionAttributeNames) {
    if (!names) {
        return undefined
    }
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
        throw new Error(`Failure while performing an action (${operation}) on a DynamoDB table ${desc}. ${e.message}`)
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

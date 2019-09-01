import {DynamoDbClient} from './DynamoDbClient'
import * as uuidv1 from 'uuid/v1'
import * as assert from 'assert'

(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for("Symbol.asyncIterator");

//const c = new DynamoDbClient('eu-west-2', 'arn:aws:dynamodb:eu-west-2:196625562809:table/bb-example-d38-prod-geography-distances4')


function assertEq(actual: any, expected: any, message?: string) {
    assert.deepStrictEqual(actual, expected, message)
}

function assertEqUnordered(keyF, actual: any[], expected: any[], message?: string) {

    const pairsA = actual.map(curr => [keyF(curr), curr])
    const pairsB = expected.map(curr => [keyF(curr), curr])

    for (const xa of pairsA) {
        for (const xb of pairsB) {
            if ((xb.length === 2) && (xa[0] === xb[0])) {
                assertEq(xa[1], xb[1])
                xb.push('DONE')
                break
            }
        }
    }

    const unmatched = pairsB.filter(curr => curr.length === 2).map(curr => curr[1])
    if (!unmatched.length) {
        return
    }
    throw new Error(`Found unmatched expected elements:\n  ${unmatched.map(x => JSON.stringify(x)).join(',\n  ')}`)
}

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function toArr(g): Promise<any[]> {
    const ret: any[] = []
    for await (const x of g) {
        ret.push(x)
    }
    return ret
}

async function run() {
    const c = new DynamoDbClient('eu-west-2', 'bb-example-d38-prod-misc-history')

    const id = uuidv1()
    const idB = uuidv1()

    c.put({id: idB, t: 100, s: "s_0_b", text: 'foo'})
    c.put({id: idB, t: 101, s: "s_1_b", text: 'bar'})
    c.put({id: idB, t: 102, s: "s_2_b", text: 'foo'})

    c.put({id, t: 100, s: "s_0", text: 'foo'})
    c.put({id, t: 101, s: "s_1", text: 'bar'})
    c.put({id, t: 105, s: "s_5", text: 'baz'})
    c.put({id, t: 104, s: "s_4", text: 'foo'})
    c.put({id, t: 102, s: "s_2", text: 'bar'})
    c.put({id, t: 108, s: "s_8", text: 'baz'})
    c.put({id, t: 106, s: "s_6", text: 'foo'})
    c.put({id, t: 103, s: "s_3", text: 'bar'})
    c.put({id, t: 107, s: "s_7", text: 'baz'})
    await sleep(1000)
    assertEq(await c.get({id, t: 100}), {id, t: 100, s: "s_0", text: 'foo'})

    assertEq(await toArr(c.query(2, 'id = :v1 and t >= :v2', '', {values: {v1: id, v2: 102}})), [
        {id, t: 102, s: "s_2", text: 'bar'},
        {id, t: 103, s: "s_3", text: 'bar'}])

    assertEq(await toArr(c.query(5, 'id = :v1 and t >= :v2', '', {values: {v1: id, v2: 102}})), [
        {id, t: 102, s: "s_2", text: 'bar'},
        {id, t: 103, s: "s_3", text: 'bar'},
        {id, t: 104, s: "s_4", text: 'foo'},
        {id, t: 105, s: "s_5", text: 'baz'},
        {id, t: 106, s: "s_6", text: 'foo'},
    ])

    assertEq(await toArr(c.query(3, 'id = :v1 and t >= :v2', '', {values: {v1: id, v2: 105}})), [
        {id, t: 105, s: "s_5", text: 'baz'},
        {id, t: 106, s: "s_6", text: 'foo'},
        {id, t: 107, s: "s_7", text: 'baz'},
    ])

    assertEq(await toArr(c.query(5, 'id = :v1 and t between :v2 and :v3', '', {values: {v1: id, v2: 102, v3: 104}})), [
        {id, t: 102, s: "s_2", text: 'bar'},
        {id, t: 103, s: "s_3", text: 'bar'},
        {id, t: 104, s: "s_4", text: 'foo'}
    ])

    assertEq(await toArr(c.query(20, 'id = :v1', '#text = :v2', {values: {v1: id, v2: 'foo'}, aliases: ['text']})), [
        {id, t: 100, s: "s_0", text: 'foo'},
        {id, t: 104, s: "s_4", text: 'foo'},
        {id, t: 106, s: "s_6", text: 'foo'}
    ])

    await c.update({id, t: 100}, 'SET s = :v1', '', {values: {v1: '0_0_0'}})
    assertEq(await c.get({id, t: 100}, {ConsistentRead: true}), {id, t: 100, s: '0_0_0', text: 'foo'})


    await c.delete({id, t: 100})
    assertEq(await c.get({id, t: 100}, {ConsistentRead: true}), undefined)


    const scanResult = await toArr(c.scan(20, '(id = :v1 or id = :v2) and (#text = :v3)', 
            {values: {v1: id, v2: idB, v3: 'foo'}, aliases: ['text']}, {ConsistentRead: true}))
    assertEqUnordered(x => `${x.id}_${x.t}`, scanResult, [
            {id, s:"s_4", text: "foo", t: 104},
            {id, s:"s_6", text: "foo", t: 106},
            {id: idB, s:"s_0_b", text: "foo", t: 100},
            {id: idB, s:"s_2_b", text: "foo", t: 102},
        ])

    // const t0 = Date.now()
    // for (let i = 0; i < 200; ++i) {
    //     await c.put({id: 'b', t: 2000 + i, query: 'qa_' + i})
    // }
    // const t1 = Date.now()
    // const dt = (t1 - t0) / 1000;
    // console.log('dt=' + dt

    // for await (const x of c.query(10, 'id = :v1', 'begins_with(#query, :v2)', {v1: 'b', v2: 'qa_8'}, ['query'])) {
    //     console.log('x=' + JSON.stringify(x))
    // }

    // for await (const x of c.scan('t < :v2', {v2: 2005}, 10)) {
    //     console.log('x=' + JSON.stringify(x))
    // }

    // console.log('-adding two items-')
    // await c.update({id: 'd', t: 100}, 'SET #query = :v1', '', {v1: 'foo'}, ['query'])
    // await c.update({id: 'd', t: 101}, 'SET #query = :v1', '', {v1: 'bar'}, ['query'])
    // console.log(JSON.stringify(await c.get({id: 'd', t: 100}, {ProjectionExpression: 'id, #query', ExpressionAttributeNames: {"#query": "query"}})))
    // console.log(JSON.stringify(await c.get({id: 'd', t: 101})))

    // console.log('-updating to qux-')
    // await c.update({id: 'd', t: 101}, 'SET #query = :v1', '', {v1: 'qux'}, ['query'])
    // console.log(JSON.stringify(await c.get({id: 'd', t: 100})))
    // console.log(JSON.stringify(await c.get({id: 'd', t: 101})))

    // console.log('-deleting-')
    // console.log(JSON.stringify(await c.delete({id: 'c', t: 100})))
    // console.log(JSON.stringify(await c.delete({id: 'c', t: 101})))

    // console.log('-getting one last time-')
    // console.log(JSON.stringify(await c.get({id: 'c', t: 100})))
    // console.log(JSON.stringify(await c.get({id: 'c', t: 101})))


    // console.log('-redeleting-')
    // console.log(JSON.stringify(await c.delete({id: 'c', t: 100})))
    // console.log(JSON.stringify(await c.delete({id: 'c', t: 101})))


    // c.put({id: 'd', t: 100, address: { home: {city: 'TLV', street: 'S1'}, work: {city: 'HFA', street: 'S2'}}})
    // c.put({id: 'd', t: 101, address: { home: {city: 'TLV', street: 'S3'}, work: {city: 'SFO', street: 'S4'}}})

    // for await (const x of c.scan(10, '#a.#h.#c = :v1', {values: {v1: 'TLV'}, aliases: {a: 'address', h: 'home', c: 'city'}})) {
    //     console.log('xxs='  + JSON.stringify(x))
    // }

    // for await (const x of c.query(10, 'id = :v1', 'address.#work.city = :v2', {values: {v1: 'd', v2: 'HFA'}, aliases: ['work']})) {
    //     console.log('xxt=' + JSON.stringify(x))
    // }

    // console.log('----------')
    // for await (const x of c.query(10, 'id = :v1 and (t = :v2 or t = :v3)', '', {values: {v1: 'd', v2: 100, v3: 101}})) {
    //     console.log('x from sk q=' + JSON.stringify(x))
    // }
    return '-ok-'

    // for await (const x of c.scan('t < :v1', {v1: 2005}, 10, [])) {
    //     console.log('x=' + JSON.stringify(x))
    // }
} 


run()
    .then(x => console.log(x))
    .catch(e => console.error('err=', e))



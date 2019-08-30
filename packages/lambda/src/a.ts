import {DynamoDbClient} from './DynamoDbClient'


//const c = new DynamoDbClient('eu-west-2', 'arn:aws:dynamodb:eu-west-2:196625562809:table/bb-example-d38-prod-geography-distances4')
const c = new DynamoDbClient('eu-west-2', 'bb-example-d38-prod-misc-history')

async function run() {
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


    c.put({id: 'd', t: 100, address: { home: {city: 'TLV', street: 'S1'}, work: {city: 'HFA', street: 'S2'}}})
    c.put({id: 'd', t: 101, address: { home: {city: 'TLV', street: 'S3'}, work: {city: 'HFA', street: 'S4'}}})

    for await (const x of c.scan(10, '#a.#h.#c = :v1', {v1: 'TLV'}, {a: 'address', h: 'home', c: 'city'})) {
        console.log(JSON.stringify(x))
    }
    return ''

    // for await (const x of c.scan('t < :v1', {v1: 2005}, 10, [])) {
    //     console.log('x=' + JSON.stringify(x))
    // }
} 


async function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    })
}


class G {

    async* g() {
        console.log('y1')
        yield 1;
        await sleep(100);
        console.log('y2,3')
        yield* [2, 3];
        yield* (async function* () {
            await sleep(100);
            console.log('y4')
            yield 4;
        })();
    }
}

async function run2() {
    const g = new G()

    var ret: String[] = []
    for await (const x of g.g()) {
        console.log('got: ' + x)
        ret.push('x=' + x)
     }    

     return ret.join(', ')
}






run()
    .then(x => console.log('x=', JSON.stringify(x, null, 2)))
    .catch(e => console.error('err=', e))



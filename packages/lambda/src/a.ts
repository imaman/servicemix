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
    // console.log('dt=' + dt)

    for await (const x of c.query({v1: 'b', v2: 2090, v3: 2099}, 'id = :v1 and t between :v2 and :v3', 100)) {
        console.log('x=' + JSON.stringify(x))
    }
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



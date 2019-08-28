import {DynamoDbClient} from './DynamoDbClient'


//const c = new DynamoDbClient('eu-west-2', 'arn:aws:dynamodb:eu-west-2:196625562809:table/bb-example-d38-prod-geography-distances4')
const c = new DynamoDbClient('eu-west-2', 'bb-example-d38-prod-geography-distances4')

async function run() {
    await c.put({dist: -8, numAnswers: -80, query: '-800'})
    for await (const x of c.query({':d': -8}, 'dist = :d')) {
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



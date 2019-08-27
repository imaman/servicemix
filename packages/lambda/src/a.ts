import {DynamoDbClient} from './DynamoDbClient'



//const c = new DynamoDbClient('eu-west-2', 'arn:aws:dynamodb:eu-west-2:196625562809:table/bb-example-d38-prod-geography-distances4')
const c = new DynamoDbClient('eu-west-2', 'bb-example-d38-prod-geography-distances4')

async function run() {
    await c.put({dist: -8, numAnswers: -80, query: '-800'})
    return await c.get({dist: -8}, 'numAnswers', 'query')
} 


run()
    .then(x => console.log('x=', JSON.stringify(x, null, 2)))
    .catch(e => console.error('err=', e))



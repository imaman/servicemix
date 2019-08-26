import {lookup} from './model';
import AWS = require('aws-sdk');
import * as byline from 'byline';
import * as ellipsize from 'ellipsize'
import { AbstractController } from 'bigband-lambda';



export default class PlaceFinderController extends AbstractController<any, any> {
    executeScheduledEvent(): void {}
    
    async executeInputEvent(event: any): Promise<any> {
        const client = new AWS.DynamoDB.DocumentClient({region: this.mapping.distanceTable.region});
        const q = event.query;
        const answers = lookup(q);
    
        const req = {
            TableName: this.mapping.distanceTable.name,
            Item: {
                dist: answers[0].score,
                query: q,
                answers: answers,
                numAnswers: answers.length
            }
        };
        await client.put(req).promise();
    
        const timePassed = 'N/A'; //moment(`2015-09-21`).fromNow();
    
        if (answers.length) {
            console.log(`top answer for "${q}" is "${answers[0].answer}"`)
        } else {
            console.log(`No answer for "${q}"`)
        }
        return {
            statusCode: 200,
            headers: { 
              "content-type": 'application/json', 
            },
            body: {query: q, elipsized: ellipsize(q), timePassed, bylineKeys: Object.keys(byline), inputLength: "_12___" + q.length, answers: answers.map(curr => curr.answer)}
        };
    }
}

//export const controller = new PlaceFinderController()

// Run command (from the bigband directory):
// bigband-example.sh invoke --function-name placeFinder --input '{"query": "United Kingdom"}'


import {lookup} from './model';
import AWS = require('aws-sdk');
import * as byline from 'byline';
import * as ellipsize from 'ellipsize'
import { AbstractController, DynamoDbClient } from 'bigband-lambda';



interface W {
    distanceTable: DynamoDbClient
}

export default class PlaceFinderController extends AbstractController<any, any, W> {
    executeScheduledEvent(): void {}
    
    async executeInputEvent(event: any): Promise<any> {
        const client = this.wires.distanceTable;
        const q = event.query;
        const answers = lookup(q);
    
        const item = {
            dist: answers[0].score,
            query: q,
            answers: answers,
            numAnswers: answers.length
        };

        await client.put(item)
    
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


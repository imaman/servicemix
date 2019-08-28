import {AbstractController} from 'bigband-core';
import {LogSamplerModel, Item} from './LogSamplerModel';

// import * as AWS from 'aws-sdk';

// export async function sendErrorToSink(err, mapping, context, details) {
//     try {
//         const lambda = new AWS.Lambda({region: mapping.errorSinkFunction.region});
//         const req: ErrorSinkRequest = {
//             message: err.message,
//             stack: err.stack,
//             origin: context.functionName,
//             details: Object.assign({originRequestId: context.AWSrequestID}, details),
//             remainingTimeInMillis: context.getRemainingTimeInMillis()
//         }

//         const lambdaParams = {
//             FunctionName: mapping.errorSinkFunction.name,
//             InvocationType: 'Event', 
//             Payload: JSON.stringify(req),
//         };

//         await lambda.invoke(lambdaParams).promise();
//         return Promise.resolve();
//     } catch (e) {
//         // Intentionally absorb
//     }
// }


export interface LogSamplerStoreRequest {
    data: any,
    key: string
}

export interface LogSamplerFetchRequest {
    key: string
}

export class LogSamplerController extends AbstractController<any, any> {
    private readonly model = new LogSamplerModel(1000);

    executeScheduledEvent(): void {
        // NOP
    }

    private store(request: LogSamplerStoreRequest) {
        const item: Item = {
            key: request.key,
            logData: request.data
        };
        this.model.store(item);
    }

    private fetch(request: LogSamplerFetchRequest) {
        return this.model.fetch(request.key);
    }

    executeInputEvent(input: any): any {
        if (input.logSamplerStoreRequest) {
            this.store(input.logSamplerStoreRequest);
            return {}
        }

        if (input.logSamplerFetchRequest) {
            return this.fetch(input.logSamplerFetchRequest);
        }

        throw new Error('Bad input');
    }

}

let c: LogSamplerController;

export async function runLambda(context, event, mapping, buildFingerprint) {
    c = c || new LogSamplerController();
    c.initialize(mapping, buildFingerprint)
    return await c.runLambda(event, context);
}
 


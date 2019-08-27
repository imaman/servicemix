import * as AWS from 'aws-sdk'
import { InvocationRequest } from 'aws-sdk/clients/lambda';

export class LambdaClient {
    private readonly lambda: AWS.Lambda

    constructor(private readonly region: string, private readonly arn: string) {
        this.lambda = new AWS.Lambda({region})
    }

    async invoke(payload: any): Promise<any> {
        const req: InvocationRequest = {
            FunctionName: this.arn,
            InvocationType: 'RequestResponse', 
            LogType: 'None',
            Payload: JSON.stringify(JSON.parse(payload))
    
        }
        const resp = await this.lambda.invoke(req).promise();

        const parsedPayload = JSON.parse((resp.Payload || '{}').toString());
        if (resp.StatusCode !== 200 || Boolean(resp.FunctionError)) {
            throw new Error('Lambda invocation (' + this.arn + ') failed: \n' + JSON.stringify(parsedPayload))
        }
        return parsedPayload
    }

    toString(): string {
        return `(LambdaClient region "${this.region}", ARN "${this.arn}")`
    }
}

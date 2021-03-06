import { DescribeLogStreamsRequest, GetLogEventsRequest, GetLogEventsResponse, DescribeLogStreamsResponse } from 'aws-sdk/clients/cloudwatchlogs';
import {BigbandFileRunner} from '../BigbandFileRunner';
import { LookupResult } from '../models/BigbandModel';
import { CloudProvider } from '../CloudProvider';

async function main(bigbandFile: string, lambdaName: string, limit: number) {
    const model = await BigbandFileRunner.loadModel(bigbandFile);
    // TODO(imaman): fail if this is not a lambda instrument
    const lookupResult: LookupResult = await model.searchInspect(lambdaName);

    const cloudWatchLogs = CloudProvider.get(lookupResult.sectionModel).newCloudWatchLogs();
    const logGroupName = `/aws/lambda/${lookupResult.physicalName}`;

    const describeLogStreamsReq: DescribeLogStreamsRequest = {
        logGroupName,
        orderBy: 'LastEventTime',
        descending: true,
        limit: 1
    }

    let describeLogStreamsResp: DescribeLogStreamsResponse;
    try {
        describeLogStreamsResp = await cloudWatchLogs.describeLogStreams(describeLogStreamsReq).promise();
    } catch (e) {
        throw new Error(`Failed to find log streams (logGroupName = ${logGroupName}):\n${e.message}`);
    }

    if (!describeLogStreamsResp.logStreams || describeLogStreamsResp.logStreams.length < 1) {
        console.log('No log stream was found');
        return;
    }

    const stream = describeLogStreamsResp.logStreams[0];
    const logStreamName = stream.logStreamName;
    if (!logStreamName) {
        throw new Error('log stream is empty');
    }

    const getLogEventsReq: GetLogEventsRequest = {
        logGroupName,
        logStreamName,
        limit,
        startFromHead: false
    }
    const getLogEventsResp: GetLogEventsResponse = await cloudWatchLogs.getLogEvents(getLogEventsReq).promise();

    if (!getLogEventsResp.events) {
        return [];
    }
    return getLogEventsResp.events.filter(event => shouldKeep(event.message)).map(format);
}

function format(event): any[] {
    const message = event.message;
    let lines: any[] = message.split('\n').map(x => formatTabs(x)).filter(Boolean);
    if (!lines.length) {
        return [];
    }

    const firstLine = lines[0];
    const timeIndication = computeTimeIndication(firstLine);
    if (firstLine.endsWith('_BIGBAND_ERROR_SINK_')) {
        const data = JSON.parse(lines.slice(1).join('\n'));
        lines = [firstLine, data];
    }
    
    if (timeIndication) {
        lines.unshift(`<${timeIndication} ago> `);
    }

    return lines;
}

function computeTimeIndication(s: string): string {
    const tokens = s.split(' ');
    if (tokens.length <= 0) {
        return '';
    }

    const first = tokens[0];
    const d = Date.parse(first);
    const secs = (Date.now() - d) / 1000;
    return secs > 59 ? `${Math.round(secs / 60)} minutes` : `${Math.round(secs)} seconds`;
}

function formatTabs(s: string) {
    return s.replace(/\t/g, ' ');
}

function shouldKeep(message?: string) {
    if (!message) {
        return false;
    }
    if (message.startsWith("START RequestId:")) {
        return false;
    }
    if (message.startsWith("END RequestId:")) {
        return false;
    }
    if (message.startsWith("REPORT RequestId:")) {
        return false;
    }
    return true;
}


export class LogsCommand {
    static async run(argv) {
        const temp = await main(argv.bigbandFile, argv.path, argv.limit);
        return JSON.stringify(temp, null, 2);
    }
}

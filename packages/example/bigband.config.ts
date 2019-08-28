import { LambdaInstrument, DynamoDbAttributeType, Bigband, DynamoDbInstrument, Section, wire } from 'bigband-core/lib/index';


const bigband = new Bigband({
        name: 'bb-example-d38',
        profileName: 'imaman',
        s3Prefix: 'root',
        s3BucketGuid: '070b8d05-2404-4af0-b3a9-98c39592fd40'
    });

const prod = new Section('eu-west-2', 'prod');
const staging = new Section('eu-west-2', 'staging');
// const importantDates = new LambdaInstrument('chronology', 'improtant-dates', 'src/chronology/compute', {
//     Description: "returns important dates for a year",
//     MemorySize: 1024,
//     Timeout: 15   
// });

const placeFinder = new LambdaInstrument('geography', 'site-finder4', 'src/geography/placeFinder', {
    Description: 'returns names of places that best match the given query',
    MemorySize: 512,
    Timeout: 29
});

const distanceTable = new DynamoDbInstrument('geography', 'distances4', {name: 'dist', type: DynamoDbAttributeType.NUMBER});



// const healthChecker = new LambdaInstrument('geography', 'health-checker', 'src/geography/healthChecker', {
//     Description: 'is everything working correctly',
//     MemorySize: 1024,
//     Timeout: 30      
// }).invokeEveryMinutes(30);


// const statsTable = new DynamoDbInstrument('geography', 'stats', {name: 'query', type: DynamoDbAttributeType.STRING}, {name: 'when', type: DynamoDbAttributeType.NUMBER}, {
//     provisioned: {
//         readCapacityUnits: 1,
//         writeCapacityUnits: 1
//     }
// });

// const queryStream = new KinesisStreamInstrument('geography', 'query-stream', 1);

// const queryStreamAnalyzer = new KinesisStreamConsumer('geography', 'analyzer', 'src/geography/analyzer', queryStream, 1);


// TODO(imaman): check why adding :BigbandSpec breaks compilation
export function run() {
    return {
        bigband,
        sections: [
            {
                section: prod,
                instruments: [placeFinder, distanceTable],
                wiring: [
                    wire(placeFinder, "distanceTable", distanceTable)
                ]
            },
            {
                section: staging,
                instruments: [placeFinder, distanceTable],
                wiring: [
                    wire(placeFinder, "distanceTable", distanceTable)   
                ]
            }
        ]
    }
}

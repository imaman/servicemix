import * as path from 'path';
import * as fs from 'fs';

function findPackageDir() {
    let ret = path.resolve(__dirname);
    while (true) {
        const resolved = path.resolve(ret, 'node_modules')
        if (fs.existsSync(resolved)) {
            return ret;
        }

        const next = path.dirname(ret);
        if (next === ret) {
            throw new Error('package dir for bigband was not found');
        }

        ret = next;
    }
}

let bigbandLambdaPackageDir: string;

export class BigbandLambdaInstallation {
    static bigbandLambdaPackageDir() {
        bigbandLambdaPackageDir = bigbandLambdaPackageDir || findPackageDir();
        return bigbandLambdaPackageDir;
    }
}

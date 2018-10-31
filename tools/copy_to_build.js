const fs = require('fs');
var rmdir = require('rmdir');
const path = require('path');

async function removeDir(d) {
    return new Promise((resolve, reject) => {
        rmdir(d, err => {
            if (err) return reject(err);
            resolve();
        });
    });
}

async function copyFile(srcDir, dstDir, fileName) {
    const source = path.resolve(srcDir, fileName);
    const target = path.resolve(dstDir, fileName);

    console.log(`Copying ${source} => ${target}`);

    return new Promise((resolve, reject) => {
        let rd = fs.createReadStream(source);
        rd.on("error", err => resolve(err));
        
        let wr = fs.createWriteStream(target);
        wr.on("error", err => reject(err));
        wr.on("close", () => resolve());

        rd.pipe(wr);        
    });
}

async function main() {
    const baseDir = path.dirname(__dirname);
    const buildDir = path.resolve(baseDir, 'build');
    if (!path.isAbsolute(buildDir)) {
        throw new Error(`Expected build dir (${buildDir}) to be absolute.`);
    }
    try {
        await removeDir(buildDir);
    } catch (e) {
        // Intentionally abosrb
    }

    if (fs.existsSync(buildDir)) {
        throw new Error(`Removal of directory ${buildDir} did not succeed.`);
    }

    fs.mkdirSync(buildDir);
    await copyFile(baseDir, buildDir, 'package.json');
    await copyFile(baseDir, buildDir, 'README.md');
    return '';
}


main()
    .then(o => console.log('Success', o))
    .catch(e => console.error('Failure', e));

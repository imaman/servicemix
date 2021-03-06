import * as detective from 'detective-typescript';
import * as fs from 'fs';
import * as path from 'path';


let builtinModules: Set<String>;

function isBuiltin(d) {
    if (!builtinModules) {
        const builtins = require('module').builtinModules;
        if (!builtins) {
            throw new Error('Module.builtinModules is falsy. Is your Node version < 8.11.3 ?');
        }

        builtinModules = new Set<String>(builtins);
    }

    return builtinModules.has(d);
}


function read(fileName) {
    try {
        const content = fs.readFileSync(fileName, 'utf8');
        return {content, fileName};
    } catch (e) {
        return null;
    }
}

function findAndRead(fileName) {
    return read(fileName) || read(`${fileName}.ts`) || read(`${fileName}.js`);
}


export interface DepsReport {
    sourceDeps: string[]
    npmDeps: string[]    
}

export class DepsCollector {
    public readonly sourceDeps: string[] = [];
    public readonly npmDeps = new Set<string>();
    private readonly visited = new Set<string>();

    private scan(fileName: string, from: string) {
        if (this.visited.has(fileName)) {
            return this;
        }
        
        this.visited.add(fileName);
        
        const temp = findAndRead(fileName);
        if (!temp) {
            throw new Error(`Unresolved: ${fileName} from ${from}`);
        }
        this.sourceDeps.push(temp.fileName);
        
        const deps: string[] = detective(temp.content);
        if (deps.some(d => path.isAbsolute(d))) {
            throw new Error(`Found absolute-path deps in ${fileName}`);
        }

        const relativeDeps = deps.filter(d => d.startsWith('.'));

        deps
            .filter(d => !d.startsWith("."))
            .filter(d => !isBuiltin(d))
            .forEach(curr => this.npmDeps.add(curr));

        const dir = path.dirname(temp.fileName);        
        relativeDeps
            .map(d => path.resolve(dir, d))
            .forEach(d => this.scan(d, temp.fileName));
        return this;
    }   

    static scanFrom(fileName: string): DepsReport {
        const inst = new DepsCollector().scan(path.resolve(fileName), "_ROOT_");
        const npmDeps : string[] = Array.from(inst.npmDeps);
        npmDeps.sort();
        const ret = {
            sourceDeps: [...inst.sourceDeps],
            npmDeps
        }
        
        return ret;
    }
}





import * as child_process from 'child_process'
import {logger} from './logger';

export interface Execution {
    commandLine: string
    stdout: string
    stderr: string
    exitCode: any
}

function wait(millis: number) {
    return new Promise(resolve => setTimeout(resolve, millis));
}

class BoundedString {
    private readonly arr: string[] = []

    constructor(private name: string, private limit: number, private readonly reject: (_: Error) => void) {}

    push(obj: object) {
        try {
            const s = obj.toString()
            const newLimit = this.limit - s.length
            if (newLimit < 0) {
                throw new Error(`Buffer (${this.name}) exhausted`)
            }
            this.arr.push(s)    
        } catch (e) {
            this.reject(e)            
        }
    }

    get value(): string {
        return this.arr.join('')
    }
}


export class Spawner {

    static exec(command: string, args: string[], cwd: string, timeoutInMillis: number): Promise<Execution> {
        const commandLine = `${cwd}$ ${command} ${args.join(' ')}`
        return new Promise<Execution>((resolve, reject) => {
            wait(timeoutInMillis).then(() => reject(new Error(
                `Timedout after ${timeoutInMillis}ms while waiting for the following command to complete:\n${commandLine}`)))

                const stderr = new BoundedString("stderr", 1024 * 1024 * 50, reject)
                const stdout = new BoundedString("stdout", 1024 * 1024 * 50, reject)
                
            const child = child_process.spawn(command, args, {cwd})
            child.on('exit', code => {
                logger.silly(`Command <${commandLine}> exited with ${code}`)
                resolve({commandLine, stdout: stdout.value, stderr: stderr.value, exitCode: code})
            })
            
            child.stderr.on('data', data => stderr.push(data))
            child.stdout.on('data', data => stdout.push(data))
        })
    }
}

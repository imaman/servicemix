import { fileURLToPath } from "url";

export interface Item {
    key: string
    data: any
}

export class LogSamplerModel {
    private readonly map = new Map<string, Item>(); 
    private readonly fifo: Item[] = [];
    constructor(private readonly limit: number) {}

    get size() {
        return this.fifo.length;
    }

    store(request: Item) {
        if (!request.key) {
            throw new Error('key cannot be falsy');
        }

        if (request.data === undefined) {
            throw new Error('data cannot be undefined');
        }
        while (this.fifo.length >= this.limit) {
            const dropMe = this.fifo.shift();
            if (!dropMe) {
                throw new Error('dropMe is falsy');
            }
            this.map.delete(dropMe.key);
        }

        this.fifo.push(request);
        this.map.set(request.key, request);
    }

    fetch(key: string) {
        const item = this.map.get(key);
        if (!item) {
            return undefined;
        }
        return item.data;
    }
}

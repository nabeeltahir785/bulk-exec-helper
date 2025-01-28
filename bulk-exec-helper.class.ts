import { Readable } from "node:stream";

type ExecFn<T> = (...arg0: T[]) => Promise<unknown>;

export class BulkExecHelper<T> {
  private execResult: unknown[];

  private execFn: ExecFn<T>;

  private fnArgs: Array<Array<T>>;

  private indicesRemaining: number[];

  public resultStream: Readable;

  private resultsEmitted: number;

  private isStream: boolean;

  constructor(execFn: ExecFn<T>, args: Array<Array<T>>) {
    this.execFn = execFn;
    this.fnArgs = args;
    this.indicesRemaining = args.map((_, i) => i);
    this.resultStream = new Readable({
      objectMode: true,
      read: () => {},
    });
    this.execResult = [];
    this.resultsEmitted = 0;
    this.isStream = false;
  }

  public async exec(parallelExecs: number): Promise<unknown[]> {
    this.resultStream.destroy();
    const promiseChains = new Array(parallelExecs).fill(null);

    for (let i = 0; i < parallelExecs; i++) {
      promiseChains[i] = this.processNextIndex();
    }

    return Promise.all(promiseChains)
      .then(() => this.execResult)
      .catch((err) => {
        throw err;
      });
  }

  public execStream(parallelExecs: number): void {
    this.isStream = true;

    const promiseChains = new Array(parallelExecs).fill(null);

    for (let i = 0; i < parallelExecs; i++) {
      promiseChains[i] = this.processNextIndex();
    }

    Promise.all(promiseChains)
      .then(() => this.execResult)
      .catch((err) => {
        this.resultStream.destroy(err);
      });
  }

  private processResults(index: number, result: unknown) {
    if (this.isStream) {
      this.resultsEmitted++;
      this.resultStream.push(result);
      if (this.resultsEmitted === this.fnArgs.length)
        this.resultStream.push(null);
    } else {
      this.execResult[index] = result;
    }
  }

  private processNextIndex(): Promise<unknown> {
    const index = this.indicesRemaining.shift();
    if (index === undefined) return Promise.resolve(null);

    return this.execFn(...this.fnArgs[index])
      .then(this.processResults.bind(this, index))
      .then(this.processNextIndex.bind(this));
  }
}

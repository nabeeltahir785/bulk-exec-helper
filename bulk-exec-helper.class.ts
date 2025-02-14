import { Readable } from "node:stream";

type ExecFn<T> = (...args: T[]) => Promise<unknown>;

export class BulkExecHelper<T> {
  private execResult: unknown[];
  private execFn: ExecFn<T>;
  private fnArgs: Array<Array<T>>;
  private nextIndex: number;
  public resultStream: Readable;
  private resultsEmitted: number;
  private isStream: boolean;

  constructor(execFn: ExecFn<T>, args: Array<Array<T>>) {
    this.execFn = execFn;
    this.fnArgs = args;
    this.nextIndex = 0;
    this.resultStream = new Readable({
      objectMode: true,
      read: () => {},
    });
    this.execResult = new Array(args.length);
    this.resultsEmitted = 0;
    this.isStream = false;
    this.processNextIndex = this.processNextIndex.bind(this);
  }

  public async exec(parallelExecs: number): Promise<unknown[]> {
    this.resultStream.destroy();
    const promises = Array.from({ length: parallelExecs }, () => this.processNextIndex());

    try {
      await Promise.all(promises);
      return this.execResult;
    } catch (err) {
      throw err;
    }
  }

  public execStream(parallelExecs: number): void {
    this.isStream = true;
    const promises = Array.from({ length: parallelExecs }, () => this.processNextIndex());

    Promise.allSettled(promises).then(() => {
      if (this.resultsEmitted === this.fnArgs.length && !this.resultStream.destroyed) {
        this.resultStream.push(null);
      }
    });
  }

  private processResults(index: number, result: unknown): void {
    if (this.isStream) {
      this.resultsEmitted++;
      if (result instanceof Error) {
        this.resultStream.emit('error', result);
      } else {
        this.resultStream.push(result);
      }
    } else {
      this.execResult[index] = result;
    }
  }

  private async processNextIndex(): Promise<unknown> {
    const index = this.nextIndex++;
    if (index >= this.fnArgs.length) {
      return Promise.resolve(null);
    }

    try {
      const result = await this.execFn(...this.fnArgs[index]);
      this.processResults(index, result);
    } catch (error) {
      if (this.isStream) {
        this.processResults(index, error);
        return this.processNextIndex();
      }
      throw error;
    }

    return this.processNextIndex();
  }
}
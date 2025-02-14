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

    return Promise.all(promises)
        .then(() => this.execResult)
        .catch((err) => {
          throw err;
        });
  }

  public execStream(parallelExecs: number): void {
    this.isStream = true;
    const promises = Array.from({ length: parallelExecs }, () => this.processNextIndex());

    Promise.all(promises).catch((err) => {
      this.resultStream.destroy(err);
    });
  }

  private processResults(index: number, result: unknown): void {
    if (this.isStream) {
      this.resultsEmitted++;
      this.resultStream.push(result);
      if (this.resultsEmitted === this.fnArgs.length) {
        this.resultStream.push(null);
      }
    } else {
      this.execResult[index] = result;
    }
  }

  private processNextIndex(): Promise<unknown> {
    const index = this.nextIndex++;
    if (index >= this.fnArgs.length) {
      return Promise.resolve(null);
    }

    return this.execFn(...this.fnArgs[index])
        .then((result) => this.processResults(index, result))
        .then(this.processNextIndex);
  }
}
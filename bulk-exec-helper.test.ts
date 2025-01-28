import { describe, it } from "node:test";
import assert from "assert";

import { BulkExecHelper } from "./bulk-exec-helper.class";

describe("Bulk exec helper", () => {
  it("should be able to execute a function in parallel slots and get the results in the end as a promise ordered by FIFO", async () => {
    const args = [
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
      [5, 5],
      [6, 6],
      [7, 7],
      [8, 8],
      [9, 9],
      [10, 10],
    ];

    const execFn = async (a: number, b: number) => {
      return new Promise((resolve) => {
        setTimeout(
          () => {
            resolve(a * b);
          },
          Math.floor(Math.random() * 400) + 100,
        );
      });
    };

    const bulkExecHelper = new BulkExecHelper(execFn, args);
    const results = await bulkExecHelper.exec(3);
    assert.deepEqual(results, [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]);
  });

  it("should be able to execute a function in parallel slots and get the results in chunks via the resultStream unordered. First Completed, First Returned (FCFR)", (t, done) => {
    const args = [
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
      [5, 5],
      [6, 6],
      [7, 7],
      [8, 8],
      [9, 9],
      [10, 10],
    ];

    const execFn = async (a: number, b: number) => {
      return new Promise((resolve) => {
        setTimeout(
          () => {
            resolve(a * b);
          },
          Math.floor(Math.random() * 400) + 100,
        );
      });
    };

    const bulkExecHelper = new BulkExecHelper(execFn, args);
    const results: number[] = [];
    bulkExecHelper.resultStream.on("data", (data: number) => {
      results.push(data);
    });

    bulkExecHelper.resultStream.on("end", () => {
      assert.deepEqual(
        results.sort(),
        [1, 4, 9, 16, 25, 36, 49, 64, 81, 100].sort(),
      );
      done();
    });
    bulkExecHelper.execStream(3);
  });

  it("should be able to execute a function in parallel slots and bubble up the error in the promise", async () => {
    const args = [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]];

    const execFn = async (a: number) => {
      return new Promise((resolve, reject) => {
        setTimeout(
          () => {
            if (a === 5) {
              reject(new Error("This is an error"));
            }
            resolve(a * a);
          },
          Math.floor(Math.random() * 400) + 100,
        );
      });
    };

    const bulkExecHelper = new BulkExecHelper(execFn, args);

    await assert.rejects(async () => await bulkExecHelper.exec(3), {
      name: "Error",
      message: "This is an error",
    });
  });

  it("should be able to execute a function in parallel slots and bubble up the error in the stream", (t, done) => {
    const args = [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]];

    const execFn = async (a: number) => {
      return new Promise((resolve, reject) => {
        setTimeout(
          () => {
            if (a === 5) {
              reject(new Error("This is an error"));
            }
            resolve(a * a);
          },
          Math.floor(Math.random() * 400) + 100,
        );
      });
    };

    const bulkExecHelper = new BulkExecHelper(execFn, args);

    bulkExecHelper.execStream(3);

    bulkExecHelper.resultStream.on("error", (err) => {
      assert.equal(err.message, "This is an error");
      done();
    });
  });
});

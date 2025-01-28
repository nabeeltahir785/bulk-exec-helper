# BulkExecHelper

`BulkExecHelper` is a utility class for managing and executing a function in parallel slots with controlled concurrency. It supports both promise-based execution and streaming results. This class is ideal for bulk processing tasks where you need to execute multiple operations in parallel with configurable concurrency.

## Features

- Execute a function in parallel with a defined number of slots.
- Retrieve results in **FIFO (First In, First Out)** order.
- Stream results in **FCFR (First Completed, First Returned)** order.
- Gracefully handle errors during execution.
- Supports configurable parallelism.

## Installation

To use `BulkExecHelper`, ensure your environment supports TypeScript and Node.js streams. No external dependencies are required.

## Usage

### Constructor

```typescript
constructor(execFn: ExecFn<T>, args: Array<Array<T>>)
```

- **`execFn`**: A function to be executed for each set of arguments. Must return a `Promise`.
- **`args`**: An array of argument arrays. Each entry will be passed to `execFn`.

### Methods

#### `exec(parallelExecs: number): Promise<unknown[]>`

Executes the function in parallel slots and resolves with an ordered array of results (FIFO).

- **`parallelExecs`**: The number of concurrent executions allowed.

#### `execStream(parallelExecs: number): void`

Executes the function in parallel slots and streams results in FCFR order using the `resultStream` readable stream.

- **`parallelExecs`**: The number of concurrent executions allowed.

### Example

#### FIFO Execution

```typescript
const args = [[1, 1], [2, 2], [3, 3]];
const execFn = async (a: number, b: number) => Promise.resolve(a * b);

const bulkExecHelper = new BulkExecHelper(execFn, args);
const results = await bulkExecHelper.exec(2);
console.log(results); // [1, 4, 9]
```

#### FCFR Execution with Streaming

```typescript
const args = [[1, 1], [2, 2], [3, 3]];
const execFn = async (a: number, b: number) => Promise.resolve(a * b);

const bulkExecHelper = new BulkExecHelper(execFn, args);
bulkExecHelper.resultStream.on("data", (data) => {
  console.log("Result:", data);
});
bulkExecHelper.resultStream.on("end", () => {
  console.log("Stream ended.");
});

bulkExecHelper.execStream(2);
```

#### Error Handling

```typescript
const args = [[1], [2], [5]];
const execFn = async (a: number) => {
  if (a === 5) throw new Error("This is an error");
  return a * a;
};

const bulkExecHelper = new BulkExecHelper(execFn, args);

// Handling errors in promise-based execution
try {
  await bulkExecHelper.exec(2);
} catch (err) {
  console.error(err.message); // "This is an error"
}

// Handling errors in streaming execution
bulkExecHelper.resultStream.on("error", (err) => {
  console.error("Stream Error:", err.message);
});
bulkExecHelper.execStream(2);
```

## Tests

To validate the functionality, refer to the `bulk-exec-helper.class.test.ts` file, which includes unit tests for:

1. FIFO execution.
2. Streaming execution with FCFR.
3. Error handling in both promise-based and stream-based execution.

Run the tests using Node.js built-in test runner:

```bash
npm test
```

## License

This utility is available under the MIT License.

## Author

George Rempousis

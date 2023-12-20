# Redis Utilities

Utilities for Redis functionality

## MessageStream

Stream for publishing and consuming data using redis

```js
const stream = new MessageStream('workQueue');
await stream.connect();
await stream.addMessage({ hello: 'world' });
let { message } = await stream.consumeMessage();
console.log(message); // { hello: 'world' }
await disconnect();
```

## MessageQueue

A redis implementation of a queue

```js
let queue = new MessageQueue('testingQueue');
await queue.connect();
await queue.push({someKey: 'Some data'});

while (await queue.size()) {
    console.log(await queue.pop()); // { someKey: 'Some data' }
}

await queue.disconnect();
```
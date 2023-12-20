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

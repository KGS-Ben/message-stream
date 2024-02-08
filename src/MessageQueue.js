const redis = require('redis');

class MessageQueue {
    /**
     * Create a MessageQueue instance
     *
     * @param {String} queueName Name of the queue
     * @param {String} url Connection string
     */
    constructor(queueName, url) {
        this.url = url;
        this.queueName = `queue:${queueName}`;

        this.client = redis.createClient({
            url: url,
        });
        this.client.on('connect', () => {
            console.log(`Connected to Message Queue (${this.queueName})`);
        });
        this.client.on('error', (err) => console.error(err));

    }

    /**
     * Connect to the queue
     * @throws Error on failure
     */
    async connect() {
        try {
            await this.client.connect();
        } catch (error) {
            console.error(error);
            throw Error('Failed to connect to the message stream');
        }
    }

    /**
     * Disconnect from the queue
     * @throws Error on failure
     */
    async disconnect() {
        try {
            await this.client.disconnect();
        } catch (error) {
            console.error(error);
            throw Error('Failed to disconnect from queue');
        }
    }

    /**
     * Add data to the end of the queue
     *
     * @param {any} data Data to add to the queue
     * @throws Error on failure
     */
    async push(data) {
        try {
            await this.client.RPUSH(this.queueName, JSON.stringify(data));
        } catch (error) {
            console.error(error);
            throw Error(`Failed to push data to queue (${this.queueName})`);
        }
    }

    /**
     * Get the first element in the queue
     *
     * @returns {any} data that was stored in the queue
     * @throws Error on failure
     */
    async pop() {
        try {
            let data = await this.client.LPOP(this.queueName);
            return JSON.parse(data);
        } catch (error) {
            console.error(error);
            throw Error(`Failed to pop from queue (${this.queueName})`);
        }
    }

    /**
     * Get the number of elements in the queue
     *
     * @returns {Number} number of elements in the queue
     * @throws Error on failure
     */
    async size() {
        try {
            return await this.client.LLEN(this.queueName);
        } catch (error) {
            console.error(error);
            throw Error(`Failed to retrieve queue length (${this.queueName})`);
        }
    }
}

module.exports = MessageQueue;

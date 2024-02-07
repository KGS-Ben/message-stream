const redis = require('redis');

const MAX_PROCESSED_IDS = 10000;

class NoMessageFoundError extends Error {
    constructor(message) {
        super(message);
        this.name = 'NoMessageInStream';
        this.message = message ?? 'No message found';
    }
}

class MessageStream {
    /**
     * Create a MessageStream instance
     *
     * @param {String} streamName Name of the stream
     * @param {String} url Stream URL
     */
    constructor(streamName, url = 'redis://127.0.0.1:6379') {
        this.url = url;
        this.streamName = `stream:${streamName}`;
        this.consumerGroup = `${streamName}:consumer:group`;
        this.consumerId = process.env?.pm_id ?? '0';

        this.client = redis.createClient({
            url: url,
        });
        this.client.on('connect', () => {
            console.log(`Connected to Message Stream (${this.streamName})`);
        });
        this.client.on('error', (err) => console.error(err));
        
        this.processedMessageIds = new Set();
    }

    /**
     * Connect to the message stream
     */
    async connect() {
        try {
            await this.client.connect();
            await this.client.XGROUP_CREATE(this.streamName, this.consumerGroup, '$', { MKSTREAM: true });
            console.log('Created consumer group', this.consumerGroup);
        } catch (error) {
            if (error.message != 'BUSYGROUP Consumer Group name already exists') {
                console.error(error);
                throw Error(`Failed to connect to the stream (${this.streamName})`);
            }
        }
    }

    /**
     * Disconnect from the message stream
     */
    async disconnect() {
        try {
            await this.deleteProcessedMessages();
        } catch (error) {
            console.error(error);
        }

        try {
            await this.client.disconnect();
        } catch (error) {
            console.error(error);
            throw Error(`Failed to disconnect from the stream (${this.streamName})`);
        }
    }

    /**
     * Add a message to the stream
     *
     * @param {any} data
     * @returns {String} ID of the message in the stream
     */
    async addMessage(data) {
        try {
            let messageId = await this.client.XADD(this.streamName, '*', {
                data: JSON.stringify(data),
            });

            return messageId;
        } catch (error) {
            console.error(error);
            throw Error(`Failed to add message to stream (${this.streamName})`);
        }
    }

    /**
     * Get a message that failed to process
     *
     * @returns {Object} Message Id and the message
     */
    async getFailedMessage() {
        let id;
        let message;

        try {
            let res = await this.client.XAUTOCLAIM(this.streamName, this.consumerGroup, this.consumerId, 0, '0', {
                BLOCK: 0,
                COUNT: 1,
            });

            if (res && res.messages.length) {
                id = res.messages[0].id;
                message = JSON.parse(res.messages[0].message.data);
            }
        } catch (error) {
            console.error(error);
            throw error;
        }

        return {
            id,
            message,
        };
    }

    /**
     * Get a message from the stream.
     * Failed messages are retrieved before new messages.
     *
     * @returns {Object} { id, message } Message Id and Message data added with addMessage()
     */
    async consumeMessage() {
        let id;
        let message;

        try {
            let messageData = await this.getFailedMessage();
            id = messageData.id;
            message = messageData.message;
        } catch (error) {
            console.error(error);
        }

        if (!message) {
            try {
                let res = await this.client.XREADGROUP(
                    this.consumerGroup,
                    this.consumerId,
                    { key: this.streamName, id: '>' },
                    { BLOCK: 2000, COUNT: 1 }
                );

                if (!res || !res.length) {
                    throw new NoMessageFoundError();
                }

                id = res[0].messages[0].id;
                message = JSON.parse(res[0].messages[0].message.data);
            } catch (error) {
                console.error(error);
            }
        }

        if (id) {
            try {
                // Mark message as read so we don't double process
                await this.client.XACK(this.streamName, this.consumerGroup, id);
                this.processedMessageIds.add(id);

                if (this.processedMessageIds.size > MAX_PROCESSED_IDS) {
                    await this.deleteProcessedMessages();
                }
            } catch (error) {
                console.error(error);
            }
        }

        return {
            id,
            message,
        };
    }

    /**
     * Remove messages that have been processed from the message stream
     */
    async deleteProcessedMessages() {
        try {
            if (this.processedMessageIds.size > 0) {
                await this.client.XDEL(this.streamName, [...this.processedMessageIds.values()]);
                this.processedMessageIds.clear();
            }
        } catch (error) {
            console.error(error);
            throw Error(
                `Failed to delete processed messages (${this.streamName} ${this.consumerGroup} ${this.consumerId})`
            );
        }
    }

    /**
     * Get the number of messages in the stream
     *
     * @returns {Number} Number of messages in the stream
     */
    async length() {
        try {
            return await this.client.XLEN(this.streamName);
        } catch (error) {
            console.error(error);
            throw Error(`Failed to get length of stream (${this.streamName})`);
        }
    }
}

module.exports = {
    MessageStream,
};

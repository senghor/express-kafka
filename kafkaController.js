import { Kafka } from 'kafkajs'
import dotenv from 'dotenv'

dotenv.config()


class KafkaController {
    constructor() {
        this.kafka = new Kafka({
            clientId: process.env.CLIENT,
            brokers: [ process.env.BROKER_1, process.env.BROKER_2, process.env.BROKER_3, ]
        })
    }

    /**
 * Create topic with multiple partitions
 */
    async createTopic (topicName, numPartitions) {
        try {
            //Create admin client to manage kafka topic
            const admin = this.kafka.admin()
            await admin.connect()
            await admin.createTopics({
                topics: [{
                    topic: topicName.toString(), 
                    numPartitions: parseInt(numPartitions),
                    replicationFactor: 1
                }]
            })
            await admin.disconnect()

        } catch(e) {
            console.log(e)
            throw e
            }
        }
    /**
     * publish message to the topic
     */
    async publishMessage(topicName, messages) {
        const producer = this.kafka.producer()
        try {
            await producer.connect()
            await producer.send({
                topic: topicName,
                messages
            })
        } catch(e) {
            console.log(e)
            throw e
        } finally {
            producer.disconnect()
        }
    }

    /**
     * Consume message from the topic
     */
    async consumeMessage(topicName, callback) {
       const consumer = this.kafka.consumer({groupId: 'my-group'}) 
       try {
        await consumer.connect()
        await consumer.subscribe({
            topic: topicName,
            fromBeginning: true
        })
        await consumer.run({
            eachMessage: async ({
                topic,
                partition,
                message
            }) => {
                const value = `Received message: ${message.value.toLocaleString()} 
                from partition ${partition} $ topic ${topic} `;
                callback(value)
            }
        })
       } catch(e) {
            console.log(e)
            throw e
       }
    }
}

export default KafkaController

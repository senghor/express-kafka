import { Kafka } from 'kafkajs'
import dotenv from 'dotenv'

dotenv.config()


class KafkaController {
    constructor() {
        this.kafka = new Kafka({
            clientId: process.env.CLIENT,
            brokers: [ process.env.BROKER_1 ]
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
}

export default KafkaController

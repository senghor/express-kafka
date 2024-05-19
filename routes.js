import { Router } from 'express'
const router = Router()

import KafkaController from './kafkaController.js'


router.post('/create_topic', async (req, res) => {
    try {
        const kafkaController = new KafkaController()
        const { topicName, numPartitions } = req.body
        await kafkaController.createTopic(topicName, numPartitions)
        res.send({
            status: 'OK',
            message: 'Topic successifully created!'
        })
    } catch(e) {
        console.log(e)
        res.status(500).send({
            message: 'Failed to create topic'
        })
    }
})

router.post('/publish', async (req, res) => {
    try {
        const {topicName, message} = req.body
        const messages = [
            {
                key: message?.key,
                value: message?.value
            }
        ]
        const kafkaController = new KafkaController()
        await kafkaController.publishMessage(topicName, messages)
        res.send({
            status: 'OK',
            message: 'Message successifully published!'
        })
    } catch(e) {
        console.log(e)
        res.status(500).send({
            message: 'Failed to publish message to the topic'
        })
    }
})

router.post('/consume', async (req, res) => {
    try {
        const {topicName} = req.body
        const kafkaController = new KafkaController()
        await kafkaController.consumeMessage(topicName, message => {
            res.send({
                status: 'OK',
                message
            })
        })
       
    } catch(e) {
        console.log(e)
        res.status(500).send({
            message: 'Failed to consume message from the topic'
        })
    }
})

export default router
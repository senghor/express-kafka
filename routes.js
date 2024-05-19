import { Router } from 'express'
const router = Router()

import KafkaController from './kafkaController.js'
const kafkaController = new KafkaController()

router.post('/create_topic', async (req, res) => {
    try {
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

export default router
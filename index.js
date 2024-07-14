const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const app = express();
const port = 8080; // Use a different port than the consumer app

const kafka = new Kafka({
    clientId: 'seroter-publisher',
    brokers: ['bootstrap.seroter-kafka.us-west1.managedkafka.seroter-project-base.cloud.goog:9092'],
    ssl: {
      rejectUnauthorized: false
    },
    logLevel: logLevel.DEBUG,
    sasl: {
      mechanism: 'plain', // scram-sha-256 or scram-sha-512
      username: 'seroter-bq-kafka@seroter-project-base.iam.gserviceaccount.com',
      password: '...'
    },
  });

const producer = kafka.producer();

app.get('/publish', async (req, res) => {
  try {
    await producer.connect();

    const _key = req.query.key; // Extract key from querystring
    console.log('key is ' + _key);
    const _value = req.query.value // Extract value from querystring
    console.log('value is ' + _value);

    const message = {
      key: _key, // Optional key for partitioning
      value: _value
    };

    await producer.send({
      topic: 'topic1', // Replace with your topic name
      messages: [message]
    });

    res.status(200).json({ message: 'Message sent successfully' });

  } catch (error) {
    console.error('Error sending message:', error);
    res.status(500).json({ error: 'Failed to send message' });
  }
});

app.listen(port, () => {
  console.log(`Producer listening at http://localhost:${port}`);
});

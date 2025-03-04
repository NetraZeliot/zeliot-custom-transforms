const dotenv = require("dotenv");
dotenv.config();
const { Kafka } = require("kafkajs");
const Redis = require("ioredis");
Redis.Promise = require("bluebird");
const { getKeyValueSentinel, writeToRedisWithKeyValue, deleteFromRedis} = require("./cache.js");

const kafka = new Kafka({
  clientId: "alert-pipeline-" + Date.now(), // Append Current Epoch milliseconds for Random Id
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVER_URL],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const consumer = kafka.consumer({
  groupId: process.env.KAFKA_CONSUMER_GROUP,
});
const producer = kafka.producer();


const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({
    topic: process.env.KAFKA_CONSUMER_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try{
        const msgValue = message.value.toString();
        console.log("Received message:", msgValue);

        // Publish the same message to producer topic
        await producer.send({
          topic: process.env.KAFKA_PRODUCER_TOPIC, // Make sure this env variable is set
          messages: [{ value: msgValue }],
        });

        console.log("Published message to producer topic.");
      }catch(err){
        console.log("THis is the err--->",err)
      }
     
    },
  });
};

run().catch("run error: ", console.error);

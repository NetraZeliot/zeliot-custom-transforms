const dotenv = require("dotenv");
dotenv.config();
const { Kafka } = require("kafkajs");
const Redis = require("ioredis");
Redis.Promise = require("bluebird");
const { getKeyValueSentinel, writeToRedisWithKeyValue } = require("./cache.js");

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

      try {
        console.log("This is message", message.value.toString());
        let input = JSON.parse(message.value.toString());
        console.log("This is input...", input);
        
        let vehicle = input.uniqueId;
        console.log(`Vehicle Unique ID = ${vehicle}`);
    
        let messageType = input.message_type;
        console.log(`Vehicle Unique ID = ${messageType}`);
        
        if (messageType === "obd") {
            if (input.hasOwnProperty("com_soc")) {
                let comSoc = parseFloat(input.com_soc);
                let redisKey = vehicle;
    
                let redisValue = await getKeyValueSentinel(redisKey);
                
                if (redisValue) {
                    if (comSoc < 20 || comSoc > 90) {
                        let existingComSoc = parseFloat(redisValue);
                        if (existingComSoc === comSoc) {
                            await writeToRedisWithKeyValue(redisKey, String(comSoc));
                            let messageToPublish = JSON.stringify({ imei: vehicle, status: "no_alert" });
                            await producer.send({ topic: "output", messages: [{ value: messageToPublish }] });
                        } else {
                            await writeToRedisWithKeyValue(redisKey, String(comSoc));
                            let messageToPublish = JSON.stringify({ imei: vehicle, status: "soc_alert" });
                            await producer.send({ topic: process.env.KAFKA_PRODUCER_TOPIC, messages: [{ value: messageToPublish }] });
                        }
                    } else {
                        console.log(`SOC value for Unique ID ${vehicle} is within normal range: ${comSoc}`);
                        await writeToRedisWithKeyValue(redisKey, String(comSoc));
                        let messageToPublish = JSON.stringify({ imei: vehicle, status: "no_alert" });
                        await producer.send({ topic: process.env.KAFKA_PRODUCER_TOPIC, messages: [{ value: messageToPublish }] });
                    }
                } else {
                    await writeToRedisWithKeyValue(redisKey, String(comSoc));
                    let messageToPublish = JSON.stringify({ imei: vehicle, status: "soc_alert" });
                    await producer.send({ topic: process.env.KAFKA_PRODUCER_TOPIC, messages: [{ value: messageToPublish }] });
                }
            } else {
                console.warn(`com_soc field is missing for Unique ID ${vehicle}`);
            }
        } else {
            console.warn(`Unexpected message_type value: ${messageType} for unique ID ${vehicle}`);
        }
    } catch (err) {
        console.log("This is the error --->", err);
    }   
    },
  });
};

run().catch("run error: ", console.error);

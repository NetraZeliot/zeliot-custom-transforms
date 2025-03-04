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
        console.log("This is message",message.value.toString())
        let input = JSON.parse(message.value.toString());
        console.log("THis is input...",input)
        let vehicle = input.uniqueId;
        console.log(`Vehicle Unique ID = ${vehicle}`);
  
        let eventFlag = input.event_flag;
  
        let redisKey = `imei_${vehicle}`;
  
          // console.log(`Processing message for Unique ID: ${uniqueId}`);
  
          let redisValue = await getKeyValueSentinel(redisKey);
  
          if (redisValue) {
            console.log(`Unique ID ${vehicle} exists in Redis with value: ${redisValue}`);
  
            if (redisValue === "harsh_braking_start") {
              console.log(`Key '${redisKey}' has value 'harsh_braking_start'. No action needed.`);
            } else {
              let messageToPublish = JSON.stringify({ imei: vehicle, status: "harsh_braking_stop" });
              await producer.send({
                topic: process.env.KAFKA_PRODUCER_TOPIC,
                messages: [{ value: messageToPublish }],
              });
  
              await deleteFromRedis(redisKey);
              console.log(`Removed key '${redisKey}' from Redis.`);
            }
          } else {
            console.log(`Unique ID ${vehicle} does not exist in Redis.`);
  
            if ((eventFlag & 134217728) === 134217728) {
              await writeToRedisWithKeyValue(redisKey, "harsh_braking_start");
              console.log(`Stored IMEI ${vehicle} with value 'harsh_braking_start' in Redis.`);
  
              let messageToPublish = JSON.stringify({ imei: vehicle, status: "harsh_braking_start" });
              await producer.send({
                topic: process.env.KAFKA_PRODUCER_TOPIC,
                messages: [{ value: messageToPublish }],
              });
            }  else {
              console.log(`Unexpected event_flag value: ${eventFlag} for unique ID ${vehicle}`);
            }
          }
      }catch(err){
        console.log("THis is the err--->",err)
      }
     
    },
  });
};

run().catch("run error: ", console.error);

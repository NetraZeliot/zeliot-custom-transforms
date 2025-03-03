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
      try{
        console.log("This is message",message.value.toString())
        let input = JSON.parse(message.value.toString());
        console.log("THis is input...",input)
        let vehicle = input.uniqueId;
        console.log(`Vehicle Unique ID = ${vehicle}`);

        let message_type = input.messageType;
        console.log(`Vehicle Message Type = ${message_type}`);

        if (message_type.toLowerCase() === "obd") {
          if (input.hasOwnProperty("com_all_dtcs_str")) {
              let comAllDtcStr = input.com_all_dtcs_str;
              let dtcArray = comAllDtcStr.split(",").map(dtc => dtc.trim());
              let redisKey = `imei_${vehicle}_dtc`;
  
              let storedDtcList = await redisClient.lrange(redisKey, 0, -1);
  
              for (let dtc of dtcArray) {
                  if (!storedDtcList.includes(dtc)) {
                      await redisClient.lpush(redisKey, dtc);
                      console.log(`Saved DTC [${dtc}] to Redis under key [${redisKey}]`);
                      let messageToPublish = JSON.stringify({ DTC: dtc, status: "dtc_alert_start" });
                      await producer.send({
                          topic: process.env.KAFKA_PRODUCER_TOPIC,
                          messages: [{ value: messageToPublish }],
                      });
                      console.log("Published DTC alert:", messageToPublish);
                  } else {
                      await redisClient.lrem(redisKey, 0, dtc);
                      let messageToPublish = JSON.stringify({ DTC: dtc, status: "dtc_alert_stop" });
                      await producer.send({
                          topic: process.env.KAFKA_PRODUCER_TOPIC,
                          messages: [{ value: messageToPublish }],
                      });
                      console.log("Published DTC alert:", messageToPublish);
                  }
              }
          } else {
              console.warn(`com_all_dtcs_str field is missing for Unique ID ${vehicle}`);
          }
      }

      
      }catch(err){
        console.log("THis is the err--->",err)
      }
     
    },
  });
};

run().catch("run error: ", console.error);

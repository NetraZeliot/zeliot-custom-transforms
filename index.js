const dotenv = require("dotenv");
dotenv.config();
const { Kafka } = require("kafkajs");
const Redis = require("ioredis");
Redis.Promise = require("bluebird");
const { getKeyValueSentinel, writeToRedisWithKeyValue } = require("./cache.js");

const kafka = new Kafka({
  clientId: "alert-pipeline-" + Date.now(), // Append Current Epoch milliseconds for Random Id
  brokers: ["my-cluster-kafka-bootstrap.kafka:9092"],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const consumer = kafka.consumer({
  groupId: "alert-pipeline-group",
});
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({
    topic: process.env.INPUT_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let input = JSON.parse(message.value.toString());
      let vehicle = input.uniqueId;
      let speed = input.speed;
      let osState = await getKeyValueSentinel(`${vehicle}_osStatus`);
      if (osState == null) {
        await writeToRedisWithKeyValue(`${vehicle}_osStatus`, "false");
      }
      console.log(vehicle, speed, osState);
      if (speed > process.env.OS_THRESHOLD) {
        if (osState == "false") {
          await writeToRedisWithKeyValue(`${vehicle}_osStatus`, "true");
          let value = {
            device: vehicle,
            speed: speed,
            overspeed: "true",
            timestamp: input.timestamp,
            latitude: input.latitude,
            longitude: input.longitude,
          };
          await producer.send({
            topic: process.env.OUTPUT_TOPIC,
            messages: [
              {
                key: vehicle,
                value: JSON.stringify([
                  { key: vehicle, value: JSON.stringify(value) },
                ]),
              },
            ],
          });
          console.log("Overspeed start : ", value);
        }
      } else {
        if (osState == "true") {
          await writeToRedisWithKeyValue(`${vehicle}_osStatus`, "false");
          let value = {
            device: vehicle,
            speed: speed,
            overspeed: "false",
            timestamp: input.timestamp,
            latitude: input.latitude,
            longitude: input.longitude,
          };
          await producer.send({
            topic: process.env.OUTPUT_TOPIC,
            messages: [
              {
                key: vehicle,
                value: JSON.stringify([
                  { key: vehicle, value: JSON.stringify(value) },
                ]),
              },
            ],
          });
          console.log("Overspeed stop : ", value);
        }
      }
    },
  });
};

run().catch("run error: ", console.error);

const dotenv = require("dotenv");
dotenv.config();
const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  clientId: "alert-pipeline-" + Date.now(),
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

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: process.env.KAFKA_CONSUMER_TOPIC,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        console.log("This is message",message.value.toString())
        let input = message.value.toString();
        console.log("THis is input...",input)
        // Directly send HTTP POST request
        let response = await axios.post(process.env.HTTPS_URL, input, {
          headers: { "Content-Type": "application/json" },
        });

        console.log(" HTTP Status:", response.status);
        console.log("Response Body:", response.data);
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
};

run().catch((err) => console.error("Run error:", err));

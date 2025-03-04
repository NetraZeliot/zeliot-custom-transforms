const Redis = require("ioredis");
// Explicitly use bluebird promise library after ioredis v 4.X upgrade
Redis.Promise = require("bluebird");
let REDIS_MASTER_OPTIONS = {
  /* Redis Client Syntax to connect to Redis Sentinel Cluster */
  sentinels: [
    {
      host: process.env.REDIS_SENTINEL_SERVICE_DNS || "rfs-redisfailover.redis",
      port: 26379,
    },
    // { host: "rfs-redisfailover.redis", port: 26379 }
  ],
  // Default Cluster id "mymaster"
  name: "mymaster",
  role: "master",
  retryStrategy: (times) => {
    // reconnect after
    return Math.min(times * 50, 2000);
  },
  // password: process.env.REDIS_PASSWORD,
};

let REDIS_SLAVE_OPTIONS = { ...REDIS_MASTER_OPTIONS };
REDIS_SLAVE_OPTIONS.role = "slave";

console.log(
  "REDIS_MASTER_OPTIONS: ",
  REDIS_MASTER_OPTIONS,
  " || REDIS_SLAVE_OPTIONS: ",
  REDIS_SLAVE_OPTIONS
);

const redisReader = new Redis(REDIS_SLAVE_OPTIONS);
const redisWriter = new Redis(REDIS_MASTER_OPTIONS);

async function getKeyValueSentinel(key) {
  // add transaction here
  const values = await redisReader.mget(key);
  if (values && values.length) {
    return values[0];
  } else return null;
}

async function writeToRedisWithKeyValue(key, value) {
  // Sample for writing to reddis
  let writeResponse = await redisWriter.set(key, value);
  return writeResponse;
}

redisReader.on("error", (error) => {
  console.error("Redis connection error:", error);
  process.exit(0);
});

redisWriter.on("error", (error) => {
  console.error("Redis connection error:", error);
  process.exit(0);
});

module.exports = {
  getKeyValueSentinel,
  writeToRedisWithKeyValue,
};

const Redis = require("ioredis");
// Explicitly use bluebird promise library after ioredis v 4.X upgrade
Redis.Promise = require("bluebird");
let REDIS_MASTER_OPTIONS = {
  /* Redis Client Syntax to connect to Redis Sentinel Cluster */
  sentinels: [
    {
      host: process.env.REDIS_HOST,
      port: process.env.REDIS_PORT,
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
  try {
    const value = await redisReader.get(key);
    console.log(`GET ${key} -> ${value}`);
    return value;
  } catch (error) {
    console.error(`Redis GET error for key ${key}:`, error);
    return null;
  }
}

async function writeToRedisWithKeyValue(key, value) {
  try {
    await redisWriter.set(key, value);
    console.log(`SET ${key} -> ${value}`);
    return true;
  } catch (error) {
    console.error(`Redis SET error for key ${key}:`, error);
    return false;
  }
}

async function deleteFromRedis(key) {
  try {
    const result = await redisWriter.del(key);
    console.log(`DEL ${key} -> ${result ? "Success" : "Key not found"}`);
    return result;
  } catch (error) {
    console.error(`Redis DEL error for key ${key}:`, error);
    return false;
  }
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
  deleteFromRedis,
};

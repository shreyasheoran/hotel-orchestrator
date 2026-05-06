"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fetchSupplierA = fetchSupplierA;
exports.fetchSupplierB = fetchSupplierB;
exports.saveToRedis = saveToRedis;
const axios_1 = __importDefault(require("axios"));
const redis_1 = require("redis");
const config_1 = require("./config");
const BASE_URL = config_1.config.apiUrl;
// ✅ FIX: Don't connect at module load — use a lazy getter that awaits the connection.
let redisClient = null;
async function getRedisClient() {
    if (!redisClient) {
        redisClient = (0, redis_1.createClient)({
            url: process.env.REDIS_URL || "redis://127.0.0.1:6379",
        });
        redisClient.on("error", (err) => console.error("[Redis] Client error:", err.message));
        await redisClient.connect(); // properly awaited — throws if Redis is down
        console.log("[Redis] Connected successfully.");
    }
    return redisClient;
}
async function fetchSupplierA(city) {
    console.log(`[Activity] Fetching Supplier A for city="${city}"`);
    try {
        const res = await axios_1.default.get(`${BASE_URL}/supplierA/hotels?city=${city}`);
        return res.data.map((h) => ({ ...h, supplier: "Supplier A" }));
    }
    catch (err) {
        console.error("[Activity] fetchSupplierA failed:", err.message);
        throw err; // re-throw so Temporal retries
    }
}
async function fetchSupplierB(city) {
    console.log(`[Activity] Fetching Supplier B for city="${city}"`);
    try {
        const res = await axios_1.default.get(`${BASE_URL}/supplierB/hotels?city=${city}`);
        return res.data.map((h) => ({ ...h, supplier: "Supplier B" }));
    }
    catch (err) {
        console.error("[Activity] fetchSupplierB failed:", err.message);
        throw err;
    }
}
async function saveToRedis(city, hotels) {
    console.log(`[Activity] Saving ${hotels.length} hotels to Redis for city="${city}"`);
    const client = await getRedisClient();
    const multi = client.multi();
    for (const hotel of hotels) {
        // Sorted Set: score = price → enables ZRANGEBYSCORE native filtering
        multi.zAdd(`hotels:${city}`, {
            score: hotel.price,
            value: JSON.stringify(hotel),
        });
    }
    multi.expire(`hotels:${city}`, config_1.config.redisTtlSeconds); // Configurable TTL
    await multi.exec();
    console.log(`[Activity] Saved to Redis. Key=hotels:${city}, TTL=300s`);
}

import axios from "axios";
import { createClient } from "redis";
import { config } from "./config";

const BASE_URL = config.apiUrl;

let redisClient: ReturnType<typeof createClient> | null = null;

async function getRedisClient() {
  if (!redisClient) {
    redisClient = createClient({
      url: process.env.REDIS_URL || "redis://127.0.0.1:6379",
    });

    redisClient.on("error", (err) =>
      console.error("[Redis] Client error:", err.message),
    );

    await redisClient.connect();
    console.log("[Redis] Connected successfully.");
  }
  return redisClient;
}

export async function fetchSupplierA(city: string): Promise<any[]> {
  console.log(`[Activity] Fetching Supplier A for city="${city}"`);
  try {
    const res = await axios.get(`${BASE_URL}/supplierA/hotels?city=${city}`);
    return res.data.map((h: any) => ({ ...h, supplier: "Supplier A" }));
  } catch (err: any) {
    console.error("[Activity] fetchSupplierA failed:", err.message);
    throw err; // re-throw so Temporal retries
  }
}

export async function fetchSupplierB(city: string): Promise<any[]> {
  console.log(`[Activity] Fetching Supplier B for city="${city}"`);
  try {
    const res = await axios.get(`${BASE_URL}/supplierB/hotels?city=${city}`);
    return res.data.map((h: any) => ({ ...h, supplier: "Supplier B" }));
  } catch (err: any) {
    console.error("[Activity] fetchSupplierB failed:", err.message);
    throw err;
  }
}

export async function saveToRedis(city: string, hotels: any[]): Promise<void> {
  console.log(
    `[Activity] Saving ${hotels.length} hotels to Redis for city="${city}"`,
  );
  const client = await getRedisClient();

  const multi = client.multi();
  for (const hotel of hotels) {
    // Sorted Set: score = price → enables ZRANGEBYSCORE native filtering
    multi.zAdd(`hotels:${city}`, {
      score: hotel.price,
      value: JSON.stringify(hotel),
    });
  }
  multi.expire(`hotels:${city}`, config.redisTtlSeconds); // Configurable TTL
  await multi.exec();
  console.log(`[Activity] Saved to Redis. Key=hotels:${city}, TTL=300s`);
}

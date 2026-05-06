import express, { Request, Response } from "express";
import cors from "cors";
import axios from "axios";
import { Connection, Client } from "@temporalio/client";
import { hotelAggregationWorkflow } from "./workflows";
import { createClient } from "redis";
import crypto from "crypto";

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json());

// ── Redis Client (one instance for the process lifetime) ──────────────────────
const redisClient = createClient({
  url: process.env.REDIS_URL || "redis://127.0.0.1:6379",
});
redisClient.on("error", (err) =>
  console.error("[Redis] Client error:", err.message),
);
redisClient.connect().catch((err) => {
  console.error("[Redis] FATAL: Could not connect on startup.", err);
  process.exit(1);
});

let temporalClient: Client | null = null;

async function getTemporalClient(): Promise<Client> {
  if (!temporalClient) {
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
    });
    temporalClient = new Client({ connection });
    console.log("[Temporal] Client connected and ready.");
  }
  return temporalClient;
}

// ── Mock Supplier A ───────────────────────────────────────────────────────────
app.get("/supplierA/hotels", (_req: Request, res: Response) => {
  res.json([
    {
      hotelId: "a1",
      name: "Holtin",
      price: 6000,
      city: "delhi",
      commissionPct: 10,
    },
    {
      hotelId: "a2",
      name: "Radison",
      price: 5900,
      city: "delhi",
      commissionPct: 13,
    },
    {
      hotelId: "a3",
      name: "The Grand",
      price: 12000,
      city: "delhi",
      commissionPct: 15,
    },
  ]);
});

// ── Mock Supplier B ───────────────────────────────────────────────────────────
app.get("/supplierB/hotels", (_req: Request, res: Response) => {
  res.json([
    {
      hotelId: "b1",
      name: "Holtin",
      price: 5340,
      city: "delhi",
      commissionPct: 20,
    },
    {
      hotelId: "b2",
      name: "Taj",
      price: 8000,
      city: "delhi",
      commissionPct: 15,
    },
    {
      hotelId: "b3",
      name: "The Grand",
      price: 12500,
      city: "delhi",
      commissionPct: 10,
    },
  ]);
});

// ── Health Check ──────────────────────────────────────────────────────────────
app.get("/health", async (_req: Request, res: Response) => {
  const baseUrl = process.env.API_URL || "http://localhost:3000";
  const status = {
    service: "healthy",
    supplierA: "unknown",
    supplierB: "unknown",
  };

  try {
    await axios.get(`${baseUrl}/supplierA/hotels`);
    status.supplierA = "up";
  } catch {
    status.supplierA = "down";
  }

  try {
    await axios.get(`${baseUrl}/supplierB/hotels`);
    status.supplierB = "up";
  } catch {
    status.supplierB = "down";
  }

  res.status(200).json(status);
});

// ── Main Hotel Search Endpoint ────────────────────────────────────────────────
app.get("/api/hotels", async (req: Request, res: Response) => {
  const { city, minPrice, maxPrice } = req.query;

  if (!city) {
    res.status(400).json({ error: 'Query parameter "city" is required.' });
    return;
  }

  const cityKey = (city as string).toLowerCase().trim();

  try {
    // ✅ FIX: Always check the cache first — your original only checked when
    //    price filters existed, so GET /api/hotels?city=delhi ALWAYS ran a new
    //    Temporal workflow, even when data was already in Redis.
    const cacheExists = await redisClient.exists(`hotels:${cityKey}`);

    if (cacheExists) {
      // Use full range if no filters are given
      const min = minPrice !== undefined ? Number(minPrice) : "-inf";
      const max = maxPrice !== undefined ? Number(maxPrice) : "+inf";

      const cached = await redisClient.zRangeByScore(
        `hotels:${cityKey}`,
        min,
        max,
      );

      if (cached.length > 0) {
        console.log(
          `[Server] Cache HIT for "${cityKey}" — ${cached.length} results.`,
        );
        res.json({
          source: "redis-cache",
          data: cached.map((h) => JSON.parse(h.toString())),
        });
        return;
      }

      // Cache exists but nothing matched the price range
      if (minPrice !== undefined || maxPrice !== undefined) {
        res.json({ source: "redis-cache", data: [] });
        return;
      }
    }

    // Cache miss — run the Temporal workflow
    console.log(
      `[Server] Cache MISS for "${cityKey}" — starting Temporal workflow.`,
    );
    const client = await getTemporalClient();

    const result: any[] = await client.workflow.execute(
      hotelAggregationWorkflow,
      {
        args: [cityKey],
        taskQueue: "hotel-queue",
        workflowId: `hotel-workflow-${crypto.randomUUID()}`,
      },
    );

    // Apply price filter to the freshly-fetched results if needed
    let data = result;
    if (minPrice !== undefined || maxPrice !== undefined) {
      const min = minPrice !== undefined ? Number(minPrice) : 0;
      const max = maxPrice !== undefined ? Number(maxPrice) : Infinity;
      data = result.filter((h) => h.price >= min && h.price <= max);
    }

    res.json({ source: "temporal-workflow", data });
  } catch (error: any) {
    console.error("[Server] /api/hotels error:", error.message);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
app
  .listen(3000, () => console.log("[Server] Running on http://localhost:3000"))
  .on("error", (err) => {
    console.error("[Server] FATAL: Could not start.", err);
    process.exit(1);
  });

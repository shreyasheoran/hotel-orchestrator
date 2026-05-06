import express, { Request, Response } from "express";
import cors from "cors";
import axios from "axios";
import { Connection, Client } from "@temporalio/client";
import { hotelAggregationWorkflow } from "./workflows";
import { createClient } from "redis";
import crypto from "crypto";
import { config } from "./config";

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json());

const redisClient = createClient({
  url: config.redisUrl,
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
    const maxRetries = 5;
    const retryDelay = 1000; // 1 second

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const connection = await Connection.connect({
          address: config.temporalAddress,
        });
        temporalClient = new Client({ connection });
        console.log("[Temporal] Client connected and ready.");
        break;
      } catch (error: any) {
        if (attempt === maxRetries) {
          throw new Error(
            `Failed to connect to Temporal after ${maxRetries} attempts: ${error.message}`,
          );
        }
        console.warn(
          `[Temporal] Connection attempt ${attempt}/${maxRetries} failed, retrying in ${retryDelay}ms...`,
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
      }
    }
  }
  return temporalClient!;
}

app.get("/supplierA/hotels", (_req: Request, res: Response) => {
  const { city } = _req.query;
  const cityStr = (city as string)?.toLowerCase().trim();

  if (!cityStr || !config.knownCities.includes(cityStr)) {
    return res.json([]);
  }

  const cityData: Record<string, any[]> = {
    delhi: [
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
      {
        hotelId: "a4",
        name: "ITC Maurya",
        price: 15000,
        city: "delhi",
        commissionPct: 12,
      },
      {
        hotelId: "a5",
        name: "Le Meridien",
        price: 8500,
        city: "delhi",
        commissionPct: 14,
      },
      {
        hotelId: "a15",
        name: "The Imperial",
        price: 11000,
        city: "delhi",
        commissionPct: 16,
      },
      {
        hotelId: "a16",
        name: "Shangri-La Eros",
        price: 14000,
        city: "delhi",
        commissionPct: 18,
      },
    ],
    mumbai: [
      {
        hotelId: "a6",
        name: "Taj Mahal Palace",
        price: 25000,
        city: "mumbai",
        commissionPct: 18,
      },
      {
        hotelId: "a7",
        name: "The Oberoi",
        price: 22000,
        city: "mumbai",
        commissionPct: 16,
      },
      {
        hotelId: "a8",
        name: "Trident",
        price: 12000,
        city: "mumbai",
        commissionPct: 11,
      },
      {
        hotelId: "a17",
        name: "Four Seasons",
        price: 30000,
        city: "mumbai",
        commissionPct: 20,
      },
      {
        hotelId: "a18",
        name: "The St. Regis",
        price: 28000,
        city: "mumbai",
        commissionPct: 19,
      },
    ],
    bangalore: [
      {
        hotelId: "a9",
        name: "ITC Gardenia",
        price: 14000,
        city: "bangalore",
        commissionPct: 13,
      },
      {
        hotelId: "a10",
        name: "The Leela Palace",
        price: 18000,
        city: "bangalore",
        commissionPct: 17,
      },
      {
        hotelId: "a19",
        name: "Taj West End",
        price: 16000,
        city: "bangalore",
        commissionPct: 15,
      },
      {
        hotelId: "a20",
        name: "The Ritz-Carlton",
        price: 25000,
        city: "bangalore",
        commissionPct: 22,
      },
    ],
    chennai: [
      {
        hotelId: "a11",
        name: "ITC Grand Chola",
        price: 16000,
        city: "chennai",
        commissionPct: 15,
      },
      {
        hotelId: "a12",
        name: "The Park",
        price: 9500,
        city: "chennai",
        commissionPct: 12,
      },
      {
        hotelId: "a21",
        name: "Taj Coromandel",
        price: 13000,
        city: "chennai",
        commissionPct: 14,
      },
      {
        hotelId: "a22",
        name: "Radisson Blu",
        price: 10000,
        city: "chennai",
        commissionPct: 13,
      },
    ],
    kolkata: [
      {
        hotelId: "a13",
        name: "The Astor",
        price: 11000,
        city: "kolkata",
        commissionPct: 14,
      },
      {
        hotelId: "a14",
        name: "ITC Sonar",
        price: 13000,
        city: "kolkata",
        commissionPct: 16,
      },
      {
        hotelId: "a23",
        name: "The Park",
        price: 9000,
        city: "kolkata",
        commissionPct: 12,
      },
    ],
    hyderabad: [
      {
        hotelId: "a24",
        name: "ITC Kakatiya",
        price: 12000,
        city: "hyderabad",
        commissionPct: 14,
      },
      {
        hotelId: "a25",
        name: "Taj Falaknuma Palace",
        price: 35000,
        city: "hyderabad",
        commissionPct: 25,
      },
      {
        hotelId: "a26",
        name: "The Park",
        price: 10000,
        city: "hyderabad",
        commissionPct: 13,
      },
    ],
    pune: [
      {
        hotelId: "a27",
        name: "ITC Grand Central",
        price: 11000,
        city: "pune",
        commissionPct: 13,
      },
      {
        hotelId: "a28",
        name: "JW Marriott",
        price: 15000,
        city: "pune",
        commissionPct: 16,
      },
    ],
    ahmedabad: [
      {
        hotelId: "a29",
        name: "The House of MG",
        price: 8000,
        city: "ahmedabad",
        commissionPct: 12,
      },
      {
        hotelId: "a30",
        name: "Radisson Blu",
        price: 9500,
        city: "ahmedabad",
        commissionPct: 13,
      },
    ],
    jaipur: [
      {
        hotelId: "a31",
        name: "ITC Rajputana",
        price: 14000,
        city: "jaipur",
        commissionPct: 15,
      },
      {
        hotelId: "a32",
        name: "The Oberoi Rajvilas",
        price: 45000,
        city: "jaipur",
        commissionPct: 28,
      },
    ],
    lucknow: [
      {
        hotelId: "a33",
        name: "Clarks Avadh",
        price: 9000,
        city: "lucknow",
        commissionPct: 12,
      },
      {
        hotelId: "a34",
        name: "The Piccadily",
        price: 7500,
        city: "lucknow",
        commissionPct: 11,
      },
    ],
  };

  res.json(cityData[cityStr] || []);
});

// ── Mock Supplier B ───────────────────────────────────────────────────────────
app.get("/supplierB/hotels", (_req: Request, res: Response) => {
  const { city } = _req.query;
  const cityStr = (city as string)?.toLowerCase().trim();

  if (!cityStr || !config.knownCities.includes(cityStr)) {
    return res.json([]);
  }

  const cityData: Record<string, any[]> = {
    delhi: [
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
      {
        hotelId: "b4",
        name: "Radison",
        price: 6200,
        city: "delhi",
        commissionPct: 11,
      },
      {
        hotelId: "b5",
        name: "Hyatt Regency",
        price: 10500,
        city: "delhi",
        commissionPct: 13,
      },
      {
        hotelId: "b16",
        name: "The Imperial",
        price: 11500,
        city: "delhi",
        commissionPct: 17,
      },
      {
        hotelId: "b17",
        name: "Shangri-La Eros",
        price: 13500,
        city: "delhi",
        commissionPct: 19,
      },
    ],
    mumbai: [
      {
        hotelId: "b6",
        name: "Taj Mahal Palace",
        price: 23500,
        city: "mumbai",
        commissionPct: 19,
      },
      {
        hotelId: "b7",
        name: "JW Marriott",
        price: 19000,
        city: "mumbai",
        commissionPct: 14,
      },
      {
        hotelId: "b8",
        name: "Four Seasons",
        price: 28000,
        city: "mumbai",
        commissionPct: 20,
      },
      {
        hotelId: "b18",
        name: "The Oberoi",
        price: 21000,
        city: "mumbai",
        commissionPct: 17,
      },
      {
        hotelId: "b19",
        name: "The St. Regis",
        price: 27000,
        city: "mumbai",
        commissionPct: 20,
      },
    ],
    bangalore: [
      {
        hotelId: "b9",
        name: "ITC Gardenia",
        price: 13500,
        city: "bangalore",
        commissionPct: 14,
      },
      {
        hotelId: "b10",
        name: "Shangri-La",
        price: 17000,
        city: "bangalore",
        commissionPct: 18,
      },
      {
        hotelId: "b11",
        name: "The Ritz-Carlton",
        price: 25000,
        city: "bangalore",
        commissionPct: 22,
      },
      {
        hotelId: "b20",
        name: "The Leela Palace",
        price: 17500,
        city: "bangalore",
        commissionPct: 18,
      },
      {
        hotelId: "b21",
        name: "Taj West End",
        price: 15500,
        city: "bangalore",
        commissionPct: 16,
      },
    ],
    chennai: [
      {
        hotelId: "b12",
        name: "ITC Grand Chola",
        price: 15500,
        city: "chennai",
        commissionPct: 16,
      },
      {
        hotelId: "b13",
        name: "Park Hyatt",
        price: 14000,
        city: "chennai",
        commissionPct: 15,
      },
      {
        hotelId: "b22",
        name: "Taj Coromandel",
        price: 12500,
        city: "chennai",
        commissionPct: 15,
      },
      {
        hotelId: "b23",
        name: "The Park",
        price: 9200,
        city: "chennai",
        commissionPct: 13,
      },
    ],
    kolkata: [
      {
        hotelId: "b14",
        name: "The Astor",
        price: 11500,
        city: "kolkata",
        commissionPct: 15,
      },
      {
        hotelId: "b15",
        name: "Novotel",
        price: 7500,
        city: "kolkata",
        commissionPct: 10,
      },
      {
        hotelId: "b24",
        name: "ITC Sonar",
        price: 12500,
        city: "kolkata",
        commissionPct: 17,
      },
    ],
    hyderabad: [
      {
        hotelId: "b25",
        name: "ITC Kakatiya",
        price: 11500,
        city: "hyderabad",
        commissionPct: 15,
      },
      {
        hotelId: "b26",
        name: "Taj Falaknuma Palace",
        price: 34000,
        city: "hyderabad",
        commissionPct: 26,
      },
      {
        hotelId: "b27",
        name: "The Park",
        price: 9800,
        city: "hyderabad",
        commissionPct: 14,
      },
      {
        hotelId: "b28",
        name: "Radisson Blu",
        price: 11000,
        city: "hyderabad",
        commissionPct: 13,
      },
    ],
    pune: [
      {
        hotelId: "b29",
        name: "ITC Grand Central",
        price: 10500,
        city: "pune",
        commissionPct: 14,
      },
      {
        hotelId: "b30",
        name: "JW Marriott",
        price: 14500,
        city: "pune",
        commissionPct: 17,
      },
      {
        hotelId: "b31",
        name: "The Westin",
        price: 12000,
        city: "pune",
        commissionPct: 15,
      },
    ],
    ahmedabad: [
      {
        hotelId: "b32",
        name: "The House of MG",
        price: 7800,
        city: "ahmedabad",
        commissionPct: 13,
      },
      {
        hotelId: "b33",
        name: "Radisson Blu",
        price: 9200,
        city: "ahmedabad",
        commissionPct: 14,
      },
      {
        hotelId: "b34",
        name: "Courtyard by Marriott",
        price: 8500,
        city: "ahmedabad",
        commissionPct: 12,
      },
    ],
    jaipur: [
      {
        hotelId: "b35",
        name: "ITC Rajputana",
        price: 13500,
        city: "jaipur",
        commissionPct: 16,
      },
      {
        hotelId: "b36",
        name: "The Oberoi Rajvilas",
        price: 44000,
        city: "jaipur",
        commissionPct: 29,
      },
      {
        hotelId: "b37",
        name: "Rambagh Palace",
        price: 38000,
        city: "jaipur",
        commissionPct: 27,
      },
    ],
    lucknow: [
      {
        hotelId: "b38",
        name: "Clarks Avadh",
        price: 8800,
        city: "lucknow",
        commissionPct: 13,
      },
      {
        hotelId: "b39",
        name: "The Piccadily",
        price: 7200,
        city: "lucknow",
        commissionPct: 12,
      },
      {
        hotelId: "b40",
        name: "Vivanta by Taj",
        price: 9500,
        city: "lucknow",
        commissionPct: 14,
      },
    ],
  };

  res.json(cityData[cityStr] || []);
});

app.get("/health", async (_req: Request, res: Response) => {
  const baseUrl = config.apiUrl;
  const status = {
    service: "healthy",
    supplierA: "unknown",
    supplierB: "unknown",
  };

  try {
    await axios.get(
      `${baseUrl}/supplierA/hotels?city=${config.healthCheckCity}`,
      { timeout: config.healthCheckTimeoutMs },
    );
    status.supplierA = "up";
  } catch {
    status.supplierA = "down";
  }

  try {
    await axios.get(
      `${baseUrl}/supplierB/hotels?city=${config.healthCheckCity}`,
      { timeout: config.healthCheckTimeoutMs },
    );
    status.supplierB = "up";
  } catch {
    status.supplierB = "down";
  }

  res.status(200).json(status);
});

app.get("/api/hotels", async (req: Request, res: Response) => {
  const { city, minPrice, maxPrice } = req.query;

  if (!city) {
    res.status(400).json({ error: 'Query parameter "city" is required.' });
    return;
  }

  const cityKey = (city as string).toLowerCase().trim();

  // Validate city
  if (
    cityKey.length > config.maxCityLength ||
    !config.cityRegex.test(cityKey)
  ) {
    res.status(400).json({
      error:
        "Invalid city parameter. Must be alphanumeric with spaces/hyphens only.",
    });
    return;
  }

  // Validate price parameters
  const minPriceNum = minPrice !== undefined ? Number(minPrice) : undefined;
  const maxPriceNum = maxPrice !== undefined ? Number(maxPrice) : undefined;

  if (minPrice !== undefined && (isNaN(minPriceNum!) || minPriceNum! < 0)) {
    res.status(400).json({ error: "minPrice must be a non-negative number." });
    return;
  }

  if (maxPrice !== undefined && (isNaN(maxPriceNum!) || maxPriceNum! < 0)) {
    res.status(400).json({ error: "maxPrice must be a non-negative number." });
    return;
  }

  if (
    minPriceNum !== undefined &&
    maxPriceNum !== undefined &&
    minPriceNum > maxPriceNum
  ) {
    res
      .status(400)
      .json({ error: "minPrice cannot be greater than maxPrice." });
    return;
  }

  try {
    const cacheExists = await redisClient.exists(`hotels:${cityKey}`);

    if (cacheExists) {
      // Use full range if no filters are given
      const min = minPriceNum !== undefined ? minPriceNum : "-inf";
      const max = maxPriceNum !== undefined ? maxPriceNum : "+inf";

      const cached = await redisClient.zRangeByScore(
        `hotels:${cityKey}`,
        min,
        max,
      );

      if (cached.length > 0) {
        console.log(
          `[Server] Cache HIT for "${cityKey}" — ${cached.length} results.`,
        );
        const data = cached.map((h) => JSON.parse(h.toString()));
        res.json(data);
        return;
      }

      // Cache exists but nothing matched the price range
      if (minPrice !== undefined || maxPrice !== undefined) {
        res.json([]);
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
        taskQueue: config.temporalTaskQueue,
        workflowId: `${config.temporalWorkflowIdPrefix}-${cityKey}-${Date.now()}`,
      },
    );

    // Apply price filter to the freshly-fetched results if needed
    let data = result;
    if (minPriceNum !== undefined || maxPriceNum !== undefined) {
      const min = minPriceNum !== undefined ? minPriceNum : 0;
      const max = maxPriceNum !== undefined ? maxPriceNum : Infinity;
      data = result.filter((h) => h.price >= min && h.price <= max);
    }

    res.json(data);
  } catch (error: any) {
    console.error("[Server] /api/hotels error:", error.message);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
app
  .listen(config.port, () =>
    console.log(`[Server] Running on http://localhost:${config.port}`),
  )
  .on("error", (err) => {
    console.error("[Server] FATAL: Could not start.", err);
    process.exit(1);
  });

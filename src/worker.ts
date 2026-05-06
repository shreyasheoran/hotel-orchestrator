import { Worker, NativeConnection } from "@temporalio/worker";
import * as activities from "./activities";

async function bootWorker() {
  const MAX_RETRIES = 15;
  const RETRY_DELAY_MS = 2000;
  let connection: NativeConnection | null = null;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(
        `[Worker] Connecting to Temporal (attempt ${attempt}/${MAX_RETRIES})...`,
      );
      connection = await NativeConnection.connect({
        address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
      });
      console.log("[Worker] Connected to Temporal.");
      break;
    } catch (error: any) {
      if (attempt === MAX_RETRIES) {
        console.error(
          `[Worker] FATAL: Could not connect after ${MAX_RETRIES} attempts.`,
        );
        process.exit(1);
      }
      console.warn(
        `[Worker] Not ready. Retrying in ${RETRY_DELAY_MS / 1000}s...`,
      );
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
    }
  }

  if (!connection) return;

  try {
    const worker = await Worker.create({
      connection,
      workflowsPath: require.resolve("./workflows"),
      activities,
      taskQueue: "hotel-queue",
    });
    console.log("[Worker] Listening on queue: hotel-queue");
    await worker.run();
  } catch (err: any) {
    console.error("[Worker] FATAL: Crashed at runtime.", err.message);
    process.exit(1);
  }
}

bootWorker();

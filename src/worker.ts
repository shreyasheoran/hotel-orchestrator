import { Worker, NativeConnection } from "@temporalio/worker";
import * as activities from "./activities";
import { config } from "./config";

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
        address: config.temporalAddress,
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
      taskQueue: config.temporalTaskQueue,
    });
    console.log(`[Worker] Listening on queue: ${config.temporalTaskQueue}`);
    await worker.run();
  } catch (err: any) {
    console.error("[Worker] FATAL: Crashed at runtime.", err.message);
    process.exit(1);
  }
}

bootWorker();

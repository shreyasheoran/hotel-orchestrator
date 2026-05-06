"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const worker_1 = require("@temporalio/worker");
const activities = __importStar(require("./activities"));
const config_1 = require("./config");
async function bootWorker() {
    const MAX_RETRIES = 15;
    const RETRY_DELAY_MS = 2000;
    let connection = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            console.log(`[Worker] Connecting to Temporal (attempt ${attempt}/${MAX_RETRIES})...`);
            connection = await worker_1.NativeConnection.connect({
                address: config_1.config.temporalAddress,
            });
            console.log("[Worker] Connected to Temporal.");
            break;
        }
        catch (error) {
            if (attempt === MAX_RETRIES) {
                console.error(`[Worker] FATAL: Could not connect after ${MAX_RETRIES} attempts.`);
                process.exit(1);
            }
            console.warn(`[Worker] Not ready. Retrying in ${RETRY_DELAY_MS / 1000}s...`);
            await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
        }
    }
    if (!connection)
        return;
    try {
        const worker = await worker_1.Worker.create({
            connection,
            workflowsPath: require.resolve("./workflows"),
            activities,
            taskQueue: config_1.config.temporalTaskQueue,
        });
        console.log(`[Worker] Listening on queue: ${config_1.config.temporalTaskQueue}`);
        await worker.run();
    }
    catch (err) {
        console.error("[Worker] FATAL: Crashed at runtime.", err.message);
        process.exit(1);
    }
}
bootWorker();

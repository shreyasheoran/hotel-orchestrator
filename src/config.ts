export const config = {
  port: parseInt(process.env.PORT || "3000", 10),

  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  redisTtlSeconds: parseInt(process.env.REDIS_TTL_SECONDS || "300", 10),

  temporalAddress: process.env.TEMPORAL_ADDRESS || "localhost:7233",
  temporalTaskQueue: process.env.TEMPORAL_TASK_QUEUE || "hotel-queue",
  temporalActivityTimeout: process.env.TEMPORAL_ACTIVITY_TIMEOUT || "1 minute",
  temporalActivityMaxAttempts: parseInt(
    process.env.TEMPORAL_ACTIVITY_MAX_ATTEMPTS || "3",
    10,
  ),
  temporalWorkflowIdPrefix:
    process.env.TEMPORAL_WORKFLOW_ID_PREFIX || "hotel-workflow",

  apiUrl: process.env.API_URL || "http://localhost:3000",

  healthCheckCity: process.env.HEALTH_CHECK_CITY || "delhi",
  healthCheckTimeoutMs: parseInt(
    process.env.HEALTH_CHECK_TIMEOUT_MS || "3000",
    10,
  ),

  maxCityLength: parseInt(process.env.MAX_CITY_LENGTH || "50", 10),
  cityRegex: /^[a-zA-Z\s\-]+$/,

  knownCities: [
    "delhi",
    "mumbai",
    "bangalore",
    "chennai",
    "kolkata",
    "hyderabad",
    "pune",
    "ahmedabad",
    "jaipur",
    "lucknow",
  ],
};

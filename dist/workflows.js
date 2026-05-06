"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.hotelAggregationWorkflow = hotelAggregationWorkflow;
const workflow_1 = require("@temporalio/workflow");
const config_1 = require("./config");
// Configure how the activities should be executed
const { fetchSupplierA, fetchSupplierB, saveToRedis } = (0, workflow_1.proxyActivities)({
    startToCloseTimeout: config_1.config.temporalActivityTimeout,
    retry: { maximumAttempts: config_1.config.temporalActivityMaxAttempts },
});
async function hotelAggregationWorkflow(city) {
    // 1. Parallel fetch from both suppliers
    const [hotelsA, hotelsB] = await Promise.all([
        fetchSupplierA(city),
        fetchSupplierB(city),
    ]);
    const hotelMap = new Map();
    const allHotels = [...hotelsA, ...hotelsB];
    // 2. Deduplicate by name and pick the cheapest
    for (const hotel of allHotels) {
        const existing = hotelMap.get(hotel.name);
        if (!existing || hotel.price < existing.price) {
            hotelMap.set(hotel.name, {
                name: hotel.name,
                price: hotel.price,
                supplier: hotel.supplier,
                commissionPct: hotel.commissionPct,
            });
        }
    }
    const deduplicated = Array.from(hotelMap.values());
    // 3. Save to cache
    if (deduplicated.length > 0) {
        await saveToRedis(city, deduplicated);
    }
    return deduplicated;
}

import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

// Configure how the activities should be executed
const { fetchSupplierA, fetchSupplierB, saveToRedis } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 }
});

export async function hotelAggregationWorkflow(city: string): Promise<any> {
  // 1. Parallel fetch from both suppliers
  const [hotelsA, hotelsB] = await Promise.all([
    fetchSupplierA(city), 
    fetchSupplierB(city)
  ]);
  
  const hotelMap = new Map<string, any>();
  const allHotels = [...hotelsA, ...hotelsB];

  // 2. Deduplicate by name and pick the cheapest
  for (const hotel of allHotels) {
    const existing = hotelMap.get(hotel.name);
    if (!existing || hotel.price < existing.price) {
      hotelMap.set(hotel.name, {
        name: hotel.name, 
        price: hotel.price, 
        supplier: hotel.supplier, 
        commissionPct: hotel.commissionPct
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
export const transform = <
  T extends Record<string, string | number | Date | boolean>,
>(
  jobData: T
): T => {
  // jobData already got `received_at` and `sent_at` columns.
  // Default transformer add a property "timestamp" which is equal to "received_at":
  return {
    timestamp: jobData.received_at,
    ...jobData,
  };
};

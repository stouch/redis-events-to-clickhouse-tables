export const transform = <
  T extends Record<string, string | number | Date | boolean>,
>(
  jobData: T,
  _initialJobData: Record<string, unknown> // Before applying `message_id` and `sent_at` on it, and before snakification of the keys.
): T => {
  // jobData already got `received_at` and `sent_at` columns.
  // Default transformer add a property "timestamp" which is equal to "received_at":
  return {
    timestamp: jobData.received_at,
    ...jobData,
  };
};

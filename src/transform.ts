export const transform = <
  T extends Record<string, string | number | Date | boolean>,
>(
  jobData: T
): T => {
  return jobData;
};

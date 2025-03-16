// Note that `jobData` has already its keys in a snake_case format.
export const transform = <
  T extends Record<string, string | number | Date | boolean>,
>(
  jobData: T,
  _initialJobData: Record<string, unknown>
): T => {
  //
  // Write your code here
  // return {
  //  ...jobData,
  //  some_custom_key: '<override_something>'
  // }
  //
  const newJobData: T = {
    ...jobData,
    // test_key: "<override_something>",
  };
  return newJobData;
};

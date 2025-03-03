import { dayjs } from "./dayjs-utc.js";

export const isDateString = (str: string) => {
  return (
    str.trim().match(/^([0-9]{2}([0-9]{2})?(\/|-)|)+/gi) && dayjs(str).isValid()
  );
};

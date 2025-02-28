import dayJsModule from "dayjs";
import utc from "dayjs/plugin/utc.js";

dayJsModule.extend(utc);

export const dayjs = (objToParse?: string | Date) => {
  if (typeof objToParse === "string") {
    if (objToParse.indexOf("T") > -1 && objToParse.indexOf("Z") > -1) {
      return dayJsModule(objToParse).utc();
    }
    // No timezone is known in the string, we assume the string is an UTC string:
    return dayJsModule(objToParse).utc(true); // keep local time = true -> we assume what we receive is UTC
  }
  return dayJsModule(objToParse).utc();
};

import Queue from "bull";
import cluster from "cluster";
import Bulker from "./bulker.class.js";
import { createClient } from "@clickhouse/client";
import ClickhouseBatchClient from "./clickhouse-batch-client.class.js";
import dotenv from "dotenv";
dotenv.config();

// Define process.env.REDIS_JOB_EVENT_TYPE_PROPERTY type:
declare global {
  namespace NodeJS {
    interface ProcessEnv {
      [key: string]: string | undefined;
      USE_CLICKHOUSE_ASYNC_INSERT: string;
      CLICKHOUSE_ALTERED_COLUMN_NULLABLE: string;
      TAKE_UP_TO_PER_BATCH: string;
      REDIS_BULL_DB: string;
      REDIS_BULL_EVENTS_QUEUNAME: string;
      DESTINATION_CLICKHOUSE_DB: string;
      DESTINATION_CLICKHOUSE_DB_NAME: string;
      BULK_REPEAT_INTERVAL_SEC: string;
      REDIS_JOB_EVENT_TYPE_PROPERTY:
        | "event_type"
        | "__event_type"
        | "clickhouse_table";
      // add more environment variables and their types here
    }
  }
}

// Define type of the redis job events payload:
export type EventToInjest = {
  [key in typeof process.env.REDIS_JOB_EVENT_TYPE_PROPERTY]: string;
} & { __process_single?: true } & Record<
    // In case the
    string,
    string | number | boolean | Date
  >;

if (
  !process.env.DESTINATION_CLICKHOUSE_DB ||
  !process.env.DESTINATION_CLICKHOUSE_DB_NAME
) {
  throw new Error(
    "MISSING ENV VAR: `DESTINATION_CLICKHOUSE_DB`, `DESTINATION_CLICKHOUSE_DB_NAME`"
  );
}
if (!process.env.REDIS_BULL_DB) {
  throw new Error("MISSING ENV VAR: `REDIS_BULL_DB`");
}
if (!process.env.REDIS_BULL_EVENTS_QUEUNAME) {
  throw new Error("MISSING ENV VAR: `REDIS_BULL_EVENTS_QUEUNAME`");
}
if (!process.env.BULK_REPEAT_INTERVAL_SEC) {
  throw new Error("MISSING ENV VAR: `BULK_REPEAT_INTERVAL_SEC`");
}
if (!process.env.REDIS_JOB_EVENT_TYPE_PROPERTY) {
  throw new Error("MISSING ENV VAR: `REDIS_JOB_EVENT_TYPE_PROPERTY`");
}

export const EVENT_TYPE_PROPERTY = process.env.REDIS_JOB_EVENT_TYPE_PROPERTY;

export const CLICKHOUSE_NEW_COL_NULLABLE =
  process.env.CLICKHOUSE_ALTERED_COLUMN_NULLABLE === "1";

const TAKE_UP_TO_PER_BATCH = +(process.env.TAKE_UP_TO_PER_BATCH || 10);
const BULKER_REPEAT_INTERVAL_MS =
  +(process.env.BULK_REPEAT_INTERVAL_SEC || 1) * 1000;

const queue = new Queue(
  process.env.REDIS_BULL_EVENTS_QUEUNAME,
  process.env.REDIS_BULL_DB
);

const destinationClickhouseClient = createClient({
  url: process.env.DESTINATION_CLICKHOUSE_DB,
  database: process.env.DESTINATION_CLICKHOUSE_DB_NAME,
  clickhouse_settings:
    process.env.USE_CLICKHOUSE_ASYNC_INSERT === "1"
      ? {
          async_insert: 1,
          wait_for_async_insert: 1,
        }
      : undefined,
});
const emergencyBatchClient = new ClickhouseBatchClient(
  destinationClickhouseClient
);

// We gonna deploy with one worker only.
const numWorkers = 1;
if (cluster.isMaster) {
  // Forker part!
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }
  cluster.on("online", function (_worker) {
    console.log("Cluster is online");
  });
  cluster.on("exit", function (worker, _code, _signal) {
    console.log("Worker " + worker.process.pid + " died");
  });
} else {
  // We are in one the slave from here:

  // Store bulkers: One bulker per `eventName`
  //  `eventName` is supposed to be a snake_case string.
  // Each event name is going to be one clickhouse table in the destination clickhouse db
  const bulkers: Record<string, Bulker> = {};

  destinationClickhouseClient.ping().then((v) => {
    console.log(`Destination DB : ${v.success ? "ON" : "OFF"}`);
  });
  queue.client.on("connecting", () => {
    console.debug("Connecting...");
  });
  queue.client.on("reconnecting", () => {
    console.debug("Reconnecting !...");
  });
  queue.client.on("ready", () => {
    console.log("Ready");
  });
  queue.process(async (job): Promise<boolean> => {
    console.log(`Job #${job.id} done by worker ${cluster.worker.id}`);

    // An event is a simple Record<string, string | boolean | Date | number>
    // The Record keys as supposed to be in snake_case (if they're not, they gonna be converted):
    const eventData: EventToInjest = job.data;
    // The Record MUST contains an `event_type` (${EVENT_TYPE_PROPERTY}) key:
    const eventName = eventData[EVENT_TYPE_PROPERTY];

    if (!eventName) {
      console.error(`No ${EVENT_TYPE_PROPERTY} set`, eventData);
      return true;
    }

    if (eventData.__process_single === true) {
      console.log(
        `Single failed event to process... Attempt made: ${job.attemptsMade}. ID: ${job.id}`
      );
      try {
        // This case is when an event has failed in the bulker,
        // This unitary try can throw (see the below code of bulk processing):
        await emergencyBatchClient.prepareSchemaAndInjest({
          tableName: eventName,
          rows: [eventData],
        });
        console.log(`Single event success. ID: ${job.id}`);
      } catch (err) {
        console.error(err);
        throw err; // Throw error, and the job.backoff strategy is applied (see the below code of bulk processing).
      }
    } else {
      // If the `eventName` bulker does not exist yet:
      if (!bulkers[eventName]) {
        bulkers[eventName] = new Bulker({
          eventName,
          takeUpToPerBatch: TAKE_UP_TO_PER_BATCH,
          clickhouseClient: destinationClickhouseClient,
        });
      }
      // And we enqueue the event in the bulker for later:
      //  We cannot throw error here, we just gonna enqueue and it will throw in the below bulker processing.
      bulkers[eventName].enqueue(eventData);
    }

    return true;
  });

  // ------------------------------------------------
  // ------------------------------------------------
  // Bulker processing (forever Interval repeating)
  // ------------------------------------------------
  // ------------------------------------------------
  setInterval(() => {
    // For each bulker (meaning each `eventName`):
    for (const eventName in bulkers) {
      // We request for a batch to be process:
      bulkers[eventName].processBatch((failedEvents) => {
        // Here is the way we process the failed events:
        //  These are gonna be spread in the future, using an unitary processing, because it's too hard to split sub-batches of them:
        console.log(
          `A batch of ${eventName} failed! We gonna split its events to process them later, and without bulking`
        );
        const failDelayMs = 2 * 1000;
        for (const failedEvent of failedEvents) {
          queue.add(
            {
              ...failedEvent,
              [EVENT_TYPE_PROPERTY]: eventName, // <-- already in `failedEvent`, but let's set it again to be clear.
              __process_single: true, // As mentioned, it will not be process here anymore in the bulk processing (see above code in the queue.process())
            },
            {
              delay: failDelayMs,
              backoff: {
                // .. and make the unitary retry with an exponential backoff:
                type: "exponential",
                delay: 3 * 1000,
              },
              attempts: 5, // And try few times at least.
            }
          );
        }
      });
    }
  }, BULKER_REPEAT_INTERVAL_MS);
}

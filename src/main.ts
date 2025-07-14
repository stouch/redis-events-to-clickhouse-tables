import Queue, { Job, type Queue as QueueType } from "bull";
import Bulker from "./bulker.class.js";
import { createClient } from "@clickhouse/client";
import ClickhouseBatchClient from "./clickhouse-batch-client.class.js";
import * as fs from "fs";
import dotenv from "dotenv";
import { dayjs } from "./dayjs-utc.js";
dotenv.config();

export const log = (...strings: unknown[]) => {
  strings.map((str) =>
    console.log(`${dayjs().format("YYYY-MM-DDTHH:mm:ssZ[Z]")}: ${str}`)
  );
};
export const error = (...strings: unknown[]) => {
  strings.map((str) =>
    console.error(
      `${dayjs().format("YYYY-MM-DDTHH:mm:ssZ[Z]")}: [ERROR] ${str}`
    )
  );
};
export const warn = (...strings: unknown[]) => {
  strings.map((str) =>
    console.warn(
      `${dayjs().format("YYYY-MM-DDTHH:mm:ssZ[Z]")}: [WARNING] ${str}`
    )
  );
};
export const debug = (...strings: unknown[]) => {
  strings.map((str) =>
    console.debug(
      `${dayjs().format("YYYY-MM-DDTHH:mm:ssZ[Z]")}: [DEBUG] ${str}`
    )
  );
};

// Define process.env.REDIS_JOB_EVENT_TYPE_PROPERTY type:
declare global {
  namespace NodeJS {
    interface ProcessEnv {
      [key: string]: string | undefined;
      DEBUG_STORE_LOG?: "1" | "0";
      DEBUG_STORE_LOG_PATH?: string;
      USE_CLICKHOUSE_ASYNC_INSERT?: "1" | "0";
      CLICKHOUSE_ALTERED_COLUMN_NULLABLE?: "1" | "0";
      SPLIT_RECORDS_AS_COLUMNS?: "1" | "0";
      SPLIT_ARRAY_ITEMS_AS_COLUMNS?: "1" | "0";
      REDIS_BULL_DB: string;
      REDIS_BULL_EVENTS_QUEUNAME: string;
      DESTINATION_CLICKHOUSE_DB: string;
      DESTINATION_CLICKHOUSE_DB_USER?: string;
      DESTINATION_CLICKHOUSE_DB_PW?: string;
      DESTINATION_CLICKHOUSE_DB_NAME: string;
      BULK_REPEAT_INTERVAL_SEC: string; // In seconds
      SOMETHING_IS_WRONG_DELAY_SEC: string; // In seconds
      READ_MAX_CONCURRENCY: string;
      // Maximum per-batch INSERT INTO in the clickhouse db:
      TAKE_UP_TO_PER_BATCH: string; // Integer
      // Number of events we can keep in memory of the instance that hosts
      //  the bulker.
      //
      // The bigger it is, the bigger it's risky (loss of events) if the instance stops to work.
      // It must be greater than `TAKE_UP_TO_PER_BATCH`.
      // We wish to have it big when you want to avoid too much events in the redis queue,
      //  in cases the processing (INSERT INTO) of events is slow.
      BULKER_MAX_LENGTH: string;
      RE_ENQUEUE_OLD_BULL_EVENTS?: "1" | "0";
      RE_ENQUEUE_OLD_BULL_EVENTS_JOBNAME?: string;
      REDIS_JOB_EVENT_TYPE_PROPERTY:
        | "event_type"
        | "__event_type"
        | "clickhouse_table";
      // add more environment variables and their types here
    }
  }
}

// Define type of the redis job events payload:
export type EventDataValue = string | number | boolean | Date;
export type ArrayOfEventDataValue = EventDataValue[];
export type RecordOfEventDataValue = Record<string, EventDataValue>;
export type ArrayOfRecordOfEventDataValue = RecordOfEventDataValue[];
export const isRecordOfEventData = (
  obj: unknown
): obj is RecordOfEventDataValue => {
  if (
    typeof obj === "object" &&
    !(obj instanceof Date) &&
    obj !== null &&
    obj !== undefined
  ) {
    return true;
  }
  return false;
};

export type EventData = Record<
  string,
  | EventDataValue
  | RecordOfEventDataValue
  | ArrayOfEventDataValue
  | ArrayOfRecordOfEventDataValue
>;
export type EventToInjest = {
  [key in typeof process.env.REDIS_JOB_EVENT_TYPE_PROPERTY]: string;
} & {
  __is_single_retry?: true;
  __is_from_old_queue?: true; // In case it's an event that was initially in `RE_ENQUEUE_OLD_BULL_EVENTS_JOBNAME`
  __bulker_full_attempts?: number; // Store this in the event for how many times we tried the event but bulker was full so we re-enqueued it.
  __received_at?: Date | string;
} & EventData;

// ----------------------------
// Config variables definitions
// ----------------------------

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

export const DEBUG_STORE_LOG = process.env.DEBUG_STORE_LOG === "1";
export const DEBUG_STORE_LOG_PATH =
  process.env.DEBUG_STORE_LOG_PATH || "/tmp/debug.log";

export const EVENT_TYPE_PROPERTY = process.env.REDIS_JOB_EVENT_TYPE_PROPERTY;
export const CLICKHOUSE_NEW_COL_NULLABLE =
  process.env.CLICKHOUSE_ALTERED_COLUMN_NULLABLE === "1";
export const SPLIT_RECORDS_AS_COLUMNS =
  process.env.SPLIT_RECORDS_AS_COLUMNS === "1";
export const SPLIT_ARRAY_ITEMS_AS_COLUMNS =
  process.env.SPLIT_ARRAY_ITEMS_AS_COLUMNS === "1";

const TAKE_UP_TO_PER_BATCH = +(process.env.TAKE_UP_TO_PER_BATCH || 10);
const BULKER_MAX_LENGTH = +(process.env.BULKER_MAX_LENGTH || 10);
if (BULKER_MAX_LENGTH < TAKE_UP_TO_PER_BATCH) {
  throw new Error(
    `BULKER_MAX_LENGTH cannot be lower than TAKE_UP_TO_PER_BATCH`
  );
}
const BULKER_REPEAT_INTERVAL_MS =
  +(process.env.BULK_REPEAT_INTERVAL_SEC || 1) * 1000;

const NB_CONCURRENCY = +(process.env.READ_MAX_CONCURRENCY || 1);

// -----------------------------------
// End of config variables definitions
// -----------------------------------

const trace = ({
  pre,
  obj,
  outputSuffix,
  full,
}: {
  pre?: string;
  obj: Record<string, any> | Record<string, any>[];
  outputSuffix?: string;
  full?: boolean;
}) => {
  if (DEBUG_STORE_LOG) {
    fs.appendFile(
      DEBUG_STORE_LOG_PATH + (outputSuffix || ""),
      `${pre ? pre : ""}` +
        JSON.stringify(
          full
            ? obj
            : Array.isArray(obj)
              ? obj.map((row) => ({
                  type: row[EVENT_TYPE_PROPERTY],
                  received_at: (row as any).__received_at,
                  from_old_queue: (row as any).__is_from_old_queue
                    ? true
                    : undefined,
                  // Nb attempts when bulker was full:
                  bulker_full_attempts: (row as any).__bulker_full_attempts
                    ? (row as any).__bulker_full_attempts
                    : undefined,
                }))
              : {
                  type: obj[EVENT_TYPE_PROPERTY],
                  received_at: (obj as any).__received_at,
                  from_old_queue: (obj as any).__is_from_old_queue
                    ? true
                    : undefined,
                  // Nb attempts when bulker was full:
                  bulker_full_attempts: (obj as any).__bulker_full_attempts
                    ? (obj as any).__bulker_full_attempts
                    : undefined,
                }
        ) +
        "\n",
      (_err) => {}
    );
  }
};

// We use a `let` because eventsQueue might be manually closed and recreated when something goes wrong
//  see below `listenQueue({ queue: eventsQueue });` that initialize the listening of the queue:
let eventsQueue = new Queue(
  process.env.REDIS_BULL_EVENTS_QUEUNAME,
  process.env.REDIS_BULL_DB
);
let lastQueueCreatedAt = dayjs().unix();
let lastReadRedisEventAt: null | number = null;

const destinationClickhouseClient = createClient({
  url: process.env.DESTINATION_CLICKHOUSE_DB,
  username: process.env.DESTINATION_CLICKHOUSE_DB_USER || undefined,
  password: process.env.DESTINATION_CLICKHOUSE_DB_PW || undefined,
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
let lastPingShowsDisconnectedDestination = false;

let bulkerInterval: NodeJS.Timeout = undefined;

let onExit = async () => {};

// We are in one the slave from here:
log(`Hello, I'm worker ${process.pid}`);

// Store bulkers: One bulker per `eventName`
//  `eventName` is supposed to be a snake_case string.
// Each event name is going to be one clickhouse table in the destination clickhouse db
const bulkers: Record<string, Bulker> = {};

destinationClickhouseClient.ping().then((v) => {
  log(`Destination DB : ${v.success ? "ON" : "OFF"}`);
  lastPingShowsDisconnectedDestination = v.success;
});

const onReadRedisJob = async (job: Job<any>): Promise<boolean> => {
  // log(`Job #${job.id} done by worker ${cluster.worker.id}`);
  lastReadRedisEventAt = dayjs().unix();

  // An event is a simple Record<string, string | boolean | Date | number>
  // The Record keys as supposed to be in snake_case (if they're not, they gonna be converted):
  const eventData: EventToInjest = job.data;
  // The Record MUST contains an `event_type` (${EVENT_TYPE_PROPERTY}) key:
  const eventName = eventData[EVENT_TYPE_PROPERTY];

  if (!eventName) {
    error(`No ${EVENT_TYPE_PROPERTY} set`, eventData);
    return true;
  }

  if (!eventData.__received_at) {
    // First time we process this event, let's flag it:
    //  this `eventData` (with __received_at) will be kept whatever if we re-enqueue it or not:
    eventData.__received_at = dayjs().toDate();
  }

  if (eventData.__is_single_retry === true) {
    debug(
      `Single failed event to process again... Attempt made: ${job.attemptsMade}. ID: ${job.id}`
    );
    try {
      // This case is when an event has failed in the bulker,
      // This unitary try can throw (see the below code of bulk processing):
      trace({
        pre: "process/failed-single:",
        obj: eventData,
        outputSuffix: ".failedsingle.process.log",
      });
      await emergencyBatchClient.prepareSchemaAndInjest({
        tableName: eventName,
        rows: [eventData],
      });
      trace({
        pre: "success:",
        obj: [eventData],
        outputSuffix: ".success.log",
      });
      debug(`Single event success. ID: ${job.id}`);
    } catch (err) {
      error(`Failing ${JSON.stringify(eventData)}`);
      trace({
        pre: "process/failed-single/error:",
        obj: eventData,
        full: true,
        outputSuffix: ".failedsingle.error.log",
      });
      error(err);
      throw err; // Throw error, and the job.backoff strategy is applied (see the below code of bulk processing).
    }
  } else {
    // If the `eventName` bulker does not exist yet:
    if (!bulkers[eventName]) {
      bulkers[eventName] = new Bulker({
        eventName,
        takeUpToPerBatch: TAKE_UP_TO_PER_BATCH,
        maxBulkerLength: BULKER_MAX_LENGTH,
        clickhouseClient: destinationClickhouseClient,
      });
    }
    // And we enqueue the event in the bulker for later:
    // We just gonna enqueue and it will throw in case of INSERT errors in the below bulker processing.
    try {
      trace({
        pre: "enqueue/inbulk:",
        obj: eventData,
        outputSuffix: ".bulkenqueue.log",
      });
      bulkers[eventName].enqueue(eventData);
    } catch (err) {
      // We can have a throw during the enqueue if the bulker is full,
      //  and in that common case, we dont want to loose the event
      //  (and dont loose it neither because of too many attempts of `job` redis),
      //  so we just re-enqueue:
      if (err.message === "errors.bulker_full") {
        warn(
          `Bulker is full, reenqueue event of ${eventData[EVENT_TYPE_PROPERTY]}:${eventData.__received_at} (${eventData.__bulker_full_attempts || 0})...`
        );
        // Just re-enqueue for later, whatever the attempts value is in current `job`:
        const bulkerFullDelayMs = 5000;
        trace({
          pre: "enqueue/forlater:",
          obj: eventData,
          outputSuffix: ".fullretrylater.log",
        });
        eventsQueue.add(
          {
            ...eventData,
            __bulker_full_attempts: eventData.__bulker_full_attempts
              ? eventData.__bulker_full_attempts + 1
              : 1,
          },
          {
            removeOnComplete: true,
            delay: bulkerFullDelayMs,
            // attempts: 1 // Dont set attempts because retry is made by accepting the event
            //  and re-injecting it (like we are doing here)
          }
        );
      } else {
        throw err; // No supposed to have any ther possible error ..but just in case of
      }
    }
  }

  return true;
};

const onReadRedisFailed = (job: Job<any>, _err) => {
  // See .env.sample docs,
  // This case mostly occurs when a redis event is incompatible with the __default__ job name
  if (process.env.RE_ENQUEUE_OLD_BULL_EVENTS === "1") {
    if (job && job.name === process.env.RE_ENQUEUE_OLD_BULL_EVENTS_JOBNAME) {
      // And these jobs have this strange timestamp in seconds: (While ms has timestamp str length >= 13)
      if (job.timestamp && `${job.timestamp}`.length <= 10) {
        const dataToReenqueue = job.data;

        if (!dataToReenqueue.__received_at) {
          // First time we process this failed event from `main` queue, let's flag its date:
          dataToReenqueue.__received_at = dayjs().toDate();
        }

        trace({
          pre: "re-enqueue:",
          obj: dataToReenqueue,
          outputSuffix: ".reenqueue.log",
        });
        eventsQueue.add(
          {
            ...dataToReenqueue,
            __is_from_old_queue: true,
          },
          {
            removeOnComplete: true,
            // TODO: These strange events propably have a delay in seconds too,
            //  but should we keep "delay"?
            //  While our goal is to process events which are not supposed to be delayed anyway.
            // delay: (job as any).delay ? (job as any).delay * 1000  : undefined
            // For now, we just re-enqueue them.
          }
        );

        // And request to remove this strange old job:
        job.remove();
      }
    }
  }
};

const bulkingIntervalFunction = () => {
  if (!lastPingShowsDisconnectedDestination) {
    error(
      `Wait.. Destination clickhouse is disconnected. We have wait it comes back, or kill this app to re-enqueue waiting Bulker events.`
    );
    return;
  }

  // For each bulker (meaning each `eventName`):
  for (const eventName in bulkers) {
    // We request for a batch to be process:
    bulkers[eventName].processBatch({
      onSuccess: (successedEvents) => {
        trace({
          pre: "success:",
          obj: successedEvents,
          outputSuffix: ".success.log",
        });
      },
      onFailed: (failedEvents) => {
        error(`Batching of ${eventName} failed ${failedEvents.length} events`);
        // Here is the way we process the failed events:
        //  These are gonna be spread in the future, using an unitary processing, because it's too hard to split sub-batches of them:
        const failDelayMs = 2 * 1000;
        for (const failedEvent of failedEvents) {
          trace({
            pre: "enqueue/failed:",
            obj: failedEvent,
            outputSuffix: ".bulkjobfailed.log",
          });
          eventsQueue.add(
            {
              ...failedEvent,
              [EVENT_TYPE_PROPERTY]: eventName, // <-- already in `failedEvent`, but let's set it again to be clear.
              __is_single_retry: true, // As mentioned, it will not be process here anymore in the bulk processing (see above code in the queue.process())
            },
            {
              removeOnComplete: true,
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
      },
    });
  }
};

// --------------------------------------
// ---------- Bind redis queue ----------
// --------------------------------------

const listenQueue = ({ queue }: { queue: QueueType }) => {
  queue.client.on("connecting", () => {
    debug("Connecting...");
  });
  queue.client.on("reconnecting", () => {
    debug("Reconnecting !...");
  });
  queue.client.on("ready", () => {
    log("Ready");
  });
  queue.client.on("end", () => {
    warn("Queue ended!");
  });
  queue.client.on("close", () => {
    warn("Queue closed.");
  });
  queue.client.on("error", (err) => {
    error("Queue client error !", err);
  });
  queue.on("failed", onReadRedisFailed);
  queue.process(NB_CONCURRENCY, onReadRedisJob);
};
// .. and listen:
listenQueue({ queue: eventsQueue });

// ------------------------------------------------
// ------------------------------------------------
// Status checking on repeated interval -----------
// ------------------------------------------------
// ------------------------------------------------

let recreating = false;
let recreateInterval: NodeJS.Timeout | null = null;
// Check if we need to re-create a Redis Queue connection if something looks wrong with no new read events since some delay:
const SOMETHING_IS_WRONG_DELAY_SEC = +(
  process.env.SOMETHING_IS_WRONG_DELAY_SEC || 60 * 2
); // When we dont get any new events for more than this delay, something will be judged as wrong.
const SOMETHING_IS_WRONG_CHECK_EVERY_SEC = 10;
recreateInterval = setInterval(async () => {
  if (recreating) {
    warn("Still recreating..");
  }
  const someLongTimeAgo = dayjs()
    .subtract(SOMETHING_IS_WRONG_DELAY_SEC, "second")
    .unix();
  const now = dayjs().unix();
  if (now > lastQueueCreatedAt + SOMETHING_IS_WRONG_DELAY_SEC) {
    if (
      (lastReadRedisEventAt === null && lastQueueCreatedAt < someLongTimeAgo) ||
      lastReadRedisEventAt < someLongTimeAgo
    ) {
      warn(
        `Something is wrong, we got no new event processed from Redis since ${someLongTimeAgo}. Might means the queue socket is dead. Re-create the queue:`
      );
      recreating = true;

      debug(`Temporary pause the bulker interval`);
      bulkerInterval && clearInterval(bulkerInterval);
      bulkerInterval = null;

      // First, completely close the queue, ignoring errors because we are going to re-create anyway
      try {
        await eventsQueue.close();
      } catch (err) {
        error(`${err}`);
      }

      // Before full create a new Redis Queue listening, clean what might stay from the previously closed queue:
      let oneOfBulkerHasWaitingEvents = false;
      for (const eventName in bulkers) {
        const nbWaitingEvents = bulkers[eventName].getBulkWaitingQueueLength();
        const nbProcessingEvents = bulkers[eventName].getBulkingQueueLength();
        if (nbWaitingEvents > 0 || nbProcessingEvents > 0) {
          warn(
            `#${eventName}: That looks even wronger, because we still got processing/waiting event (waiting: ${nbWaitingEvents}, processing: ${nbProcessingEvents}).`
          );
          log(
            `#${eventName}: We gonna wait the processing been pushed to destination, and gonna re-enqueue waiting events..`
          );
          oneOfBulkerHasWaitingEvents = true;
        }
      }
      if (oneOfBulkerHasWaitingEvents) {
        const waitingEventsQueue = new Queue(
          process.env.REDIS_BULL_EVENTS_QUEUNAME,
          process.env.REDIS_BULL_DB
        );
        await new Promise((resolve) => {
          waitingEventsQueue.client.on("ready", () => {
            log("Ready");
            resolve(true);
          });
        });
        for (const eventName in bulkers) {
          log(`Re-inject bulker waiting events for ${eventName} queue...`);
          await bulkers[eventName].finishLastBatchAndReenqueueWaitingEvents(
            waitingEventsQueue
          );
        }
      }

      //
      // And recreate/reset the listening of the new queue
      //  using the global variable:
      //
      // If this re-connect throws an error,
      //  as we ensured above to reinject the events in Redis, we will just have to restart thanks to the docker-compose
      eventsQueue = null;
      eventsQueue = new Queue(
        process.env.REDIS_BULL_EVENTS_QUEUNAME,
        process.env.REDIS_BULL_DB
      );
      lastQueueCreatedAt = dayjs().unix();
      listenQueue({ queue: eventsQueue });

      log("Restart the bulker interval.");
      bulkerInterval = setInterval(
        bulkingIntervalFunction,
        BULKER_REPEAT_INTERVAL_MS
      );

      recreating = false;
    }
  }
}, SOMETHING_IS_WRONG_CHECK_EVERY_SEC * 1000);

//
// Check the destination database status:
setInterval(() => {
  destinationClickhouseClient.ping().then((v) => {
    log(`Check destination DB : ${v.success ? "ON" : "OFF"}`);
    lastPingShowsDisconnectedDestination = v.success;
  });
}, 30 * 1000);

//
// Check how are filled the bulkers:
setInterval(() => {
  for (const eventName in bulkers) {
    log(
      `Bulker #${eventName} has ${bulkers[eventName].getBulkingQueueLength()} processing, and ${bulkers[eventName].getBulkWaitingQueueLength()} pending jobs`
    );
  }
}, 20 * 1000);

// ------------------------------------------------
// ------------------------------------------------
// Bulker processing (forever Interval repeating)
// ------------------------------------------------
// ------------------------------------------------
bulkerInterval = setInterval(
  bulkingIntervalFunction,
  BULKER_REPEAT_INTERVAL_MS
);

// -----------------------------------------------------------------------------------
// --- Gracefully stop a bulker and re-enqueue the events that were waiting in it ----
// -----------------------------------------------------------------------------------
onExit = async () => {
  log("Stop the repeated bulk INSERT.");

  recreateInterval && clearInterval(recreateInterval);
  recreateInterval = null;
  if (recreating) {
    warn("Wait! A recreating was occurring, just wait it to be done:");
    let recreatingStatusInterval: NodeJS.Timeout | null = null;
    await new Promise((resolve) => {
      recreatingStatusInterval = setInterval(() => {
        if (recreating) {
          warn(
            "...Wait, a recreating was occurring, just wait it to be done..."
          );
        } else {
          log(
            `OK recreating is done, we can gracefully close the current Redis queue.`
          );
          clearInterval(recreatingStatusInterval);
          resolve(true);
        }
      }, 1000);
    });
  }

  bulkerInterval && clearInterval(bulkerInterval);
  bulkerInterval = null;

  log(
    "Close the queue (stop to .process() any redis job!), and ignore error.."
  );
  try {
    await eventsQueue.close();
  } catch (err) {
    error(`${err}`);
  }

  log(
    "Open a queue just to reinject if there are some waiting events in `currentBatchToProcess`..."
  );
  const waitingEventsQueue = new Queue(
    process.env.REDIS_BULL_EVENTS_QUEUNAME,
    process.env.REDIS_BULL_DB
  );
  await new Promise((resolve) => {
    waitingEventsQueue.client.on("ready", () => {
      log("Ready");
      resolve(true);
    });
  });
  for (const eventName in bulkers) {
    log(`Re-inject bulker waiting events for ${eventName} queue...`);
    await bulkers[eventName].finishLastBatchAndReenqueueWaitingEvents(
      waitingEventsQueue
    );
  }
  log("We can exit !");
};

const gracefulShutdown = async () => {
  log(`Shutting down Worker ${process.pid} gracefully...`);
  onExit && (await onExit());
  process.exit(0);
};

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

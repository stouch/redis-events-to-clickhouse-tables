import Queue from "bull";
import cluster from "cluster";
import Bulker from "./bulker.class.js";
import { createClient } from "@clickhouse/client";
import dotenv from "dotenv";
dotenv.config();

const bulkerRepeatMs = 2 * 1000;
const numWorkers = 2;

if (
  !process.env.DESTINATION_CLICKHOUSE_DB ||
  !process.env.DESTINATION_CLICKHOUSE_DB_NAME
) {
  throw new Error(
    "MISSING ENV VAR: `DESTINATION_CLICKHOUSE_DB`, `DESTINATION_CLICKHOUSE_DB_NAME`"
  );
}
if (!process.env.REDIS_DB) {
  throw new Error("MISSING ENV VAR: `REDIS_DB`");
}

const queue = new Queue("event-track", process.env.REDIB_DB);

const destinatonDb = createClient({
  url: process.env.DESTINATION_CLICKHOUSE_DB,
  database: process.env.DESTINATION_CLICKHOUSE_DB_NAME,
});

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

  // Store bulkers: One bulker per event name
  // Each event name is going to be one clickhouse table in the destination db
  const bulkers: Record<string, Bulker> = {};

  destinatonDb.ping().then((v) => {
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
  queue.process(function (job, jobDone) {
    console.log("Job done by worker", cluster.worker.id, job.id);

    const eventData = job.data;

    const eventName = eventData.event_type;

    if (!bulkers[eventName]) {
      bulkers[eventName] = new Bulker({
        eventName,
        takeUpToPerBatch: 10,
        clickhouseClient: destinatonDb,
      });
    }
    bulkers[eventName].process(eventData);

    console.log("job", eventData);
    jobDone();
  });

  setInterval(() => {
    console.log("Bulk process");
    for (const bulker in bulkers) {
      console.log(`Bulker ${bulker}`);
      bulkers[bulker].batch();
    }
  }, bulkerRepeatMs);
}

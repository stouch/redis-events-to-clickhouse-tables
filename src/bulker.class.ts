import { ClickHouseClient } from "@clickhouse/client";
import ClickhouseBatchClient from "./clickhouse-batch-client.class.js";
import { configDotenv } from "dotenv";
import { EventToInjest } from "./main.js";

if (!process.env || Object.keys(process.env).length === 0) {
  configDotenv();
}

export type BatchProcessingMetadata = {
  startedAt: number;
  length: number;
  status: "creating_updating_table" | "inserting";
};

// We create one bulker per event:
// 1 bulker = 1 clickhouse table
class Bulker {
  private takeUpToPerBatch: number;
  private clickhouseBatchClient: ClickhouseBatchClient;
  private destinationClickhouseTable: string;
  constructor({
    clickhouseClient,
    takeUpToPerBatch,
    eventName,
  }: {
    clickhouseClient: ClickHouseClient;
    takeUpToPerBatch: number;
    eventName: string;
  }) {
    this.destinationClickhouseTable = eventName;
    this.takeUpToPerBatch = takeUpToPerBatch;

    this.clickhouseBatchClient = new ClickhouseBatchClient(clickhouseClient);
  }

  private currentBatchToProcess: EventToInjest[] = [];
  enqueue(eventToInjest: EventToInjest) {
    this.currentBatchToProcess.push(eventToInjest);
  }

  private batchProcessingMetadata: BatchProcessingMetadata | null = null;
  private batchProcessing: EventToInjest[] = [];
  async processBatch(onFailed: (failedEvents: EventToInjest[]) => void) {
    if (this.batchProcessing.length > 0) {
      console.log("Already pushing a batch");
      return;
    }
    if (this.currentBatchToProcess.length === 0) {
      // Nothing to batch!
      return;
    }

    console.log(
      `Gonna batch ${Math.min(this.takeUpToPerBatch, this.currentBatchToProcess.length)}/${this.currentBatchToProcess.length}`
    );
    this.batchProcessing = this.currentBatchToProcess.splice(
      0,
      this.takeUpToPerBatch
    );
    this.batchProcessingMetadata = {
      length: this.batchProcessing.length,
      startedAt: new Date().getUTCMilliseconds(),
      status: "creating_updating_table",
    };

    console.log("Batch goes...");

    try {
      console.log("Migrate table...");

      await this.clickhouseBatchClient.prepareSchema({
        tableName: this.destinationClickhouseTable,
        rows: this.batchProcessing,
      });
      console.log("Migrate done.");

      this.batchProcessingMetadata.status = "inserting";

      console.log("Inserting..");
      await this.clickhouseBatchClient.insertRows({
        tableName: this.destinationClickhouseTable,
        rows: this.batchProcessing,
      });

      console.log("Inserting done.");
    } catch (err) {
      console.error(err);
      // If an error occur, we dont throw and loose everything, we just gonna reinject the rows we tried to injest:
      onFailed(this.batchProcessing);
    }
    this.batchProcessing = [];
    this.batchProcessingMetadata = null;
  }
}

export default Bulker;

import { ClickHouseClient } from "@clickhouse/client";
import ClickhouseBatchClient from "./clickhouse-batch-client.class.js";
import { configDotenv } from "dotenv";
import { EventToInjest } from "./main.js";
import { Queue } from "bull";

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
  private maxBulkerLength: number;
  private clickhouseBatchClient: ClickhouseBatchClient;
  private destinationClickhouseTable: string;
  constructor({
    clickhouseClient,
    takeUpToPerBatch,
    maxBulkerLength,
    eventName,
  }: {
    clickhouseClient: ClickHouseClient;
    takeUpToPerBatch: number;
    maxBulkerLength: number;
    eventName: string;
  }) {
    this.destinationClickhouseTable = eventName;
    this.takeUpToPerBatch = takeUpToPerBatch;
    if (maxBulkerLength < takeUpToPerBatch) {
      throw new Error("errors.max_length_cannot_be_lower_than_bulk_batch");
    }
    this.maxBulkerLength = maxBulkerLength;

    this.clickhouseBatchClient = new ClickhouseBatchClient(clickhouseClient);
  }

  private currentBatchToProcess: EventToInjest[] = [];
  enqueue(eventToInjest: EventToInjest) {
    if (this.currentBatchToProcess.length >= this.maxBulkerLength) {
      throw new Error("errors.bulker_full");
    }
    this.currentBatchToProcess.push(eventToInjest);
  }

  private batchProcessingMetadata: BatchProcessingMetadata | null = null;
  private batchProcessing: EventToInjest[] = [];

  async processBatch({
    onFailed,
    onSuccess,
  }: {
    onFailed: (failedEvents: EventToInjest[]) => void;
    onSuccess: (successedEvents: EventToInjest[]) => void;
  }) {
    if (this.batchProcessing.length > 0) {
      console.warn(
        `Already processing ${this.batchProcessing.length} events !`
      );
      return;
    }
    if (this.currentBatchToProcess.length === 0) {
      // Nothing to batch!
      return;
    }

    console.log(
      `Event Queue: ${this.destinationClickhouseTable}, Gonna batch ${Math.min(this.takeUpToPerBatch, this.currentBatchToProcess.length)}/${this.currentBatchToProcess.length}`
    );
    // `splice` remove events from `this.currentBatchToProcess`
    this.batchProcessing = this.currentBatchToProcess.splice(
      0,
      this.takeUpToPerBatch
    );
    this.batchProcessingMetadata = {
      length: this.batchProcessing.length,
      startedAt: new Date().getUTCMilliseconds(),
      status: "creating_updating_table",
    };

    try {
      await this.clickhouseBatchClient.prepareSchema({
        tableName: this.destinationClickhouseTable,
        rows: this.batchProcessing,
      });

      this.batchProcessingMetadata.status = "inserting";
      await this.clickhouseBatchClient.insertRows();
      onSuccess(this.batchProcessing);
    } catch (err) {
      console.error(err);
      // If an error occur, we dont throw and loose everything, we just gonna reinject the rows we tried to injest:
      onFailed(this.batchProcessing);
    } finally {
      this.batchProcessing = [];
      this.batchProcessingMetadata = null;
    }
  }

  async finishLastBatchAndReenqueueWaitingEvents(queue: Queue) {
    console.log(
      `#${this.destinationClickhouseTable}: Wait for finishing the last batch..`
    );
    await new Promise((resolve) => {
      setInterval(() => {
        if (this.batchProcessingMetadata !== null) {
          console.log(
            `#${this.destinationClickhouseTable}: Still processing a batch...`
          );
        } else {
          console.log(
            `#${this.destinationClickhouseTable}: OK current batch processing is done.`
          );
          resolve(true);
        }
      }, 1000);
    });
    if (this.currentBatchToProcess.length > 0) {
      console.log(
        `#${this.destinationClickhouseTable}, Still has ${this.currentBatchToProcess.length} events that were waiting. Re-enqueue them:`
      );
      for (const eventData of this.currentBatchToProcess) {
        queue.add({ ...eventData });
      }
    }
    console.log(`#${this.destinationClickhouseTable}: Done.`);
  }
}

export default Bulker;

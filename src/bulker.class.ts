import { ClickHouseClient } from "@clickhouse/client";
import ClickhouseBatchClient from "./clickhouse-batch-client.class.js";

export type EventToInjest = Record<string, string | number | boolean>;

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
  process(eventData: EventToInjest & { event_type: string }) {
    // We need to unset `event_type` that comes from our redis job (to define the clickhouse table name / event name):
    const eventToInjest = { ...eventData };
    delete eventToInjest.event_type;
    this.currentBatchToProcess.push(eventToInjest);
    console.log("Added to batch", eventToInjest);
  }

  private batchProcessingMetadata: BatchProcessingMetadata | null = null;
  private batchProcessing: EventToInjest[] = [];
  async batch() {
    if (this.batchProcessing.length > 0) {
      console.log("Already pushing a batch");
      return;
    }
    if (this.currentBatchToProcess.length === 0) {
      // Nothing to batch!
      return;
    }

    console.log(`Gonna batch ${this.currentBatchToProcess.length}`);

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
    console.log("Migrate table...");

    await this.clickhouseBatchClient.prepareClickhouseTable({
      tableName: this.destinationClickhouseTable,
      rows: this.batchProcessing,
    });
    console.log("Migrate done.");

    this.batchProcessingMetadata.status = "inserting";

    console.log("Inserting..");
    await this.clickhouseBatchClient.insert({
      tableName: this.destinationClickhouseTable,
      rows: this.batchProcessing,
    });

    console.log("Inserting done.");
    this.batchProcessing = [];
    this.batchProcessingMetadata = null;
  }
}

export default Bulker;

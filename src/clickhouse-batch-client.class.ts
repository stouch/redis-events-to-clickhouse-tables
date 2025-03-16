import { ClickHouseClient } from "@clickhouse/client";
import { dayjs } from "./dayjs-utc.js";
import snakeCase from "lodash.snakecase";
import {
  CLICKHOUSE_NEW_COL_NULLABLE,
  EVENT_TYPE_PROPERTY,
  EventDataValue,
  EventToInjest,
  isRecordOfEventData,
} from "./main.js";
import { randomUUID } from "crypto";
import { transform } from "./transform.js";
import { isDateString, isFloat } from "./utils.js";

const RECEIVED_AT_TS_COLUMN_NAME = "received_at";
const SENT_AT_TS_COLUMN_NAME = "sent_at";
const MID_COLUMN_NAME = "message_id";

export type EventToInjestInTable = Record<string, EventDataValue>;

enum ColumnType {
  DATE = "DateTime",
  DATE64 = "DateTime64(6)",
  STRING = "String",
  INTEGER = "Int64",
  FLOAT = "Float64",
  BOOLEAN = "UInt8",
}

type ClickhouseColumnDefinition =
  | { type: ColumnType.BOOLEAN; default?: boolean; nullable?: true }
  | { type: ColumnType.INTEGER; default?: number; nullable?: true }
  | { type: ColumnType.FLOAT; default?: number; nullable?: true }
  | {
      type: Exclude<
        ColumnType,
        ColumnType.BOOLEAN | ColumnType.INTEGER | ColumnType.FLOAT
      >; // String, Date, ...
      default?: string;
      nullable?: true;
    };

type TableName = string;
type ColumnName = string;

type ClickhouseTableSchema = Record<ColumnName, ClickhouseColumnDefinition>;

/**
 * This class allows to batch-insert rows in a Clickhouse database, by:
 *
 * - Analyzing rows we want to batch-insert in the according Clickhouse table:
 *   These rows must be simple records of [string: <string | Date | number | boolean>],
 *    and all of these rows must have the same structure.
 *   Once we analyzed the rows we want to insert, we create or update the Clickhouse table
 *    schema that is gonna receive the rows.
 *   When some columns of some of the rows are not undefined, while some of these same columns
 *    are defined in the other rows, we'll try to injest the column value when it exists,
 *    and we'll try to set it to NULL when it does not exist.
 *   Method: `prepareSchema`
 *
 * - Insert the rows in the according Clickhouse table:
 *   Method: `insertRows`
 *
 * We can also use method: `prepareSchemaAndInjest`, which does both at same time.
 *
 */
class ClickhouseBatchClient {
  private preparedSchema: {
    table: string;
    schema: ClickhouseTableSchema;
  } | null = null;
  private preparedRows: Record<string, EventDataValue>[] | null = null;

  constructor(private readonly clickhouseClient: ClickHouseClient) {
    this.preparedSchema = null;
    this.preparedRows = null;
  }

  // --------------------
  // --------------------
  // -- Public methods --
  // --------------------
  // --------------------

  async prepareSchema({
    tableName,
    rows,
  }: {
    tableName: TableName;
    rows: EventToInjest[];
  }): Promise<void> {
    if (rows.length === 0) {
      throw new Error("errors.no_rows_to_insert");
    }
    this.preparedRows = this.prepareRows(rows);
    const requestedSchema = this.getRowsMinimumSchema(this.preparedRows);
    if (Object.keys(requestedSchema).length === 0) {
      throw new Error("errors.no_columns_found");
    }
    const tableExists = await this.doesTableExist(tableName);
    if (tableExists) {
      const existingSchema = await this.getClickhouseTableSchema(tableName);
      this.preparedSchema = {
        table: tableName,
        schema: await this.addMissingColumns({
          tableName,
          currentSchema: existingSchema,
          // In `addMissingColumns`, we may update `requestedSchema` if the `existingSchema` is a bit different
          //  for example the requested DateTime64(6) is probably just a DateTime (see `addMissingColumns`)
          requestedSchema: requestedSchema,
        }),
      };
    } else {
      await this.createTable({
        tableName,
        requestedSchema: requestedSchema,
      });

      this.preparedSchema = { table: tableName, schema: requestedSchema };
    }
  }

  async insertRows() {
    if (this.preparedSchema === null) {
      throw new Error("errors.no_prepared_schema");
    }
    if (this.preparedRows === null) {
      throw new Error("errors.no_prepared_schema");
    }
    const rowsQueries = this.getClickhouseRowsSql({
      rowsData: this.preparedRows,
      requestedSchema: this.preparedSchema.schema,
    });
    const sqlQuery = `INSERT INTO ${this.preparedSchema.table} 
      (${Object.keys(this.preparedSchema.schema).join(",")}) VALUES 
        (${rowsQueries.join(`),
        (`)});`;
    try {
      // Ensure we waint going to double-insert:
      this.preparedRows = null;
      this.preparedSchema = null;

      // And start the INSERT INTO:
      await this.clickhouseClient.exec({
        query: sqlQuery,
      });
    } catch (err) {
      console.error(sqlQuery);
      throw err;
    }
  }

  async prepareSchemaAndInjest({
    tableName,
    rows,
  }: {
    tableName: TableName;
    rows: EventToInjest[];
  }) {
    try {
      await this.prepareSchema({ tableName, rows });
      await this.insertRows();
    } catch (err) {
      throw err;
    }
  }

  // --------------------
  // --------------------
  // -- Helper methods --
  // --------------------
  // --------------------

  /*
   * Prepare a 1-dimension row from an event:
   * eg:
   *  {
   *     "event_type": "<clickhouse table name>",
   *     "__is_single_retry": true,
   *     "someTest": "value",
   *     "someKey": ["withArray", "value"],
   *     "correct_case": {"subRecord": "withValue"}
   *  }
   *  becomes, once prepared, :
   *  {
   *     "some_test": "value",
   *     "some_key_0": "withArray",
   *     "some_key_1": "value",
   *     "correct_case_sub_record": "withValue"
   *  }
   */
  private prepareRowColumns(
    rowColumnValues: EventToInjest
  ): Record<string, EventDataValue> {
    const rowWithoutEvenType: Record<string, EventDataValue> = {};
    // For each column:
    for (const eventKey in rowColumnValues) {
      // First exclude the forbidden column name:
      if (
        eventKey === EVENT_TYPE_PROPERTY ||
        eventKey === "__is_single_retry" ||
        eventKey === "__is_from_old_queue" ||
        eventKey === "__bulker_full_attempts" ||
        eventKey === "__received_at"
      ) {
        continue;
      }
      // And parse array or sub records:
      if (Array.isArray(rowColumnValues[eventKey])) {
        const rowValues: EventDataValue[] = rowColumnValues[eventKey];
        rowValues.map((rowValue, idx) => {
          const snakifiedKey: string = snakeCase<string>(eventKey);
          rowWithoutEvenType[`${snakifiedKey}_${idx}`] = rowValue;
        });
      } else if (isRecordOfEventData(rowColumnValues[eventKey])) {
        Object.keys(rowColumnValues[eventKey]).map((eachRecordKey) => {
          const snakifiedKey: string = snakeCase<string>(
            `${eventKey}_${eachRecordKey}`
          );
          rowWithoutEvenType[snakifiedKey] =
            rowColumnValues[eventKey][eachRecordKey];
        });
      } else {
        const rowValue: EventDataValue = rowColumnValues[eventKey];
        const snakifiedKey: string = snakeCase<string>(eventKey);
        rowWithoutEvenType[snakifiedKey] = rowValue;
      }
    }
    return rowWithoutEvenType;
  }

  // Ensure we gonna use column names in snake_case, and that we aint going to persist "event_type" (${EVENT_TYPE_PROPERTY}) from the redis bull event job.
  private prepareRows(rows: EventToInjest[]): Record<string, EventDataValue>[] {
    return rows.map((row) => {
      const rowWithPrimaryKey = {
        ...this.prepareRowColumns(row),
        [`${RECEIVED_AT_TS_COLUMN_NAME}`]:
          typeof row.__received_at === "string"
            ? dayjs(row.__received_at).toDate()
            : row.__received_at,
        [`${SENT_AT_TS_COLUMN_NAME}`]: dayjs().toDate(),
        [`${MID_COLUMN_NAME}`]: `${randomUUID()}`,
      };
      // Apply the custom-transform (if it's defined):
      return transform(rowWithPrimaryKey);
    });
  }

  private getClickhouseColumnsSql(
    columnsToAdd: ClickhouseTableSchema,
    isUpdateSyntax: boolean = false
  ) {
    // Build the SQL for CREATE TABLE of colums, or ALTER TABLE columns:
    const addColumnQueries = Object.keys(columnsToAdd).map(
      (columnName: ColumnName) => {
        const column = columnsToAdd[columnName];
        const type = column.nullable ? `Nullable(${column.type})` : column.type;
        const defaultValue = column.default;
        // ex: [..., `age` UInt64 DEFAULT 18, ...]
        return `${columnName} ${isUpdateSyntax ? "TYPE " : ""}${type} ${
          defaultValue !== undefined
            ? typeof defaultValue === "string"
              ? `DEFAULT '${defaultValue.replace(/'/g, "\\'")}'`
              : typeof defaultValue === "number"
                ? `DEFAULT ${defaultValue}`
                : `DEFAULT ${defaultValue ? "1" : "0"}`
            : ""
        }`;
      }
    );
    return addColumnQueries;
  }

  private getClickhouseRowsSql({
    rowsData,
    requestedSchema,
  }: {
    rowsData: EventToInjestInTable[];
    requestedSchema: ClickhouseTableSchema;
  }) {
    const addRowsQueries = rowsData.map((row: EventToInjestInTable) => {
      let rowSql: string[] = [];
      for (const column in requestedSchema) {
        const columnContent = row[column];
        if (columnContent === undefined) {
          rowSql.push("NULL");
          continue;
        }
        const colType = requestedSchema[column].type;
        const dateFormat =
          colType === ColumnType.DATE64
            ? "YYYY-MM-DD HH:mm:ss.SSS"
            : "YYYY-MM-DD HH:mm:ss";
        rowSql.push(
          `${
            columnContent instanceof Date
              ? `'${dayjs(columnContent).format(dateFormat)}'`
              : typeof columnContent === "string" && isDateString(columnContent)
                ? `'${dayjs(columnContent).format(dateFormat)}'`
                : typeof columnContent === "number"
                  ? columnContent
                  : typeof columnContent === "string"
                    ? `'${columnContent.replace(/'/g, "\\'")}'`
                    : columnContent === true
                      ? "1"
                      : "0"
          }`
        );
      }
      return rowSql.join(","); // (1, 'Alice', 25, '2024-02-27 10:00:00.000'),
    });
    return addRowsQueries; // [ (1, 'Alice', 25, '2024-02-27 10:00:00.000'), (2, 'Bob', 30, '2024-02-26 15:30:00.000') ]
  }

  // ----------------------------------
  // ----------------------------------
  // -- Methods that compare schemas --
  // ----------------------------------
  // ----------------------------------

  // Get the columns of a set of rows.
  // We need to crawl all the rows to find all the columns because some of rows might not have all the columns set.
  private getColsMinimumList(rows: EventToInjestInTable[]) {
    return [
      // Deduplicated columns from all the rows
      ...new Set(
        rows
          .map((row) =>
            // For each row, we only find columns which have a value !== undefined:
            Object.keys(row).filter((rowColumn) => row[rowColumn] !== undefined)
          )
          .flat()
      ),
    ];
  }

  // Get the columns of a set of rows, and for each column we get their Clickhouse data-type
  private getRowsMinimumSchema(
    rows: EventToInjestInTable[]
  ): ClickhouseTableSchema {
    const requestedSchema: ClickhouseTableSchema = {};

    // Some rows may have more columns that the others,
    //  we need to find the minimum common properties between the rows:
    const columnsOfRowsToIngest = this.getColsMinimumList(rows);
    const firstFoundValuePerColumn: Record<
      string,
      string | number | Date | boolean
    > = {};
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        if (firstFoundValuePerColumn[key] === undefined) {
          firstFoundValuePerColumn[key] = row[key];
        }
      }
      if (
        Object.keys(firstFoundValuePerColumn).length ===
        columnsOfRowsToIngest.length
      ) {
        // Once we found at least one value for each of the columns we need to ingest
        break;
      }
    }

    for (const property of columnsOfRowsToIngest) {
      const propertyValue = firstFoundValuePerColumn[property];
      if (typeof propertyValue === "string") {
        if (isDateString(propertyValue)) {
          requestedSchema[property] = { type: ColumnType.DATE64 };
        } else {
          requestedSchema[property] = { type: ColumnType.STRING };
        }
      } else if (typeof propertyValue === "number") {
        if (isFloat(propertyValue)) {
          requestedSchema[property] = { type: ColumnType.FLOAT };
        } else {
          requestedSchema[property] = { type: ColumnType.INTEGER };
        }
      } else if (propertyValue instanceof Date) {
        requestedSchema[property] = { type: ColumnType.DATE64 };
      } else {
        // boolean
        requestedSchema[property] = { type: ColumnType.BOOLEAN };
      }
    }

    return requestedSchema;
  }

  private async getClickhouseTableSchema(
    tableName: TableName
  ): Promise<ClickhouseTableSchema> {
    const currentSchema = (
      await (
        await this.clickhouseClient.query({
          query: `DESCRIBE ${tableName}`,
        })
      ).json<{ name: string; type: string; default_expression: string }>()
    ).data;

    /*
    [
      {
        name: 'toto',
        type: 'String',
        default_type: '',
        default_expression: '',
        comment: '',
        codec_expression: '',
        ttl_expression: ''
      },
      ...
    ]
    */
    const mappedSchema: ClickhouseTableSchema = {};
    for (const column of currentSchema) {
      const columnType = column.type;
      mappedSchema[column.name] = {
        type: columnType.replace(/Nullable\((.*)\)/gm, "$1") as ColumnType,
        nullable: columnType.indexOf("Nullable(") > -1 || undefined,
      };
    }
    return mappedSchema;
  }

  // --------------------------------------------
  // --------------------------------------------
  // -- Methods that execute SQL in Clickhouse --
  // --------------------------------------------
  // --------------------------------------------

  private async addMissingColumns({
    tableName,
    currentSchema,
    requestedSchema,
  }: {
    tableName: TableName;
    currentSchema: ClickhouseTableSchema;
    requestedSchema: ClickhouseTableSchema;
  }): Promise<ClickhouseTableSchema> {
    const missingColumns: Record<ColumnName, ClickhouseColumnDefinition> = {};
    const modifiedColumns: Record<ColumnName, ClickhouseColumnDefinition> = {};
    for (const requestedColumn in requestedSchema) {
      if (currentSchema[requestedColumn]) {
        // Column exists!
        if (
          currentSchema[requestedColumn].type === ColumnType.INTEGER ||
          currentSchema[requestedColumn].type === ColumnType.BOOLEAN ||
          currentSchema[requestedColumn].type === ColumnType.FLOAT
        ) {
          // This is a specific case of when we want to inject STRING in existing columns
          //  of integer/boolean/float and this might not going to do any trouble.
          // So in this specific case we ALTER the column:
          if (requestedSchema[requestedColumn].type === ColumnType.STRING) {
            modifiedColumns[requestedColumn] = {
              ...requestedSchema[requestedColumn],
              nullable: currentSchema[requestedColumn].nullable
                ? true
                : undefined,
            };
          }
        }
        continue;
      } else {
        // Table exists, we gonna make the columns not required:
        missingColumns[requestedColumn] = {
          ...requestedSchema[requestedColumn],
          nullable: CLICKHOUSE_NEW_COL_NULLABLE ? true : undefined,
        };
      }
    }
    if (Object.keys(missingColumns).length > 0) {
      const addQueries = this.getClickhouseColumnsSql(missingColumns);
      const sqlQuery = `ALTER TABLE \`${tableName}\` ADD COLUMN ${addQueries.join(", ADD COLUMN ")};`;
      console.debug({ sqlQuery });
      try {
        await this.clickhouseClient.query({
          query: sqlQuery,
        });
      } catch (err) {
        throw err;
      }
    }
    if (Object.keys(modifiedColumns).length > 0) {
      const updateQueries = this.getClickhouseColumnsSql(modifiedColumns, true);
      const sqlQuery = `ALTER TABLE \`${tableName}\` ALTER COLUMN ${updateQueries.join(", ALTER COLUMN ")};`;
      console.debug({ sqlQuery });
      try {
        await this.clickhouseClient.query({
          query: sqlQuery,
        });
      } catch (err) {
        throw err;
      }
    }

    // Check corner-case of existing column of DateTime in a different format (DateTime64(6) -> DateTime):
    Object.keys(requestedSchema).map((column) => {
      if (
        currentSchema[column] &&
        currentSchema[column].type === ColumnType.DATE &&
        requestedSchema[column].type === ColumnType.DATE64
      ) {
        requestedSchema[column].type = currentSchema[column].type;
      }
    });
    return requestedSchema;
  }

  private async createTable({
    tableName,
    requestedSchema,
  }: {
    tableName: TableName;
    requestedSchema: ClickhouseTableSchema;
  }): Promise<void> {
    if (Object.keys(requestedSchema).length > 0) {
      const columnsToCreate = this.getClickhouseColumnsSql(requestedSchema);
      const sqlQuery = `CREATE TABLE \`${tableName}\` (
          ${columnsToCreate.join(`,
          `)} 
         ) 
         ENGINE = MergeTree() 
         ORDER BY ${SENT_AT_TS_COLUMN_NAME};`;
      console.debug({ sqlQuery });
      await this.clickhouseClient.query({
        query: sqlQuery,
      });
    }
    return;
  }

  private async doesTableExist(tableName: string): Promise<boolean> {
    try {
      await this.clickhouseClient.exec({
        query: `SELECT * FROM ${tableName} LIMIT 1`,
      });
      return true;
    } catch {
      return false;
    }
  }
}

export default ClickhouseBatchClient;

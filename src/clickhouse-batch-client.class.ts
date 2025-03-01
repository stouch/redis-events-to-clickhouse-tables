import { ClickHouseClient } from "@clickhouse/client";
import { dayjs } from "./dayjs-utc.js";
import snakeCase from "lodash.snakecase";
import {
  CLICKHOUSE_NEW_COL_NULLABLE,
  EVENT_TYPE_PROPERTY,
  EventToInjest,
} from "./main.js";
import { randomUUID } from "crypto";

const TS_COLUMN_NAME = "timestamp";
const MID_COLUMN_NAME = "message_id";

export type EventToInjestInTable = Omit<
  EventToInjest,
  typeof EVENT_TYPE_PROPERTY
>;

enum ColumnType {
  DATE = "DateTime",
  DATE64 = "DateTime64(6)",
  STRING = "String",
  INTEGER = "UInt64",
  FLOAT = "Float64",
  BOOLEAN = "UInt8",
}

type ClickhouseColumnDefinition =
  | { type: ColumnType.BOOLEAN; default?: boolean; nullable?: true }
  | { type: ColumnType.INTEGER; default?: number; nullable?: true }
  | {
      type: Exclude<ColumnType, ColumnType.BOOLEAN | ColumnType.INTEGER>; // String, Date, ...
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
  constructor(private readonly clickhouseClient: ClickHouseClient) {}

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
    const preparedRows = this.prepareRows(rows);
    const rowsToInjectSchema = this.getRowsMinimumSchema(preparedRows);
    if (Object.keys(rowsToInjectSchema).length === 0) {
      throw new Error("errors.no_columns_found");
    }
    const tableExists = await this.doesTableExist(tableName);
    if (tableExists) {
      const existingSchema = await this.getClickhouseTableSchema(tableName);
      await this.addMissingColumns({
        tableName,
        currentSchema: existingSchema,
        requestedSchema: rowsToInjectSchema,
      });
    } else {
      await this.createTable({
        tableName,
        requestedSchema: rowsToInjectSchema,
      });
    }
  }

  async insertRows({
    tableName,
    rows,
  }: {
    tableName: TableName;
    rows: EventToInjest[];
  }) {
    const preparedRows = this.prepareRows(rows);
    const minimumRowColumns = this.getColsMinimumList(preparedRows);
    const rowsQueries = this.getClickhouseRowsSql({
      rowsToInjest: preparedRows,
      columns: minimumRowColumns,
    });
    const sqlQuery = `INSERT INTO ${tableName} 
      (${minimumRowColumns.join(",")}) VALUES 
        (${rowsQueries.join(`),
        (`)});`;
    try {
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
      await this.insertRows({ tableName, rows });
    } catch (err) {
      throw err;
    }
  }

  // --------------------
  // --------------------
  // -- Helper methods --
  // --------------------
  // --------------------

  // Ensure we gonna use column names in snake_case, and that we aint going to persist "event_type" (${EVENT_TYPE_PROPERTY}) from the redis bull event job.
  private prepareRows(rows: EventToInjest[]): EventToInjestInTable[] {
    return rows.map((row) => {
      const rowWithoutEvenType: EventToInjestInTable = {};
      for (const eventKey in row) {
        if (
          eventKey === EVENT_TYPE_PROPERTY ||
          eventKey === "__process_single"
        ) {
          continue;
        }
        rowWithoutEvenType[snakeCase(eventKey)] = row[eventKey];
      }
      return rowWithoutEvenType;
    });
  }

  private getClickhouseColumnsSql(columnsToAdd: ClickhouseTableSchema) {
    // Build the SQL for CREATE TABLE of colums, or ALTER TABLE columns:
    const addColumnQueries = Object.keys(columnsToAdd).map(
      (columnName: ColumnName) => {
        const column = columnsToAdd[columnName];
        const type = column.nullable ? `Nullable(${column.type})` : column.type;
        const defaultValue = column.default;
        // ex: [..., `age` UInt64 DEFAULT 18, ...]
        return `${columnName} ${type} ${
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
    rowsToInjest,
    columns,
  }: {
    rowsToInjest: EventToInjestInTable[];
    columns: string[];
  }) {
    const addRowsQueries = rowsToInjest.map((row: EventToInjestInTable) => {
      let rowSql: string[] = [];
      for (const column of columns) {
        // Among the columns, we got the required columns (timestamp, message_id, ...)
        if (column === TS_COLUMN_NAME) {
          rowSql.push(`'${dayjs().unix()}'`);
          continue;
        } else if (column === MID_COLUMN_NAME) {
          rowSql.push(`'${randomUUID()}'`);
          continue;
        }

        const columnContent = row[column];
        if (columnContent === undefined) {
          rowSql.push("NULL");
          continue;
        }
        rowSql.push(
          `${
            columnContent instanceof Date
              ? dayjs(columnContent).unix()
              : typeof columnContent === "string" &&
                  dayjs(columnContent).isValid()
                ? dayjs(columnContent).unix()
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
      return rowSql.join(","); // (1, 'Alice', 25, '2024-02-27 10:00:00'),
    });
    return addRowsQueries; // [ (1, 'Alice', 25, '2024-02-27 10:00:00'), (2, 'Bob', 30, '2024-02-26 15:30:00') ]
  }

  // ----------------------------------
  // ----------------------------------
  // -- Methods that compare schemas --
  // ----------------------------------
  // ----------------------------------

  // Get the columns of a set of rows.
  // We need to crawl all the rows to find all the columns because some of rows might not have all the columns set.
  // And we prefix the minimum column list with the two required columns (timestamp, message_id,...)
  private getColsMinimumList(rows: EventToInjestInTable[]) {
    return [
      `${TS_COLUMN_NAME}`,
      `${MID_COLUMN_NAME}`,
      ...new Set(rows.map((row) => Object.keys(row)).flat()),
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
      if (property === TS_COLUMN_NAME) {
        requestedSchema[property] = { type: ColumnType.DATE64 };
      } else if (property === MID_COLUMN_NAME) {
        requestedSchema[property] = { type: ColumnType.STRING };
      } else if (typeof propertyValue === "string") {
        if (dayjs(propertyValue).isValid()) {
          requestedSchema[property] = { type: ColumnType.DATE };
        } else {
          requestedSchema[property] = { type: ColumnType.STRING };
        }
      } else if (typeof propertyValue === "number") {
        requestedSchema[property] = { type: ColumnType.INTEGER };
      } else if (propertyValue instanceof Date) {
        requestedSchema[property] = { type: ColumnType.DATE };
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
      ).json<{ name: string; type: ColumnType; default_expression: string }>()
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
      mappedSchema[column.name] = { type: column.type };
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
  }): Promise<void> {
    const missingColumns: Record<ColumnName, ClickhouseColumnDefinition> = {};

    for (const requestedColumn in requestedSchema) {
      if (currentSchema[requestedColumn]) {
        // Column exists!
        // We 'd suppsoed to check the value of the column type ?
        // TODO: ?
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
    return;
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
         ORDER BY ${TS_COLUMN_NAME};`;
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

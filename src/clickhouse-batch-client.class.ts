import { ClickHouseClient } from "@clickhouse/client";
import { EventToInjest } from "./bulker.class.js";

enum RequestedColumnType {
  DATE = "Date",
  STRING = "String",
  INTEGER = "Integer",
  FLOAT = "Float",
  BOOLEAN = "Boolean",
}

type ClickhouseColumnDefinition =
  | { type: RequestedColumnType.BOOLEAN; default?: boolean }
  | { type: RequestedColumnType.INTEGER; default?: number }
  | {
      type: Exclude<
        RequestedColumnType,
        RequestedColumnType.BOOLEAN | RequestedColumnType.INTEGER
      >;
      default?: string;
    };

type TableName = string;
type ColumnName = string;
type ClickhouseSchema = Record<ColumnName, ClickhouseColumnDefinition>;

const CLICKHOUSE_SCHEMA_MAPPING = {
  [RequestedColumnType.BOOLEAN]: "UInt8",
  [RequestedColumnType.STRING]: "String",
  [RequestedColumnType.DATE]: "DateTime",
  [RequestedColumnType.FLOAT]: "Float64",
  [RequestedColumnType.INTEGER]: "UInt64",
} as const;

class ClickhouseBatchClient {
  constructor(private readonly clickhouseClient: ClickHouseClient) {}

  async prepareClickhouseTable({
    tableName,
    rows,
  }: {
    tableName: TableName;
    rows: EventToInjest[];
  }): Promise<void> {
    if (rows.length === 0) {
      throw new Error("errors.no_rows_to_insert");
    }
    const rowsToInjectSchema = this.getRowsSchema(/*rows[0]*/);
    if (Object.keys(rowsToInjectSchema).length === 0) {
      throw new Error("errors.no_columns_found");
    }
    const tableExists = await this.doesTableExist(tableName);
    if (tableExists) {
      const existingSchema = await this.getClickhouseTableSchema(/*tableName*/);

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

  async insert({
    tableName,
    rows,
  }: {
    tableName: TableName;
    rows: EventToInjest[];
  }) {
    const rowsQueries = this.getClickhouseRowsSql(rows);
    const firstRowColumns = Object.keys(rows[0]);
    const sqlQuery = `INSERT INTO ${tableName} (${firstRowColumns.join(",")}) VALUES (${rowsQueries.join("), (")});`;
    console.debug({ sqlQuery });
    await this.clickhouseClient.exec({
      query: sqlQuery,
    });
  }

  // -------------------
  // -------------------
  // Helper methods
  // -------------------
  // -------------------

  private getClickhouseColumnsSql(columnsToAdd: ClickhouseSchema) {
    const addColumnQueries = Object.keys(columnsToAdd).map(
      (columnName: ColumnName) => {
        const column = columnsToAdd[columnName];
        const type = column.type;
        const defaultValue = column.default;
        return `${columnName} ${CLICKHOUSE_SCHEMA_MAPPING[type]} ${
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

  private getClickhouseRowsSql(rowsToInjest: EventToInjest[]) {
    const addRowsQueries = rowsToInjest.map((row: EventToInjest) => {
      const columns = Object.keys(row);
      let rowSql: string[] = [];
      for (const column of columns) {
        const columnContent = row[column];
        rowSql.push(
          `${typeof columnContent === "number" ? columnContent : typeof columnContent === "string" ? `'${columnContent.replace(/'/g, "\\'")}'` : columnContent === true ? "1" : "0"}`
        );
      }
      return rowSql.join(","); // (1, 'Alice', 25, '2024-02-27 10:00:00'),
    });
    return addRowsQueries; // [ (1, 'Alice', 25, '2024-02-27 10:00:00'), (2, 'Bob', 30, '2024-02-26 15:30:00') ]
  }

  private async addMissingColumns({
    tableName,
    currentSchema,
    requestedSchema,
  }: {
    tableName: TableName;
    currentSchema: ClickhouseSchema;
    requestedSchema: ClickhouseSchema;
  }): Promise<void> {
    const missingColumns: Record<ColumnName, ClickhouseColumnDefinition> = {};
    for (const requestedColumn in requestedSchema) {
      if (currentSchema[requestedColumn]) {
        // Column exists!
        // We 'd suppsoed to check the value of the column type !
        continue;
      } else {
        missingColumns[requestedColumn] = requestedSchema[requestedColumn];
      }
    }

    if (Object.keys(missingColumns).length > 0) {
      const addQueries = this.getClickhouseColumnsSql(missingColumns);
      const sqlQuery = `ALTER TABLE \`${tableName}\` ADD COLUMN ${addQueries.join(", ADD COLUMN ")};`;
      console.debug({ sqlQuery });
      await this.clickhouseClient.query({
        query: sqlQuery,
      });
    }
    return;
  }

  private async createTable({
    tableName,
    requestedSchema,
  }: {
    tableName: TableName;
    requestedSchema: ClickhouseSchema;
  }): Promise<void> {
    if (Object.keys(requestedSchema).length > 0) {
      const columnsToCreate = this.getClickhouseColumnsSql(requestedSchema);
      const sqlQuery = `CREATE TABLE \`${tableName}\` ( 
          timestamp DateTime64(6),
          ${columnsToCreate.join(",\n")} 
         ) 
         ENGINE = MergeTree() 
         ORDER BY timestamp;`;
      console.debug({ sqlQuery });
      await this.clickhouseClient.query({
        query: sqlQuery,
      });
    }
    return;
  }

  private getRowsSchema(/*firstRow: EventToInjest*/): ClickhouseSchema {
    // TODO
    const requestedSchema: ClickhouseSchema = {
      toto: { type: RequestedColumnType.BOOLEAN },
      zozo: { type: RequestedColumnType.STRING, default: "allo" },
    };
    return requestedSchema;
  }

  private async getClickhouseTableSchema() //tableName: TableName
  : Promise<ClickhouseSchema> {
    // TODO:
    /*const currentSchema = await this.clickhouseClient.query({
      query: `DESCRIBE ${tableName}`,
    });*/
    const mappedSchema: ClickhouseSchema = {
      toto: { type: RequestedColumnType.BOOLEAN },
      zozo: { type: RequestedColumnType.STRING, default: "allo" },
    };
    return mappedSchema;
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

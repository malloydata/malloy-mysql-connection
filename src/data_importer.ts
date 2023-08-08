/* eslint-disable no-console */
/*import {DuckDBConnection} from '@malloydata/db-duckdb';
import {MySqlConnection} from './connection/mysql_connection';
import {QueryValue} from '@malloydata/malloy/dist/model';
import {DateTime} from 'luxon';
export class DataImporter {
  async importData(): Promise<void> {

    const sourceConnection = new DuckDBConnection(
      'duckdb',
      'data/duckdb_test.db',
      undefined,
      {rowLimit: 100000000}
    );
    const targetConnection = new MySqlConnection({
      host: '127.0.0.1',
      port: 3306,
      user: 'root',
      password: 'Malloydev123',
      database: 'malloytest',
    });

    const tables = [
      'malloytest.aircraft',
      'malloytest.aircraft_models',
      'malloytest.airports',
      'malloytest.alltypes',
      'malloytest.flights',
      'malloytest.state_facts',
    ];

    for (const table of tables) {
      await this.importDataFrom(
        sourceConnection,
        targetConnection,
        table,
        true
      );
    }
  }

  async importDataFrom(
    sourceConnection: DuckDBConnection,
    targetConnection: MySqlConnection,
    table: string,
    dryRun?: boolean
  ) {
    const rowCountResult = await targetConnection.runSQL(
      `SELECT count(*) as row_count FROM ${table};`
    );

    if ((rowCountResult.rows[0]['row_count'] as number) > 0) {
      console.log(`Table ${table} already has some data`);
      return;
    }
    console.log(`Reading data from: ${table} from db ${sourceConnection.name}`);

    let rowCount = 0;

    let inserts: string[] = [];
    for await (const row of sourceConnection.runSQLStream(
      `SELECT * FROM ${table}`
    )) {
      const sql = `INSERT INTO ${table} (${Object.keys(row)
        .map(col => `\`${col}\``)
        .join(',')}) VALUES (${Object.keys(row)
        .map(col => row[col])
        .map(value => this.formatValue(value))
        .join(',')});`;

      inserts.push(sql);
      if (dryRun) {
        if (inserts.length === 500) {
          await targetConnection.runSQL(inserts.join('\n'));
          console.log(`Flushing: ${rowCount}`);
          inserts = [];
        }
      } else {
        console.log(`${sql}`);
      }

      ++rowCount;
      // console.log(`Writing row: ${rowCount}`);
    }

    if (inserts.length > 0) {
      await targetConnection.runSQL(inserts.join('\n'));
    }

    console.log(`----> ${rowCount} rows written.`);
  }

  formatValue(queryValue: QueryValue): string {
    if (queryValue === null) {
      return 'NULL';
    } else if (typeof queryValue === 'string') {
      return `'${queryValue.replace(/'/g, "''")}'`;
    } else if (queryValue instanceof Date) {
      let dt = DateTime.fromJSDate(queryValue, {zone: 'UTC'});
      if (dt.year < 1971) {
        dt = dt.plus({years: 1971 - dt.year});
      }
      return `'${dt.toFormat('yyyy-MM-dd HH:mm:ss')}'`;
    } else if (
      typeof queryValue === 'number' ||
      typeof queryValue === 'boolean'
    ) {
      return `${queryValue}`;
    } else if (Array.isArray(queryValue)) {
      return `JSON_ARRAY(${queryValue
        .map(v => this.formatValue(v))
        .join(',')})`;
    }

    throw new Error(
      `Unsupported type: ${JSON.stringify(queryValue)} ${typeof queryValue}`
    );
  }
}*/

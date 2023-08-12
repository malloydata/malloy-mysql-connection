import {
  AtomicFieldTypeInner,
  Connection,
  FieldTypeDef,
  MalloyQueryData,
  NamedStructDefs,
  PersistSQLResults,
  PooledConnection,
  QueryDataRow,
  DialectProvider,
  QueryRunStats,
  RunSQLOptions,
  SQLBlock,
  StreamingConnection,
  StructDef,
  TestableConnection,
} from '@malloydata/malloy';
import {randomUUID} from 'crypto';
import {
  FieldInfo,
  createConnection,
  Connection as mySqlConnection,
} from 'mysql';
import {MySqlDialect} from '../dialect/mysql_dialect';
import {DateTime} from 'luxon';
import {MySqlConnectionConfiguration} from './mysql_connection_configuration';
import {decode} from 'fastestsmallesttextencoderdecoder';
import 'reflect-metadata';

const mySqlToMalloyTypes: {[key: string]: AtomicFieldTypeInner} = {
  // TODO: This assumes tinyint is always going to be a boolean.
  'tinyint': 'boolean',
  'smallint': 'number',
  'mediumint': 'number',
  'int': 'number',
  'bigint': 'number',
  'tinyint unsigned': 'number',
  'smallint unsigned': 'number',
  'mediumint unsigned': 'number',
  'int unsigned': 'number',
  'bigint unsigned': 'number',
  'double': 'number',
  'varchar': 'string',
  'varbinary': 'string',
  'char': 'string',
  'text': 'string',
  'date': 'date',
  'datetime': 'timestamp',
  'timestamp': 'timestamp',
  'time': 'string',
  'decimal': 'number',
  // TODO: Check if we need special handling for boolean.
  'tinyint(1)': 'boolean',
};

export class MySqlConnection
  extends DialectProvider
  implements Connection, TestableConnection
{
  private schemaCache = new Map<
    string,
    {schema: StructDef; error?: undefined} | {error: string; schema?: undefined}
  >();

  private sqlSchemaCache = new Map<
    string,
    | {structDef: StructDef; error?: undefined}
    | {error: string; structDef?: undefined}
  >();

  get dialectName(): string {
    return 'mysql';
  }

  readonly connection: mySqlConnection;

  constructor(configuration: MySqlConnectionConfiguration) {
    super(new MySqlDialect());
    // TODO: handle when connection fails.
    this.connection = createConnection({
      host: configuration.host,
      port: configuration.port ?? 3306,
      user: configuration.user,
      password: configuration.password,
      // TODO: Figure out how to allow users not to provide database.
      database: configuration.database,
      multipleStatements: true,
    });
  }

  public async test(): Promise<void> {
    await this.runRawSQL('SELECT 1');
  }

  runSQL(sql: string, _options?: RunSQLOptions): Promise<MalloyQueryData> {
    // TODO: what are options here?
    return this.runRawSQL(sql);
  }

  isPool(): this is PooledConnection {
    return false;
  }

  canPersist(): this is PersistSQLResults {
    // TODO: implement;
    throw new Error('Method not implemented.1');
  }

  canStream(): this is StreamingConnection {
    // TODO: implement;
    throw new Error('Method not implemented.2');
  }

  async close(): Promise<void> {
    return this.connection.end();
  }

  estimateQueryCost(_sqlCommand: string): Promise<QueryRunStats> {
    // TODO: implement;
    throw new Error('Method not implemented.3');
  }

  async fetchSchemaForTables(tables: Record<string, string>): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: {[name: string]: string} = {};

    for (const tableKey in tables) {
      let inCache = this.schemaCache.get(tableKey);
      if (!inCache) {
        const tablePath = tables[tableKey];
        try {
          inCache = {
            schema: await this.getTableSchema(tableKey, tablePath),
          };
          this.schemaCache.set(tableKey, inCache);
        } catch (error) {
          inCache = {error: (error as Error).message};
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableKey] = inCache.schema;
      } else {
        errors[tableKey] = inCache.error || 'Unknown schema fetch error';
      }
    }
    return {schemas, errors};
  }

  public async fetchSchemaForSQLBlock(
    sqlRef: SQLBlock
  ): Promise<
    | {structDef: StructDef; error?: undefined}
    | {error: string; structDef?: undefined}
  > {
    const key = sqlRef.name;
    let inCache = this.sqlSchemaCache.get(key);
    if (!inCache) {
      try {
        inCache = {
          structDef: await this.getSQLBlockSchema(sqlRef),
        };
      } catch (error) {
        inCache = {error: (error as Error).message};
      }
      this.sqlSchemaCache.set(key, inCache);
    }
    return inCache;
  }

  get name(): string {
    return 'mysql';
  }

  // TODO: make sure this is exercised.
  async getTableSchema(tableName: string, tablePath: string) {
    const structDef: StructDef = {
      type: 'struct',
      name: tableName,
      // TODO: Should this be an enum or similar?
      dialect: this.dialectName,
      structSource: {type: 'table', tablePath},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const quotedTablePath = tablePath.match(/[:*/]/)
      ? `\`${tablePath}\``
      : tablePath;
    const infoQuery = `DESCRIBE ${quotedTablePath}`;
    await this.schemaFromQuery(infoQuery, structDef);
    return structDef;
  }

  private async getSQLBlockSchema(sqlRef: SQLBlock): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      dialect: this.dialectName,
      name: sqlRef.name,
      structSource: {
        type: 'sql',
        method: 'subquery',
        sqlBlock: sqlRef,
      },
      structRelationship: {
        // TODO: check what is this.
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    await this.schemaFromQuery(
      `DROP TABLE IF EXISTS ${tempTableName};
       CREATE TEMPORARY TABLE ${tempTableName} AS (${sqlRef.selectStr});
       DESCRIBE ${tempTableName};
       DROP TABLE IF EXISTS ${tempTableName};`,
      structDef
    );
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string,
    structDef: StructDef
  ): Promise<void> {
    const typeMap: {[key: string]: string} = {};

    const result = await this.runRawSQL(infoQuery);
    for (const row of result.rows) {
      typeMap[row['Field'] as string] = row['Type'] as string;
    }
    this.fillStructDefFromTypeMap(structDef, typeMap);
  }

  async runRawSQL(
    sql: string,
    _options?: RunSQLOptions
  ): Promise<MalloyQueryData> {
    // TODO: what are options here?
    return new Promise((resolve, reject) =>
      // TODO: Remove hack.
      this.connection.query(
        `set @@session.time_zone = 'UTC'; \n ${sql}`,
        (error, result, fields) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          let resultSets: Array<Array<Record<string, any>> | null> = [];
          let fieldsSet: Array<Array<FieldInfo> | null> = [];
          /*if (fields instanceof Array<FieldInfo>) {
          resultSets = [result];
          fieldsSet = [fields];
        } else {*/
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          resultSets = result as Array<Array<Record<string, any>> | null>;
          fieldsSet = fields as unknown as Array<Array<FieldInfo> | null>;
          //}

          if (error) {
            // TODO: how to parse this error?
            return reject(
              new Error(
                `Failed to execute MySQL query: ${error} \n For Query: ${sql}`
              )
            );
          }

          // TODO: use proper type instead of any.
          const rows: QueryDataRow[] = [];
          for (const resultSetIndex in resultSets) {
            const resultSet = resultSets[resultSetIndex];
            if (Array.isArray(resultSet)) {
              for (const entry of resultSet) {
                const dataRow: QueryDataRow = {};
                // TODO: report fix to types.
                for (const field of fieldsSet![resultSetIndex]!) {
                  // TODO: proper parsing here. also recursive (nest); MEGA HACK
                  if (field.type === 245) {
                    // TODO: this is needed due to limitations with JSON array/object manipulation in mysql.
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    dataRow[field.name] = MySqlConnection.removeNulls(
                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                      JSON.parse(entry[field.name]) as Record<string, any>
                    );
                  } else {
                    if (entry[field.name] instanceof Date) {
                      const abc = entry[field.name] as Date;
                      const date = DateTime.fromJSDate(abc);
                      dataRow[field.name] = date
                        .plus({minutes: date.offset})
                        .toUTC()
                        .toJSDate();
                    } else if (
                      entry[field.name] instanceof Uint8Array ||
                      entry[field.name] instanceof Uint16Array ||
                      entry[field.name] instanceof Uint32Array
                    ) {
                      dataRow[field.name] = decode(
                        entry[field.name] as Uint32Array
                      );
                    } else {
                      dataRow[field.name] = entry[field.name];
                    }
                  }
                }
                rows.push(dataRow);
              }
            }
          }

          // TODO: Parse result.
          resolve({rows: rows, totalRows: rows.length});
        }
      )
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static removeNulls(jsonObj: any): any {
    if (Array.isArray(jsonObj)) {
      return MySqlConnection.removeNullsArray(jsonObj);
    }
    return MySqlConnection.removeNullsObject(jsonObj);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static removeNullsObject(jsonObj: Record<string, any>): Record<string, any> {
    for (const key in jsonObj) {
      if (Array.isArray(jsonObj[key])) {
        jsonObj[key] = MySqlConnection.removeNullsArray(jsonObj[key]);
      } else if (typeof jsonObj === 'object') {
        jsonObj[key] = MySqlConnection.removeNullsObject(jsonObj[key]);
      }
    }
    return jsonObj;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static removeNullsArray(jsonArray: any[]): any[] {
    const metadata = jsonArray
      .filter(MySqlConnection.checkIsMalloyMetadata)
      .shift();
    if (!metadata) {
      return jsonArray;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const filteredArray = jsonArray
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .filter(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (element: any) =>
          element !== null && !MySqlConnection.checkIsMalloyMetadata(element)
      )
      .map(MySqlConnection.removeNulls);

    if (metadata['limit']) {
      return filteredArray.slice(0, metadata['limit']);
    }

    return filteredArray;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static checkIsMalloyMetadata(jsonObj: any) {
    return (
      jsonObj !== null &&
      typeof jsonObj === 'object' &&
      jsonObj['_is_malloy_metadata']
    );
  }
  private fillStructDefFromTypeMap(
    structDef: StructDef,
    typeMap: {[name: string]: string}
  ) {
    // TODO: handle mysql types properly.
    for (const fieldName in typeMap) {
      let mySqlType = typeMap[fieldName].toLocaleLowerCase();
      mySqlType = mySqlType.trim().split('(')[0];
      let malloyType = mySqlToMalloyTypes[mySqlType];
      const arrayMatch = mySqlType.startsWith('json');
      if (arrayMatch) {
        // TODO: Is not having inner type a problem?
        const innerStructDef: StructDef = {
          type: 'struct',
          name: fieldName,
          dialect: this.dialectName,
          structSource: {type: 'nested'},
          structRelationship: {
            type: 'nested',
            field: fieldName,
            isArray: false,
          },
          // TODO: this makes the tests pass but is weak.
          fields: [
            {type: mySqlToMalloyTypes['text'], name: 'value'} as FieldTypeDef,
          ],
        };
        structDef.fields.push(innerStructDef);
      } else {
        if (arrayMatch) {
          malloyType = mySqlToMalloyTypes[mySqlType];
          const innerStructDef: StructDef = {
            type: 'struct',
            name: fieldName,
            dialect: this.dialectName,
            structSource: {type: 'nested'},
            structRelationship: {
              type: 'nested',
              field: fieldName,
              isArray: true,
            },
            fields: [{type: malloyType, name: 'value'} as FieldTypeDef],
          };
          structDef.fields.push(innerStructDef);
        } else {
          if (malloyType) {
            structDef.fields.push({type: malloyType, name: fieldName});
          } else {
            structDef.fields.push({
              type: 'unsupported',
              rawType: mySqlType.toLowerCase(),
              name: fieldName,
            });
          }
        }
      }
    }
  }
}

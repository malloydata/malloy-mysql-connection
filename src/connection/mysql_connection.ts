import {
  AtomicFieldTypeInner,
  Connection,
  DialectProviderConnection,
  FieldTypeDef,
  MalloyQueryData,
  NamedStructDefs,
  PersistSQLResults,
  PooledConnection,
  QueryDataRow,
  QueryRunStats,
  RunSQLOptions,
  SQLBlock,
  StreamingConnection,
  StructDef,
} from '@malloydata/malloy';
import {Dialect} from '@malloydata/malloy/dist/dialect';
import {randomUUID} from 'crypto';
import {
  FieldInfo,
  createConnection,
  Connection as mySqlConnection,
} from 'mysql';
import {MySqlDialect} from '../dialect/mysql_dialect';

const mySqlToMalloyTypes: {[key: string]: AtomicFieldTypeInner} = {
  BIGINT: 'number',
  int: 'number',
  TINYINT: 'number',
  SMALLINT: 'number',
  UBIGINT: 'number',
  UINTEGER: 'number',
  UTINYINT: 'number',
  USMALLINT: 'number',
  HUGEINT: 'number',
  DOUBLE: 'number',
  varchar: 'string',
  DATE: 'date',
  TIMESTAMP: 'timestamp',
  TIME: 'string',
  decimal: 'number',
  BOOLEAN: 'boolean',
};

export class MySqlConnection implements Connection, DialectProviderConnection {
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

  constructor() {
    // TODO: handle when connection fails.
    // TODO: Pass parameters in constructor parameter.
    this.connection = createConnection({
      host: '127.0.0.1',
      port: 3306,
      user: 'root',
      password: 'Malloydev123',
      database: 'appointments',
      multipleStatements: true,
    });
  }
  dialect(): Dialect {
    return new MySqlDialect();
  }
  providesDialect(): this is DialectProviderConnection {
    return true;
  }

  runSQL(sql: string, _options?: RunSQLOptions): Promise<MalloyQueryData> {
    // TODO: what are options here?
    return this.runRawSQL(sql);
  }

  isPool(): this is PooledConnection {
    return false;
  }

  canPersist(): this is PersistSQLResults {
    throw new Error('Method not implemented.1');
  }

  canStream(): this is StreamingConnection {
    throw new Error('Method not implemented.2');
  }

  async close(): Promise<void> {
    return this.connection.end();
  }

  estimateQueryCost(_sqlCommand: string): Promise<QueryRunStats> {
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

  /*
   INSERT INTO Persons (AppointmentId, City, Name, Cost, Vaccines, Month) VALUES (1, 'San Diego', 'Sebastian', 250.50, 2, 1);
   */

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
    //console.log(`SQL ---> \n ${sql}`);
    // TODO: what are options here?
    return new Promise((resolve, reject) =>
      // TODO: Remove hack.
      this.connection.query(
        `DROP TABLE IF EXISTS tmp11111; ${sql}`,
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
            return reject(error);
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

                    console.log(
                      `---------> AFTER ${JSON.stringify(dataRow[field.name])}`
                    );
                  } else {
                    dataRow[field.name] = entry[field.name];
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

  // TODO: is this needed?
  private stringToTypeMap(s: string): {[name: string]: string} {
    const ret: {[name: string]: string} = {};
    const columns = this.splitColumns(s);
    for (const c of columns) {
      //const [name, type] = c.split(" ", 1);
      const columnMatch = c.match(/^(?<name>[^\s]+) (?<type>.*)$/);
      if (columnMatch && columnMatch.groups) {
        ret[columnMatch.groups['name']] = columnMatch.groups['type'];
      } else {
        throw new Error(`Badly form Structure definition ${s}`);
      }
    }
    return ret;
  }

  // TODO: is this needed?
  private splitColumns(s: string) {
    const columns: string[] = [];
    let parens = 0;
    let column = '';
    let eatSpaces = true;
    for (let idx = 0; idx < s.length; idx++) {
      const c = s.charAt(idx);
      if (eatSpaces && c === ' ') {
        // Eat space
      } else {
        eatSpaces = false;
        if (!parens && c === ',') {
          columns.push(column);
          column = '';
          eatSpaces = true;
        } else {
          column += c;
        }
        if (c === '(') {
          parens += 1;
        } else if (c === ')') {
          parens -= 1;
        }
      }
    }
    columns.push(column);
    return columns;
  }

  private fillStructDefFromTypeMap(
    structDef: StructDef,
    typeMap: {[name: string]: string}
  ) {
    // TODO: handle mysql types properly.
    for (const fieldName in typeMap) {
      // TODO: replace duckdb.
      let mySqlType = typeMap[fieldName].toLocaleLowerCase();
      // Remove varchar(255) size to simplify lookup
      mySqlType = mySqlType.replace(/^varchar\(\d+\)/g, 'varchar');
      // Remove decimal(10,0) dimensions to simplify lookup
      mySqlType = mySqlType.replace(/^decimal\(\d+,\d+\)/g, 'decimal');
      let malloyType = mySqlToMalloyTypes[mySqlType];
      const arrayMatch = mySqlType.match(/(?<duckDBType>.*)\[\]$/);
      if (arrayMatch && arrayMatch.groups) {
        mySqlType = arrayMatch.groups['duckDBType'];
      }
      const structMatch = mySqlType.match(/^STRUCT\((?<fields>.*)\)$/);
      if (structMatch && structMatch.groups) {
        const newTypeMap = this.stringToTypeMap(structMatch.groups['fields']);
        const innerStructDef: StructDef = {
          type: 'struct',
          name: fieldName,
          dialect: this.dialectName,
          structSource: {type: arrayMatch ? 'nested' : 'inline'},
          structRelationship: {
            type: arrayMatch ? 'nested' : 'inline',
            field: fieldName,
            isArray: false,
          },
          fields: [],
        };
        this.fillStructDefFromTypeMap(innerStructDef, newTypeMap);
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

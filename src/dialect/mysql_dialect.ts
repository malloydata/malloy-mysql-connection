/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

//import {Dialect, DialectFieldList, QueryInfo, qtz} from '../dialect';
import {
  DateUnit,
  Expr,
  ExtractUnit,
  TimeFieldType,
  TimeValue,
  TimestampUnit,
} from '@malloydata/malloy';
import {
  Sampling,
  TypecastFragment,
  isSamplingEnable,
  isSamplingPercent,
  mkExpr,
  isSamplingRows,
} from '@malloydata/malloy/dist/model';
// TODO: May need to expose these through index (not dist).
import {indent} from '@malloydata/malloy/dist/model/utils';
import {DialectFunctionOverloadDef} from '@malloydata/malloy/dist/dialect/functions';
import {Dialect, DialectFieldList} from '@malloydata/malloy/dist/dialect';
import {QueryInfo, qtz} from '@malloydata/malloy/dist/dialect/dialect';
import {MYSQL_FUNCTIONS} from './functions';

const castMap: Record<string, string> = {
  number: 'double precision',
  string: 'varchar(255)',
};

const msExtractionMap: Record<string, string> = {
  day_of_week: 'DAYOFWEEK',
  day_of_year: 'DAYOFYEAR',
};

const inSeconds: Record<string, number> = {
  second: 1,
  minute: 60,
  hour: 3600,
  day: 24 * 3600,
  week: 7 * 24 * 3600,
};

export class MySqlDialect extends Dialect {
  name = 'mysql';
  defaultNumberType = 'DOUBLE PRECISION';
  defaultDecimalType = 'DECIMAL';
  udfPrefix = 'ms_temp.__udf';
  hasFinalStage = false;
  // TODO: this may not be enough for lager casts.
  stringTypeName = 'VARCHAR(255)';
  divisionIsInteger = true;
  supportsSumDistinctFunction = false;
  unnestWithNumbers = false;
  defaultSampling = {rows: 50000};
  supportUnnestArrayAgg = true;
  supportsAggDistinct = true;
  supportsCTEinCoorelatedSubQueries = true;
  supportsSafeCast = false;
  dontUnionIndex = false;
  supportsQualify = false;
  globalFunctions = MYSQL_FUNCTIONS;
  supportsNesting = false;

  quoteTablePath(tablePath: string): string {
    return tablePath
      .split('.')
      .map(part => `\`${part}\``)
      .join('.');
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (select number - 1 as group_set from JSON_TABLE(cast(concat("[1", repeat(",1", ${groupSetCount}), "]") as JSON),"$[*]" COLUMNS(number FOR ORDINALITY)) group_set) as group_set`;
  }

  sqlAnyValue(_groupSet: number, fieldName: string): string {
    return `MAX(${fieldName})`;
  }

  mapFields(fieldList: DialectFieldList): string {
    return fieldList
      .map(f => `\n  ${f.sqlExpression} as ${f.sqlOutputName}`)
      .join(', ');
  }

  sqlAggregateTurtle(
    _groupSet: number,
    _fieldList: DialectFieldList,
    _orderBy: string | undefined,
    _limit: number | undefined
  ): string {
    throw new Error('MySql dialect does not support nesting.');
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = this.mapFieldsForJsonObject(fieldList);
    return `MAX(CASE WHEN group_set=${groupSet} THEN JSON_OBJECT(${fields}) END)`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `MAX(CASE WHEN group_set=${groupSet} AND ${name} IS NOT NULL THEN ${name} END) as ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    groupSet: number,
    fieldList: DialectFieldList
  ): string {
    const fields = this.mapFieldsForJsonObject(fieldList);
    const nullValues = this.mapFieldsForJsonObject(fieldList, true);

    return `COALESCE(MAX(CASE WHEN group_set=${groupSet} THEN JSON_OBJECT(${fields}) END),JSON_OBJECT(${nullValues}))`;
  }

  // TODO: investigate if its possible to make it work when source is table.field.
  sqlUnnestAlias(
    _source: string,
    _alias: string,
    _fieldList: DialectFieldList,
    _needDistinctKey: boolean,
    _isArray: boolean,
    _isInNestedPipeline: boolean
  ): string {
    throw new Error('MySql dialect does not support unnest.');
    /* if (isArray) {
      throw new Error('MySql dialect does not support unnest.');
    } else if (needDistinctKey) {
      return `LEFT JOIN JSON_TABLE(cast(concat("[1",repeat(",1",JSON_LENGTH(${source})),"]") as JSON),"$[*]" COLUMNS(__row_id FOR ORDINALITY)) as ${alias} ON ${alias}.\`__row_id\` <= JSON_LENGTH(${source})`;
    } else {
      return `LEFT JOIN (SELECT json_unquote(json_extract(${source}, CONCAT('$[', __row_id - 1, ']'))) as ${alias}  FROM (SELECT json_unquote(json_extract(${source}, CONCAT('$[', __row_id, ']'))) as d) as b LEFT JOIN JSON_TABLE(cast(concat("[1",repeat(",1",JSON_LENGTH(${source}) - 1),"]") as JSON),"$[*]" COLUMNS(__row_id FOR ORDINALITY)) as e on TRUE) as __tbl ON true`;
    } */
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    sqlDistinctKey = `CONCAT(${sqlDistinctKey}, '')`;
    const upperPart = `CAST(CONV(SUBSTRING(MD5(${sqlDistinctKey}), 1, 16), 16, 10) AS DECIMAL(38, 0)) * 4294967296`;
    const lowerPart = `CAST(CONV(SUBSTRING(MD5(${sqlDistinctKey}), 16, 8), 16, 10) AS DECIMAL(38, 0))`;
    // See the comment below on `sql_sum_distinct` for why we multiply by this decimal
    const precisionShiftMultiplier = '0.000000001';
    return `(${upperPart} + ${lowerPart}) * ${precisionShiftMultiplier}`;
  }

  sqlGenerateUUID(): string {
    // TODO: This causes the query to become slow, figure out another way to make UUID deterministic.
    return 'CONCAT(ROW_NUMBER() OVER(), UUID())';
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    fieldType: string,
    isNested: boolean,
    _isArray: boolean
  ): string {
    let ret = `${alias}.\`${fieldName}\``;
    if (isNested) {
      switch (fieldType) {
        case 'string':
          ret = `CONCAT(${ret}, '')`;
          break;
        // TODO: Fix this.
        case 'number':
          ret = `CAST(${ret} as double)`;
          break;
        case 'struct':
          ret = `CAST(${ret} as JSON)`;
          break;
      }
      return ret;
    } else {
      return `${alias}.\`${fieldName}\``;
    }
  }

  sqlUnnestPipelineHead(
    _isSingleton: boolean,
    _sourceSQLExpression: string
  ): string {
    throw new Error('MySql dialect does not support nesting.');
  }

  sqlCreateFunction(id: string, funcText: string): string {
    // TODO:
    return `CREATE FUNCTION ${id}(JSONB) RETURNS JSONB AS $$\n${indent(
      funcText
    )}\n$$ LANGUAGE SQL;\n`;
  }

  sqlCreateFunctionCombineLastStage(lastStageName: string): string {
    // TODO:
    return `SELECT ARRAY((SELECT AS STRUCT * FROM ${lastStageName}))\n`;
  }

  sqlSelectAliasAsStruct(alias: string, physicalFieldNames: string[]): string {
    return `JSON_OBJECT(${physicalFieldNames
      .map(name => `'${name.replace(/`/g, '')}', \`${alias}\`.${name}`)
      .join(',')})`;
  }

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return `\`${identifier}\``;
  }

  // TODO: Check what this is.
  sqlCreateTableAsSelect(_tableName: string, _sql: string): string {
    throw new Error('Not implemented Yet');
  }

  sqlNow(): Expr {
    return mkExpr`LOCALTIMESTAMP`;
  }

  sqlTrunc(qi: QueryInfo, sqlTime: TimeValue, units: TimestampUnit): Expr {
    let truncThis = sqlTime.value;
    if (units === 'week') {
      truncThis = mkExpr`DATE_SUB(${truncThis}, INTERVAL DAYOFWEEK(${truncThis}) - 1 DAY)`;
    }
    if (sqlTime.valueType === 'timestamp') {
      const tz = qtz(qi);
      if (tz) {
        const civilSource = mkExpr`(CONVERT_TZ(${truncThis}, 'UTC','${tz}'))`;
        const civilTrunc = mkExpr`${this.truncToUnit(civilSource, units)}`;
        const truncTsTz = mkExpr`CONVERT_TZ(${civilTrunc}, '${tz}', 'UTC')`;
        return mkExpr`(${truncTsTz})`; // TODO: should it cast?
      }
    }
    const result = mkExpr`${this.truncToUnit(truncThis, units)}`;
    return result;
  }

  truncToUnit(expr: Expr, units: TimestampUnit) {
    let format = mkExpr`'%Y-%m-%d %H:%i:%s'`;
    switch (units) {
      case 'minute':
        format = mkExpr`'%Y-%m-%d %H:%i:00'`;
        break;
      case 'hour':
        format = mkExpr`'%Y-%m-%d %H:00:00'`;
        break;
      case 'day':
      case 'week':
        format = mkExpr`'%Y-%m-%d 00:00:00'`;
        break;
      case 'month':
        format = mkExpr`'%Y-%m-01 00:00:00'`;
        break;
      case 'quarter':
        format = mkExpr`CASE WHEN MONTH(${expr}) > 9 THEN '%Y-10-01 00:00:00' WHEN MONTH(${expr}) > 6 THEN '%Y-07-01 00:00:00' WHEN MONTH(${expr}) > 3 THEN '%Y-04-01 00:00:00' ELSE '%Y-01-01 00:00:00' end`;
        break;
      case 'year':
        format = mkExpr`'%Y-01-01 00:00:00'`;
        break;
    }

    return mkExpr`TIMESTAMP(DATE_FORMAT(${expr}, ${format}))`;
  }

  sqlExtract(qi: QueryInfo, from: TimeValue, units: ExtractUnit): Expr {
    const msUnits = msExtractionMap[units] || units;
    let extractFrom = from.value;
    if (from.valueType === 'timestamp') {
      const tz = qtz(qi);
      if (tz) {
        extractFrom = mkExpr`CONVERT_TZ(${extractFrom}, 'UTC', '${tz}')`;
      }
    }
    return mkExpr`${msUnits}(${extractFrom})`;
  }

  sqlAlterTime(
    op: '+' | '-',
    expr: TimeValue,
    n: Expr,
    timeframe: DateUnit
  ): Expr {
    if (timeframe === 'quarter') {
      timeframe = 'month';
      n = mkExpr`${n}*3`;
    } else if (timeframe === 'week') {
      timeframe = 'day';
      n = mkExpr`${n}*7`;
    }
    const interval = mkExpr`INTERVAL ${n} ${timeframe} `;
    return mkExpr`((${expr.value})${op}${interval})`;
  }

  sqlCast(qi: QueryInfo, cast: TypecastFragment): Expr {
    const op = `${cast.srcType}::${cast.dstType}`;
    const tz = qtz(qi);
    if (op === 'timestamp::date' && tz) {
      return mkExpr`CAST(CONVERT_TZ(${cast.expr}, 'UTC', '${tz}') AS DATE) `;
    } else if (op === 'date::timestamp' && tz) {
      return mkExpr` CONVERT_TZ(${cast.expr}, '${tz}', 'UTC')`;
    }
    if (cast.srcType !== cast.dstType) {
      const dstType = castMap[cast.dstType] || cast.dstType;
      if (cast.safe) {
        throw new Error("Mysql dialect doesn't support Safe Cast");
      }
      if (cast.dstType === 'string') {
        return mkExpr`CONCAT(${cast.expr}, '')`;
      }
      return mkExpr`CAST(${cast.expr}  AS ${dstType})`;
    }
    return cast.expr;
  }

  sqlRegexpMatch(expr: Expr, regexp: Expr): Expr {
    return mkExpr`REGEXP_LIKE(${expr},${regexp})`;
  }

  sqlLiteralTime(
    qi: QueryInfo,
    timeString: string,
    type: TimeFieldType,
    timezone: string | undefined
  ): string {
    if (type === 'date') {
      return `DATE '${timeString}'`;
    }
    const tz = timezone || qtz(qi);
    if (tz) {
      return ` CONVERT_TZ('${timeString}', '${tz}', 'UTC')`;
    }
    return `TIMESTAMP '${timeString}'`;
  }

  sqlMeasureTime(from: TimeValue, to: TimeValue, units: string): Expr {
    let lVal = from.value;
    let rVal = to.value;
    if (inSeconds[units]) {
      lVal = mkExpr`UNIX_TIMESTAMP(${lVal})`;
      rVal = mkExpr`UNIX_TIMESTAMP(${rVal})`;
      const duration = mkExpr`${rVal}-${lVal}`;
      return units === 'second'
        ? mkExpr`FLOOR(${duration})`
        : mkExpr`FLOOR((${duration})/${inSeconds[units].toString()}.0)`;
    }
    throw new Error(`Unknown or unhandled MySql time unit: ${units}`);
  }

  sqlSumDistinct(_key: string, _value: string, _funcName: string): string {
    throw new Error('MySql dialect does not support nesting.');
  }

  sqlAggDistinct(
    _key: string,
    _values: string[],
    _func: (valNames: string[]) => string
  ): string {
    throw new Error('MySql dialect does not support nesting.');
  }

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} ORDER BY rand() LIMIT ${sample.rows} )`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY rand()) as __row_number, __source_tbl.* from ${tableSQL} as __source_tbl) as __rand_tbl where __row_number % FLOOR(100.0 / ${sample.percent}) = 1)`;
      }
    }
    return tableSQL;
  }

  sqlOrderBy(orderTerms: string[]): string {
    return `ORDER BY ${orderTerms
      .map(
        t =>
          `${t.trim().slice(0, t.trim().lastIndexOf(' '))} IS NULL DESC, ${t}`
      )
      .join(',')}`;
  }

  sqlLiteralString(literal: string): string {
    const noVirgule = literal.replace(/\\/g, '\\\\');
    return "'" + noVirgule.replace(/'/g, "\\'") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    return "'" + literal.replace(/'/g, "''") + "'";
  }

  getGlobalFunctionDef(name: string): DialectFunctionOverloadDef[] | undefined {
    return MYSQL_FUNCTIONS.get(name);
  }

  mapFieldsForJsonObject(fieldList: DialectFieldList, nullValues?: boolean) {
    return fieldList
      .map(
        f =>
          `${f.sqlOutputName.replace(/`/g, "'")}, ${
            nullValues ? 'NULL' : f.sqlExpression
          }\n`
      )
      .join(', ');
  }

  castToString(expression: string): string {
    return `CONCAT(${expression}, '')`;
  }

  concat(...values: string[]): string {
    return `CONCAT(${values.join(',')})`;
  }
}

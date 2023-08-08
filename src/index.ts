/* eslint-disable no-console */
import {DataArray, Result, Runtime} from '@malloydata/malloy';
import {MySqlConnection} from './connection/mysql_connection';
//import {DataImporter} from './data_importer';
/*import {JSDOM} from 'jsdom';
import {HTMLView} from '@malloydata/render';
*/
export async function main() {
  await runModel();
}

// TODO: linter not working.
export async function runModel() {
  const runtime = new Runtime(
    new MySqlConnection({
      host: '127.0.0.1',
      port: 3306,
      user: 'root',
      password: 'Malloydev123',
      database: 'appointments',
    })
  );
  const mq =
    runtime.loadModel(`  sql: height_sql232 is { connection: "duckdb" select: """SELECT * from Persons""" }

    source: persons is table('mysql:Persons') {
      measure: sumi is count(distinct concat(City, height))
    }

    query: abc2 is persons -> {
      project:
        *
    }

    query: abc is persons -> {
      group_by: Name
      nest: cde is {
        group_by: City
        # percent
        aggregate: ht is sum(height)
        limit: 2
      }
      nest: cde2 is {
        group_by: City2 is City
        # percent
        aggregate: ht3 is sum(Vaccines)
        limit: 2
        order_by: ht3
      }
      nest: abc is {
        group_by: City
        nest: inneri is {
          group_by: Vaccines
          aggregate: ht5 is sum(height)
          limit: 10
        }
      }
    }`);
  const qm = await mq.loadQueryByName('abc2');
  const result: Result = await qm.run();

  renderTable(result.data);
  /*const document = new JSDOM().window.document;
  const html = await new HTMLView(document).render(result, {
    dataStyles: {},
  });

  console.log(`${html.innerHTML}`);*/
}

function renderTable(data: DataArray) {
  for (const row of data) {
    for (const field of data.field.intrinsicFields) {
      if (field.isAtomicField()) {
        console.log(
          `${
            row.cell(field).isString()
              ? row.cell(field).string.value
              : row.cell(field).isNull()
              ? '<null>'
              : row.cell(field).number.value
          } |`
        );
      } else {
        const dc = row.cell(field);
        console.log(`${dc.isArray()} ${dc.isRecord()} ${dc.field.name}`);
        // renderTable(row.cell(field).array.value, `${space}\t\t`);
      }
    }
    console.log('\n');
  }
}

main();

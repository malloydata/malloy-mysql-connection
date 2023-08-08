import {
  exprSharedTests,
  indexSharedTests,
  joinSharedTests,
  noModelSharedTests,
  orderBySharedTests,
  problemsSharedTests,
  RuntimeList,
  sqlExpressionsSharedTests,
  testRuntimeFor,
  timeSharedTests,
} from '@malloydata/malloy-tests';
import {MySqlConnection} from '../../src/connection/mysql_connection';

const config = {
  host: '127.0.0.1',
  port: 3306,
  user: 'root',
  password: 'Malloydev123',
  database: 'malloytest',
};

/*allDatabaseTestSets.forEach(testSet => {
  testSet(new RuntimeList([testRuntimeFor(new MySqlConnection(config))]));
});*/

joinSharedTests(new RuntimeList([testRuntimeFor(new MySqlConnection(config))]));
noModelSharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))]),
  (column: string, splitChar: string) =>
    `CAST(CONCAT('["',REPLACE(\`${column}\`, '${splitChar}', '","'), '"]') AS JSON)`
);


exprSharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))])
);

// TODO: Missing time test.
timeSharedTests(new RuntimeList([testRuntimeFor(new MySqlConnection(config))]));

// TODO: out of sort memory.
indexSharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))])
);

// TODO: Boolean is not being parsed correctly.
orderBySharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))])
);

problemsSharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))])
);

sqlExpressionsSharedTests(
  new RuntimeList([testRuntimeFor(new MySqlConnection(config))])
);


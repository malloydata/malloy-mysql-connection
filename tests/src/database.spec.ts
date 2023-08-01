// eslint-disable-next-line node/no-unpublished-import
import {
  joinSharedTests,
  RuntimeList,
  testRuntimeFor,
  // eslint-disable-next-line node/no-unpublished-import
} from '@malloydata/malloy-tests';
import {MySqlConnection} from '../../src/connection/mysql_connection';

/*allDatabaseTestSets.forEach(testSet => {
  testSet(new RuntimeList([testRuntimeFor(new MySqlConnection())]));
});*/

joinSharedTests(new RuntimeList([testRuntimeFor(new MySqlConnection())]));

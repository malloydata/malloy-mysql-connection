import {FUNCTIONS} from '@malloydata/malloy/dist/dialect/functions';
import {fnChr} from './chr';

export const MYSQL_FUNCTIONS = FUNCTIONS.clone();
MYSQL_FUNCTIONS.add('chr', fnChr);
MYSQL_FUNCTIONS.seal();

// TODO: Add functions.

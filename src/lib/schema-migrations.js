'use strict';

// Deploy-time schema migrations: the pieces shared by the runner
// (tools/migrate.js) and the Lambdas.
//
// Schema lives in src/db/migrate/, one file per change, applied in filename
// order by `node tools/migrate.js --stage <stage> --apply`, which deploy.sh
// runs before `serverless deploy`. Each applied file gets a row in
// schema_migrations. The Lambdas run no DDL; checkSchemaOnce only raises an
// alarm when the database is behind the code (e.g. a deploy that skipped
// deploy.sh). src/db/migrations/ is the older hand-applied archive and is
// never executed.

const fs = require('fs');
const path = require('path');
const log = require('./logger');

const MIGRATIONS_DIR = path.join(__dirname, '..', 'db', 'migrate');
const LEDGER_TABLE = 'schema_migrations';
const BASELINE_FILE = '2026-09-21_00_baseline.sql';
// YYYY-MM-DD_NN_description.sql|js — date first so files sort in apply order,
// NN orders files written on the same day.
const MIGRATION_FILE_RE = /^\d{4}-\d{2}-\d{2}_\d{2}_[a-z0-9_]+\.(sql|js)$/;

// Every .sql / .js file in the folder, sorted. A .sql / .js file that does not
// follow the naming rule is an error rather than silently skipped.
function listMigrationFiles(dir = MIGRATIONS_DIR) {
    const files = fs.readdirSync(dir).filter(f => /\.(sql|js)$/.test(f)).sort();
    const bad = files.filter(f => !MIGRATION_FILE_RE.test(f));
    if (bad.length) {
        throw new Error(`badly named migration file(s) in ${dir}: ${bad.join(', ')} (expected YYYY-MM-DD_NN_name.sql|js)`);
    }
    return files;
}

// Bundled migration files not yet recorded in schema_migrations. Returns null
// when the ledger table does not exist (a database that was never migrated).
async function findPendingMigrations(queryable, files = listMigrationFiles()) {
    let rows;
    try {
        [rows] = await queryable.query(`SELECT filename FROM ${LEDGER_TABLE}`);
    } catch (e) {
        if (e.code === 'ER_NO_SUCH_TABLE') return null;
        throw e;
    }
    const applied = new Set(rows.map(r => r.filename));
    return files.filter(f => !applied.has(f));
}

// Runtime alarm, once per container, started by the first request (not at
// module load, so a pre-initialised container that is frozen before its first
// request never holds a half-open connection). Not awaited by the request and
// never fails it: a missed migration should be loud in the logs, not an outage.
let schemaCheckStarted = false;
function checkSchemaOnce(pool) {
    if (schemaCheckStarted) return;
    schemaCheckStarted = true;
    (async () => {
        const files = listMigrationFiles();
        const pending = await findPendingMigrations(pool, files);
        if (pending === null) {
            log.error('[schema] schema_migrations table missing: this database has never been migrated. Deploy with bash deploy.sh (it runs tools/migrate.js).');
        } else if (pending.length) {
            log.error(`[schema] database schema is behind the code: ${pending.length} migration(s) not applied: ${pending.join(', ')}. Deploy with bash deploy.sh (it runs tools/migrate.js).`);
        }
    })().catch(err => log.warn('[schema] schema check failed', { error: err.message }));
}

module.exports = {
    MIGRATIONS_DIR,
    LEDGER_TABLE,
    BASELINE_FILE,
    MIGRATION_FILE_RE,
    listMigrationFiles,
    findPendingMigrations,
    checkSchemaOnce,
};

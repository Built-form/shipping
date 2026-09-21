'use strict';

// Deploy-time schema migrations.
//
// Applies src/db/migrate/*.sql and *.js in filename order, once each, and
// records every applied file in schema_migrations. `bash deploy.sh` runs
//   node tools/migrate.js --stage <stage> --apply
// before `serverless deploy`, so a failed migration aborts the deploy. The
// Lambdas run no DDL.
//
//   node tools/migrate.js --stage test                   dry run: list pending files
//   node tools/migrate.js --stage test --apply           apply pending files
//   node tools/migrate.js --stage dev --status           ledger vs files, changed files, missing views
//   node tools/migrate.js --stage dev --verify-baseline  diff this database against the baseline file
//   node tools/migrate.js --stage dev --adopt            existing database: record the baseline as applied
//                                                        (--apply does this itself when the check is clean)
//   node tools/migrate.js --stage dev --dump-baseline    write the baseline file from this database
//
// --stage picks the database the way serverless.yml does (dev -> secret
// shipping/prod, test -> shipping/test) and connects to the secret's DB_HOST,
// the direct instance: DB_PROXY_HOST, which the Lambdas use, is a private VPC
// endpoint that is not reachable from here. The AWS CLI reads the secret, so
// whoever runs this needs secretsmanager:GetSecretValue, as for a deploy.
// Without --stage it uses .env, and then writing needs --confirm-host <DB_HOST>.
//
// Other options:
//   --database <name>   override the database name (e.g. a scratch schema)
//   --lock-wait <s>     SET SESSION lock_wait_timeout for DDL (default 5): an
//                       ALTER on a busy table fails fast and aborts the deploy
//                       instead of queueing every query behind its metadata lock
//   --out <file> --force   with --dump-baseline
//
// Exit codes: 0 ok, 1 a migration failed or the schema differs, 2 refused.
//
// Writing migrations: see README "Schema changes". In short, a file must be
// safe to run twice (MySQL DDL is not transactional, so a failed run can leave
// half a file applied): CREATE TABLE IF NOT EXISTS, one clause per ALTER, and
// INSERT IGNORE for seeds. Errors meaning "already there" (table / column /
// index exists, and for DROP "already gone") are skipped, so a re-run finishes
// the job. A .js migration exports `up(conn, { log })` and must be idempotent.
// Additive first: DDL lands minutes before the code that uses it.

const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');
const mysql = require('mysql2/promise');
const { splitSql } = require('../src/lib/sql-split');
const {
    MIGRATIONS_DIR, LEDGER_TABLE, BASELINE_FILE, listMigrationFiles,
} = require('../src/lib/schema-migrations');

const BASELINE_TABLES_FILE = path.join(MIGRATIONS_DIR, 'baseline-tables.json');
// serverless.yml custom.envNames: the production stack is stage "dev".
const ENV_NAMES = { dev: 'prod', test: 'test' };
const LOCK_NAME = 'shipping_schema_migrate';
// Views the app reads. They are DEFINER views over the jfpro schema, created
// by hand as admin from the archive, so they are not migrations: --status
// only reports them missing.
const REQUIRED_VIEWS = {
    product_carton_sizes: 'src/db/migrations/2026-06-26_product_carton_sizes_view.sql',
    suppliers: 'src/db/migrations/2026-06-30_suppliers_views.sql',
    supplier_emails: 'src/db/migrations/2026-06-30_suppliers_views.sql',
};
// MySQL errors meaning "this statement already took effect".
const ALREADY_APPLIED = {
    1050: 'table exists',
    1060: 'column exists',
    1061: 'index exists',
    1359: 'trigger exists',
};
const ALREADY_DROPPED = { 1091: 'already dropped' };

class Refusal extends Error {}
const refuse = msg => { throw new Refusal(msg); };

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);

const opts = {
    stage: arg('--stage'),
    apply: flag('--apply'),
    status: flag('--status'),
    adopt: flag('--adopt'),
    verify: flag('--verify-baseline'),
    dump: flag('--dump-baseline'),
    out: arg('--out'),
    force: flag('--force'),
    confirmHost: arg('--confirm-host'),
    database: arg('--database'),
    lockWait: Number(arg('--lock-wait') || 5),
};
const writes = opts.apply || opts.adopt;

function checksum(file) {
    const text = fs.readFileSync(file, 'utf8').replace(/\r\n?/g, '\n');
    return crypto.createHash('sha256').update(text).digest('hex');
}

function appliedBy() {
    let user = 'unknown';
    try { user = os.userInfo().username; } catch (e) { /* no passwd entry */ }
    return `${user}@${os.hostname()}`.slice(0, 255);
}

function resolveTarget() {
    if (opts.stage) {
        const envName = ENV_NAMES[opts.stage] || opts.stage;
        const secretId = `shipping/${envName}`;
        let secret;
        try {
            const out = execFileSync('aws', [
                'secretsmanager', 'get-secret-value',
                '--region', process.env.AWS_REGION || 'eu-north-1',
                '--secret-id', secretId,
                '--query', 'SecretString', '--output', 'text',
            ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
            secret = JSON.parse(out);
        } catch (e) {
            refuse(`could not read secret ${secretId} with the AWS CLI: ${String(e.stderr || e.message).trim()}`);
        }
        return {
            source: `secret ${secretId}`,
            host: secret.DB_HOST,
            user: secret.DB_USER,
            password: secret.DB_PASSWORD,
            database: opts.database || secret.DB_NAME,
            port: Number(secret.DB_PORT || 3306),
            env: secret, // what .js migrations see as ctx.env
        };
    }
    require('dotenv').config();
    return {
        source: '.env',
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: opts.database || process.env.DB_NAME,
        port: Number(process.env.DB_PORT || 3306),
        env: process.env,
    };
}

// Refuse a stage whose secret points somewhere unexpected, and a .env write
// that was not deliberate.
function checkTarget(t, { stage, writes: isWrite, confirmHost } = {}) {
    if (!t.host || !t.database) refuse(`no database host/name in ${t.source}`);
    const looksTest = /test/i.test(t.host) || /test/i.test(t.database);
    if (stage === 'test' && !looksTest) {
        refuse(`stage test resolved to ${t.host}/${t.database}, which does not look like the TEST instance`);
    }
    if (stage === 'dev' && looksTest) {
        refuse(`stage dev (production) resolved to ${t.host}/${t.database}, which looks like TEST`);
    }
    if (!stage && isWrite && confirmHost !== t.host) {
        refuse(`writing to the .env database needs --confirm-host ${t.host} (or use --stage)`);
    }
}

async function tableExists(conn, name) {
    const [rows] = await conn.query(
        `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, [name]);
    return rows.length > 0;
}

// Map filename -> ledger row, or null when there is no ledger table.
async function readLedger(conn) {
    if (!(await tableExists(conn, LEDGER_TABLE))) return null;
    const [rows] = await conn.query(`SELECT filename, checksum, adopted, applied_at FROM ${LEDGER_TABLE}`);
    return new Map(rows.map(r => [r.filename, r]));
}

async function ensureLedger(conn) {
    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${LEDGER_TABLE} (
            filename VARCHAR(255) NOT NULL,
            checksum CHAR(64) NOT NULL,
            adopted TINYINT(1) NOT NULL DEFAULT 0,
            duration_ms INT NULL,
            applied_by VARCHAR(255) NULL,
            applied_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (filename)
        )
    `);
}

async function record(conn, file, { adopted = false, ms = null, dir = MIGRATIONS_DIR } = {}) {
    await conn.query(
        `INSERT INTO ${LEDGER_TABLE} (filename, checksum, adopted, duration_ms, applied_by)
         VALUES (?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE checksum = VALUES(checksum), adopted = VALUES(adopted),
             duration_ms = VALUES(duration_ms), applied_by = VALUES(applied_by), applied_at = CURRENT_TIMESTAMP`,
        [file, checksum(path.join(dir, file)), adopted ? 1 : 0, ms, appliedBy()]
    );
}

const oneLine = sql => sql.replace(/\s+/g, ' ').trim().slice(0, 110);

// Why a failed statement can count as done, or null when it is a real failure.
function alreadyApplied(err, sql) {
    return ALREADY_APPLIED[err.errno] || (/\bDROP\b/i.test(sql) && ALREADY_DROPPED[err.errno]) || null;
}

// Run one file and record it. Nothing is recorded unless every statement ran
// (or was already in effect), so a failed file is retried whole next time.
async function applyFile(conn, file, target, dir = MIGRATIONS_DIR) {
    const full = path.join(dir, file);
    const started = Date.now();
    console.log(`\n-> ${file}`);
    if (file.endsWith('.sql')) {
        const statements = splitSql(fs.readFileSync(full, 'utf8'));
        for (let n = 0; n < statements.length; n++) {
            const sql = statements[n];
            try {
                await conn.query(sql);
                console.log(`   ok       ${oneLine(sql)}`);
            } catch (e) {
                const why = alreadyApplied(e, sql);
                if (!why) {
                    e.message = `statement ${n + 1} of ${statements.length} (${oneLine(sql)}): ${e.code || ''} ${e.message}`;
                    throw e;
                }
                console.log(`   skipped  ${oneLine(sql)}  [${why}]`);
            }
        }
    } else {
        const mod = require(full);
        if (typeof mod.up !== 'function') throw new Error(`${file} must export up(conn, { log })`);
        await mod.up(conn, { log: (...a) => console.log('  ', ...a), env: target.env });
    }
    const ms = Date.now() - started;
    await record(conn, file, { ms, dir });
    console.log(`   recorded (${ms} ms)`);
}

// ── baseline ────────────────────────────────────────────────────────────────

function baselineTables() {
    const { tables } = JSON.parse(fs.readFileSync(BASELINE_TABLES_FILE, 'utf8'));
    return [...tables].sort();
}

async function showCreate(conn, table) {
    try {
        const [rows] = await conn.query('SHOW CREATE TABLE ??', [table]);
        return rows[0]['Create Table'] || null; // null: it is a view
    } catch (e) {
        if (e.code === 'ER_NO_SUCH_TABLE') return undefined;
        throw e;
    }
}

const normaliseCreate = sql => sql
    .replace(/^CREATE TABLE (IF NOT EXISTS )?/i, 'CREATE TABLE IF NOT EXISTS ')
    .replace(/ AUTO_INCREMENT=\d+/g, '');

// A CREATE TABLE as a set of lines (columns, keys, then the table options), so
// two tables compare equal whatever order ALTERs added their columns in.
function definitionLines(createSql) {
    const lines = normaliseCreate(createSql).split('\n').map(l => l.trim());
    const out = new Set();
    for (const l of lines.slice(1)) {
        if (!l) continue;
        out.add(l.startsWith(')') ? `(options) ${l.slice(1).trim()}` : l.replace(/,$/, ''));
    }
    return out;
}

async function dumpBaseline(conn, target) {
    const outPath = opts.out ? path.resolve(opts.out) : path.join(MIGRATIONS_DIR, BASELINE_FILE);
    if (fs.existsSync(outPath) && !opts.force) {
        refuse(`${outPath} exists: pass --force to overwrite (changing an applied file changes its checksum)`);
    }
    const parts = [
        `-- Baseline: the app's tables as they stood when deploy-time migrations were`,
        `-- introduced, generated by \`node tools/migrate.js --dump-baseline\` from`,
        `-- ${target.host}/${target.database} on ${new Date().toISOString().slice(0, 10)}. Tables: src/db/migrate/baseline-tables.json.`,
        `--`,
        `-- Executed only on an empty database. An existing database records it`,
        `-- without running it (\`--adopt\`, after \`--verify-baseline\` is clean). Do not`,
        `-- edit it: schema changes are new files after it.`,
        '',
    ];
    const problems = [];
    for (const t of baselineTables()) {
        const create = await showCreate(conn, t);
        if (create === undefined) { problems.push(`${t}: no such table`); continue; }
        if (create === null) { problems.push(`${t}: is a view, not a table`); continue; }
        parts.push(`${normaliseCreate(create)};`, '');
    }
    if (problems.length) {
        console.error(`Not written:\n  ${problems.join('\n  ')}`);
        return 1;
    }
    fs.writeFileSync(outPath, parts.join('\n'));
    console.log(`Wrote ${baselineTables().length} tables to ${outPath}`);
    return 0;
}

// Compare the database with the baseline file. Returns the number of tables
// that are missing or differ: baseline lines the database lacks. Lines only
// the database has (it is ahead, e.g. TEST running undeployed code) are
// reported but do not count.
async function verifyBaseline(conn, file = path.join(MIGRATIONS_DIR, BASELINE_FILE)) {
    const expected = new Map();
    for (const stmt of splitSql(fs.readFileSync(file, 'utf8'))) {
        const m = /^CREATE TABLE IF NOT EXISTS `([^`]+)`/i.exec(stmt);
        if (m) expected.set(m[1], definitionLines(stmt));
    }
    let behind = 0;
    let ahead = 0;
    for (const [table, want] of expected) {
        const create = await showCreate(conn, table);
        if (!create) {
            behind++;
            console.log(`MISSING  ${table}${create === null ? ' (exists as a view)' : ''}`);
            continue;
        }
        const have = definitionLines(create);
        const lacks = [...want].filter(l => !have.has(l));
        const extra = [...have].filter(l => !want.has(l));
        if (!lacks.length && !extra.length) continue;
        if (lacks.length) behind++; else ahead++;
        console.log(`${lacks.length ? 'DIFFERS ' : 'AHEAD   '} ${table}`);
        for (const l of lacks) console.log(`    - baseline: ${l}`);
        for (const l of extra) console.log(`    + database: ${l}`);
    }
    console.log(`\n${expected.size} baseline tables: ${expected.size - behind - ahead} identical, ${ahead} ahead of the baseline, ${behind} missing or behind.`);
    return behind;
}

// ── modes ───────────────────────────────────────────────────────────────────

async function status(conn) {
    const files = listMigrationFiles();
    const ledger = await readLedger(conn);
    if (!ledger) console.log('No schema_migrations table: this database has never been migrated.');
    for (const f of files) {
        const row = ledger && ledger.get(f);
        if (!row) { console.log(`PENDING  ${f}`); continue; }
        const changed = row.checksum !== checksum(path.join(MIGRATIONS_DIR, f));
        const when = row.applied_at instanceof Date ? row.applied_at.toISOString().slice(0, 16).replace('T', ' ') : row.applied_at;
        console.log(`applied  ${f}  ${when}${row.adopted ? '  (adopted)' : ''}${changed ? '  CHANGED SINCE APPLIED' : ''}`);
    }
    for (const f of ledger ? ledger.keys() : []) {
        if (!files.includes(f)) console.log(`recorded but no such file: ${f}`);
    }
    const [views] = await conn.query(
        `SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = DATABASE()`);
    const have = new Set(views.map(v => v.TABLE_NAME));
    for (const [view, file] of Object.entries(REQUIRED_VIEWS)) {
        if (!have.has(view)) console.log(`view missing: ${view} (create it as admin from ${file})`);
    }
    return 0;
}

async function migrate(conn, target) {
    const files = listMigrationFiles();
    let ledger = await readLedger(conn);
    const populated = await tableExists(conn, 'orders');
    // A populated database with no ledger predates the runner (production's
    // first deploy), or is a TEST rebuild from a snapshot taken before
    // production had a ledger. --apply adopts it after the baseline check;
    // a database missing part of the baseline is refused.
    if ((!ledger || !ledger.size) && populated) {
        if (!opts.apply) {
            console.log('No migration ledger yet: --apply will check this database against the baseline and adopt it if it matches.\n');
        } else {
            console.log('No migration ledger yet: checking the database against the baseline before adopting it.\n');
            const behind = await verifyBaseline(conn);
            if (behind) refuse('the database lacks part of the baseline (see above). Fix that with a new migration file before deploying.');
            await ensureLedger(conn);
            await record(conn, BASELINE_FILE, { adopted: true });
            console.log(`\nAdopted: ${BASELINE_FILE} recorded as applied without running it.\n`);
            ledger = await readLedger(conn);
        }
    }
    ledger = ledger || new Map();
    for (const f of files) {
        const row = ledger.get(f);
        if (row && row.checksum !== checksum(path.join(MIGRATIONS_DIR, f))) {
            console.log(`warning: ${f} changed since it was applied (applied files are not re-run; add a new file instead)`);
        }
    }
    const pending = files.filter(f => !ledger.has(f));
    if (!pending.length) {
        console.log('Up to date: nothing pending.');
        return 0;
    }
    console.log(`Pending (${pending.length}):\n  ${pending.join('\n  ')}`);
    if (!opts.apply) {
        console.log('\nDry run. Re-run with --apply to apply them.');
        return 0;
    }
    await ensureLedger(conn);
    for (const f of pending) await applyFile(conn, f, target);
    console.log(`\nApplied ${pending.length} migration(s).`);
    return 0;
}

async function adopt(conn, dir = MIGRATIONS_DIR) {
    if (!(await tableExists(conn, 'orders'))) {
        refuse('empty database: nothing to adopt. Run --apply to build it from the migrations.');
    }
    const ledger = await readLedger(conn);
    if (ledger && ledger.has(BASELINE_FILE)) {
        console.log('Already adopted.');
        return 0;
    }
    const behind = await verifyBaseline(conn, path.join(dir, BASELINE_FILE));
    if (behind) {
        console.log('\nNot adopting: the database lacks part of the baseline. Fix that first (a new migration file, or regenerate the baseline before anything has adopted it).');
        return 1;
    }
    await ensureLedger(conn);
    await record(conn, BASELINE_FILE, { adopted: true, dir });
    console.log(`\nAdopted: ${BASELINE_FILE} recorded as applied without running it. Next: --apply.`);
    return 0;
}

async function main() {
    const modes = ['apply', 'status', 'adopt', 'verify', 'dump'].filter(m => opts[m]);
    if (flag('--help') || flag('-h') || modes.length > 1) {
        console.log(fs.readFileSync(__filename, 'utf8').split('\n').slice(2, 36).map(l => l.replace(/^\/\/ ?/, '')).join('\n'));
        return modes.length > 1 ? 2 : 0;
    }
    const target = resolveTarget();
    checkTarget(target, { stage: opts.stage, writes, confirmHost: opts.confirmHost });
    const mode = opts.apply ? 'APPLY' : opts.adopt ? 'ADOPT' : opts.status ? 'status'
        : opts.verify ? 'verify baseline' : opts.dump ? 'dump baseline' : 'dry run';
    console.log(`Database: ${target.user || '?'}@${target.host}/${target.database}  (${target.source}${opts.stage ? `, stage ${opts.stage}` : ''})`);
    console.log(`Mode: ${mode}\n`);

    const conn = await mysql.createConnection({
        host: target.host, user: target.user, password: target.password,
        database: target.database, port: target.port,
        ssl: { rejectUnauthorized: false }, connectTimeout: 20000,
    });
    try {
        if (writes) {
            const [[{ got }]] = await conn.query('SELECT GET_LOCK(?, 0) AS got', [LOCK_NAME]);
            if (got !== 1) refuse('another tools/migrate.js run holds the migration lock on this database');
            await conn.query('SET SESSION lock_wait_timeout = ?', [opts.lockWait]);
        }
        if (opts.status) return await status(conn);
        if (opts.verify) return (await verifyBaseline(conn)) ? 1 : 0;
        if (opts.dump) return await dumpBaseline(conn, target);
        if (opts.adopt) return await adopt(conn);
        return await migrate(conn, target);
    } finally {
        await conn.end();
    }
}

if (require.main === module) {
    main().then(
        code => { process.exitCode = code; },
        err => {
            if (err instanceof Refusal) {
                console.error(`Refused: ${err.message}`);
                process.exitCode = 2;
            } else {
                console.error(`FAILED: ${err.message}`);
                process.exitCode = 1;
            }
        }
    );
}

// For tools/test-migrate-lib.js.
module.exports = {
    Refusal, checkTarget, alreadyApplied, applyFile, adopt, verifyBaseline,
    normaliseCreate, definitionLines, checksum,
};

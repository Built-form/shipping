'use strict';

// Unit tests for deploy-time migrations: the SQL splitter
// (src/lib/sql-split.js), the migration file rules
// (src/lib/schema-migrations.js) and the runner's logic (tools/migrate.js).
// No database: the runner is driven through a fake connection.
//
//   node --test tools/test-migrate-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { splitSql } = require('../src/lib/sql-split');
const {
    MIGRATIONS_DIR, BASELINE_FILE, listMigrationFiles, findPendingMigrations,
} = require('../src/lib/schema-migrations');
const runner = require('./migrate');

const ARCHIVE = path.join(__dirname, '..', 'src', 'db', 'migrations');
const read = f => fs.readFileSync(f, 'utf8');

// Statements must start with SQL, never with the tail of a comment.
const SQL_START = /^(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|RENAME|SET|SELECT|REPLACE)\b/i;

function fakeConn(respond) {
    const calls = [];
    return {
        calls,
        async query(sql, params) {
            calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params: params || [] });
            return respond(sql, params || []);
        },
    };
}
const mysqlError = (errno, code) => Object.assign(new Error(code), { errno, code });

function tmpDir(files) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'migrate-test-'));
    for (const [name, text] of Object.entries(files)) fs.writeFileSync(path.join(dir, name), text);
    return dir;
}

// Silence the runner's progress output inside a test.
async function quietly(fn) {
    const log = console.log;
    console.log = () => {};
    try { return await fn(); } finally { console.log = log; }
}

// ── splitter ────────────────────────────────────────────────────────────────

test('splitter: the five archive files that break tools/sql.js split cleanly', () => {
    const expect = {
        '2026-06-26_product_carton_sizes_view.sql': 1,     // "; and revert" inside a -- comment
        '2026-06-26_orders_carton_snapshot.sql': 5,        // 4 ALTERs + the backfill UPDATE
        '2026-06-29_order_emails.sql': 3,                  // a ; inside a CREATE TABLE body comment
        '2026-08-03_planned_container_allocations.sql': 1, // same
        '2026-08-03_shipping_allowed_emails.sql': 2,       // CREATE + seed INSERT
    };
    for (const [file, n] of Object.entries(expect)) {
        const statements = splitSql(read(path.join(ARCHIVE, file)));
        assert.equal(statements.length, n, file);
        for (const s of statements) assert.match(s, SQL_START, `${file}: ${s.slice(0, 60)}`);
    }
});

test('splitter: semicolons inside strings, identifiers and comments do not split', () => {
    const sql = [
        "INSERT INTO t (a, b) VALUES ('x;y', \"p;q\");",
        'ALTER TABLE `we;ird` ADD COLUMN c INT; -- trailing; comment',
        "UPDATE t SET a = 'it''s; fine', b = 'back\\'slash;' WHERE id = 1;",
        '# hash comment; here',
        '/* block; comment */ SELECT 1;',
    ].join('\n');
    assert.deepEqual(splitSql(sql), [
        "INSERT INTO t (a, b) VALUES ('x;y', \"p;q\")",
        'ALTER TABLE `we;ird` ADD COLUMN c INT',
        "UPDATE t SET a = 'it''s; fine', b = 'back\\'slash;' WHERE id = 1",
        'SELECT 1',
    ]);
});

test('splitter: -- needs whitespace to be a comment; executable comments are kept', () => {
    assert.deepEqual(splitSql('SELECT 1--1;'), ['SELECT 1--1']);
    assert.deepEqual(splitSql('CREATE TABLE t (a INT) /*!50100 ENGINE=InnoDB */;'),
        ['CREATE TABLE t (a INT) /*!50100 ENGINE=InnoDB */']);
    assert.deepEqual(splitSql('-- only a comment;\n\n'), []);
    assert.deepEqual(splitSql('SELECT 1;\r\nSELECT 2'), ['SELECT 1', 'SELECT 2']);
});

test('splitter: DELIMITER and unterminated quotes/comments are rejected', () => {
    assert.throws(() => splitSql('DELIMITER $$\nCREATE TRIGGER x BEFORE INSERT ON t FOR EACH ROW BEGIN SET @a = 1; END$$'), /DELIMITER/);
    assert.throws(() => splitSql("SELECT 'open;"), /unterminated/);
    assert.throws(() => splitSql('SELECT 1 /* open'), /unterminated/);
});

// ── the migration folder itself ─────────────────────────────────────────────

test('every migration file is well named and every .sql file splits into SQL', () => {
    const files = listMigrationFiles();
    assert.equal(files[0], BASELINE_FILE, 'the baseline sorts first');
    for (const f of files.filter(f => f.endsWith('.sql'))) {
        const statements = splitSql(read(path.join(MIGRATIONS_DIR, f)));
        assert.ok(statements.length > 0, `${f} has no statements`);
        for (const s of statements) assert.match(s, SQL_START, `${f}: ${s.slice(0, 60)}`);
        assert.ok(!statements.some(s => /^SET\s+SESSION/i.test(s)), `${f}: the runner sets the session, files must not`);
    }
    for (const f of files.filter(f => f.endsWith('.js'))) {
        assert.equal(typeof require(path.join(MIGRATIONS_DIR, f)).up, 'function', `${f} exports up()`);
    }
});

test('the baseline holds exactly the tables in baseline-tables.json', () => {
    const { tables } = JSON.parse(read(path.join(MIGRATIONS_DIR, 'baseline-tables.json')));
    const created = splitSql(read(path.join(MIGRATIONS_DIR, BASELINE_FILE)))
        .map(s => /^CREATE TABLE IF NOT EXISTS `([^`]+)`/.exec(s))
        .map(m => m && m[1]);
    assert.ok(created.every(Boolean), 'the baseline holds only CREATE TABLE IF NOT EXISTS statements');
    assert.deepEqual([...created].sort(), [...tables].sort());
    assert.ok(!/AUTO_INCREMENT=\d/.test(read(path.join(MIGRATIONS_DIR, BASELINE_FILE))), 'no AUTO_INCREMENT counters');
});

test('listMigrationFiles: sorted, ignores other files, rejects bad names', () => {
    const ok = tmpDir({
        '2026-09-22_00_b.sql': '', '2026-09-21_01_a.js': '', '2026-09-21_00_a.sql': '', 'notes.json': '{}',
    });
    assert.deepEqual(listMigrationFiles(ok), ['2026-09-21_00_a.sql', '2026-09-21_01_a.js', '2026-09-22_00_b.sql']);
    const bad = tmpDir({ '2026-09-21_add-thing.sql': '' });
    assert.throws(() => listMigrationFiles(bad), /badly named/);
});

test('findPendingMigrations: files not in the ledger; null when there is no ledger', async () => {
    const conn = fakeConn(() => [[{ filename: 'a.sql' }]]);
    assert.deepEqual(await findPendingMigrations(conn, ['a.sql', 'b.sql']), ['b.sql']);
    const none = fakeConn(() => { throw mysqlError(1146, 'ER_NO_SUCH_TABLE'); });
    assert.equal(await findPendingMigrations(none, ['a.sql']), null);
});

// ── runner ──────────────────────────────────────────────────────────────────

test('checkTarget: a stage must resolve to the matching instance; .env writes need --confirm-host', () => {
    const prod = { host: 'explorer.x.rds.amazonaws.com', database: 'jfa', source: 's' };
    const testDb = { host: 'explorer-test.db.example', database: 'jfa', source: 's' };
    assert.throws(() => runner.checkTarget(prod, { stage: 'test' }), runner.Refusal);
    assert.throws(() => runner.checkTarget(testDb, { stage: 'dev' }), runner.Refusal);
    runner.checkTarget(prod, { stage: 'dev', writes: true });
    runner.checkTarget(testDb, { stage: 'test', writes: true });
    assert.throws(() => runner.checkTarget(testDb, { writes: true }), /--confirm-host/);
    runner.checkTarget(testDb, { writes: true, confirmHost: testDb.host });
    runner.checkTarget(testDb, {}); // read-only needs nothing
    assert.throws(() => runner.checkTarget({ host: '', database: 'jfa', source: 's' }, {}), runner.Refusal);
});

test('alreadyApplied: exists-errors count as done; "can\'t drop" only for DROP', () => {
    assert.ok(runner.alreadyApplied({ errno: 1050 }, 'CREATE TABLE t (a INT)'));
    assert.ok(runner.alreadyApplied({ errno: 1060 }, 'ALTER TABLE t ADD COLUMN a INT'));
    assert.ok(runner.alreadyApplied({ errno: 1061 }, 'ALTER TABLE t ADD KEY k (a)'));
    assert.ok(runner.alreadyApplied({ errno: 1091 }, 'ALTER TABLE t DROP COLUMN a'));
    assert.equal(runner.alreadyApplied({ errno: 1091 }, 'ALTER TABLE t ADD COLUMN a INT'), null);
    assert.equal(runner.alreadyApplied({ errno: 1054 }, 'ALTER TABLE t ADD KEY k (nope)'), null);
    assert.equal(runner.alreadyApplied({ errno: 1205 }, 'ALTER TABLE t ADD COLUMN a INT'), null); // lock wait timeout
});

test('applyFile: skips statements already in effect, then records the file with its checksum', async () => {
    const dir = tmpDir({ '2026-01-01_00_x.sql': [
        'CREATE TABLE t (a INT);',
        'ALTER TABLE t ADD COLUMN b INT;',
        'ALTER TABLE t DROP COLUMN old;',
        'ALTER TABLE t ADD KEY k (b);',
    ].join('\n') });
    const conn = fakeConn((sql) => {
        if (sql.startsWith('CREATE TABLE t')) throw mysqlError(1050, 'ER_TABLE_EXISTS_ERROR');
        if (sql.includes('ADD COLUMN b')) throw mysqlError(1060, 'ER_DUP_FIELDNAME');
        if (sql.includes('DROP COLUMN old')) throw mysqlError(1091, 'ER_CANT_DROP_FIELD_OR_KEY');
        return [{ affectedRows: 0 }];
    });
    await quietly(() => runner.applyFile(conn, '2026-01-01_00_x.sql', { env: {} }, dir));
    const recorded = conn.calls.filter(c => c.sql.startsWith('INSERT INTO schema_migrations'));
    assert.equal(recorded.length, 1);
    assert.equal(recorded[0].params[0], '2026-01-01_00_x.sql');
    assert.equal(recorded[0].params[1], runner.checksum(path.join(dir, '2026-01-01_00_x.sql')));
    assert.ok(conn.calls.some(c => c.sql === 'ALTER TABLE t ADD KEY k (b)'), 'ran the statement after the skips');
});

test('applyFile: a real failure stops the file and records nothing', async () => {
    const dir = tmpDir({ '2026-01-01_00_x.sql': 'ALTER TABLE t ADD COLUMN a INT;\nALTER TABLE t ADD COLUMN b INT;\nALTER TABLE t ADD COLUMN c INT;' });
    const conn = fakeConn((sql) => {
        if (sql.includes('COLUMN b')) throw mysqlError(1205, 'ER_LOCK_WAIT_TIMEOUT');
        return [{ affectedRows: 0 }];
    });
    await assert.rejects(
        quietly(() => runner.applyFile(conn, '2026-01-01_00_x.sql', { env: {} }, dir)),
        /statement 2 of 3.*ER_LOCK_WAIT_TIMEOUT/);
    assert.ok(!conn.calls.some(c => c.sql.includes('COLUMN c')), 'statement 3 not attempted');
    assert.ok(!conn.calls.some(c => c.sql.startsWith('INSERT INTO schema_migrations')), 'nothing recorded');
});

test('applyFile: a .js migration gets the connection, a logger and the stage env', async () => {
    const dir = tmpDir({ '2026-01-01_00_seed.js': `exports.up = async (conn, ctx) => {
        await conn.query('INSERT IGNORE INTO t VALUES (?)', [ctx.env.SEED]); ctx.log('seeded'); };` });
    const conn = fakeConn(() => [{ affectedRows: 1 }]);
    await quietly(() => runner.applyFile(conn, '2026-01-01_00_seed.js', { env: { SEED: 'v' } }, dir));
    assert.deepEqual(conn.calls[0].params, ['v']);
    assert.ok(conn.calls[1].sql.startsWith('INSERT INTO schema_migrations'));
});

test('checksum ignores line endings (a Windows checkout must not look changed)', () => {
    const dir = tmpDir({ 'lf.sql': 'SELECT 1;\nSELECT 2;\n', 'crlf.sql': 'SELECT 1;\r\nSELECT 2;\r\n' });
    assert.equal(runner.checksum(path.join(dir, 'lf.sql')), runner.checksum(path.join(dir, 'crlf.sql')));
});

test('definitionLines: column order and AUTO_INCREMENT counters do not matter', () => {
    const a = 'CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4';
    const b = 'CREATE TABLE IF NOT EXISTS `t` (\n  `b` int DEFAULT NULL,\n  `a` int NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
    assert.deepEqual(runner.definitionLines(a), runner.definitionLines(b));
    assert.equal(runner.normaliseCreate(a).split('\n')[0], 'CREATE TABLE IF NOT EXISTS `t` (');
});

// A populated database with no ledger yet, whose `t` table is `create`.
function schemaConn(create) {
    return fakeConn((sql, params) => {
        if (sql.includes('information_schema.TABLES')) return [params[0] === 'orders' ? [{ 1: 1 }] : []];
        if (sql.startsWith('SHOW CREATE TABLE')) return [[{ 'Create Table': create }]];
        return [{ affectedRows: 1 }];
    });
}

test('adopt: refuses when the database lacks part of the baseline, records it when ahead or equal', async () => {
    const baseline = 'CREATE TABLE IF NOT EXISTS `t` (\n  `a` int NOT NULL,\n  `b` varchar(64) DEFAULT NULL\n) ENGINE=InnoDB;\n';
    const dir = tmpDir({ [BASELINE_FILE]: baseline });

    // behind: b is narrower than the baseline says
    const behind = schemaConn('CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` varchar(16) DEFAULT NULL\n) ENGINE=InnoDB');
    assert.equal(await quietly(() => runner.adopt(behind, dir)), 1);
    assert.ok(!behind.calls.some(c => c.sql.startsWith('INSERT INTO schema_migrations')));

    // ahead: an extra column the baseline does not know is fine
    const ahead = schemaConn('CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` varchar(64) DEFAULT NULL,\n  `c` int DEFAULT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=9');
    assert.equal(await quietly(() => runner.adopt(ahead, dir)), 0);
    const rec = ahead.calls.find(c => c.sql.startsWith('INSERT INTO schema_migrations'));
    assert.equal(rec.params[0], BASELINE_FILE);
    assert.equal(rec.params[2], 1, 'recorded as adopted, not run');
    assert.ok(!ahead.calls.some(c => c.sql.startsWith('CREATE TABLE IF NOT EXISTS `t`')), 'the baseline was not executed');
});

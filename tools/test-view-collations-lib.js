'use strict';

// Unit tests for the view collation check (src/lib/view-collations.js), the
// runner's use of it (tools/migrate.js viewCheck), and the two migrations that
// fixed the views it found (2026-11-25_10, _11). No database: MySQL is a small
// stand-in that keeps the session's collation and refuses what the real one
// refuses.
//
//   node --test tools/test-view-collations-lib.js
//
// Against a real database (read-only):  node tools/migrate.js --stage test --check-views

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const VC = require('../src/lib/view-collations');
const { splitSql } = require('../src/lib/sql-split');
const { MIGRATIONS_DIR } = require('../src/lib/schema-migrations');
const runner = require('./migrate');

const CLIENT = 'utf8mb4_unicode_ci';
const PROXY = 'utf8mb4_0900_ai_ci';
const mixError = () => Object.assign(
    new Error("Illegal mix of collations (utf8mb4_0900_ai_ci,COERCIBLE) and (utf8mb4_unicode_ci,COERCIBLE) for operation '='"),
    { errno: 1267, code: 'ER_CANT_AGGREGATE_2COLLATIONS' });

// views: { name: [text columns] }.
// broken: [{ view, column (null: the view itself), collation }] raise "Illegal mix".
// dead: { view: message } raise another error on every read.
// deadColumns: { view: message } raise another error only when its columns are compared.
function fakeDb({ views, broken = [], dead = {}, deadColumns = {}, session = { cs: 'utf8mb4', coll: CLIENT }, failSetNames = null }) {
    const calls = [];
    const now = { ...session };
    return {
        calls,
        now,
        async query(sql) {
            const s = sql.replace(/\s+/g, ' ').trim();
            calls.push(s);
            if (/information_schema\.VIEWS/.test(s)) {
                const rows = [];
                for (const [view, columns] of Object.entries(views)) {
                    if (!columns.length) rows.push({ view_name: view, column_name: 'id', collation_name: null });
                    for (const c of columns) rows.push({ view_name: view, column_name: c, collation_name: 'utf8mb4_0900_ai_ci' });
                }
                return [rows];
            }
            if (/@@character_set_client/.test(s)) return [[{ cs: now.cs, coll: now.coll }]];
            let m = /^SET NAMES (\w+) COLLATE (\w+)$/.exec(s);
            if (m) {
                if (failSetNames === m[2]) throw new Error('connection lost');
                now.cs = m[1]; now.coll = m[2];
                return [{}];
            }
            m = /^SELECT 1 FROM `(\w+)`(?: WHERE (.+))? LIMIT 0$/.exec(s);
            if (m) {
                const view = m[1];
                if (dead[view]) throw Object.assign(new Error(dead[view]), { errno: 1356, code: 'ER_VIEW_INVALID' });
                const asked = m[2] ? [...m[2].matchAll(/`(\w+)` = 'x'/g)].map(x => x[1]) : [];
                if (asked.length && deadColumns[view]) throw Object.assign(new Error(deadColumns[view]), { errno: 1105, code: 'ER_UNKNOWN_ERROR' });
                const hit = broken.some(b => b.view === view && b.collation === now.coll
                    && (b.column == null || asked.includes(b.column)));
                if (hit) throw mixError();
                return [[]];
            }
            throw new Error(`unexpected SQL in the test: ${s}`);
        },
    };
}
const probes = (db, view) => db.calls.filter(s => s.startsWith(`SELECT 1 FROM \`${view}\``));

test('probeSql: resolves the view, and compares each text column with a literal', () => {
    assert.equal(VC.probeSql('v'), 'SELECT 1 FROM `v` LIMIT 0');
    assert.equal(VC.probeSql('v', ['a']), "SELECT 1 FROM `v` WHERE `a` = 'x' LIMIT 0");
    assert.equal(VC.probeSql('v', ['a', 'b']), "SELECT 1 FROM `v` WHERE `a` = 'x' OR `b` = 'x' LIMIT 0");
});

test('the two collations are the driver default and the server default', () => {
    assert.equal(VC.CLIENT_COLLATION, CLIENT);
    assert.equal(VC.PROXY_COLLATION, PROXY);
    assert.deepEqual(VC.SESSION_COLLATIONS, [CLIENT, PROXY]);
});

test('viewsCreatedBy: every form of CREATE VIEW, and nothing a comment only mentions', () => {
    const names = VC.viewsCreatedBy([
        'CREATE VIEW plain_v AS SELECT 1;',
        'CREATE OR REPLACE SQL SECURITY DEFINER VIEW definer_v AS SELECT 1;',
        'CREATE OR REPLACE ALGORITHM=MERGE DEFINER=`admin`@`%` SQL SECURITY INVOKER VIEW `quoted_v` AS SELECT 1;',
        '-- CREATE OR REPLACE VIEW only_in_a_comment AS\nCREATE TABLE IF NOT EXISTS t (id INT);',
        "// CREATE VIEW js_comment_v AS\nawait conn.query('CREATE OR REPLACE VIEW from_js_v AS SELECT 1');",
    ]);
    assert.deepEqual([...names].sort(), ['definer_v', 'from_js_v', 'plain_v', 'quoted_v']);
});

test('clean views: no failure, both collations tried, the session put back as it was', async () => {
    const db = fakeDb({ views: { a_v: ['name'], b_v: [] }, session: { cs: 'latin1', coll: 'latin1_swedish_ci' } });
    const out = await VC.checkViewCollations(db);
    assert.deepEqual(out, { views: 2, failures: [], skipped: [] });
    assert.deepEqual(db.calls.filter(s => s.startsWith('SET NAMES')), [
        `SET NAMES utf8mb4 COLLATE ${CLIENT}`,
        `SET NAMES utf8mb4 COLLATE ${PROXY}`,
        'SET NAMES latin1 COLLATE latin1_swedish_ci',
    ]);
    assert.deepEqual(db.now, { cs: 'latin1', coll: 'latin1_swedish_ci' });
    // A view with a text column: resolved, then its column compared, under each collation.
    assert.deepEqual(probes(db, 'a_v'), [
        'SELECT 1 FROM `a_v` LIMIT 0', "SELECT 1 FROM `a_v` WHERE `name` = 'x' LIMIT 0",
        'SELECT 1 FROM `a_v` LIMIT 0', "SELECT 1 FROM `a_v` WHERE `name` = 'x' LIMIT 0",
    ]);
    // One with none: resolved only.
    assert.deepEqual(probes(db, 'b_v'), ['SELECT 1 FROM `b_v` LIMIT 0', 'SELECT 1 FROM `b_v` LIMIT 0']);
});

test('a view that cannot be read through the proxy (2026-11-24: DATE_FORMAT against a literal)', async () => {
    const db = fakeDb({
        views: { portal_forwarder_containers_v: ['link_source', 'reference'], fine_v: ['x'] },
        broken: [{ view: 'portal_forwarder_containers_v', column: null, collation: PROXY }],
    });
    const out = await VC.checkViewCollations(db);
    assert.equal(out.views, 2);
    assert.equal(out.failures.length, 1);
    assert.deepEqual(
        { view: out.failures[0].view, collation: out.failures[0].collation, column: out.failures[0].column },
        { view: 'portal_forwarder_containers_v', collation: PROXY, column: null });
    assert.match(out.failures[0].message, /Illegal mix of collations/);
    assert.deepEqual(out.skipped, []);
    // Under the direct collation it reads, so its columns were compared there; under the proxy's, not asked again.
    assert.deepEqual(probes(db, 'portal_forwarder_containers_v'), [
        'SELECT 1 FROM `portal_forwarder_containers_v` LIMIT 0',
        "SELECT 1 FROM `portal_forwarder_containers_v` WHERE `link_source` = 'x' OR `reference` = 'x' LIMIT 0",
        'SELECT 1 FROM `portal_forwarder_containers_v` LIMIT 0',
    ]);
});

test('a bare literal column (2026-11-23: link_source): exactly the columns that clash are named', async () => {
    const db = fakeDb({
        views: { forwarder_duty_holders_v: ['duty', 'note', 'source'] },
        broken: [
            { view: 'forwarder_duty_holders_v', column: 'duty', collation: PROXY },
            { view: 'forwarder_duty_holders_v', column: 'source', collation: PROXY },
        ],
    });
    const out = await VC.checkViewCollations(db);
    assert.deepEqual(out.failures.map(f => [f.view, f.column, f.collation]), [
        ['forwarder_duty_holders_v', 'duty', PROXY],
        ['forwarder_duty_holders_v', 'source', PROXY],
    ]);
});

test('a view made under the server default fails for a direct connection: reported for that collation', async () => {
    const db = fakeDb({ views: { made_by_hand_v: ['kind'] }, broken: [{ view: 'made_by_hand_v', column: 'kind', collation: CLIENT }] });
    const out = await VC.checkViewCollations(db);
    assert.deepEqual(out.failures.map(f => [f.view, f.column, f.collation]), [['made_by_hand_v', 'kind', CLIENT]]);
});

test('a view that fails for another reason is skipped once, and the rest are still checked', async () => {
    const db = fakeDb({
        views: { dead_v: ['a'], live_v: ['b'] },
        dead: { dead_v: "View 'jfa.dead_v' references invalid table(s)" },
        broken: [{ view: 'live_v', column: 'b', collation: PROXY }],
    });
    const out = await VC.checkViewCollations(db);
    assert.deepEqual(out.skipped, [{ view: 'dead_v', message: "View 'jfa.dead_v' references invalid table(s)" }]);
    assert.deepEqual(out.failures.map(f => [f.view, f.column]), [['live_v', 'b']]);
    assert.equal(probes(db, 'dead_v').length, 1);
});

test('a view whose columns cannot be compared for another reason is skipped, not passed in silence', async () => {
    const db = fakeDb({ views: { odd_v: ['a'], live_v: ['b'] }, deadColumns: { odd_v: 'something else' } });
    const out = await VC.checkViewCollations(db);
    assert.deepEqual(out, { views: 2, failures: [], skipped: [{ view: 'odd_v', message: 'something else' }] });
    assert.equal(probes(db, 'odd_v').length, 2);
    assert.equal(probes(db, 'live_v').length, 4);
});

test('only: checks the named views and no other', async () => {
    const db = fakeDb({ views: { a_v: ['x'], b_v: ['x'] }, broken: [{ view: 'b_v', column: 'x', collation: PROXY }] });
    const out = await VC.checkViewCollations(db, { only: new Set(['a_v']) });
    assert.deepEqual(out, { views: 1, failures: [], skipped: [] });
    assert.equal(probes(db, 'b_v').length, 0);
});

test('the session collation is put back even when the check itself dies', async () => {
    const db = fakeDb({ views: { a_v: ['x'] }, failSetNames: PROXY });
    await assert.rejects(VC.checkViewCollations(db), /connection lost/);
    assert.equal(db.calls[db.calls.length - 1], `SET NAMES utf8mb4 COLLATE ${CLIENT}`);
    assert.deepEqual(db.now, { cs: 'utf8mb4', coll: CLIENT });
});

test('describe: a problem in a view this repository creates fails, with where it fails', () => {
    const result = {
        views: 3,
        failures: [
            { view: 'ours_v', collation: PROXY, column: null, message: 'm' },
            { view: 'ours_v', collation: PROXY, column: 'kind', message: 'm' },
            { view: 'theirs_v', collation: CLIENT, column: 'kind', message: 'm' },
        ],
        skipped: [{ view: 'dead_v', message: 'gone' }],
    };
    const out = VC.describe(result, new Set(['ours_v']));
    assert.equal(out.failed, true);
    assert.deepEqual(out.lines, [
        'VIEW COLLATION CHECK FAILED: 2 problem(s) in views this repository creates.',
        '  ours_v: cannot be read at all through the RDS Proxy (the live Lambdas) [session utf8mb4_0900_ai_ci]',
        '  ours_v.kind: cannot be compared with a literal through the RDS Proxy (the live Lambdas) [session utf8mb4_0900_ai_ci]',
        '  Why and how to fix: src/lib/view-collations.js, README "Views and the proxy\'s collation".',
        '  Fix with a NEW migration that re-creates the view. Until then every request that reads it fails.',
        '  note: 1 problem(s) in views this repository does not create:',
        '  theirs_v.kind: cannot be compared with a literal through a direct connection (TEST, a local server) [session utf8mb4_unicode_ci]',
        '  note: dead_v was not checked: gone',
    ]);
});

test('describe: someone else\'s view is a note, not a failure; a clean run says so', () => {
    const foreign = VC.describe(
        { views: 2, failures: [{ view: 'theirs_v', collation: PROXY, column: 'kind', message: 'm' }], skipped: [] },
        new Set(['ours_v']));
    assert.equal(foreign.failed, false);
    assert.equal(foreign.lines[0], 'View collation check: 2 view(s) checked, no problem in the views this repository creates.');
    assert.equal(foreign.lines.length, 3);

    const clean = VC.describe({ views: 26, failures: [], skipped: [] }, new Set(['ours_v']));
    assert.deepEqual(clean, { lines: ['View collation check: 26 view(s) readable under both session collations.'], failed: false });

    // No list of ours: every failure is ours.
    assert.equal(VC.describe({ views: 1, failures: [{ view: 'any_v', collation: PROXY, column: null, message: 'm' }], skipped: [] }).failed, true);
});

// ── the runner ──────────────────────────────────────────────────────────────

test('migrate.js viewCheck: 1 when a view of ours fails, 0 when only another\'s does, and it prints the report', async () => {
    const ours = [...runner.ownedViews()];
    assert.ok(ours.includes('portal_forwarder_containers_v'));
    const printed = [];
    const bad = fakeDb({ views: { portal_forwarder_containers_v: ['link_source'] }, broken: [{ view: 'portal_forwarder_containers_v', column: null, collation: PROXY }] });
    assert.equal(await runner.viewCheck(bad, { print: l => printed.push(l) }), 1);
    assert.equal(printed[0], 'VIEW COLLATION CHECK FAILED: 1 problem(s) in views this repository creates.');

    const foreign = fakeDb({ views: { someone_elses_v: ['kind'] }, broken: [{ view: 'someone_elses_v', column: 'kind', collation: PROXY }] });
    assert.equal(await runner.viewCheck(foreign, { print: () => {} }), 0);

    const clean = fakeDb({ views: { portal_forwarder_containers_v: ['link_source'] } });
    assert.equal(await runner.viewCheck(clean, { print: () => {} }), 0);
});

test('migrate.js ownedViews: the views the migration files create', () => {
    const ours = runner.ownedViews();
    for (const v of ['forwarder_duty_holders_v', 'portal_forwarder_containers_v', 'portal_forwarder_clearance_v',
        'portal_forwarder_payments_v', 'portal_payments_v', 'qc_kit_documents']) {
        assert.ok(ours.has(v), `${v} is created by a migration`);
    }
    assert.equal(ours.has('orders'), false);
});

// ── the migrations that fixed what the check found ──────────────────────────

const readMigration = f => fs.readFileSync(path.join(MIGRATIONS_DIR, f), 'utf8').replace(/\r\n/g, '\n');
const flat = sql => sql.replace(/\s+/g, ' ').trim();
// `from` -> `to`, exactly `times` times: a swap that no longer finds its text fails here.
function swap(text, from, to, times) {
    assert.equal(text.split(from).length - 1, times, `expected ${times} of ${JSON.stringify(from)}`);
    return text.split(from).join(to);
}

test('2026-11-25_10: the two forwarder views of 2026-11-24_11 with nothing changed but the collation-safe forms', () => {
    const [oldHolders, oldContainers] = splitSql(readMigration('2026-11-24_11_portal_forwarder_duty_views.sql'));
    const [holders, containers, ...rest] = splitSql(readMigration('2026-11-25_10_forwarder_views_any_session_collation.sql'));
    assert.deepEqual(rest, []);

    let h = flat(oldHolders);
    h = swap(h, "'freight' AS duty,", "CONVERT('freight' USING utf8mb4) AS duty,", 1);
    h = swap(h, "IF(bk.id IS NULL, 'container', 'booking') AS source,", "CONVERT(IF(bk.id IS NULL, 'container', 'booking') USING utf8mb4) AS source,", 1);
    h = swap(h, "s.id, 'delivery',", "s.id, CONVERT('delivery' USING utf8mb4),", 1);
    h = swap(h, "IF(bk.id IS NULL, 'container', 'booking'), bk.id", "CONVERT(IF(bk.id IS NULL, 'container', 'booking') USING utf8mb4), bk.id", 1);
    h = swap(h, "s.id, 'customs', s.customs_forwarder_id, 'container', NULL", "s.id, CONVERT('customs' USING utf8mb4), s.customs_forwarder_id, CONVERT('container' USING utf8mb4), NULL", 1);
    assert.equal(flat(holders), h);

    let c = flat(oldContainers);
    c = swap(c, "IF(DATE_FORMAT(o.delivery_date, '%H:%i') = '00:00',", 'IF(HOUR(o.delivery_date) = 0 AND MINUTE(o.delivery_date) = 0,', 2);
    c = swap(c, "IF(a.does_freight = 1, a.freight_source, 'duty') AS link_source,", "CONVERT(IF(a.does_freight = 1, a.freight_source, 'duty') USING utf8mb4) AS link_source,", 1);
    c = swap(c, "'payment' AS link_source,", "CONVERT('payment' USING utf8mb4) AS link_source,", 1);
    c = swap(c, 'ELT(GREATEST(FIELD(s.stage,', 'CONVERT(ELT(GREATEST(FIELD(s.stage,', 1);
    c = swap(c, "'BOOKED','IN_TRANSIT','ARRIVED','CLOSED') AS stage,", "'BOOKED','IN_TRANSIT','ARRIVED','CLOSED') USING utf8mb4) AS stage,", 1);
    assert.equal(flat(containers), c);

    // What broke production is gone, and no column is a bare literal any more.
    assert.equal(/DATE_FORMAT\([^)]*\)\s*=/.test(containers), false);
    assert.ok(containers.includes("'BOOKED','IN_TRANSIT','ARRIVED','CLOSED') USING utf8mb4) AS stage,"));
    assert.equal(/^\s*'[a-z_]+'\s*(AS\s+\w+\s*)?,\s*$/m.test(holders + containers), false);
});

test('2026-11-25_11: qc_kit_documents of 2026-10-28_20 with nothing changed but attached_via', () => {
    const [old] = splitSql(readMigration('2026-10-28_20_qc_kit_documents_view.sql'));
    const [now, ...rest] = splitSql(readMigration('2026-11-25_11_qc_kit_documents_any_session_collation.sql'));
    assert.deepEqual(rest, []);
    let q = flat(old);
    q = swap(q, "'product' AS attached_via,", "CONVERT('product' USING utf8mb4) AS attached_via,", 1);
    q = swap(q, "'product_group' AS attached_via,", "CONVERT('product_group' USING utf8mb4) AS attached_via,", 1);
    assert.equal(flat(now), q);
});

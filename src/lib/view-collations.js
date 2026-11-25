'use strict';

// Can every view be read under BOTH collations our sessions run with?
//
// THE TRAP (found 2026-11-25, after it had shipped twice). A view keeps the
// collation of the session that CREATED it: every string literal in it has
// that collation for good. tools/migrate.js connects straight to the database,
// where mysql2 asks for utf8mb4_unicode_ci. The deployed Lambdas of the live
// stage connect through the RDS Proxy, which drops what the driver asks for:
// their sessions run under the server's default, utf8mb4_0900_ai_ci. The TEST
// stage and a local server connect direct, so they never see the difference.
//
// A string that is neither a column nor explicitly collated is "coercible",
// and MySQL refuses to compare two coercible strings of different collations
// (ER_CANT_AGGREGATE_2COLLATIONS, "Illegal mix of collations"). So, in a view:
//
//   1. a function's text against a literal fails on production only:
//          DATE_FORMAT(o.delivery_date, '%H:%i') = '00:00'
//      DATE_FORMAT answers in the SESSION's collation, '00:00' has the view's.
//      Every read of the view then fails, whatever the query asks of it.
//   2. a column that is text made only of literals ('container' AS link_source,
//      and just as much IF(…, 'a', 'b'), CASE … THEN 'a', ELT(n, 'a', 'b'))
//      fails when a caller compares it with a literal of its own
//      (link_source IN ('a')).
//
// What to write instead (each form checked on MySQL 8.4, under both sessions):
//   1. compare numbers, dates or real columns: HOUR(x) = 0 AND MINUTE(x) = 0.
//   2. wrap the column: CONVERT('container' USING utf8mb4) AS link_source. Its
//      answer is utf8mb4_0900_ai_ci in every session and behaves like a real
//      column, so a caller's literal yields to it. In a UNION wrap EVERY
//      branch: one bare branch makes the whole column a literal again.
//
// The check: under each collation, resolve every view, then compare each of
// its string columns with a literal. LIMIT 0: MySQL raises this error when it
// resolves the statement, so no row is ever read. Read-only.

const CLIENT_COLLATION = 'utf8mb4_unicode_ci'; // mysql2's default: a direct connection (TEST, local, tools)
const PROXY_COLLATION = 'utf8mb4_0900_ai_ci'; // the server's default: a session through the RDS Proxy
const SESSION_COLLATIONS = [CLIENT_COLLATION, PROXY_COLLATION];
const WHERE_RUN = {
    [CLIENT_COLLATION]: 'a direct connection (TEST, a local server)',
    [PROXY_COLLATION]: 'the RDS Proxy (the live Lambdas)',
};
// "Illegal mix of collations" for two, three and more operands.
const MIX_ERRNOS = new Set([1267, 1270, 1271]);
const isMix = e => MIX_ERRNOS.has(e && e.errno);

const PLAIN_NAME = /^[A-Za-z0-9_]+$/;
const quoted = name => `\`${String(name).replace(/`/g, '``')}\``;

// The statement that resolves `view`, and compares each of `columns` with a
// literal when there are any.
function probeSql(view, columns = []) {
    const where = columns.length
        ? ` WHERE ${columns.map(c => `${quoted(c)} = 'x'`).join(' OR ')}`
        : '';
    return `SELECT 1 FROM ${quoted(view)}${where} LIMIT 0`;
}

// The views a set of migration files creates, by name. Comments are dropped
// first: a header may quote a CREATE VIEW it only talks about.
function viewsCreatedBy(sqlTexts) {
    const names = new Set();
    const re = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:ALGORITHM\s*=\s*\w+\s+)?(?:DEFINER\s*=\s*\S+\s+)?(?:SQL\s+SECURITY\s+\w+\s+)?VIEW\s+`?([A-Za-z0-9_]+)`?/gi;
    for (const text of sqlTexts) {
        const code = String(text).replace(/^\s*(--|\/\/).*$/gm, '');
        for (const m of code.matchAll(re)) names.add(m[1]);
    }
    return names;
}

async function listViews(conn) {
    const [rows] = await conn.query(
        `SELECT v.TABLE_NAME AS view_name, c.COLUMN_NAME AS column_name, c.COLLATION_NAME AS collation_name
           FROM information_schema.VIEWS v
           LEFT JOIN information_schema.COLUMNS c
                  ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
          WHERE v.TABLE_SCHEMA = DATABASE()
          ORDER BY v.TABLE_NAME, c.ORDINAL_POSITION`);
    const views = new Map();
    for (const r of rows) {
        if (!views.has(r.view_name)) views.set(r.view_name, []);
        // Only a column with a collation is text; a date or a number has none.
        if (r.column_name && r.collation_name) views.get(r.view_name).push(r.column_name);
    }
    return views;
}

// Run the check on `conn`'s database. Answers
//   { views, failures: [{ view, collation, column, message }], skipped: [{ view, message }] }
// `column` is null when the view itself cannot be read. A view that fails for
// another reason (a table under it is gone, no right to read it) is `skipped`:
// that is not this check's business, and it must not hide the rest.
// The session's own collation is put back before this returns.
async function checkViewCollations(conn, { only } = {}) {
    const all = await listViews(conn);
    const names = [...all.keys()].filter(n => PLAIN_NAME.test(n) && (!only || only.has(n)));
    const failures = [];
    const skipped = new Map();
    const [[was]] = await conn.query('SELECT @@character_set_client AS cs, @@collation_connection AS coll');
    try {
        for (const collation of SESSION_COLLATIONS) {
            await conn.query(`SET NAMES utf8mb4 COLLATE ${collation}`);
            for (const view of names) {
                if (skipped.has(view)) continue;
                const columns = all.get(view).filter(c => PLAIN_NAME.test(c));
                try {
                    await conn.query(probeSql(view));
                } catch (e) {
                    if (isMix(e)) failures.push({ view, collation, column: null, message: e.message });
                    else skipped.set(view, e.message);
                    continue;
                }
                if (!columns.length) continue;
                try {
                    await conn.query(probeSql(view, columns));
                    continue;
                } catch (e) {
                    if (!isMix(e)) { skipped.set(view, e.message); continue; }
                }
                // One of them clashes: name which.
                for (const column of columns) {
                    try {
                        await conn.query(probeSql(view, [column]));
                    } catch (e) {
                        if (isMix(e)) failures.push({ view, collation, column, message: e.message });
                    }
                }
            }
        }
    } finally {
        if (was && PLAIN_NAME.test(String(was.cs)) && PLAIN_NAME.test(String(was.coll))) {
            await conn.query(`SET NAMES ${was.cs} COLLATE ${was.coll}`);
        }
    }
    return {
        views: names.length,
        failures,
        skipped: [...skipped].map(([view, message]) => ({ view, message })),
    };
}

// The report, as lines. `owned` (names) splits the failures into ours, which
// this repository's migrations created and must fix, and other people's.
function describe(result, owned) {
    const mine = result.failures.filter(f => !owned || owned.has(f.view));
    const others = result.failures.filter(f => owned && !owned.has(f.view));
    const line = f => (f.column == null
        ? `  ${f.view}: cannot be read at all through ${WHERE_RUN[f.collation]} [session ${f.collation}]`
        : `  ${f.view}.${f.column}: cannot be compared with a literal through ${WHERE_RUN[f.collation]} [session ${f.collation}]`);
    const lines = [];
    if (mine.length) {
        lines.push(`VIEW COLLATION CHECK FAILED: ${mine.length} problem(s) in views this repository creates.`);
        lines.push(...mine.map(line));
        lines.push('  Why and how to fix: src/lib/view-collations.js, README "Views and the proxy\'s collation".');
        lines.push('  Fix with a NEW migration that re-creates the view. Until then every request that reads it fails.');
    } else if (!others.length) {
        lines.push(`View collation check: ${result.views} view(s) readable under both session collations.`);
    } else {
        lines.push(`View collation check: ${result.views} view(s) checked, no problem in the views this repository creates.`);
    }
    if (others.length) {
        lines.push(`  note: ${others.length} problem(s) in views this repository does not create:`);
        lines.push(...others.map(line));
    }
    for (const s of result.skipped) lines.push(`  note: ${s.view} was not checked: ${s.message}`);
    return { lines, failed: mine.length > 0 };
}

module.exports = {
    CLIENT_COLLATION, PROXY_COLLATION, SESSION_COLLATIONS,
    probeSql, viewsCreatedBy, checkViewCollations, describe,
};

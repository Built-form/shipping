'use strict';

// The activation check for sample orders (docs/sample-orders.md "Switching it
// on"). Sample orders are armed on a stage by a row in app_migrations, and that
// row goes in only after every check here passes on THAT stage's database:
//
//   · the schema is there (columns, the unique key on the sample code, the
//     view over JFPRO's sample lots);
//   · the other apps that read these tables can see the kind (the supplier
//     portal's login, JFlow's login), and JFlow says its payments model knows
//     the sample rule, so the two apps never disagree about a live sample;
//   · the data holds no row that breaks a sample invariant.
//
// Read-only. Used by tools/check-sample-orders.js; the data checks are the
// reconciliation that keeps running after arming.
//
// A check is 'ok', 'fail' (with the rows), or 'unchecked' (it could not be
// answered: the schema is missing, or the caller did not say which login JFlow
// uses). Unchecked does not fail the report, but nothing is armed with a
// question left open.

const SAMPLE_STATUSES = require('../lib/sample-orders').SAMPLE_STATUSES;

const ORDER_COLUMNS = ['order_kind', 'courier_name', 'courier_tracking', 'sample_received_qty', 'sample_status_changed_at', 'sample_ever_sent', 'sample_lot_key'];
const UNIQUE_KEY = 'uq_orders_sample_lot_key';
const LOT_INDEX = 'idx_orders_lot_number';
const LIST_LIMIT = 50;

const ok = (name, detail = '') => ({ name, status: 'ok', detail, rows: [] });
const fail = (name, detail, rows = []) => ({ name, status: 'fail', detail, rows });
const unchecked = (name, detail) => ({ name, status: 'unchecked', detail, rows: [] });

async function userExists(conn, user) {
    const [rows] = await conn.query('SELECT 1 AS ok FROM mysql.user WHERE user = ? LIMIT 1', [user]);
    return rows.length > 0;
}

// Can `user` SELECT `table`.`column` of this schema? By a privilege on
// everything, on the schema, on the table, or on the column. Any host.
// A login and the roles it has active when it signs in. A login's rights can be
// a role's and not its own: RDS gives its master login everything through
// rds_superuser_role, and the privilege tables then hold nothing under the
// login's name. A role counts only when it is one of the login's default roles,
// or the server activates every granted role at login. A server with no role
// tables has no roles.
async function granteesOf(conn, user) {
    try {
        const [rows] = await conn.query(
            `SELECT e.FROM_USER AS role FROM mysql.role_edges e
              WHERE e.TO_USER = ?
                AND (@@global.activate_all_roles_on_login = 1
                     OR EXISTS (SELECT 1 FROM mysql.default_roles d
                                 WHERE d.USER = e.TO_USER AND d.DEFAULT_ROLE_USER = e.FROM_USER))`,
            [user]
        );
        return [user, ...rows.map(r => r.role)];
    } catch (err) {
        return [user];
    }
}

// Can `user` SELECT this column: by its own rights, or by a role's?
async function canSelect(conn, user, table, column) {
    for (const grantee of await granteesOf(conn, user)) {
        if (await holdsSelect(conn, grantee, table, column)) return true;
    }
    return false;
}

async function holdsSelect(conn, user, table, column) {
    // information_schema spells a grantee 'user'@'host'.
    const [rows] = await conn.query(
        `SELECT 1 AS ok FROM information_schema.USER_PRIVILEGES
          WHERE GRANTEE LIKE CONCAT(QUOTE(?), '@%') AND PRIVILEGE_TYPE = 'SELECT'
         UNION ALL
         SELECT 1 FROM information_schema.SCHEMA_PRIVILEGES
          WHERE GRANTEE LIKE CONCAT(QUOTE(?), '@%') AND TABLE_SCHEMA = DATABASE() AND PRIVILEGE_TYPE = 'SELECT'
         UNION ALL
         SELECT 1 FROM information_schema.TABLE_PRIVILEGES
          WHERE GRANTEE LIKE CONCAT(QUOTE(?), '@%') AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND PRIVILEGE_TYPE = 'SELECT'
         UNION ALL
         SELECT 1 FROM information_schema.COLUMN_PRIVILEGES
          WHERE GRANTEE LIKE CONCAT(QUOTE(?), '@%') AND TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
            AND PRIVILEGE_TYPE = 'SELECT'
          LIMIT 1`,
        [user, user, user, table, user, table, column]
    );
    return rows.length > 0;
}

async function schemaChecks(conn) {
    const out = [];
    const [cols] = await conn.query(
        `SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME) AS name, COLUMN_NAME AS col, TABLE_NAME AS tbl
           FROM information_schema.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND ((TABLE_NAME = 'orders' AND COLUMN_NAME IN (${ORDER_COLUMNS.map(() => '?').join(', ')}))
              OR (TABLE_NAME = 'purchase_orders' AND COLUMN_NAME = 'kind'))`,
        ORDER_COLUMNS
    );
    const have = new Set(cols.map(c => c.col || String(c.name).split('.').pop()));
    const missing = [...ORDER_COLUMNS, 'kind'].filter(c => !have.has(c));
    out.push(missing.length
        ? fail('schema: columns', `missing: ${missing.join(', ')}. Run the migrations (2026-11-25_20).`)
        : ok('schema: columns'));

    const [idx] = await conn.query(
        `SELECT DISTINCT INDEX_NAME AS name FROM information_schema.STATISTICS
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders' AND INDEX_NAME IN (?, ?)`,
        [UNIQUE_KEY, LOT_INDEX]
    );
    const indexes = new Set(idx.map(i => i.name));
    out.push(indexes.has(UNIQUE_KEY)
        ? ok('schema: unique key on the sample code')
        : fail('schema: unique key on the sample code', `${UNIQUE_KEY} is missing: two lines could carry one sample code.`));
    out.push(indexes.has(LOT_INDEX)
        ? ok('schema: index on the lot number')
        : fail('schema: index on the lot number', `${LOT_INDEX} is missing: JFPRO's guard would scan every order.`));

    try {
        await conn.query('SELECT lot_id FROM sample_lots_v LIMIT 1');
        out.push(ok('schema: sample_lots_v can be read'));
    } catch (err) {
        out.push(fail('schema: sample_lots_v can be read', `${err.message}. Run migration 2026-11-25_21 (it needs JFPRO's "samples are lots" schema on this server).`));
    }
    return { results: out, complete: missing.length === 0 && out.every(r => r.status === 'ok') };
}

async function loginChecks(conn, { portalUser, jflowUser, jflowReportsSamples }, schemaOk) {
    const out = [];
    const portalName = 'portal login can read purchase_orders.kind';
    if (!schemaOk) {
        out.push(unchecked(portalName, 'the schema is not there yet'));
    } else if (!await userExists(conn, portalUser)) {
        out.push(ok(portalName, `no '${portalUser}' login on this server: the portal does not read this database`));
    } else if (await canSelect(conn, portalUser, 'purchase_orders', 'kind')) {
        out.push(ok(portalName));
    } else {
        out.push(fail(portalName, `'${portalUser}' cannot SELECT purchase_orders.kind. Run migration 2026-11-25_22: without it the portal shows a sample order read-only.`));
    }

    const jflowName = 'JFlow login can read the kind columns';
    if (!jflowUser) {
        out.push(unchecked(jflowName, 'say which login JFlow uses: --jflow-user <name>'));
    } else if (!schemaOk) {
        out.push(unchecked(jflowName, 'the schema is not there yet'));
    } else if (!await userExists(conn, jflowUser)) {
        out.push(fail(jflowName, `no '${jflowUser}' login on this server`));
    } else {
        const po = await canSelect(conn, jflowUser, 'purchase_orders', 'kind');
        const line = await canSelect(conn, jflowUser, 'orders', 'order_kind');
        out.push(po && line ? ok(jflowName)
            : fail(jflowName, `'${jflowUser}' cannot SELECT ${[!po && 'purchase_orders.kind', !line && 'orders.order_kind'].filter(Boolean).join(' and ')}: JFlow would forecast a sample as a stock order.`));
    }

    const modelName = 'JFlow reports the sample payment rule';
    if (jflowReportsSamples === true) out.push(ok(modelName));
    else if (jflowReportsSamples === false) out.push(fail(modelName, 'the deployed JFlow does not report the sample rule: re-pin and deploy it first, or its forecast will disagree with ShipLine.'));
    else out.push(unchecked(modelName, 'ask the deployed JFlow: --jflow-health <url of its health route>'));
    return out;
}

// A deleted draft that was never sent holds no reservation and is not checked.
const COUNTED = '(o.deleted_at IS NULL OR o.sample_ever_sent = 1)';

const DATA_CHECKS = [
    ['data: every line has its PO\'s kind',
        `SELECT o.id, o.order_kind, po.id AS purchase_order_id, po.kind
           FROM orders o INNER JOIN purchase_orders po ON po.id = o.purchase_order_id
          WHERE o.order_kind <> po.kind AND ${COUNTED} ORDER BY o.id LIMIT ${LIST_LIMIT}`],
    ['data: every sample line has a PO',
        `SELECT o.id FROM orders o
          WHERE o.order_kind = 'sample' AND o.purchase_order_id IS NULL AND ${COUNTED} ORDER BY o.id LIMIT ${LIST_LIMIT}`],
    ['data: no unknown kind',
        `SELECT o.id, o.order_kind FROM orders o
          WHERE o.order_kind NOT IN ('stock', 'sample') ORDER BY o.id LIMIT ${LIST_LIMIT}`],
    ['data: no sample in a container, AWB or shipment',
        `SELECT o.id, o.container_number, o.awb_number, o.external_container_number, o.shipment_id FROM orders o
          WHERE o.order_kind = 'sample' AND o.deleted_at IS NULL
            AND (o.container_number IS NOT NULL AND o.container_number <> ''
              OR o.awb_number IS NOT NULL AND o.awb_number <> ''
              OR o.external_container_number IS NOT NULL AND o.external_container_number <> ''
              OR o.shipment_id IS NOT NULL)
          ORDER BY o.id LIMIT ${LIST_LIMIT}`],
    ['data: every sample\'s lot is a sample of its product in JFPRO',
        `SELECT o.id, o.jf_code, o.lot_number FROM orders o
          WHERE o.order_kind = 'sample' AND ${COUNTED}
            AND NOT EXISTS (SELECT 1 FROM sample_lots_v v
                             WHERE UPPER(TRIM(v.lot_number)) = UPPER(TRIM(o.lot_number))
                               AND UPPER(TRIM(v.jf_code)) = UPPER(TRIM(o.jf_code)))
          ORDER BY o.id LIMIT ${LIST_LIMIT}`],
    ['data: every sample is in a sample state',
        `SELECT o.id, o.status FROM orders o
          WHERE o.order_kind = 'sample' AND o.deleted_at IS NULL
            AND o.status NOT IN (${SAMPLE_STATUSES.map(s => `'${s}'`).join(', ')})
          ORDER BY o.id LIMIT ${LIST_LIMIT}`],
];

async function dataChecks(conn, schemaOk) {
    const out = [];
    for (const [name, sql] of DATA_CHECKS) {
        if (!schemaOk) { out.push(unchecked(name, 'the schema is not there yet')); continue; }
        const [rows] = await conn.query(sql);
        out.push(rows.length ? fail(name, `${rows.length}${rows.length === LIST_LIMIT ? '+' : ''} row(s)`, rows) : ok(name));
    }
    return out;
}

/**
 * Every check, in order. opts: { portalUser, jflowUser, jflowReportsSamples }.
 * Returns [{ name, status: 'ok' | 'fail' | 'unchecked', detail, rows }].
 */
async function runChecks(conn, opts = {}) {
    const schema = await schemaChecks(conn);
    return [
        ...schema.results,
        ...await loginChecks(conn, { portalUser: opts.portalUser || 'jportal_app', jflowUser: opts.jflowUser || null, jflowReportsSamples: opts.jflowReportsSamples ?? null }, schema.complete),
        ...await dataChecks(conn, schema.complete),
    ];
}

// ok: nothing failed. canArm: nothing failed AND nothing was left unanswered.
function summarise(results) {
    const failed = results.filter(r => r.status === 'fail').map(r => r.name);
    const open = results.filter(r => r.status === 'unchecked').map(r => r.name);
    return { ok: failed.length === 0, canArm: failed.length === 0 && open.length === 0, failed, unchecked: open };
}

// Does the deployed JFlow say its payments model knows the sample rule? Its
// health route answers { ..., paymentsFlow: { sampleOrders: true } } when the
// model it runs does (JFlow asks the model itself). The route sits behind
// JFlow's gateway sign-in, so it is read with a token (a signed-in Google ID
// token); the token is sent and never printed.
//   → { answer: true | false | null, detail }   null = no URL given (unasked);
//     anything short of a clear yes is false, with the reason in `detail`.
async function askJflowHealth(url, { fetchImpl = fetch, token = null } = {}) {
    if (!url) return { answer: null, detail: null };
    try {
        const headers = { accept: 'application/json' };
        if (token) headers.authorization = `Bearer ${token}`;
        const res = await fetchImpl(url, { headers });
        if (!res.ok) {
            const signIn = res.status === 401 || res.status === 403
                ? ' (the route needs a signed-in token: set JFLOW_TOKEN)'
                : '';
            return { answer: false, detail: `JFlow's health route answered ${res.status}${signIn}` };
        }
        const body = await res.json();
        const yes = Boolean(body && (body.sampleOrders === true || (body.paymentsFlow && body.paymentsFlow.sampleOrders === true)));
        return yes
            ? { answer: true, detail: null }
            : { answer: false, detail: "JFlow's health route does not report the sample rule: its payments model is not re-pinned yet" };
    } catch (err) {
        return { answer: false, detail: `JFlow's health route could not be read: ${err.message}` };
    }
}

const reportsSampleRule = body =>
    Boolean(body && (body.sampleOrders === true || (body.paymentsFlow && body.paymentsFlow.sampleOrders === true)));

// The same question, asked of JFlow's Lambda directly instead of through its
// gateway: the route an operator with deploy rights always has, and it needs no
// browser token. `invoke(functionName, event)` runs the function and gives back
// what it returned (the tool does it with `aws lambda invoke`). The event is a
// plain GET of the health route, which answers without a signed-in user.
//   → { answer: true | false | null, detail }, as askJflowHealth.
const HEALTH_EVENT = Object.freeze({
    version: '2.0',
    routeKey: 'ANY /{proxy+}',
    rawPath: '/api/v1/health',
    rawQueryString: '',
    headers: { accept: 'application/json', host: 'direct-invoke' },
    requestContext: { http: { method: 'GET', path: '/api/v1/health', sourceIp: '127.0.0.1', protocol: 'HTTP/1.1' }, stage: '$default' },
    isBase64Encoded: false,
});

async function askJflowLambda(functionName, { invoke } = {}) {
    if (!functionName) return { answer: null, detail: null };
    try {
        const res = await invoke(functionName, JSON.parse(JSON.stringify(HEALTH_EVENT)));
        if (res && res.errorType) {
            return { answer: false, detail: `JFlow's function failed: ${res.errorType}${res.errorMessage ? ` (${res.errorMessage})` : ''}` };
        }
        const status = res ? Number(res.statusCode) : NaN;
        if (status !== 200) return { answer: false, detail: `JFlow's health route answered ${Number.isFinite(status) ? status : 'nothing'}` };
        const body = JSON.parse(res.body);
        return reportsSampleRule(body)
            ? { answer: true, detail: null }
            : { answer: false, detail: "JFlow's health route does not report the sample rule: its payments model is not re-pinned yet" };
    } catch (err) {
        return { answer: false, detail: `JFlow's function could not be asked: ${err.message}` };
    }
}

module.exports = { runChecks, summarise, canSelect, DATA_CHECKS, askJflowHealth, askJflowLambda };

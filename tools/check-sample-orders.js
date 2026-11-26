'use strict';

// Sample orders: the activation check, and the switch (docs/sample-orders.md
// "Switching it on").
//
//   node tools/check-sample-orders.js --stage test                 report; exit 1 on any failure
//   node tools/check-sample-orders.js --stage test --jflow-user <login> --jflow-health <url>
//   node tools/check-sample-orders.js --stage test ... --arm       the same, then switch sample orders ON for this stage
//   node tools/check-sample-orders.js --stage test --disarm        switch them OFF (existing sample orders stay usable)
//
// --stage picks the database the way tools/migrate.js does ('dev' is
// PRODUCTION, 'test' is TEST). The switch is one row in app_migrations named
// for the stage (sample_orders_on:<stage>), so the row a TEST rebuild copies
// from production never arms TEST: after every rebuild, run this again for test.
//
// --arm writes ONLY when every check passed and none was left unanswered:
//   --jflow-user    the MySQL login JFlow reads this schema with (it must be
//                   able to read purchase_orders.kind and orders.order_kind)
//   --jflow-health  the URL of the deployed JFlow's health route for this
//                   stage (https://<its api>/api/v1/health); it must answer
//                   { ..., paymentsFlow: { sampleOrders: true } }, which it
//                   does once its payments model is re-pinned to a ShipLine
//                   commit with the sample rule. The route sits behind
//                   JFlow's sign-in: put a signed-in Google ID token in the
//                   JFLOW_TOKEN environment variable (never on the command
//                   line; it is sent, not printed).
//   --jflow-function  instead of --jflow-health: the name of JFlow's Lambda for
//                   this stage (jflow-test-jflowApi). The same health route,
//                   asked of the function directly with `aws lambda invoke`:
//                   no token, only the rights a deploy already needs.
// Without them the report still runs; those two lines read "unchecked".
//
// Read-only except --arm and --disarm, each of which writes one row.

const mysql = require('mysql2/promise');
const { resolveTarget, checkTarget, Refusal } = require('./migrate');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { runChecks, summarise, askJflowHealth, askJflowLambda } = require('../src/services/sample-orders-check');
const S = require('../src/lib/sample-orders');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);

// Does the deployed JFlow say its payments model knows the sample rule?
// true / false, or null when no URL was given. Anything short of a clear yes
// is a no, and says why (src/services/sample-orders-check.js askJflowHealth).
async function askJflow(url, functionName) {
    const { answer, detail } = functionName
        ? await askJflowLambda(functionName, { invoke: invokeLambda })
        : await askJflowHealth(url, { token: process.env.JFLOW_TOKEN || null });
    if (detail) console.log(`  (${detail})`);
    return answer;
}

// Run a Lambda with one event and give back what it returned. Read-only for the
// health route. The event and the answer pass through two files in a folder of
// their own under the system's temp folder, removed afterwards.
async function invokeLambda(functionName, event) {
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(functionName)) throw new Error('not a function name');
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sample-check-'));
    try {
        const eventFile = path.join(dir, 'event.json');
        const outFile = path.join(dir, 'out.json');
        fs.writeFileSync(eventFile, JSON.stringify(event));
        execFileSync('aws', [
            'lambda', 'invoke', '--region', process.env.AWS_REGION || 'eu-north-1',
            '--function-name', functionName, '--cli-binary-format', 'raw-in-base64-out',
            '--payload', `file://${eventFile}`, outFile,
        ], { stdio: ['ignore', 'ignore', 'pipe'], shell: process.platform === 'win32' });
        return JSON.parse(fs.readFileSync(outFile, 'utf8'));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
}

async function main() {
    const stage = arg('--stage');
    if (!stage) throw new Refusal('--stage is required (test, or dev for production): the switch is named for the stage');
    const arm = flag('--arm');
    const disarm = flag('--disarm');
    if (arm && disarm) throw new Refusal('--arm or --disarm, not both');

    const target = resolveTarget();
    checkTarget(target, { stage, writes: arm || disarm });
    console.log(`Database: ${target.user || '?'}@${target.host}/${target.database}  (${target.source}, stage ${stage})`);
    const rowName = S.armingRowName(stage);

    const conn = await mysql.createConnection({
        host: target.host, user: target.user, password: target.password,
        database: target.database, port: target.port,
        ssl: { rejectUnauthorized: false }, connectTimeout: 20000,
    });
    try {
        const [armedRows] = await conn.query('SELECT name FROM app_migrations WHERE name = ?', [rowName]);
        console.log(`Sample orders on this stage: ${armedRows.length ? 'ON' : 'off'}  (${rowName})\n`);

        if (disarm) {
            const [done] = await conn.query('DELETE FROM app_migrations WHERE name = ?', [rowName]);
            console.log(done.affectedRows ? 'Switched OFF. No new sample order can be created; existing ones stay usable.' : 'Already off.');
            return 0;
        }

        const results = await runChecks(conn, {
            portalUser: arg('--portal-user') || 'jportal_app',
            jflowUser: arg('--jflow-user'),
            jflowReportsSamples: await askJflow(arg('--jflow-health'), arg('--jflow-function')),
        });
        const mark = { ok: 'ok  ', fail: 'FAIL', unchecked: '??  ' };
        for (const r of results) {
            console.log(`  ${mark[r.status]}  ${r.name}${r.detail ? `: ${r.detail}` : ''}`);
            for (const row of r.rows.slice(0, 20)) console.log(`          ${JSON.stringify(row)}`);
            if (r.rows.length > 20) console.log(`          ... ${r.rows.length - 20} more`);
        }
        const s = summarise(results);
        console.log(`\n${s.failed.length} failed, ${s.unchecked.length} unchecked.`);

        if (!arm) return s.ok ? 0 : 1;
        if (!s.canArm) {
            console.log('NOT switched on: every check must pass, with none unchecked.');
            return 1;
        }
        await conn.query('INSERT IGNORE INTO app_migrations (name) VALUES (?)', [rowName]);
        console.log(`Switched ON for stage ${stage}. A running server sees it within a minute.`);
        return 0;
    } finally {
        await conn.end();
    }
}

main().then(code => process.exit(code)).catch((err) => {
    console.error(err instanceof Refusal ? `Refused: ${err.message}` : err);
    process.exit(2);
});

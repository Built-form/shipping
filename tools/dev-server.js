'use strict';

// Local API for frontend development — the same Express app the Lambda runs,
// served on http://localhost:3031 with auth bypassed, so a ShipLine dev server
// can exercise routes that are not deployed yet (the JFPRO api/ has the same
// arrangement — and it owns port 3001, which is why this one is 3031: the two
// backends are routinely up at the same time).
//
//   npm run dev:local                      # this file
//   ORDERS_PORT=3011 npm run dev:local     # another port
//   LOCAL_USER_TYPE=standard npm run dev:local
//
// Then in ShipLine, `.env.development.local` with
//   VITE_API_BASE_URL=http://localhost:3031
// and `npm run dev:local` (port 3030). See README "Local development".
//
// Reads .env. Whichever DB_HOST / DB_NAME it names is the database this
// server WRITES to — it is printed below before anything else happens. Keep
// .env pointed at the TEST instance for this. Requests are attributed to
// `local@dev` with the role from LOCAL_USER_TYPE (admin here by default, so
// admin-gated routes can be tried; the deployed default is `standard`).
// Emails (Front), Mintsoft and S3 are the real integrations from .env.

process.env.IS_OFFLINE = process.env.IS_OFFLINE || '1';
process.env.LOCAL_USER_TYPE = process.env.LOCAL_USER_TYPE || 'admin';
process.env.ORDERS_PORT = process.env.ORDERS_PORT || '3031';

require('dotenv').config();

const host = process.env.DB_HOST || '(DB_HOST unset)';
const db = process.env.DB_NAME || '(DB_NAME unset)';
const looksTest = /test/i.test(host) || /test/i.test(db);
console.log(`[dev-server] database ${process.env.DB_USER || '?'}@${host}/${db}${looksTest ? '' : '   <-- does not look like the TEST instance'}`);
console.log(`[dev-server] auth bypassed: every request is local@dev (${process.env.LOCAL_USER_TYPE})`);

const { app } = require('../src/handlers/orders');

// Which database this server writes to, so the HTTP suites that create, pack
// and close real orders (tools/test-shipments-*.js) can refuse to run against
// anything that does not look like the TEST instance. Local server only.
app.get('/api/v1/_dev/db', (req, res) => res.json({ host, database: db, looksTest }));

const port = Number(process.env.ORDERS_PORT);
app.listen(port, () => {
    console.log(`[dev-server] http://localhost:${port}  (Ctrl+C to stop)`);
});

// The app runs no DDL: schema comes from src/db/migrate/. Say so up front when
// this database is behind, rather than as a 500 on the first route that needs it.
const { getPool } = require('../src/db');
const { findPendingMigrations } = require('../src/lib/schema-migrations');
findPendingMigrations(getPool()).then((pending) => {
    const fix = `node tools/migrate.js --apply --confirm-host ${host}`;
    if (pending === null) console.log(`[dev-server] this database has never been migrated (no schema_migrations): ${fix}`);
    else if (pending.length) console.log(`[dev-server] ${pending.length} migration(s) not applied here (${pending.join(', ')}): ${fix}`);
}).catch(err => console.log(`[dev-server] schema check failed: ${err.message}`));

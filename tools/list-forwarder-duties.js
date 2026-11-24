'use strict';
// Several forwarders on one container, by duty (docs/forwarder-containers.md,
// 2026-11-24). READ ONLY: lists everything on this database that a shipping
// build from BEFORE that change would misread. Run it before rolling shipping
// back below it; nothing else needs it.
//
//   node tools/list-forwarder-duties.js
//
// It lists:
//   1. containers with a delivery or a customs pick;
//   2. live freight bookings that cover more than the freight (scope is not
//      'port_to_port');
//   3. what a forwarder sent and nobody has decided, when it needs a duty other
//      than freight: a proposal with a field of another duty, a file of another
//      duty's kind, or anything sent by a company that holds no freight there;
//   4. documents kept for a container that are not bills of lading (an approved
//      draft customs entry, or a customs entry with its MRN);
//   5. delivery requests (a quote request of kind 'delivery'), in any state;
//   6. containers with two live bookings, one per leg.
//
// Shipping may go back only when 1 to 3 and 6 are empty (clear the picks, set
// the scopes back, decide what waits). 4 and 5 are different: an older build
// lists every kept document as a bill of lading, and reads a delivery request
// as a freight request (its booking would make the haulier the container's
// forwarder); neither kind of record is deleted to make a rollback possible.
// Once 4 or 5 lists anything, fix forward instead. 5 holds for the portal too.
//
// Exit code 0 when it lists nothing, 1 when it lists something, 2 on a fault.
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const FC = require('../src/lib/forwarder-containers');
const { jportalSchema } = require('../src/lib/jportal');

const ER_NO_SUCH_TABLE = 1146;
const ER_BAD_DB = 1049;
const missing = e => Boolean(e) && (e.errno === ER_NO_SUCH_TABLE || e.errno === ER_BAD_DB);

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    console.log(`Database ${process.env.DB_USER || '?'}@${process.env.DB_HOST || '?'}/${process.env.DB_NAME || '?'}; read only`);
    let listed = 0;
    const section = (title, lines, note) => {
        console.log(`\n${title}: ${lines.length}`);
        for (const line of lines) console.log(`  ${line}`);
        if (lines.length && note) console.log(`  -> ${note}`);
        listed += lines.length;
    };
    try {
        const [picks] = await conn.query(
            `SELECT s.id, s.reference, s.name, s.stage, s.delivery_forwarder_id, df.name AS delivery_name, s.customs_forwarder_id, cf.name AS customs_name
               FROM shipments s
               LEFT JOIN forwarders df ON df.id = s.delivery_forwarder_id
               LEFT JOIN forwarders cf ON cf.id = s.customs_forwarder_id
              WHERE s.deleted_at IS NULL AND (s.delivery_forwarder_id IS NOT NULL OR s.customs_forwarder_id IS NOT NULL)
              ORDER BY s.id`
        );
        section('1. Containers with a delivery or customs pick', picks.map(p => `container ${p.reference || p.name || p.id} (shipment ${p.id}, ${p.stage}): `
            + [p.delivery_forwarder_id != null ? `delivery = ${p.delivery_name || p.delivery_forwarder_id}` : null,
                p.customs_forwarder_id != null ? `customs = ${p.customs_name || p.customs_forwarder_id}` : null].filter(Boolean).join(', ')),
        'clear them on the container (Shipments page), or PATCH /shipments/:id { deliveryForwarderId: null, customsForwarderId: null }');

        const [bookings] = await conn.query(
            `SELECT fb.id, fb.status, fb.scope, fb.shipment_id, fr.forwarder_name
               FROM freight_bookings fb JOIN freight_quote_requests fr ON fr.id = fb.request_id
              WHERE fb.status IN (?) AND fb.scope <> ?
              ORDER BY fb.id`,
            [FC.LIVE_BOOKING_STATUSES, FC.DEFAULT_SCOPE]
        );
        section('2. Live bookings that cover more than the freight', bookings.map(b => `booking ${b.id} (${b.status}, ${b.forwarder_name || '?'}): scope ${b.scope}${b.shipment_id != null ? `, on shipment ${b.shipment_id}` : ''}`),
            `set them back on the Freight page, or PATCH /freight-bookings/:id { scope: '${FC.DEFAULT_SCOPE}' }`);

        const jp = jportalSchema(process.env);
        let waiting = [];
        try {
            const [proposals] = await conn.query(
                `SELECT d.id, d.shipment_id, d.forwarder_id, d.requested_json, c.link_source
                   FROM ${jp}.forwarder_container_details d
                   LEFT JOIN portal_forwarder_containers_v c ON c.shipment_id = d.shipment_id AND c.forwarder_id = d.forwarder_id AND c.link_source <> 'payment'
                  WHERE d.status IN ('pending', 'applying')
                    AND d.id = (SELECT MAX(x.id) FROM ${jp}.forwarder_container_details x WHERE x.shipment_id = d.shipment_id AND x.forwarder_id = d.forwarder_id)
                  ORDER BY d.id`
            );
            for (const p of proposals) {
                let raw = {};
                try { raw = typeof p.requested_json === 'string' ? JSON.parse(p.requested_json) : (p.requested_json || {}); } catch { raw = {}; }
                // Every key it carries, known to this build or not: an older build knows fewer.
                const other = Object.keys(raw || {}).filter(f => FC.FIELD_DUTY[f] !== 'freight');
                if (other.length || p.link_source === 'duty') {
                    waiting.push(`proposal ${p.id} (shipment ${p.shipment_id}, forwarder ${p.forwarder_id}): ${other.length ? `asks for ${other.join(', ')}` : 'from a company that does not carry the freight'}`);
                }
            }
            const [uploads] = await conn.query(
                `SELECT u.id, u.shipment_id, u.forwarder_id, u.kind, c.link_source
                   FROM ${jp}.forwarder_uploads u
                   LEFT JOIN portal_forwarder_containers_v c ON c.shipment_id = u.shipment_id AND c.forwarder_id = u.forwarder_id AND c.link_source <> 'payment'
                  WHERE u.upload_status = 'sent' AND u.review_status = 'open'
                  ORDER BY u.id`
            );
            for (const u of uploads) {
                const duty = FC.KIND_DUTY[u.kind];
                const otherKind = duty !== null && duty !== 'freight';
                if (otherKind || u.link_source === 'duty') {
                    waiting.push(`file ${u.id} (${u.kind}, shipment ${u.shipment_id}, forwarder ${u.forwarder_id}): ${otherKind ? 'a kind an older build does not know' : 'from a company that does not carry the freight'}`);
                }
            }
        } catch (e) {
            if (!missing(e)) throw e;
            waiting = [];
            console.log("\n(The portal's tables are not on this server: nothing a forwarder sent can be read.)");
        }
        section('3. Sent by a forwarder, undecided, and not about the freight', waiting, 'accept or send back each one in ShipLine first');

        const [documents] = await conn.query(
            `SELECT d.id, d.shipment_id, d.shipment_reference, d.kind, d.forwarder_name, d.filename
               FROM shipment_forwarder_documents d
              WHERE d.deleted_at IS NULL AND d.kind <> 'bill_of_lading'
              ORDER BY d.id`
        );
        section('4. Documents kept that are not bills of lading', documents.map(d => `document ${d.id} (${d.kind}, container ${d.shipment_reference || d.shipment_id}, ${d.forwarder_name || '?'}): ${d.filename}`),
            'these are never deleted for a rollback: do NOT roll shipping back below 2026-11-24, fix forward');

        // 5. The delivery leg through Freight quotes. A build from before it reads a
        // delivery request as a freight request and a delivery booking as a freight
        // booking (the container's forwarder would become the haulier), and it no
        // longer has a key behind "one live booking per container".
        const [deliveries] = await conn.query(
            `SELECT r.id, r.forwarder_name, r.revoked_at, r.created_at,
                    (SELECT COUNT(*) FROM freight_bookings b WHERE b.request_id = r.id) AS bookings
               FROM freight_quote_requests r
              WHERE r.kind = 'delivery'
              ORDER BY r.id`
        );
        section('5. Delivery requests (a UK haulier, port to warehouse), in any state',
            deliveries.map(r => `request ${r.id} (${r.forwarder_name || '?'}${r.revoked_at ? ', closed' : ''}): ${Number(r.bookings)} booking(s)`),
            'once one exists, do NOT roll shipping or the portal back below 2026-11-24: fix forward');
        const [twice] = await conn.query(
            `SELECT b.shipment_id, COUNT(*) AS n, GROUP_CONCAT(CONCAT(b.id, ':', b.scope) ORDER BY b.id SEPARATOR ', ') AS which
               FROM freight_bookings b
              WHERE b.status IN ('requested', 'confirmed') AND b.shipment_id IS NOT NULL
              GROUP BY b.shipment_id
             HAVING COUNT(*) > 1`
        );
        section('6. Containers with two live bookings (one per leg)', twice.map(t => `shipment ${t.shipment_id}: bookings ${t.which}`),
            'an older build allows one live booking per container');

        console.log(listed ? `\n${listed} thing(s) listed. Shipping is NOT safe to roll back below 2026-11-24 yet.` : '\nNothing listed: an older shipping build would read this database as it did.');
        process.exitCode = listed ? 1 : 0;
    } catch (e) {
        console.error('STOPPED:', e.message);
        process.exitCode = 2;
    } finally {
        conn.release();
        await closePool();
    }
})();

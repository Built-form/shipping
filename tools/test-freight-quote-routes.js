'use strict';

// Route tests for src/services/freight-quote-routes.js (docs/freight-quotes.md)
// against an in-memory stand-in for the five tables: a send to two forwarders
// and its retry, a Front failure, booking an option, the forwarder's
// confirmation closing the other link as `filled`, a cancel reopening it, and
// the contents email's claim. The fake answers the statements the routes
// actually send and throws on any other, so a changed query shows up here.
// No database, no Front.
//   node --test tools/test-freight-quote-routes.js

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');
const { registerFreightQuoteRoutes } = require('../src/services/freight-quote-routes');

process.env.SUPPLIER_PORTAL_URL = 'https://portal.example';

const BATCH = 'a1b2c3d4-0000-4000-8000-000000000001';
const NOW = () => new Date('2026-11-16T10:00:00Z');
const future = days => new Date(NOW().getTime() + days * 86400000);

function harness({ userType = 'standard', frontOk = () => true, documentSet = null } = {}) {
    const db = {
        forwarders: [
            { id: 1, name: 'DCG', notes: null, deleted_at: null },
            { id: 2, name: 'Savino', notes: null, deleted_at: null },
        ],
        contacts: [
            { id: 1, forwarder_id: 1, name: 'Amy', email: 'amy@dcg.com', deleted_at: null },
            { id: 2, forwarder_id: 2, name: null, email: 'ops@savino.com', deleted_at: null },
        ],
        requests: [], options: [], bookings: [], movements: [], movementSeq: 0,
        deliveryOptions: [{ id: 1, label: 'Door to door', deleted_at: null }, { id: 2, label: 'Door to port', deleted_at: null }],
        shipments: [
            { id: 50, name: 'Draft 12', reference: null, stage: 'DRAFT', deleted_at: null, merged_into_id: null },
            // A second draft, for the same quote booked for another container.
            { id: 52, name: 'Draft 13', reference: null, stage: 'DRAFT', deleted_at: null, merged_into_id: null },
            // A booked container: a delivery leg is usually booked when it is already on the water.
            { id: 51, name: null, reference: '339', stage: 'IN_TRANSIT', deleted_at: null, merged_into_id: null },
        ],
        audits: [], receipts: [], receiptReminders: [], sent: [], preapproved: [],
        // draft_container_documents rows and their sends (a contents email with documents).
        documents: [], documentSends: [],
        // payment_extras rows priced from a booking or a movement (forwarder charges).
        extras: [],
        // A shipment id another booking takes at the last moment (the unique key refuses).
        raceShipment: null,
    };
    // One live booking per LEG of a container: two unique keys (2026-11-24).
    const containerDup = (id, leg = 'freight') => Object.assign(
        new Error(`Duplicate entry '${id}' for key 'freight_bookings.uq_freight_bookings_live_${leg}'`), { errno: 1062, code: 'ER_DUP_ENTRY' });
    const legsOfScope = scope => ({ port_to_port: ['freight'], end_to_end: ['freight', 'delivery'], delivery: ['delivery'] }[scope || 'port_to_port']);
    const kindOf = requestId => (db.requests.find(r => r.id === requestId) || {}).kind || 'freight';
    const live = b => b.status === 'requested' || b.status === 'confirmed';
    const requestRow = r => ({ ...r, expired: r.expires_at <= NOW() ? 1 : 0 });
    // DATE_ADD(o.sailing_date, INTERVAL (o.transit_days + days) DAY), as 'YYYY-MM-DD'.
    const arrivesPlus = (b, days) => {
        const o = db.options.find(x => x.id === b.option_id);
        return new Date(Date.parse(`${o.sailing_date}T00:00:00Z`) + (Number(o.transit_days) + days) * 86400000).toISOString().slice(0, 10);
    };
    const ok = n => [{ affectedRows: n }];

    const conn = {
        beginTransaction: async () => {}, commit: async () => {}, rollback: async () => {},
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            // forwarders
            if (s.startsWith('SELECT id, name, notes, created_by_email, created_at, updated_at FROM forwarders')) {
                const ids = params[0];
                return [db.forwarders.filter(f => !f.deleted_at && (!ids || ids.includes(f.id)))];
            }
            if (s.startsWith('SELECT id, forwarder_id, name, email FROM forwarder_contacts')) {
                return [db.contacts.filter(c => !c.deleted_at && params[0].includes(c.forwarder_id))];
            }
            if (s.startsWith('SELECT email FROM forwarder_contacts')) {
                return [db.contacts.filter(c => !c.deleted_at && c.forwarder_id === params[0])];
            }
            // delivery options
            if (s.startsWith('SELECT id, label FROM freight_delivery_options WHERE deleted_at IS NULL')) {
                return [db.deliveryOptions.filter(o => !o.deleted_at)];
            }
            if (s.startsWith('INSERT INTO freight_delivery_options (label, created_by_email)')) {
                const id = db.deliveryOptions.length + 1;
                db.deliveryOptions.push({ id, label: params[0], deleted_at: null });
                return [{ insertId: id }];
            }
            if (s.startsWith('UPDATE freight_delivery_options SET label = ? WHERE id = ?')) {
                db.deliveryOptions.find(o => o.id === params[1]).label = params[0];
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_delivery_options SET deleted_at = UTC_TIMESTAMP() WHERE id = ?')) {
                db.deliveryOptions.find(o => o.id === params[0]).deleted_at = NOW();
                return ok(1);
            }
            // requests
            if (s.startsWith('SELECT id, token, sent_at, revoked_at, revoke_reason FROM freight_quote_requests WHERE batch_id = ? AND forwarder_id = ?')) {
                return [db.requests.filter(r => r.batch_id === params[0] && r.forwarder_id === params[1])];
            }
            // a send Front refused, tried again on the same row
            if (s.startsWith('UPDATE freight_quote_requests SET forwarder_name = ?')) {
                assert.ok(s.endsWith("WHERE id = ? AND revoke_reason = 'send_failed'"), s);
                const [forwarder_name, window_start_date, window_end_date, origin, destination, container_size, services,
                    staff_note, sent_to, subject, created_by_email, days, id] = params;
                const r = db.requests.find(x => x.id === id && x.revoke_reason === 'send_failed');
                if (!r) return ok(0);
                Object.assign(r, {
                    forwarder_name, window_start_date, window_end_date, origin, destination, container_size, services, staff_note,
                    sent_to, subject, created_by_email, sent_at: null, revoked_at: null, revoke_reason: null,
                    expires_at: future(days), row_version: r.row_version + 1,
                });
                return ok(1);
            }
            // reminders
            if (s.startsWith('UPDATE freight_quote_requests SET reminder_count = reminder_count + 1, expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY) WHERE id = ?')) {
                const r = db.requests.find(x => x.id === params[1]);
                r.reminder_count += 1;
                if (r.expires_at < future(params[0])) r.expires_at = future(params[0]);
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET reminder_count = reminder_count - 1 WHERE id = ? AND reminder_count > 0')) {
                db.requests.find(x => x.id === params[0]).reminder_count -= 1;
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET reminded_at = UTC_TIMESTAMP(), sent_at = COALESCE(sent_at, UTC_TIMESTAMP()) WHERE id = ?')) {
                const r = db.requests.find(x => x.id === params[0]);
                Object.assign(r, { reminded_at: NOW(), sent_at: r.sent_at || NOW() });
                return ok(1);
            }
            if (s.startsWith("SELECT id, reminder_count FROM email_receipts WHERE send_table = 'freight_quote_requests' AND send_id = ? AND email_type = ? ORDER BY id DESC LIMIT 1")) {
                const rows = db.receipts.map((p, i) => ({ id: i + 1, email_type: p[1], send_table: p[2], send_id: p[3] }))
                    .filter(x => x.send_table === 'freight_quote_requests' && x.send_id === params[0] && x.email_type === params[1])
                    .map(x => ({ id: x.id, reminder_count: db.receiptReminders.filter(m => m.receiptId === x.id).length }));
                return [rows.slice(-1)];
            }
            if (s.startsWith('INSERT INTO email_receipt_reminders')) {
                db.receiptReminders.push({ receiptId: params[0], number: params[1], subject: params[3], by: params[6] });
                return [{ insertId: db.receiptReminders.length }];
            }
            if (s.startsWith('UPDATE email_receipts SET reminder_count = reminder_count + 1')) return ok(1);
            // removing a forwarder
            if (s.startsWith('SELECT COUNT(*) AS n FROM freight_bookings b JOIN freight_quote_requests r ON r.id = b.request_id LEFT JOIN freight_quote_options o ON o.id = b.option_id WHERE r.forwarder_id = ?')) {
                const [forwarderId, days, today] = params;
                return [[{ n: db.bookings.filter((b) => {
                    if (db.requests.find(r => r.id === b.request_id).forwarder_id !== forwarderId) return false;
                    if (b.status === 'requested') return true;
                    return b.status === 'confirmed' && arrivesPlus(b, days) >= today;
                }).length }]];
            }
            if (s.startsWith('UPDATE forwarders SET deleted_at = UTC_TIMESTAMP() WHERE id = ?')) {
                db.forwarders.find(f => f.id === params[0]).deleted_at = NOW();
                return ok(1);
            }
            if (s.startsWith('UPDATE forwarder_contacts SET deleted_at = UTC_TIMESTAMP() WHERE forwarder_id = ? AND deleted_at IS NULL')) {
                db.contacts.filter(c => c.forwarder_id === params[0]).forEach((c) => { c.deleted_at = NOW(); });
                return ok(1);
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'forwarder_removed'")) {
                const rows = db.requests.filter(r => r.forwarder_id === params[0] && !r.revoked_at);
                rows.forEach(r => Object.assign(r, { revoked_at: NOW(), revoke_reason: 'forwarder_removed', row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            // the badge
            if (s.startsWith("SELECT COUNT(*) AS n FROM freight_bookings b JOIN freight_quote_requests r ON r.id = b.request_id JOIN freight_quote_options o ON o.id = b.option_id WHERE b.status = 'confirmed' AND b.contents_sent_at IS NULL AND r.revoked_at IS NULL")) {
                const [days, today] = params;
                assert.match(s, /AND b\.scope <> 'delivery'$/, 'a delivery booking is owed no contents email');
                return [[{ n: db.bookings.filter(b => b.status === 'confirmed' && !b.contents_sent_at && b.scope !== 'delivery'
                    && !db.requests.find(r => r.id === b.request_id).revoked_at && arrivesPlus(b, days) >= today).length }]];
            }
            if (s.startsWith('SELECT COUNT(DISTINCT r.batch_id) AS n FROM freight_quote_requests r JOIN forwarders f')) {
                assert.match(s, /AND \(\? = 'all' OR r\.kind = 'freight'\)/);
                const batches = new Set(db.requests.filter(r => !r.revoked_at && r.expires_at > NOW() && r.answered_at && !r.declined
                    && (params[0] === 'all' || r.kind === 'freight')
                    && !db.forwarders.find(f => f.id === r.forwarder_id).deleted_at
                    && !db.bookings.some(b => live(b) && db.requests.find(x => x.id === b.request_id).batch_id === r.batch_id))
                    .map(r => r.batch_id));
                return [[{ n: batches.size }]];
            }
            // a copy, made to book the quote for another container (book-another)
            if (s.startsWith('SELECT id, copied_from_request_id FROM freight_quote_requests WHERE batch_id = ? LIMIT 1')) {
                return [db.requests.filter(r => r.batch_id === params[0]).slice(0, 1)];
            }
            if (s.startsWith('INSERT INTO freight_quote_requests') && s.includes('copied_from_request_id')) {
                assert.doesNotMatch(s, /stock_movements/);
                const [token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, created_by_email, kind, copied_from_request_id,
                    answered_by_name, answered_by_email, answer_source, days] = params;
                if (db.requests.some(r => r.batch_id === batch_id && r.forwarder_id === forwarder_id)) throw Object.assign(new Error('dup'), { errno: 1062 });
                assert.equal(typeof services, 'string', 'the delivery options are written as the JSON text they are kept as');
                assert.match(window_start_date, /^\d{4}-\d{2}-\d{2}$/, 'a date is written as a day, never as a Date object');
                const id = db.requests.length + 1;
                db.requests.push({
                    id, token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, subject: null, created_by_email, kind, copied_from_request_id,
                    created_at: NOW(), sent_at: NOW(), expires_at: future(days), revoked_at: null, revoke_reason: null,
                    answer_count: 1, answered_at: NOW(), answered_by_name, answered_by_email, answer_source, answer_note: null,
                    declined: 0, row_version: 1, reminded_at: null, reminder_count: 0,
                });
                return [{ insertId: id }];
            }
            if (s.startsWith("UPDATE freight_quote_requests r SET r.revoked_at = UTC_TIMESTAMP(), r.revoke_reason = 'copy_closed'")) {
                const rows = db.requests.filter(r => r.copied_from_request_id != null && !r.revoked_at
                    && !db.bookings.some(b => b.request_id === r.id && params[0].includes(b.status)));
                rows.forEach(r => Object.assign(r, { revoked_at: NOW(), revoke_reason: 'copy_closed', row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'copy_closed'")) {
                assert.ok(s.endsWith('WHERE id = ? AND revoked_at IS NULL'), s);
                const r = db.requests.find(x => x.id === params[0] && !x.revoked_at);
                if (!r) return ok(0);
                Object.assign(r, { revoked_at: NOW(), revoke_reason: 'copy_closed', row_version: r.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('INSERT INTO freight_quote_requests')) {
                assert.match(s, /created_by_email, kind, expires_at\)/, 'the kind is written with the request');
                const [token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, subject, created_by_email, kind, days] = params;
                assert.doesNotMatch(s, /stock_movements/); // the old JSON column is no longer written
                const id = db.requests.length + 1;
                db.requests.push({
                    id, token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, subject, created_by_email, kind, created_at: NOW(), sent_at: null,
                    expires_at: future(days), revoked_at: null, revoke_reason: null, answer_count: 0, answered_at: null,
                    answered_by_name: null, answered_by_email: null, answer_source: null, answer_note: null, declined: 0, row_version: 1,
                    reminded_at: null, reminder_count: 0,
                });
                return [{ insertId: id }];
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'send_failed'")
                || s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'staff'")) {
                const r = db.requests.find(x => x.id === params[0]);
                Object.assign(r, { revoked_at: NOW(), revoke_reason: /send_failed/.test(s) ? 'send_failed' : 'staff', row_version: r.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET sent_at = UTC_TIMESTAMP()')) {
                Object.assign(db.requests.find(x => x.id === params[2]), { sent_at: NOW(), front_message_uid: params[0] });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET row_version = row_version + 1, expires_at = GREATEST')) {
                db.requests.find(x => x.id === params[1]).row_version += 1;
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET answer_count = ?')) {
                const [answerNo, email, note, declined, id] = params;
                const r = db.requests.find(x => x.id === id);
                Object.assign(r, { answer_count: answerNo, answered_at: NOW(), answered_by_email: email, answer_source: 'staff', answer_note: note, declined, row_version: r.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('SELECT r.id, r.token') && s.includes('WHERE r.id = ?')) {
                return [db.requests.filter(r => r.id === params[0]).map(requestRow)];
            }
            if (s.startsWith('SELECT r.id, r.token')) { // the board
                // Delivery requests are read only for a caller that asked for both legs.
                const both = !s.includes("AND r.kind = 'freight'");
                return [[...db.requests].reverse().filter(r => both || r.kind === 'freight').map(requestRow)];
            }
            // closing and reopening `filled` requests
            if (s.startsWith('SELECT r.id FROM freight_quote_requests r WHERE r.revoked_at IS NULL AND EXISTS')) {
                const confirmedBatches = db.bookings.filter(b => b.status === 'confirmed')
                    .map(b => db.requests.find(r => r.id === b.request_id));
                return [db.requests.filter(r => !r.revoked_at
                    && confirmedBatches.some(c => c.batch_id === r.batch_id && c.id !== r.id)
                    && !db.bookings.some(b => b.request_id === r.id && live(b)))];
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'filled'")) {
                const rows = db.requests.filter(r => params[0].includes(r.id) && !r.revoked_at);
                rows.forEach(r => Object.assign(r, { revoked_at: NOW(), revoke_reason: 'filled', row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            if (s.startsWith("SELECT COUNT(*) AS n FROM freight_bookings b JOIN freight_quote_requests r ON r.id = b.request_id WHERE r.batch_id = ? AND b.status = 'confirmed'")) {
                return [[{ n: db.bookings.filter(b => b.status === 'confirmed' && db.requests.find(r => r.id === b.request_id).batch_id === params[0]).length }]];
            }
            if (s.startsWith('UPDATE freight_quote_requests SET revoked_at = NULL, revoke_reason = NULL')) {
                const rows = db.requests.filter(r => r.batch_id === params[1] && r.revoke_reason === 'filled');
                rows.forEach(r => Object.assign(r, { revoked_at: null, revoke_reason: null, row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            // stock movements
            if (s.startsWith('INSERT INTO freight_stock_movements')) {
                for (let i = 0; i < params.length; i += 7) {
                    const [request_id, add_key, line_no, factory, cbm, weight_kg, created_by_email] = params.slice(i, i + 7);
                    if (db.movements.some(m => m.request_id === request_id && m.add_key === add_key && m.line_no === line_no)) {
                        throw Object.assign(new Error('dup'), { errno: 1062 });
                    }
                    db.movements.push({
                        id: ++db.movementSeq, request_id, add_key, line_no, factory, cbm, weight_kg, created_by_email, created_at: NOW(),
                        deleted_at: null, price: null, currency: null, price_note: null, priced_at: null, priced_by_name: null,
                        priced_by_email: null, price_source: null, row_version: 1,
                    });
                }
                return ok(params.length / 7);
            }
            if (s.startsWith('SELECT * FROM freight_stock_movements WHERE request_id IN (?) AND deleted_at IS NULL ORDER BY request_id ASC, id ASC')) {
                return [db.movements.filter(m => params[0].includes(m.request_id) && !m.deleted_at)];
            }
            if (s.startsWith('SELECT COALESCE(SUM(add_key = ?), 0) AS same_call, COALESCE(SUM(deleted_at IS NULL), 0) AS live FROM freight_stock_movements WHERE request_id = ?')) {
                const rows = db.movements.filter(m => m.request_id === params[1]);
                return [[{ same_call: rows.filter(m => m.add_key === params[0]).length, live: rows.filter(m => !m.deleted_at).length }]];
            }
            if (s.startsWith('DELETE FROM freight_stock_movements WHERE request_id = ? AND add_key = ?')) {
                const before = db.movements.length;
                db.movements = db.movements.filter(m => !(m.request_id === params[0] && m.add_key === params[1]));
                return ok(before - db.movements.length);
            }
            if (s.startsWith('SELECT * FROM freight_stock_movements WHERE id = ? FOR UPDATE')) {
                return [db.movements.filter(m => m.id === params[0])];
            }
            if (s.startsWith('UPDATE freight_stock_movements SET price = ?, currency = ?, price_note = ?, priced_at =')) {
                const [price, currency, price_note, priced_by_email, price_source, id] = params;
                const m = db.movements.find(x => x.id === id);
                Object.assign(m, {
                    price, currency, price_note, priced_at: /priced_at = UTC_TIMESTAMP\(\)/.test(s) ? NOW() : null, priced_by_name: null,
                    priced_by_email, price_source, row_version: m.row_version + 1,
                });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_stock_movements SET deleted_at = UTC_TIMESTAMP(), deleted_by_email = ?, row_version = row_version + 1 WHERE id = ?')) {
                const m = db.movements.find(x => x.id === params[1]);
                Object.assign(m, { deleted_at: NOW(), deleted_by_email: params[0], row_version: m.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY) WHERE id = ?')) {
                const r = db.requests.find(x => x.id === params[1]);
                if (r.expires_at < future(params[0])) r.expires_at = future(params[0]);
                return ok(1);
            }
            // options
            if (s.startsWith('INSERT INTO freight_quote_options')) {
                const firstId = db.options.length + 1;
                for (let i = 0; i < params.length; i += 15) {
                    const [request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from, port_to,
                        service, container_size, price, currency, carrier, valid_until, note] = params.slice(i, i + 15);
                    db.options.push({
                        id: db.options.length + 1, request_id, answer_no, line_no, loading_date, sailing_date, transit_days,
                        port_from, port_to, service, container_size, price, currency, carrier, valid_until, note,
                    });
                }
                return [{ affectedRows: params.length / 15, insertId: firstId }];
            }
            if (s.startsWith('SELECT * FROM freight_quote_options WHERE id = ?')) {
                return [db.options.filter(o => o.id === params[0])];
            }
            if (s.startsWith('SELECT o.*, (o.answer_no = r.answer_count) AS is_current FROM freight_quote_options o')) {
                const booked = params[1] || [];
                return [db.options.map((o) => {
                    const r = db.requests.find(x => x.id === o.request_id);
                    return { ...o, is_current: o.answer_no === r.answer_count ? 1 : 0 };
                }).filter(o => o.is_current || booked.includes(o.id))];
            }
            // bookings
            if (s.startsWith('SELECT * FROM freight_bookings WHERE request_id IN (?)')) {
                return [db.bookings.filter(b => params[0].includes(b.request_id))];
            }
            if (s.startsWith('SELECT * FROM freight_bookings WHERE request_id = ? AND status IN (?) LIMIT 1')) {
                return [db.bookings.filter(b => b.request_id === params[0] && live(b)).slice(0, 1)];
            }
            if (s.startsWith('SELECT * FROM freight_bookings WHERE id = ?')) {
                return [db.bookings.filter(b => b.id === params[0])];
            }
            if (s.startsWith('SELECT id, scope FROM freight_bookings WHERE shipment_id = ? AND status IN (?) AND id <> ?')) {
                return [db.bookings.filter(b => b.shipment_id === params[0] && live(b) && b.id !== params[2]).map(b => ({ id: b.id, scope: b.scope }))];
            }
            if (s.startsWith('INSERT INTO freight_bookings')) {
                assert.match(s, /shipment_id, draft_container_name, scope\)/, 'what the booking covers is written with it');
                const [request_id, option_id, requested_by_email, request_note, sent_to, subject, shipment_id, draft_container_name, scope] = params;
                if (db.bookings.some(b => b.request_id === request_id && live(b))) throw Object.assign(new Error('dup'), { errno: 1062 });
                // Another booking took the container between the check and this insert.
                if (shipment_id != null && db.raceShipment === shipment_id) throw containerDup(shipment_id, scope === 'delivery' ? 'delivery' : 'freight');
                // The request's kind decides the scope: the route never writes one the kind does not allow.
                assert.equal(scope === 'delivery', kindOf(request_id) === 'delivery', 'a delivery request books delivery, and only a delivery request does');
                const id = db.bookings.length + 1;
                db.bookings.push({
                    id, request_id, option_id, status: 'requested', scope, requested_by_email, requested_at: NOW(), request_note, sent_to,
                    subject, shipment_id, draft_container_name, booking_ref: null, response_note: null, responded_at: null,
                    responded_by_name: null, responded_by_email: null, response_source: null, cancelled_at: null, cancel_reason: null,
                    contents_sent_at: null, contents_sent_to: null, contents_json: null, contents_document_ids: null, row_version: 1,
                });
                return [{ insertId: id }];
            }
            if (s.startsWith("UPDATE freight_bookings SET status = 'cancelled', cancel_reason = 'send_failed'")) {
                Object.assign(db.bookings.find(b => b.id === params[0]), { status: 'cancelled', cancel_reason: 'send_failed' });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_bookings SET front_message_uid = ?')) return ok(1);
            if (s.startsWith('UPDATE freight_bookings SET row_version = row_version + 1 WHERE id = ? AND row_version = ? AND status IN (?)')) {
                const b = db.bookings.find(x => x.id === params[0] && x.row_version === params[1] && live(x));
                if (!b) return ok(0);
                b.row_version += 1;
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_bookings SET contents_sent_at = UTC_TIMESTAMP()')) {
                assert.ok(s.endsWith('contents_json = ?, contents_document_ids = ?, row_version = row_version + 1 WHERE id = ?'), s);
                const b = db.bookings.find(x => x.id === params[3]);
                Object.assign(b, {
                    contents_sent_at: NOW(), contents_sent_to: params[0], contents_json: params[1], contents_document_ids: params[2],
                    row_version: b.row_version + 1,
                });
                return ok(1);
            }
            // the documents of a contents email
            if (s.startsWith('UPDATE draft_container_documents SET shipment_id = ? WHERE id IN (?)')) {
                for (const d of db.documents) if (params[1].includes(d.id)) d.shipment_id = params[0];
                return ok(params[1].length);
            }
            if (s.startsWith("SELECT id, s3_key, csv_s3_key, supplier FROM draft_container_documents WHERE batch_id = ? AND type = 'forwarder-quote'")) {
                return [db.documents.filter(d => d.batch_id === params[0])];
            }
            if (s.startsWith('INSERT INTO draft_container_document_sends')) {
                db.documentSends.push({ documentId: params[0], subject: params[2] });
                return [{ insertId: db.documentSends.length }];
            }
            if (s.startsWith('UPDATE freight_bookings SET ') && s.endsWith('row_version = row_version + 1 WHERE id = ?')) { // PATCH
                const b = db.bookings.find(x => x.id === params[params.length - 1]);
                const p = [...params];
                if (s.includes('shipment_id = ?')) {
                    assert.match(s, /contents_document_ids = NULL/);
                    if (p[0] != null && db.raceShipment === p[0]) throw containerDup(p[0]);
                    Object.assign(b, { shipment_id: p.shift(), draft_container_name: p.shift(), contents_json: null, contents_sent_at: null, contents_document_ids: null });
                }
                if (s.includes('scope = ?')) {
                    const scope = p.shift();
                    // The keys: no other live booking on its container covers a leg this scope covers.
                    const clash = db.bookings.some(x => x.id !== b.id && live(x) && b.shipment_id != null && x.shipment_id === b.shipment_id
                        && legsOfScope(x.scope).some(l => legsOfScope(scope).includes(l)));
                    if (clash) throw containerDup(b.shipment_id, 'delivery');
                    b.scope = scope;
                }
                if (s.includes("status = 'confirmed'")) Object.assign(b, { status: 'confirmed', booking_ref: p.shift(), response_note: p.shift(), responded_by_email: p.shift(), response_source: 'staff', responded_at: NOW() });
                if (s.includes("status = 'cancelled'")) Object.assign(b, { status: 'cancelled', cancel_reason: 'staff', cancelled_by_email: p.shift(), cancelled_at: NOW() });
                b.row_version += 1;
                return ok(1);
            }
            // shipments, receipts
            if (s.startsWith('SELECT id, name, reference, stage, deleted_at, merged_into_id FROM shipments WHERE id = ?')) {
                return [db.shipments.filter(x => x.id === params[0])];
            }
            if (s.startsWith('SELECT id, name, reference, stage FROM shipments WHERE id IN (?)')) {
                return [db.shipments.filter(x => params[0].includes(x.id))];
            }
            if (s.startsWith('INSERT INTO email_receipts')) { db.receipts.push(params); return [{ insertId: db.receipts.length }]; }
            // forwarder charges of a container (2026-11-20)
            if (s.startsWith('SELECT b.id, b.status, b.scope, b.booking_ref, b.request_id, b.option_id, r.forwarder_id, r.forwarder_name FROM freight_bookings b JOIN freight_quote_requests r ON r.id = b.request_id WHERE b.shipment_id = ? AND b.status IN (?) ORDER BY b.id DESC')) {
                assert.doesNotMatch(s, /LIMIT 1/, 'a container may hold a booking per leg');
                return [db.bookings.filter(x => x.shipment_id === params[0] && params[1].includes(x.status)).sort((x, y) => y.id - x.id).map((b) => {
                    const r = db.requests.find(x => x.id === b.request_id);
                    return { id: b.id, status: b.status, scope: b.scope, booking_ref: b.booking_ref, request_id: b.request_id, option_id: b.option_id, forwarder_id: r.forwarder_id, forwarder_name: r.forwarder_name };
                })];
            }
            if (s.startsWith('SELECT id, name, deleted_at FROM forwarders WHERE id = ?')) {
                return [db.forwarders.filter(f => f.id === params[0])];
            }
            if (s.startsWith('SELECT id, factory, cbm, price, currency FROM freight_stock_movements WHERE request_id = ? AND deleted_at IS NULL ORDER BY id ASC')) {
                return [db.movements.filter(m => m.request_id === params[0] && !m.deleted_at).sort((a, b) => a.id - b.id)];
            }
            if (s.startsWith('SELECT id, freight_booking_id, freight_movement_id, amount, currency FROM payment_extras WHERE deleted_at IS NULL AND (freight_booking_id IN (?) OR freight_movement_id IN (?))')) {
                return [db.extras.filter(x => !x.deleted_at && (params[0].includes(x.freight_booking_id) || params[1].includes(x.freight_movement_id)))];
            }
            throw new Error(`unexpected statement: ${s.slice(0, 140)}`);
        },
    };

    const lines = [
        { sku: 'Gauze', jfCode: 'JF0001', lot: 'L1', poNumber: 'PO-1', orderedUnits: 1000, noOfCartons: 10, totalCbm: 1.2, cartonWeight: 12, supplier: 'Acme', port: 'Ningbo' },
    ];
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => { req.userType = userType; req.userEmail = 'staff@example.com'; next(); });
    const logs = [];
    registerFreightQuoteRoutes(app, {
        withConnection: fn => fn(conn),
        recordAudit: async (_c, a) => { db.audits.push(a); },
        getEmailTemplate: async (_c, key) => ({ subject: `${key} {containerSize}{sailingDate}`, bodyHtml: '<p>Body for {forwarder}</p>' }),
        renderTemplate: (str, vars) => String(str).replace(/\{(\w+)\}/g, (m, k) => (vars[k] != null ? String(vars[k]) : '')),
        loadForwarderQuoteForPdf: async () => ({ lines, originPorts: ['Ningbo'] }),
        // `documentSet` (a test that attaches documents): the rows one generate run files.
        generateDraftDocumentSet: async () => {
            if (!documentSet) throw new Error('not expected: attachDocuments is false in this test');
            db.documents.push(...documentSet.map(d => ({ ...d, batch_id: 'batch-1', shipment_id: null })));
            const [main, ...rest] = documentSet;
            return { batchId: 'batch-1', main: { documentId: main.id }, supplierDocuments: rest.map(d => ({ documentId: d.id })) };
        },
        s3: documentSet ? { send: async (cmd) => ({ Body: { transformToByteArray: async () => Buffer.from(cmd.input.Key) } }) } : null,
        poBucket: () => (documentSet ? 'bucket' : null),
        sendEmail: async (msg) => {
            db.sent.push(msg);
            return frontOk(msg) ? { ok: true, messageId: `m${db.sent.length}` } : { ok: false, reason: 'ses_error', status: 500 };
        },
        preapprove: async (_c, { emails }) => { db.preapproved.push(...emails); return { status: 'ok', users: [] }; },
        londonToday: () => '2026-11-16',
        log: { error: (...a) => logs.push(['error', ...a]), warn: (...a) => logs.push(['warn', ...a]), info: () => {} },
    });

    const call = (method, urlPath, body) => new Promise((resolve, reject) => {
        const server = app.listen(0, () => {
            const { port } = server.address();
            const req = http.request({ port, method, path: urlPath, headers: { 'content-type': 'application/json' } }, (res) => {
                let text = '';
                res.on('data', (d) => { text += d; });
                res.on('end', () => { server.close(); resolve({ status: res.statusCode, body: text ? JSON.parse(text) : null }); });
            });
            req.on('error', (e) => { server.close(); reject(e); });
            if (body !== undefined) req.write(JSON.stringify(body));
            req.end();
        });
    });
    return { db, call, logs };
}

const SEND = {
    forwarderIds: [1, 2], batchId: BATCH, windowStart: '2026-12-01', windowEnd: '2026-12-31',
    services: ['door_to_port'], containerSize: '40HQ',
};
const OPTION = {
    loadingDate: '2026-12-05', sailingDate: '2026-12-10', transitDays: 32, portFrom: 'Ningbo', portTo: 'Felixstowe',
    service: 'door_to_port', price: '1850', currency: 'USD',
};

// Two forwarders asked, both answered (typed in by staff), ready to book.
async function answered(h) {
    await h.call('POST', '/api/v1/freight-quotes', SEND);
    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] });
    await h.call('POST', '/api/v1/freight-quotes/2/options', { options: [{ ...OPTION, price: '1700' }] });
}

test('send: one row, one link and one email per forwarder; the same batchId again emails nobody', async () => {
    const h = harness();
    const first = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.equal(first.status, 200);
    assert.deepEqual(first.body.results.map(r => r.outcome), ['sent', 'sent']);
    assert.equal(h.db.sent.length, 2);
    assert.deepEqual(h.db.sent[0].to, ['amy@dcg.com']);
    assert.match(h.db.sent[0].html, /https:\/\/portal\.example\/freight\/[a-f0-9]{48}/);
    assert.match(h.db.sent[0].html, /Body for DCG/);
    assert.deepEqual(h.db.preapproved, ['amy@dcg.com', 'ops@savino.com']);
    assert.equal(first.body.requests.length, 2);
    assert.equal(first.body.requests[0].state, 'open');
    assert.match(first.body.requests[0].url, /\/freight\//);
    // The audit trail never holds the token.
    assert.doesNotMatch(JSON.stringify(h.db.audits), /[a-f0-9]{48}/);

    const again = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.deepEqual(again.body.results.map(r => r.outcome), ['already_sent', 'already_sent']);
    assert.equal(h.db.sent.length, 2);
    assert.equal(h.db.requests.length, 2);
});

test('send: delivery options as free text, stock movements, and the email as staff edited it', async () => {
    const h = harness();
    const out = await h.call('POST', '/api/v1/freight-quotes', {
        ...SEND, forwarderIds: [1],
        services: ['Door to port', 'Deliver to Unit 5, tail-lift', 'door to port'],
        // No "to" any more; one sent by an older client is ignored.
        stockMovements: [
            { from: 'Acme Medical, Ningbo', to: 'Ningbo port', cbm: '12.5', weightKg: 2400 },
            { from: 'Best Gloves <Shanghai>', cbm: 3.25, weightKg: '610.5' },
        ],
        subject: 'Quote please, {forwarder}: {containerSize}',
        body: '<p>Dear {forwarder}, our own words. Options: {services}.</p>',
    });
    assert.equal(out.status, 200);
    const request = out.body.requests[0];
    // The same option twice (in any case) counts once; the text is kept as typed.
    assert.deepEqual(request.services, ['Door to port', 'Deliver to Unit 5, tail-lift']);
    // Rows of their own, unpriced, each to be priced on its own line.
    assert.deepEqual(request.stockMovements.map(m => [m.id, m.from, m.cbm, m.weightKg, m.price, m.currency, m.rowVersion]), [
        [1, 'Acme Medical, Ningbo', 12.5, 2400, null, null, 1],
        [2, 'Best Gloves <Shanghai>', 3.25, 610.5, null, null, 1],
    ]);
    assert.ok(!('to' in request.stockMovements[0]));
    assert.equal(h.db.movements[0].add_key, BATCH);
    const mail = h.db.sent[0];
    assert.equal(mail.subject, 'Quote please, DCG: 40HQ');
    assert.match(mail.html, /Dear DCG, our own words\. Options: Door to port, Deliver to Unit 5, tail-lift\./);
    assert.doesNotMatch(mail.html, /Body for DCG/);
    // The fixed block still follows the edited text: the link, the options, the movements.
    assert.match(mail.html, /https:\/\/portal\.example\/freight\/[a-f0-9]{48}/);
    assert.match(mail.html, /Door to port \/ 门到港/);
    assert.match(mail.html, /Deliver to Unit 5, tail-lift/);
    assert.match(mail.html, /Best Gloves &lt;Shanghai&gt;/);
    assert.match(mail.html, /15\.75/); // total CBM
    assert.match(mail.html, /3010\.5/); // total kg
    assert.match(mail.html, /each priced separately/);
    assert.doesNotMatch(mail.html, /Ningbo port|To port or consolidation/);

    // An option must name one of the delivery options asked about, exactly.
    const option = { ...OPTION, service: 'Deliver to Unit 5, tail-lift' };
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [option] })).status, 200);
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [{ ...OPTION, service: 'door_to_door' }] })).body.field, 'options[0].service');

    const bad = body => h.call('POST', '/api/v1/freight-quotes', { ...SEND, batchId: undefined, forwarderIds: [2], ...body }).then(r => r.body.field);
    assert.equal(await bad({ stockMovements: [{ from: '', cbm: 1, weightKg: 1 }] }), 'stockMovements[0].from');
    assert.equal(await bad({ stockMovements: [{ from: 'A', cbm: 0, weightKg: 1 }] }), 'stockMovements[0].cbm');
    assert.equal(await bad({ stockMovements: [{ from: 'A', cbm: 1, weightKg: 'heavy' }] }), 'stockMovements[0].weightKg');
    assert.equal(await bad({ services: ['x'.repeat(101)] }), 'services');
});

const ADD_KEY = 'b2c3d4e5-0000-4000-8000-000000000002';

test('stock movements: added after the booking, the forwarder is emailed once; a retry adds nothing', async () => {
    const h = harness();
    await answered(h);
    const optionId = h.db.options.find(o => o.request_id === 1).id;
    assert.equal((await h.call('POST', `/api/v1/freight-quotes/options/${optionId}/book`, {})).status, 200);
    const versionBefore = h.db.requests[0].row_version;
    const mails = h.db.sent.length;

    const body = { addKey: ADD_KEY, stockMovements: [{ from: 'Late Factory', cbm: 2, weightKg: 300 }], subject: 'Price please, {forwarder} ({count})' };
    const out = await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', body);
    assert.equal(out.status, 200);
    assert.deepEqual([out.body.added, out.body.alreadyAdded], [1, false]);
    const request = out.body.requests.find(r => r.id === 1);
    assert.equal(request.state, 'booked');
    assert.deepEqual(request.stockMovements.map(m => [m.from, m.price]), [['Late Factory', null]]);
    // The other forwarder of the send was not asked.
    assert.deepEqual(out.body.requests.find(r => r.id === 2).stockMovements, []);
    // One email, to the forwarder's contact, with the movement and the link to the same page.
    assert.equal(h.db.sent.length, mails + 1);
    const mail = h.db.sent[mails];
    assert.deepEqual(mail.to, ['amy@dcg.com']);
    assert.equal(mail.subject, 'Price please, DCG (1)');
    assert.match(mail.html, /Late Factory/);
    assert.match(mail.html, new RegExp(`https://portal\\.example/freight/${h.db.requests[0].token}`));
    assert.match(mail.html, /Price the stock movements/);
    // The forwarder's open form is not disturbed: the request's version stays.
    assert.equal(h.db.requests[0].row_version, versionBefore);
    assert.equal(h.db.audits.at(-1).action, 'movements_added');
    assert.doesNotMatch(JSON.stringify(h.db.audits), /[a-f0-9]{48}/);

    const again = await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', body);
    assert.deepEqual([again.status, again.body.added, again.body.alreadyAdded], [200, 0, true]);
    assert.equal(h.db.sent.length, mails + 1);
    assert.equal(h.db.movements.length, 1);

    // Without an email when staff say so; never on a closed link; never past the limit.
    const quiet = await h.call('POST', '/api/v1/freight-quotes/2/stock-movements', { notify: false, stockMovements: [{ from: 'Quiet', cbm: 1, weightKg: 1 }] });
    assert.equal(quiet.status, 200);
    assert.equal(h.db.sent.length, mails + 1);
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', { stockMovements: [] })).body.field, 'stockMovements');
    const many = Array.from({ length: 30 }, (_, i) => ({ from: `F${i}`, cbm: 1, weightKg: 1 }));
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', { stockMovements: many })).body.code, 'TOO_MANY');
    await h.call('POST', '/api/v1/freight-quotes/2/revoke');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/2/stock-movements', { stockMovements: [{ from: 'A', cbm: 1, weightKg: 1 }] })).body.code, 'REVOKED');
});

test('stock movements: a Front failure leaves nothing behind, and the same call can go again', async () => {
    let fail = false;
    const h = harness({ frontOk: () => !fail });
    await answered(h);
    fail = true;
    const body = { addKey: ADD_KEY, stockMovements: [{ from: 'Late Factory', cbm: 2, weightKg: 300 }] };
    const out = await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', body);
    assert.equal(out.status, 502);
    assert.equal(h.db.movements.length, 0);
    fail = false;
    const again = await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', body);
    assert.deepEqual([again.status, again.body.added], [200, 1]);
});

test('stock movements: staff type in a price, take it back off, and remove a movement', async () => {
    const h = harness();
    await h.call('POST', '/api/v1/freight-quotes', {
        ...SEND, forwarderIds: [1],
        stockMovements: [{ from: 'Acme', cbm: 12.5, weightKg: 2400 }, { from: 'Best', cbm: 3, weightKg: 600 }],
    });
    const priced = await h.call('PATCH', '/api/v1/freight-stock-movements/1', { price: '120.5', currency: 'USD', note: 'by phone' });
    assert.equal(priced.status, 200);
    const [first, second] = priced.body.requests[0].stockMovements;
    assert.deepEqual([first.price, first.currency, first.priceNote, first.priceSource, first.pricedBy, first.rowVersion], [120.5, 'USD', 'by phone', 'staff', 'staff@example.com', 2]);
    assert.equal(second.price, null);

    assert.equal((await h.call('PATCH', '/api/v1/freight-stock-movements/1', { price: '0', currency: 'USD' })).body.field, 'price');
    assert.equal((await h.call('PATCH', '/api/v1/freight-stock-movements/1', { price: '10', currency: 'XXX' })).body.field, 'currency');
    assert.equal((await h.call('PATCH', '/api/v1/freight-stock-movements/99', { price: '10', currency: 'USD' })).status, 404);

    const cleared = await h.call('PATCH', '/api/v1/freight-stock-movements/1', { price: null });
    assert.deepEqual([cleared.body.requests[0].stockMovements[0].price, cleared.body.requests[0].stockMovements[0].pricedAt], [null, null]);

    const removed = await h.call('DELETE', '/api/v1/freight-stock-movements/2');
    assert.deepEqual(removed.body.requests[0].stockMovements.map(m => m.from), ['Acme']);
    assert.equal((await h.call('DELETE', '/api/v1/freight-stock-movements/2')).status, 200); // twice is a no-op
    assert.equal((await h.call('PATCH', '/api/v1/freight-stock-movements/2', { price: '10', currency: 'USD' })).status, 404);

    const reader = harness({ userType: 'readonly' });
    assert.equal((await reader.call('POST', '/api/v1/freight-quotes/1/stock-movements', { stockMovements: [{ from: 'A', cbm: 1, weightKg: 1 }] })).status, 403);
});

test('delivery options: list, add, reword, remove; no duplicates; read-only refused', async () => {
    const h = harness();
    assert.deepEqual((await h.call('GET', '/api/v1/freight-delivery-options')).body.deliveryOptions.map(o => o.label), ['Door to door', 'Door to port']);
    const added = await h.call('POST', '/api/v1/freight-delivery-options', { label: '  Deliver to Unit 5  ' });
    assert.equal(added.status, 201);
    assert.deepEqual(added.body.deliveryOptions[2], { id: 3, label: 'Deliver to Unit 5' });
    assert.equal((await h.call('POST', '/api/v1/freight-delivery-options', { label: 'door to DOOR' })).body.code, 'DUPLICATE');
    assert.equal((await h.call('POST', '/api/v1/freight-delivery-options', { label: ' ' })).body.field, 'label');

    const reworded = await h.call('PATCH', '/api/v1/freight-delivery-options/3', { label: 'Deliver to Unit 5, tail-lift' });
    assert.equal(reworded.body.deliveryOptions[2].label, 'Deliver to Unit 5, tail-lift');
    assert.equal((await h.call('PATCH', '/api/v1/freight-delivery-options/3', { label: 'Door to port' })).body.code, 'DUPLICATE');
    assert.equal((await h.call('PATCH', '/api/v1/freight-delivery-options/9', { label: 'X' })).status, 404);

    const removed = await h.call('DELETE', '/api/v1/freight-delivery-options/1');
    assert.deepEqual(removed.body.deliveryOptions.map(o => o.id), [2, 3]);
    assert.equal((await h.call('DELETE', '/api/v1/freight-delivery-options/1')).status, 200); // twice is a no-op
    // A removed entry's text can be added again.
    assert.equal((await h.call('POST', '/api/v1/freight-delivery-options', { label: 'Door to door' })).status, 201);
    assert.deepEqual(h.db.audits.map(a => `${a.entityType}:${a.action}`), [
        'freight_delivery_option:create', 'freight_delivery_option:update', 'freight_delivery_option:delete', 'freight_delivery_option:create',
    ]);

    const reader = harness({ userType: 'accountant' });
    assert.equal((await reader.call('POST', '/api/v1/freight-delivery-options', { label: 'X' })).body.code, 'READ_ONLY');
    assert.equal((await reader.call('GET', '/api/v1/freight-delivery-options')).status, 200);
});

test('booking and contents: the subject and body staff edited are what is sent', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {
        shipmentId: 50, subject: 'Please book {sailingDate}', body: '<p>Edited booking words for {forwarder}.</p>', to: 'amy@dcg.com, second@dcg.com',
    });
    const booking = h.db.sent[h.db.sent.length - 1];
    assert.equal(booking.subject, 'Please book 2026-12-10');
    assert.deepEqual(booking.to, ['amy@dcg.com', 'second@dcg.com']);
    assert.match(booking.html, /Edited booking words for DCG\./);
    assert.match(booking.html, /Confirm or decline/);

    const rowVersion = h.db.bookings[0].row_version;
    await h.call('POST', '/api/v1/freight-bookings/1/contents-email', {
        rowVersion, attachDocuments: false, subject: 'Contents of {containerName}', body: '<p>Edited contents words.</p>',
    });
    const contents = h.db.sent[h.db.sent.length - 1];
    assert.equal(contents.subject, 'Contents of Draft 12');
    assert.match(contents.html, /Edited contents words\./);
    assert.match(contents.html, /Acme/);
});

test('send: a Front failure revokes that forwarder\'s link and the other still goes', async () => {
    const h = harness({ frontOk: msg => msg.to[0] !== 'amy@dcg.com' });
    const out = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.deepEqual(out.body.results.map(r => r.outcome), ['failed', 'sent']);
    assert.equal(h.db.requests[0].revoke_reason, 'send_failed');
    assert.equal(h.db.requests[1].revoked_at, null);
});

test('send: refusals', async () => {
    assert.equal((await harness({ userType: 'accountant' }).call('POST', '/api/v1/freight-quotes', SEND)).body.code, 'READ_ONLY');
    const h = harness();
    assert.equal((await h.call('POST', '/api/v1/freight-quotes', { ...SEND, services: [] })).body.field, 'services');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [9] })).body.field, 'forwarderIds');
    assert.equal(h.db.sent.length, 0);
});

test('staff enter an answer; booking an option emails the link and holds the request', async () => {
    const h = harness();
    await answered(h);
    assert.equal(h.db.requests[0].answer_source, 'staff');
    const booked = await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50, note: 'Please hurry' });
    assert.equal(booked.status, 200);
    assert.equal(booked.body.bookings[0].status, 'requested');
    assert.equal(booked.body.bookings[0].containerName, 'Draft 12');
    assert.equal(booked.body.bookings[0].option.price, 1850);
    assert.equal(booked.body.requests.find(r => r.id === 1).state, 'booked');
    assert.equal(booked.body.requests.find(r => r.id === 2).state, 'answered');
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.match(mail.html, /USD 1,850\.00/);
    assert.match(mail.html, /Please hurry/);
    // One live booking per request; and a booked request takes no new answer.
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', {})).body.code, 'ALREADY_BOOKED');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] })).body.code, 'HAS_BOOKING');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/revoke')).body.code, 'HAS_BOOKING');
});

test('booking: an option from an older answer is refused; a Front failure cancels the booking', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [{ ...OPTION, price: '1900' }] }); // answer 2
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', {})).body.code, 'STALE_OPTION');

    const f = harness({ frontOk: msg => !/Confirm or decline/.test(msg.html) });
    await answered(f);
    const out = await f.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal(out.status, 502);
    assert.equal(f.db.bookings[0].status, 'cancelled');
    assert.equal(f.db.bookings[0].cancel_reason, 'send_failed');
});

// ── the same quote for another container (book-another, 2026-11-26) ──────
const COPY_KEY = 'c0ffee00-0000-4000-8000-000000000002';
const another = (h, body = {}, optionId = 1) => h.call('POST', `/api/v1/freight-quotes/options/${optionId}/book-another`, { copyKey: COPY_KEY, ...body });
// Request 1's option booked for Draft 12: the quote a second container is booked on.
async function bookedOnce(h) {
    await answered(h);
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 })).status, 200);
}

test('book-another: the quote is copied with a link of its own and booked for the second container; the original is untouched', async () => {
    const h = harness();
    await bookedOnce(h);
    const original = { ...h.db.requests[0] };
    const sentBefore = h.db.sent.length;

    const out = await another(h, { shipmentId: 52, note: 'Second box, same ship' });
    assert.equal(out.status, 200);
    assert.deepEqual([out.body.ok, out.body.bookingId, out.body.requestId, out.body.alreadyBooked], [true, 2, 3, undefined]);

    // The copy: the same forwarder and facts, answered already, a new token, pointing at the original.
    const copy = h.db.requests[2];
    assert.deepEqual(
        [copy.forwarder_id, copy.forwarder_name, copy.window_start_date, copy.window_end_date, copy.container_size, copy.services, copy.kind],
        [1, 'DCG', '2026-12-01', '2026-12-31', '40HQ', original.services, 'freight']);
    assert.deepEqual([copy.batch_id, copy.copied_from_request_id, copy.answer_count, copy.answer_source], [COPY_KEY, 1, 1, 'staff']);
    assert.match(copy.token, /^[a-f0-9]{48}$/);
    assert.notEqual(copy.token, original.token);
    // Its one option is the option chosen, as answer 1.
    const copied = h.db.options.filter(o => o.request_id === 3);
    assert.equal(copied.length, 1);
    assert.deepEqual(
        [copied[0].answer_no, copied[0].line_no, copied[0].sailing_date, copied[0].loading_date, copied[0].transit_days, copied[0].port_from, copied[0].port_to, Number(copied[0].price), copied[0].currency],
        [1, 1, '2026-12-10', '2026-12-05', 32, 'Ningbo', 'Felixstowe', 1850, 'USD']);
    // The booking is on the copy, for the second draft.
    const b = h.db.bookings[1];
    assert.deepEqual([b.request_id, b.option_id, b.status, b.shipment_id, b.draft_container_name, b.scope], [3, copied[0].id, 'requested', 52, 'Draft 13', 'port_to_port']);

    // One email, with the COPY's link, never the original's.
    assert.equal(h.db.sent.length, sentBefore + 1);
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.deepEqual(mail.to, ['amy@dcg.com']);
    assert.ok(mail.html.includes(`/freight/${copy.token}`));
    assert.ok(!mail.html.includes(original.token));
    assert.match(mail.html, /Second box, same ship/);
    assert.match(mail.html, /USD 1,850\.00/);

    // The original: its booking, its answer and its version as they were.
    assert.deepEqual(h.db.requests[0], original);
    assert.deepEqual([h.db.bookings[0].request_id, h.db.bookings[0].status, h.db.bookings[0].shipment_id], [1, 'requested', 50]);

    // On the board: two requests booked, two bookings, each with its own container.
    const req1 = out.body.requests.find(r => r.id === 1);
    const req3 = out.body.requests.find(r => r.id === 3);
    assert.deepEqual([req1.state, req1.copiedFromRequestId], ['booked', null]);
    assert.deepEqual([req3.state, req3.copiedFromRequestId, req3.options.length, req3.stockMovements.length], ['booked', 1, 1, 0]);
    assert.match(req3.url, /\/freight\/[a-f0-9]{48}$/);
    assert.deepEqual(out.body.bookings.map(x => [x.id, x.requestId, x.containerName]), [[2, 3, 'Draft 13'], [1, 1, 'Draft 12']]);

    // The audit trail says what was copied, and never holds a token.
    const audit = h.db.audits.find(a => a.entityType === 'freight_quote_request' && a.action === 'copied');
    assert.deepEqual([audit.entityId, audit.after], [3, { copiedFromRequestId: 1, originalRequestId: 1, optionId: 1, bookingId: 2 }]);
    assert.equal(h.db.audits.filter(a => a.entityType === 'freight_booking' && a.action === 'requested').pop().after.copiedFromRequestId, 1);
    assert.doesNotMatch(JSON.stringify(h.db.audits), /[a-f0-9]{48}/);
    // The email is listed as a booking email of the copy's booking.
    assert.deepEqual(h.db.receipts[h.db.receipts.length - 1].filter(v => v === 'freight_booking_request' || v === 'freight_bookings'), ['freight_booking_request', 'freight_bookings']);

    // The copy's booking is a booking like any other: confirmed with its own reference.
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/2', { status: 'confirmed', bookingRef: 'BK-2' });
    assert.deepEqual(confirmed.body.bookings.map(x => [x.id, x.status, x.bookingRef]), [[2, 'confirmed', 'BK-2'], [1, 'requested', null]]);
    // ... and its confirmation closes nobody: it is a send of its own.
    assert.equal(confirmed.body.requests.find(r => r.id === 1).state, 'booked');
    assert.equal(confirmed.body.requests.find(r => r.id === 2).state, 'answered');
    assert.deepEqual(h.logs, []);
});

test('book-another: only where a booking stands; plain book still refuses a second booking', async () => {
    const h = harness();
    await answered(h);
    const none = await another(h, { shipmentId: 52 });
    assert.deepEqual([none.status, none.body.code], [409, 'NOT_BOOKED']);
    assert.equal(h.db.requests.length, 2);

    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 52 })).body.code, 'ALREADY_BOOKED');
    // A key is needed, and it is a UUID.
    for (const bad of [undefined, '', 'abc', 7]) {
        const r = await h.call('POST', '/api/v1/freight-quotes/options/1/book-another', { copyKey: bad });
        assert.deepEqual([r.status, r.body.code, r.body.field], [400, 'BAD_FIELD', 'copyKey'], String(bad));
    }
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/99/book-another', { copyKey: COPY_KEY })).status, 404);
    // The same container cannot carry both bookings.
    const same = await another(h, { shipmentId: 50 });
    assert.deepEqual([same.status, same.body.code], [409, 'CONTAINER_TAKEN']);
    assert.equal(h.db.requests.length, 2);
    assert.equal(h.db.bookings.length, 1);

    // A read-only role books nothing.
    assert.equal((await another(harness({ userType: 'viewer' }))).status, 403);
});

test('book-another: the same key again answers the booking it made and emails nobody', async () => {
    const h = harness();
    await bookedOnce(h);
    const first = await another(h, { shipmentId: 52 });
    const sent = h.db.sent.length;
    const again = await another(h, { shipmentId: 52 });
    assert.deepEqual([again.status, again.body.alreadyBooked, again.body.bookingId, again.body.requestId], [200, true, first.body.bookingId, first.body.requestId]);
    assert.equal(h.db.sent.length, sent);
    assert.equal(h.db.requests.length, 3);
    assert.equal(h.db.bookings.length, 2);
    // A third container is another press, with another key.
    const third = await h.call('POST', '/api/v1/freight-quotes/options/1/book-another', { copyKey: 'c0ffee00-0000-4000-8000-000000000003' });
    assert.equal(third.status, 200);
    assert.deepEqual(h.db.requests.map(r => r.copied_from_request_id ?? null), [null, null, 1, 1]);
    // A key that belongs to a send is never a copy's.
    const clash = await h.call('POST', '/api/v1/freight-quotes/options/1/book-another', { copyKey: BATCH });
    assert.deepEqual([clash.status, clash.body.code], [409, 'COPY_KEY_USED']);
});

test('book-another: a copy of a copy points at the request the forwarder answered', async () => {
    const h = harness();
    await bookedOnce(h);
    await another(h, { shipmentId: 52 });
    const copyOption = h.db.options.find(o => o.request_id === 3).id;
    const out = await h.call('POST', `/api/v1/freight-quotes/options/${copyOption}/book-another`, { copyKey: 'c0ffee00-0000-4000-8000-000000000004' });
    assert.equal(out.status, 200);
    assert.equal(h.db.requests[3].copied_from_request_id, 1);
    assert.deepEqual(h.db.audits.filter(a => a.action === 'copied').pop().after, { copiedFromRequestId: 3, originalRequestId: 1, optionId: copyOption, bookingId: 3 });
});

test('book-another: an email the service refused cancels the booking and closes the copy, off the board', async () => {
    let refuse = false;
    const h = harness({ frontOk: () => !refuse });
    await bookedOnce(h);
    refuse = true;
    const out = await another(h, { shipmentId: 52 });
    assert.equal(out.status, 502);
    assert.deepEqual([h.db.bookings[1].status, h.db.bookings[1].cancel_reason], ['cancelled', 'send_failed']);
    assert.deepEqual([h.db.requests[2].revoke_reason, Boolean(h.db.requests[2].revoked_at)], ['send_failed', true]);
    refuse = false;
    // That key is spent; the dialog makes another, and the container is free again.
    assert.equal((await another(h, { shipmentId: 52 })).body.code, 'COPY_KEY_USED');
    const retry = await h.call('POST', '/api/v1/freight-quotes/options/1/book-another', { copyKey: 'c0ffee00-0000-4000-8000-000000000005', shipmentId: 52 });
    assert.equal(retry.status, 200);
    // The closed copy is not a request to list; its cancelled booking stays in the history.
    assert.deepEqual(retry.body.requests.map(r => r.id).sort(), [1, 2, 4]);
    assert.deepEqual(retry.body.bookings.map(b => [b.id, b.status]), [[3, 'requested'], [2, 'cancelled'], [1, 'requested']]);
});

test('book-another: a copy lives for its booking: declined by the forwarder or cancelled by staff, its link is closed', async () => {
    const h = harness();
    await bookedOnce(h);
    await another(h, { shipmentId: 52 });
    // The forwarder declines on the copy's page (the portal writes the booking, and cannot revoke).
    Object.assign(h.db.bookings[1], { status: 'declined', response_source: 'link' });
    const board = await h.call('GET', '/api/v1/freight-quotes');
    assert.deepEqual([h.db.requests[2].revoke_reason, Boolean(h.db.requests[2].revoked_at)], ['copy_closed', true]);
    assert.deepEqual(board.body.requests.map(r => r.id).sort(), [1, 2]);
    assert.deepEqual(board.body.bookings.map(b => [b.id, b.status]), [[2, 'declined'], [1, 'requested']]);
    // The badge read closes it too, and never counts it as a quote waiting to be booked.
    const g = harness();
    await bookedOnce(g);
    await another(g, { shipmentId: 52 });
    Object.assign(g.db.bookings[1], { status: 'declined' });
    assert.equal((await g.call('GET', '/api/v1/freight-quotes/summary')).body.answered, 0);
    assert.equal(g.db.requests[2].revoke_reason, 'copy_closed');
    // The original is never closed by the sweep, booked or not.
    assert.equal(h.db.requests[0].revoked_at, null);

    // Staff cancel the copy's booking: the copy closes with it, and the quote can be booked again from the original.
    const c = harness();
    await bookedOnce(c);
    await another(c, { shipmentId: 52 });
    const cancelled = await c.call('PATCH', '/api/v1/freight-bookings/2', { status: 'cancelled' });
    assert.equal(cancelled.status, 200);
    assert.equal(c.db.requests[2].revoke_reason, 'copy_closed');
    assert.equal(c.db.audits.filter(a => a.entityType === 'freight_booking' && a.action === 'cancelled').pop().after.copyClosed, true);
    assert.deepEqual(cancelled.body.requests.map(r => r.id).sort(), [1, 2]);
    assert.equal((await c.call('POST', '/api/v1/freight-quotes/options/1/book-another', { copyKey: 'c0ffee00-0000-4000-8000-000000000006', shipmentId: 52 })).status, 200);
    // Cancelling the ORIGINAL's booking leaves the request open, as before, and the copy's booking standing.
    const first = await c.call('PATCH', '/api/v1/freight-bookings/1', { status: 'cancelled' });
    assert.equal(first.body.requests.find(r => r.id === 1).state, 'answered');
    assert.equal(c.db.requests[0].revoked_at, null);
    assert.equal(first.body.bookings.find(b => b.id === 3).status, 'requested');
});

test('booking: what it covers is said by staff; left out, it covers the freight only, whatever the option is called', async () => {
    const h = harness();
    await answered(h);
    const booked = await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal(booked.body.bookings[0].scope, 'port_to_port');
    assert.equal(h.db.bookings[0].scope, 'port_to_port');
    // Corrected while it is live.
    const patched = await h.call('PATCH', '/api/v1/freight-bookings/1', { scope: 'end_to_end' });
    assert.equal(patched.status, 200);
    assert.equal(patched.body.bookings[0].scope, 'end_to_end');
    assert.deepEqual(h.db.audits.filter(a => a.entityType === 'freight_booking' && a.action === 'update').map(a => [a.before, a.after]), [[{ scope: 'port_to_port' }, { scope: 'end_to_end' }]]);
    // The same again changes nothing.
    const version = h.db.bookings[0].row_version;
    assert.equal((await h.call('PATCH', '/api/v1/freight-bookings/1', { scope: 'end_to_end' })).status, 200);
    assert.equal(h.db.bookings[0].row_version, version);
    // Not something a booking can cover.
    for (const bad of ['delivery', 'door', '', 5, null]) {
        const r = await h.call('PATCH', '/api/v1/freight-bookings/1', { scope: bad });
        assert.deepEqual([r.status, r.body.code, r.body.field], [400, 'BAD_FIELD', 'scope'], String(bad));
    }
    assert.equal(h.db.bookings[0].scope, 'end_to_end');

    // Said when booking.
    const g = harness();
    await answered(g);
    assert.equal((await g.call('POST', '/api/v1/freight-quotes/options/1/book', { scope: 'end_to_end' })).body.bookings[0].scope, 'end_to_end');
    const wrong = harness();
    await answered(wrong);
    const r = await wrong.call('POST', '/api/v1/freight-quotes/options/1/book', { scope: 'everything' });
    assert.deepEqual([r.status, r.body.code, r.body.field], [400, 'BAD_FIELD', 'scope']);
    assert.equal(wrong.db.bookings.length, 0);
});

test('a confirmation closes the other forwarder\'s link as filled; cancelling reopens it', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    // Staff mark it for a forwarder who confirmed by phone.
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: ' BK-77 ' });
    assert.equal(confirmed.status, 200);
    assert.equal(confirmed.body.bookings[0].status, 'confirmed');
    assert.equal(confirmed.body.bookings[0].bookingRef, 'BK-77');
    assert.equal(confirmed.body.bookings[0].responseSource, 'staff');
    const other = confirmed.body.requests.find(r => r.id === 2);
    assert.equal(other.state, 'revoked');
    assert.equal(other.revokeReason, 'filled');
    assert.equal(other.url, null);

    const cancelled = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'cancelled' });
    assert.equal(cancelled.body.bookings[0].status, 'cancelled');
    assert.equal(cancelled.body.requests.find(r => r.id === 1).state, 'answered');
    assert.equal(cancelled.body.requests.find(r => r.id === 2).state, 'answered');
    // And the next forwarder can be booked at once, with no new request.
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/2/book', {})).status, 200);
    assert.equal((await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: 'X' })).body.code, 'NOT_LIVE');
});

test('a forwarder\'s own confirmation (written by the portal) is picked up on the next read', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    Object.assign(h.db.bookings[0], { status: 'confirmed', booking_ref: 'BK-1', response_source: 'link' });
    const board = await h.call('GET', '/api/v1/freight-quotes');
    assert.equal(board.body.requests.find(r => r.id === 2).revokeReason, 'filled');
});

test('contents: preview by supplier, the email, the snapshot, and the claim that stops a second send', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal((await h.call('GET', '/api/v1/freight-bookings/1/contents')).body.code, 'NO_CONTAINER');
    const attached = await h.call('PATCH', '/api/v1/freight-bookings/1', { shipmentId: 50 });
    const rowVersion = attached.body.bookings[0].rowVersion;

    const preview = await h.call('GET', '/api/v1/freight-bookings/1/contents');
    assert.equal(preview.body.containerName, 'Draft 12');
    assert.equal(preview.body.groups[0].supplier, 'Acme');
    assert.equal(preview.body.changedSinceSent, null);

    const body = { rowVersion, attachDocuments: false };
    const sent = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', body);
    assert.equal(sent.status, 200);
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.match(mail.html, /Acme/);
    assert.match(mail.html, /Subtotal/);
    assert.equal(sent.body.bookings[0].contentsChanged, false);
    assert.ok(sent.body.bookings[0].contentsSentAt);
    // The same call again (a retry of one that did send) is refused, not sent twice.
    const mails = h.db.sent.length;
    const retry = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', body);
    assert.equal(retry.status, 409);
    assert.equal(retry.body.code, 'STALE');
    assert.equal(h.db.sent.length, mails);
    assert.equal((await h.call('POST', '/api/v1/freight-bookings/1/contents-email', {})).body.field, 'rowVersion');
    // Sent without documents: the booking names none.
    assert.equal(h.db.bookings[0].contents_document_ids, null);
});

test('contents: the documents attached are kept on the booking, replaced by a resend, cleared with the container', async () => {
    const h = harness({
        documentSet: [
            { id: 698, supplier: null, s3_key: 'draft-containers/a/forwarder-quote-Draft_12-v1.pdf', csv_s3_key: 'draft-containers/a/forwarder-quote-Draft_12-v1.csv' },
            { id: 699, supplier: 'Acme', s3_key: 'draft-containers/b/forwarder-quote-Acme-Draft_12-v1.pdf', csv_s3_key: null },
        ],
    });
    h.db.shipments.push({ id: 51, name: 'Draft 13', reference: null, stage: 'DRAFT', deleted_at: null, merged_into_id: null });
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    const attached = await h.call('PATCH', '/api/v1/freight-bookings/1', { shipmentId: 50 });

    const sent = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', { rowVersion: attached.body.bookings[0].rowVersion });
    assert.equal(sent.status, 200);
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.deepEqual(mail.attachments.map(a => a.filename), [
        'forwarder-quote-Draft_12-v1.pdf', 'forwarder-quote-Draft_12-v1.csv', 'forwarder-quote-Acme-Draft_12-v1.pdf',
    ]);
    // The ids, the combined document first: what the portal lists for the forwarder.
    assert.equal(h.db.bookings[0].contents_document_ids, '[698,699]');
    assert.deepEqual(h.db.documentSends.map(x => x.documentId), [698, 699]);
    assert.deepEqual(h.db.documents.map(d => d.shipment_id), [50, 50]);

    // Another container: what was sent about the old one is forgotten.
    await h.call('PATCH', '/api/v1/freight-bookings/1', { shipmentId: 51 });
    assert.equal(h.db.bookings[0].contents_document_ids, null);
    assert.equal(h.db.bookings[0].contents_sent_at, null);
});

// ── a booking and the link's expiry; finished bookings ─────────────────────

// Booking 1 confirmed, and its sailing due to have arrived on 2026-10-01: more
// than a fortnight before the harness's today (2026-11-16).
function arrivedLongAgo(h) {
    Object.assign(h.db.bookings[0], { status: 'confirmed', booking_ref: 'BK-1', response_source: 'link' });
    Object.assign(h.db.options.find(o => o.id === 1), { sailing_date: '2026-09-01', transit_days: 30 });
}

test('state: a booking that stands outranks the link\'s expiry; a finished one no longer holds it open', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    h.db.requests[0].expires_at = future(-1); // the link's own days have run out
    const read = async () => {
        const board = (await h.call('GET', '/api/v1/freight-quotes')).body;
        return { request: board.requests.find(r => r.id === 1), booking: board.bookings.find(b => b.id === 1) };
    };
    let now = await read();
    assert.equal(now.request.state, 'booked');
    assert.equal(now.request.hasLiveBooking, true);
    assert.match(now.request.url, /\/freight\//);
    assert.equal(now.booking.finished, false);

    arrivedLongAgo(h);
    now = await read();
    assert.equal(now.booking.finished, true);
    assert.equal(now.request.state, 'expired');
    // Still no second booking on it, whatever the state says.
    assert.equal(now.request.hasLiveBooking, true);
    assert.equal(now.request.url, null);

    // Finished, but the link has days left (a movement was added late): still the booking's page.
    h.db.requests[0].expires_at = future(5);
    assert.equal((await read()).request.state, 'booked');
});

test('a finished booking is not compared with its container, and stops neither closing the link nor removing the forwarder', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: 'BK-1' });
    const sent = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', { rowVersion: confirmed.body.bookings[0].rowVersion, attachDocuments: false });
    assert.equal(sent.body.bookings[0].contentsChanged, false);
    assert.equal((await h.call('DELETE', '/api/v1/forwarders/1')).body.code, 'HAS_BOOKING');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/revoke')).body.code, 'HAS_BOOKING');

    arrivedLongAgo(h);
    const board = (await h.call('GET', '/api/v1/freight-quotes')).body;
    assert.equal(board.bookings[0].finished, true);
    assert.equal(board.bookings[0].contentsChanged, null);
    const closed = await h.call('POST', '/api/v1/freight-quotes/1/revoke');
    assert.equal(closed.status, 200);
    assert.equal(closed.body.requests.find(r => r.id === 1).state, 'revoked');
    const removed = await h.call('DELETE', '/api/v1/forwarders/1');
    assert.equal(removed.status, 200);
    assert.notEqual(h.db.forwarders[0].deleted_at, null);

    // A booking still asked for does stop it.
    const g = harness();
    await answered(g);
    await g.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal((await g.call('DELETE', '/api/v1/forwarders/1')).body.code, 'HAS_BOOKING');
    assert.equal((await g.call('DELETE', '/api/v1/forwarders/2')).status, 200);
    assert.equal(g.db.requests[1].revoke_reason, 'forwarder_removed');
});

// ── a refused send tried again; a forwarder added to a send ────────────────

test('send: a forwarder Front refused is tried again under the same batchId, as the dialog now has it', async () => {
    let fail = true;
    const h = harness({ frontOk: () => !fail });
    const body = { ...SEND, forwarderIds: [1], stockMovements: [{ from: 'Acme', cbm: 1, weightKg: 100 }] };
    const first = await h.call('POST', '/api/v1/freight-quotes', body);
    assert.deepEqual(first.body.results.map(r => r.outcome), ['failed']);
    assert.equal(first.body.requests[0].state, 'revoked');
    const { token } = h.db.requests[0];

    fail = false;
    const again = await h.call('POST', '/api/v1/freight-quotes', { ...body, windowEnd: '2026-12-20', stockMovements: [{ from: 'Best', cbm: 2, weightKg: 200 }] });
    assert.deepEqual(again.body.results.map(r => [r.requestId, r.outcome]), [[1, 'sent']]);
    // The same row and the same link, open again, with what the dialog said the second time.
    assert.equal(h.db.requests.length, 1);
    const row = h.db.requests[0];
    assert.deepEqual([row.token, row.revoked_at, row.revoke_reason, row.window_end_date], [token, null, null, '2026-12-20']);
    assert.ok(row.sent_at);
    assert.equal(again.body.requests[0].state, 'open');
    assert.deepEqual(again.body.requests[0].stockMovements.map(m => m.from), ['Best']);
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.match(mail.html, new RegExp(`/freight/${token}`));
    assert.match(mail.html, /Best/);
    assert.doesNotMatch(mail.html, /Acme/);

    // And now that it has gone, the same batchId emails nobody.
    const mails = h.db.sent.length;
    assert.deepEqual((await h.call('POST', '/api/v1/freight-quotes', body)).body.results.map(r => r.outcome), ['already_sent']);
    assert.equal(h.db.sent.length, mails);
});

test('ask another forwarder: one added to a send later shares its batch, and a confirmation closes it as filled', async () => {
    const h = harness();
    await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [1] });
    const added = await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [2] });
    assert.deepEqual(added.body.results.map(r => [r.forwarderId, r.outcome]), [[2, 'sent']]);
    assert.equal(h.db.sent.length, 2);
    assert.deepEqual(h.db.requests.map(r => r.batch_id), [BATCH, BATCH]);

    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] });
    await h.call('POST', '/api/v1/freight-quotes/2/options', { options: [OPTION] });
    // Two forwarders answered one send: one thing to decide.
    assert.equal((await h.call('GET', '/api/v1/freight-quotes/summary')).body.answered, 1);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: 'BK-9' });
    assert.equal(confirmed.body.requests.find(r => r.id === 2).revokeReason, 'filled');
});

// ── reminders ──────────────────────────────────────────────────────────────

test('remind: the same link again for what still waits, claimed once; refused when nothing waits', async () => {
    const h = harness();
    await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [1], stockMovements: [{ from: 'Acme', cbm: 1, weightKg: 100 }] });
    h.db.requests[0].expires_at = future(-1); // the link has run out
    const version = h.db.requests[0].row_version;
    const mails = h.db.sent.length;

    const out = await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 0 });
    assert.deepEqual([out.status, out.body.kind], [200, 'quote']);
    const request = out.body.requests[0];
    assert.equal(request.reminderCount, 1);
    assert.ok(request.remindedAt);
    assert.equal(request.state, 'open'); // open again
    assert.equal(h.db.sent.length, mails + 1);
    const mail = h.db.sent[mails];
    assert.deepEqual(mail.to, ['amy@dcg.com']);
    assert.match(mail.subject, /^Reminder: /);
    assert.match(mail.html, new RegExp(`/freight/${h.db.requests[0].token}`));
    assert.match(mail.html, /Enter your quote/);
    assert.match(mail.html, /Acme/);
    // Listed under the email it repeats, not as an email of its own.
    assert.equal(h.db.receipts.length, 1);
    assert.deepEqual(h.db.receiptReminders.map(r => [r.receiptId, r.number, r.by]), [[1, 1, 'staff@example.com']]);
    // The forwarder's open form is not disturbed.
    assert.equal(h.db.requests[0].row_version, version);
    assert.equal(h.db.audits.at(-1).action, 'reminded');
    assert.doesNotMatch(JSON.stringify(h.db.audits), /[a-f0-9]{48}/);

    // The same call again (a retry of one that did send) is refused.
    const retry = await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 0 });
    assert.deepEqual([retry.status, retry.body.code], [409, 'STALE']);
    assert.equal(h.db.sent.length, mails + 1);

    // Quoted, one stock movement still without a price: that is what is asked for.
    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] });
    const prices = await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 1, subject: 'Prices please, {forwarder} ({count})' });
    assert.deepEqual([prices.status, prices.body.kind], [200, 'movements']);
    const second = h.db.sent[h.db.sent.length - 1];
    assert.equal(second.subject, 'Prices please, DCG (1)');
    assert.match(second.html, /Price the stock movements/);
    // No price request went before (the movement was on the first email), so it is listed on its own.
    assert.equal(h.db.receipts.length, 2);

    await h.call('PATCH', '/api/v1/freight-stock-movements/1', { price: '100', currency: 'USD' });
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 2 })).body.code, 'NOTHING_WAITING');

    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/remind', {})).body.field, 'reminderCount');
    await h.call('POST', '/api/v1/freight-quotes/1/revoke');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 2 })).body.code, 'REVOKED');
    assert.equal((await harness({ userType: 'accountant' }).call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 0 })).body.code, 'READ_ONLY');
});

test('remind: a Front failure gives the claim back; a send Front never confirmed counts as sent once reminded', async () => {
    let fail = false;
    const h = harness({ frontOk: () => !fail });
    await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [1] });
    h.db.requests[0].sent_at = null; // the first call was cut short
    fail = true;
    const refused = await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 0 });
    assert.equal(refused.status, 502);
    assert.deepEqual([h.db.requests[0].reminder_count, h.db.requests[0].reminded_at], [0, null]);
    fail = false;
    const out = await h.call('POST', '/api/v1/freight-quotes/1/remind', { reminderCount: 0 });
    assert.equal(out.status, 200);
    assert.ok(out.body.requests[0].sentAt);
});

// ── the badge ──────────────────────────────────────────────────────────────

test('summary: quotes to decide, and confirmed bookings whose contents have not gone', async () => {
    const h = harness();
    await answered(h);
    const summary = async () => (await h.call('GET', '/api/v1/freight-quotes/summary')).body;
    assert.deepEqual(await summary(), { answered: 1, contentsDue: 0, total: 1 });
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    assert.deepEqual(await summary(), { answered: 0, contentsDue: 0, total: 0 });
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: 'BK-1' });
    assert.deepEqual(await summary(), { answered: 0, contentsDue: 1, total: 1 });
    await h.call('POST', '/api/v1/freight-bookings/1/contents-email', { rowVersion: confirmed.body.bookings[0].rowVersion, attachDocuments: false });
    assert.deepEqual(await summary(), { answered: 0, contentsDue: 0, total: 0 });

    // A booking the forwarder declined puts its send back among the quotes to decide.
    const d = harness();
    await answered(d);
    await d.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    Object.assign(d.db.bookings[0], { status: 'declined', response_source: 'link' });
    assert.equal((await d.call('GET', '/api/v1/freight-quotes/summary')).body.answered, 1);
    assert.equal((await d.call('GET', '/api/v1/freight-quotes')).body.requests.find(r => r.id === 1).lastBookingStatus, 'declined');
});

// Forwarder links (2026-11-20): one live booking per container is now a
// unique key, so two attaches at the same moment end as one booking and one
// CONTAINER_TAKEN; and a container's live booking prices its forwarder
// payments (GET /shipments/:id/forwarder-charges).
test('book and attach: a container another booking took at the last moment is CONTAINER_TAKEN', async () => {
    const h = harness();
    await answered(h);
    h.db.raceShipment = 50;
    const booked = await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    assert.equal(booked.status, 409);
    assert.equal(booked.body.code, 'CONTAINER_TAKEN');
    h.db.raceShipment = null;
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    h.db.raceShipment = 50;
    const attached = await h.call('PATCH', '/api/v1/freight-bookings/1', { shipmentId: 50 });
    assert.equal(attached.status, 409);
    assert.equal(attached.body.code, 'CONTAINER_TAKEN');
    assert.equal(h.db.bookings[0].shipment_id, null);
});

// A draft keeps its name on the record after it is made real (TEST shipment
// 1214, 2026-11-20: BOOKED, reference 339, name still "DRAFT-SEA-… - 339 …").
test('bookings: a container made real is named by its number, on the board and when it is taken', async () => {
    const h = harness();
    await answered(h);
    const booked = await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    assert.equal(booked.body.bookings[0].containerName, 'Draft 12');
    assert.equal(booked.body.bookings[0].containerStage, 'DRAFT');

    Object.assign(h.db.shipments[0], { name: 'DRAFT-SEA-261008-104309 - 339 Ningbo ETD 21 Oct', reference: '339', stage: 'BOOKED' });
    const board = await h.call('GET', '/api/v1/freight-quotes');
    assert.equal(board.body.bookings[0].shipmentId, 50);
    assert.equal(board.body.bookings[0].containerName, '339');
    assert.equal(board.body.bookings[0].containerStage, 'BOOKED');

    const taken = await h.call('POST', '/api/v1/freight-quotes/options/2/book', { shipmentId: 50 });
    assert.equal(taken.status, 409);
    assert.equal(taken.body.code, 'CONTAINER_TAKEN');
    assert.equal(taken.body.error, '339 is already attached to another booking.');
});

test('forwarder charges: the live booking on a container, its all-in price, and each stock movement priced on its own', async () => {
    const h = harness();
    await answered(h);
    assert.deepEqual((await h.call('GET', '/api/v1/shipments/50/forwarder-charges')).body, { booking: null, movements: [] });
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50 });
    await h.call('POST', '/api/v1/freight-quotes/1/stock-movements', { notify: false, stockMovements: [{ from: 'Factory A', cbm: 2.5, weightKg: 300 }, { from: 'Factory B', cbm: 1, weightKg: 100 }] });
    const [m1, m2] = h.db.movements.filter(m => m.request_id === 1);
    await h.call('PATCH', `/api/v1/freight-stock-movements/${m1.id}`, { price: '120', currency: 'GBP' });
    h.db.extras.push({ id: 900, freight_booking_id: 1, freight_movement_id: null, amount: '1850.00', currency: 'USD', deleted_at: null });
    h.db.extras.push({ id: 901, freight_booking_id: null, freight_movement_id: m2.id, amount: '10.00', currency: 'GBP', deleted_at: new Date() });
    const r = await h.call('GET', '/api/v1/shipments/50/forwarder-charges');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body, {
        booking: {
            id: 1, status: 'requested', bookingRef: null, forwarderId: 1, forwarderName: 'DCG', forwarderLive: true,
            option: { price: 1850, currency: 'USD', service: 'door_to_port', portFrom: 'Ningbo', portTo: 'Felixstowe', sailingDate: '2026-12-10', arrivalDate: '2027-01-11', containerSize: '40HQ' },
            extraId: 900, extraAmount: 1850, extraCurrency: 'USD',
        },
        movements: [
            { id: m1.id, from: 'Factory A', cbm: 2.5, price: 120, currency: 'GBP', extraId: null, extraAmount: null, extraCurrency: null },
            { id: m2.id, from: 'Factory B', cbm: 1, price: null, currency: null, extraId: null, extraAmount: null, extraCurrency: null },
        ],
    });
    // The forwarder taken off the list: still shown (as the request recorded it), flagged.
    h.db.forwarders[0].deleted_at = new Date();
    assert.equal((await h.call('GET', '/api/v1/shipments/50/forwarder-charges')).body.booking.forwarderLive, false);
    // Cancelled: nothing to price from any more.
    await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'cancelled' });
    assert.deepEqual((await h.call('GET', '/api/v1/shipments/50/forwarder-charges')).body, { booking: null, movements: [] });
});

// ── the delivery leg through Freight quotes (2026-11-24) ─────────────────
//
// A request has a kind. A DELIVERY request goes to a UK haulier: collect at the
// UK port, deliver to the warehouse. Its booking covers the delivery alone, so
// a container can hold it beside its freight booking: one live booking per leg.

const BATCH_D = 'a1b2c3d4-0000-4000-8000-0000000000d1';
const DELIVERY_SEND = { kind: 'delivery', forwarderIds: [2], batchId: BATCH_D, windowStart: '2026-12-20', windowEnd: '2026-12-31', services: ['UK port to warehouse'], containerSize: '40HQ' };
const DELIVERY_OPTION = { sailingDate: '2026-12-22', transitDays: 0, portFrom: 'Felixstowe', portTo: 'Unit 5, Leeds', service: 'UK port to warehouse', price: '650', currency: 'GBP' };
const ALL = '?legs=all';
// The freight requests 1 and 2 (answered), then the delivery request 3 to forwarder 2, answered with option 3.
async function withDelivery(h) {
    await answered(h);
    const sent = await h.call('POST', `/api/v1/freight-quotes${ALL}`, DELIVERY_SEND);
    assert.equal(sent.status, 200, JSON.stringify(sent.body));
    const typed = await h.call('POST', `/api/v1/freight-quotes/3/options${ALL}`, { options: [DELIVERY_OPTION] });
    assert.equal(typed.status, 200, JSON.stringify(typed.body));
}

test('a delivery request: its kind is written with it, its email has its own template, and its option needs no loading date', async () => {
    const h = harness();
    await withDelivery(h);
    assert.deepEqual(h.db.requests.map(r => r.kind), ['freight', 'freight', 'delivery']);
    assert.match(h.db.requests[2].subject, /^freight_delivery_request /, 'not the sailing template');
    assert.match(h.db.requests[0].subject, /^freight_quote_request /);
    // Left blank, it is collected at the UK port and delivered to our warehouse.
    assert.deepEqual([h.db.requests[2].origin, h.db.requests[2].destination], ['UK port', 'Our UK warehouse']);
    const o = h.db.options.find(x => x.request_id === 3);
    assert.deepEqual([String(o.loading_date).slice(0, 10), String(o.sailing_date).slice(0, 10), Number(o.transit_days)], ['2026-12-22', '2026-12-22', 0], 'the collection day in both, delivered the same day');
    assert.match(h.db.sent.at(-1).html, /UK delivery quote request/);
    assert.match(h.db.sent.at(-1).html, /Collection between/);
    // A kind that is not one is refused.
    const bad = await h.call('POST', '/api/v1/freight-quotes', { ...DELIVERY_SEND, batchId: 'a1b2c3d4-0000-4000-8000-0000000000d2', kind: 'air' });
    assert.deepEqual([bad.status, bad.body.field], [400, 'kind']);
});

test('the board gives delivery requests, their options and their bookings only to a caller that asks for both legs', async () => {
    const h = harness();
    await withDelivery(h);
    await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, {});
    const old = await h.call('GET', '/api/v1/freight-quotes');
    assert.deepEqual(old.body.requests.map(r => r.id).sort(), [1, 2], 'a ShipLine from before delivery requests never holds one');
    assert.deepEqual(old.body.bookings, []);
    assert.equal(JSON.stringify(old.body).includes('Unit 5, Leeds'), false);
    const now = await h.call('GET', `/api/v1/freight-quotes${ALL}`);
    assert.deepEqual(now.body.requests.map(r => [r.id, r.kind]).sort(), [[1, 'freight'], [2, 'freight'], [3, 'delivery']]);
    assert.deepEqual(now.body.bookings.map(b => [b.requestId, b.scope]), [[3, 'delivery']]);
    // A write answers with the board the same way.
    const wrote = await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.deepEqual(wrote.body.requests.map(r => r.id).sort(), [1, 2]);
    assert.deepEqual(wrote.body.bookings.map(b => b.requestId), [1]);
});

test('booking: the scope follows the request\'s kind, whatever the body says', async () => {
    const h = harness();
    await withDelivery(h);
    // A delivery request books the delivery alone.
    for (const scope of ['end_to_end', 'port_to_port']) {
        const r = await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { scope });
        assert.deepEqual([r.status, r.body.code, r.body.field], [400, 'BAD_FIELD', 'scope'], scope);
    }
    assert.equal(h.db.bookings.length, 0);
    const booked = await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, {});
    assert.equal(booked.status, 200, JSON.stringify(booked.body));
    assert.equal(h.db.bookings[0].scope, 'delivery');
    assert.match(h.db.bookings[0].subject, /^freight_delivery_booking /);
    assert.match(h.db.sent.at(-1).html, /Please book this delivery/);
    assert.doesNotMatch(h.db.sent.at(-1).html, /this sailing|Loading \/ /);
    // A freight request never books the delivery alone.
    const wrong = await h.call('POST', '/api/v1/freight-quotes/options/1/book', { scope: 'delivery' });
    assert.deepEqual([wrong.status, wrong.body.code, wrong.body.field], [400, 'BAD_FIELD', 'scope']);
});

test('one live booking per leg: a delivery booking sits beside the freight booking, and nothing else does', async () => {
    const h = harness();
    await withDelivery(h);
    // The freight booking (port to port) on container 51, then the delivery booking on the same one.
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 51 })).status, 200);
    const both = await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    assert.equal(both.status, 200, JSON.stringify(both.body));
    assert.deepEqual(h.db.bookings.map(b => [b.shipment_id, b.scope, b.status]), [[51, 'port_to_port', 'requested'], [51, 'delivery', 'requested']]);
    // A second freight booking on it is still refused.
    const second = await h.call('POST', '/api/v1/freight-quotes/options/2/book', { shipmentId: 51 });
    assert.deepEqual([second.status, second.body.code], [409, 'CONTAINER_TAKEN']);
    // The freight booking cannot grow to end to end while another booking delivers it.
    const grow = await h.call('PATCH', '/api/v1/freight-bookings/1', { scope: 'end_to_end' });
    assert.deepEqual([grow.status, grow.body.code], [409, 'CONTAINER_TAKEN']);
    assert.equal(h.db.bookings[0].scope, 'port_to_port');
    // Once the delivery booking is cancelled, it can.
    assert.equal((await h.call('PATCH', `/api/v1/freight-bookings/2${ALL}`, { status: 'cancelled' })).status, 200);
    assert.equal((await h.call('PATCH', '/api/v1/freight-bookings/1', { scope: 'end_to_end' })).status, 200);
});

test('an end-to-end booking already covers the delivery: a delivery booking cannot join it, by the check or by the key', async () => {
    const h = harness();
    await withDelivery(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 51, scope: 'end_to_end' });
    const r = await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    assert.deepEqual([r.status, r.body.code], [409, 'CONTAINER_TAKEN']);
    assert.match(r.body.error, /already has a booking that covers its delivery/);
    assert.equal(h.db.bookings.length, 1);
    // Another delivery booking took the container between the check and the insert: the key, read the same way.
    const g = harness();
    await withDelivery(g);
    g.db.raceShipment = 51;
    const raced = await g.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    assert.deepEqual([raced.status, raced.body.code], [409, 'CONTAINER_TAKEN']);
});

test('a delivery booking stays a delivery booking, and is changed only by a ShipLine that knows what one is', async () => {
    const h = harness();
    await withDelivery(h);
    await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, {});
    for (const scope of ['port_to_port', 'end_to_end', null]) {
        const r = await h.call('PATCH', `/api/v1/freight-bookings/1${ALL}`, { scope });
        assert.deepEqual([r.status, r.body.code, r.body.field], [400, 'BAD_FIELD', 'scope'], String(scope));
    }
    assert.equal(h.db.bookings[0].scope, 'delivery');
    // An older build (no legs=all) cannot attach, confirm or cancel it.
    for (const body of [{ shipmentId: 51 }, { status: 'confirmed', bookingRef: 'JOB-1' }, { status: 'cancelled' }]) {
        const r = await h.call('PATCH', '/api/v1/freight-bookings/1', body);
        assert.deepEqual([r.status, r.body.code, r.body.allowedNext], [409, 'CLIENT_TOO_OLD', ['reload']], JSON.stringify(body));
    }
    assert.deepEqual([h.db.bookings[0].status, h.db.bookings[0].shipment_id], ['requested', null]);
    // The present one attaches it to a booked container and confirms it.
    assert.equal((await h.call('PATCH', `/api/v1/freight-bookings/1${ALL}`, { shipmentId: 51 })).status, 200);
    assert.equal((await h.call('PATCH', `/api/v1/freight-bookings/1${ALL}`, { status: 'confirmed', bookingRef: 'JOB-1' })).status, 200);
    assert.deepEqual([h.db.bookings[0].status, h.db.bookings[0].shipment_id, h.db.bookings[0].scope], ['confirmed', 51, 'delivery']);
});

test('the badge: a delivery booking is owed no contents email, and a delivery quote counts only for a caller that sees them', async () => {
    const h = harness();
    await withDelivery(h);
    // Three answered requests wait: two freight (one batch), one delivery.
    assert.equal((await h.call('GET', '/api/v1/freight-quotes/summary')).body.answered, 1);
    assert.equal((await h.call('GET', `/api/v1/freight-quotes/summary${ALL}`)).body.answered, 2);
    await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    await h.call('PATCH', `/api/v1/freight-bookings/1${ALL}`, { status: 'confirmed', bookingRef: 'JOB-1' });
    assert.equal((await h.call('GET', `/api/v1/freight-quotes/summary${ALL}`)).body.contentsDue, 0, 'confirmed, and nothing to send it');
});

test('a container\'s charges: the freight booking as before, and the delivery booking beside it for a caller that asks', async () => {
    const h = harness();
    await withDelivery(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 51 });
    await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    const old = await h.call('GET', '/api/v1/shipments/51/forwarder-charges');
    assert.equal(old.body.booking.id, 1);
    assert.equal(old.body.booking.option.price, 1850);
    assert.equal('deliveryBooking' in old.body, false);
    const now = await h.call('GET', `/api/v1/shipments/51/forwarder-charges${ALL}`);
    assert.equal(now.body.booking.id, 1, 'the freight booking, never the newer delivery one');
    assert.deepEqual([now.body.deliveryBooking.id, now.body.deliveryBooking.forwarderName, now.body.deliveryBooking.option.price, now.body.deliveryBooking.option.currency], [2, 'Savino', 650, 'GBP']);
    // A container with a delivery booking alone has no freight booking to price.
    const g = harness();
    await withDelivery(g);
    await g.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 });
    assert.deepEqual((await g.call('GET', '/api/v1/shipments/51/forwarder-charges')).body, { booking: null, movements: [] });
    const alone = (await g.call('GET', `/api/v1/shipments/51/forwarder-charges${ALL}`)).body;
    assert.deepEqual([alone.booking, alone.movements, alone.deliveryBooking.id], [null, [], 1]);
});

test('a request never changes kind: no statement that updates a request names it', () => {
    const source = require('fs').readFileSync(require('path').join(__dirname, '..', 'src', 'services', 'freight-quote-routes.js'), 'utf8');
    const updates = source.match(/UPDATE freight_quote_requests[\s\S]*?(?:`|'\s*[,)])/g) || [];
    assert.ok(updates.length >= 8, `found ${updates.length} updates`);
    for (const u of updates) assert.doesNotMatch(u, /\bkind\b/, u.slice(0, 120));
});

test('book-another on a delivery request: the copy is a delivery request and its booking covers the delivery alone', async () => {
    const h = harness();
    await withDelivery(h);
    assert.equal((await h.call('POST', `/api/v1/freight-quotes/options/3/book${ALL}`, { shipmentId: 51 })).status, 200);
    // A freight scope is not something a delivery request's copy can be given.
    const wrong = await h.call('POST', `/api/v1/freight-quotes/options/3/book-another${ALL}`, { copyKey: COPY_KEY, scope: 'end_to_end' });
    assert.deepEqual([wrong.status, wrong.body.field], [400, 'scope']);
    assert.equal(h.db.requests.length, 3);
    const out = await h.call('POST', `/api/v1/freight-quotes/options/3/book-another${ALL}`, { copyKey: COPY_KEY });
    assert.equal(out.status, 200, JSON.stringify(out.body));
    assert.deepEqual([h.db.requests[3].kind, h.db.requests[3].copied_from_request_id, h.db.requests[3].forwarder_id], ['delivery', 3, 2]);
    assert.deepEqual([h.db.bookings[1].request_id, h.db.bookings[1].scope], [4, 'delivery']);
    assert.match(h.db.sent.at(-1).subject, /^freight_delivery_booking /, 'the delivery booking template');
    // Kept from a ShipLine that does not ask for both legs, as every delivery request is.
    assert.deepEqual((await h.call('GET', '/api/v1/freight-quotes')).body.requests.map(r => r.id).sort(), [1, 2]);
});

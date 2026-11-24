'use strict';

// A container's own forwarder pair (shipments.forwarder + forwarder_id, one
// value) from a decision of src/lib/forwarder-links.js (decideShipmentForwarder
// / decideBookForwarder). Used by PATCH /shipments/:id and the booking modal
// (shipment-book.js). Assigning only: a merge copies a stored pair unchanged.

const FL = require('../lib/forwarder-links');
const { ShipmentError } = require('../lib/shipments');

async function forwarderById(conn, id) {
    const [rows] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE id = ?', [id]);
    return rows[0] || null;
}

// Is this forwarder on the list and not removed? A LOCKING read (shared): inside
// a write of a pick it makes a removal of that forwarder wait for the write.
async function onTheList(conn, id) {
    const [rows] = await conn.query('SELECT id, deleted_at FROM forwarders WHERE id = ? FOR SHARE', [id]);
    return Boolean(rows[0]) && !rows[0].deleted_at;
}

// The pair to write, or null for "leave it". Throws a ShipmentError (400) for
// a forwarder that is not on the list.
async function pairFor(conn, decision) {
    switch (decision.action) {
        case 'untouched': return null;
        case 'clear': return { id: null, name: null };
        case 'set': return { id: null, name: decision.name };
        case 'byId': {
            const f = await forwarderById(conn, decision.id);
            if (!f || f.deleted_at) {
                throw new ShipmentError(400, 'BAD_FIELD', `Forwarder ${decision.id} is not on the Forwarders list.`, { field: 'forwarderId' });
            }
            return { id: Number(f.id), name: f.name };
        }
        case 'byText': {
            const [list] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE deleted_at IS NULL');
            return FL.pairFromText(decision.text, list);
        }
        default: throw new Error(`forwarder decision ${decision.action}`);
    }
}

module.exports = { forwarderById, onTheList, pairFor };

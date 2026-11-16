'use strict';

// The first entries of the delivery option list (2026-11-16_30,
// docs/freight-quotes.md): the four services a request could ask for before
// the list became staff's own. Only into a table that has never held a row, so
// a re-run, or a list staff have already emptied, is left alone.

const { DEFAULT_DELIVERY_OPTIONS } = require('../../lib/freight-quotes');

exports.up = async (conn, { log }) => {
    const [[{ n }]] = await conn.query('SELECT COUNT(*) AS n FROM freight_delivery_options');
    if (Number(n) > 0) {
        log(`freight_delivery_options already holds ${n} row(s); nothing seeded`);
        return;
    }
    await conn.query(
        `INSERT INTO freight_delivery_options (label) VALUES ${DEFAULT_DELIVERY_OPTIONS.map(() => '(?)').join(', ')}`,
        DEFAULT_DELIVERY_OPTIONS
    );
    log(`${DEFAULT_DELIVERY_OPTIONS.length} delivery options seeded`);
};

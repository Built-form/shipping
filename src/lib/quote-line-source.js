'use strict';

// The rows a quote document is built from, as FROM / WHERE / ORDER BY parts
// that yield `allocated` per row. A draft name reads that draft's allocations
// (legacy truth while it is open). { reference } reads a booked container's own
// live orders at their quantities (legacy truth is TRIM(orders.container_number)):
// booking deleted the draft's allocations, and a partial pack split the order so
// the packed slice is its own row. See lib/shipments.js documentSource.
// (Moved here from src/handlers/orders.js so the Request to send status reads
// lines exactly as the PDF does.)
function quoteLineSource(source) {
    if (source && typeof source === 'object') {
        return {
            allocated: 'o.quantity AS allocated',
            from: 'orders o',
            where: 'o.deleted_at IS NULL AND TRIM(o.container_number) = ?',
            orderBy: 'o.id ASC',
            params: [source.reference],
        };
    }
    return {
        allocated: 'dca.allocated',
        from: `draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL`,
        where: 'dca.draft_container_name = ?',
        orderBy: 'dca.id ASC',
        params: [source],
    };
}

// quoteLineSource for many drafts or many references in one query: the same
// tables, quantity column and filters, plus `source_key` (the draft name or the
// trimmed reference) naming which source each row belongs to. Keep in step
// with quoteLineSource above.
function quoteLineSourceMany(kind, keys) {
    if (kind === 'booked') {
        return {
            sourceKey: 'TRIM(o.container_number) AS source_key',
            allocated: 'o.quantity AS allocated',
            from: 'orders o',
            where: 'o.deleted_at IS NULL AND TRIM(o.container_number) IN (?)',
            orderBy: 'o.id ASC',
            params: [keys],
        };
    }
    return {
        sourceKey: 'dca.draft_container_name AS source_key',
        allocated: 'dca.allocated',
        from: `draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL`,
        where: 'dca.draft_container_name IN (?)',
        orderBy: 'dca.id ASC',
        params: [keys],
    };
}

module.exports = { quoteLineSource, quoteLineSourceMany };

'use strict';

// The draft_containers registry backfill that src/handlers/orders.js used to
// run at cold start (draftRegistryReady): registers every legacy draft name and
// reconstructs its history in audit_log. Self-guarded by its app_migrations
// marker ('draft_container_audit_backfill_v1'), so it is a no-op wherever it
// already ran — every existing database — and only does work on a fresh one.

const draftAudit = require('../../lib/draft-audit');

exports.up = async (conn, { log }) => {
    const { ran } = await draftAudit.backfillDraftRegistry(conn);
    log(ran ? 'draft_containers registry backfilled' : 'already done (app_migrations marker present)');
};

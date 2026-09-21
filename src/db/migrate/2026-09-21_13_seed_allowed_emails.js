'use strict';

// First users for shipping_allowed_emails (was ensureAllowedEmailsSchema in
// src/lib/allowed-emails.js, run on every cold start). Only acts when the
// table is empty: copies joshdex's shared allowed_emails if it exists, else
// seeds BOOTSTRAP_ADMIN_EMAILS (from the stage's secret, or .env) as admins.
// A no-op on every existing database.

const { seedAllowedEmails } = require('../../lib/allowed-emails');

exports.up = async (conn, { env }) => {
    await seedAllowedEmails(conn, { bootstrapAdminEmails: env.BOOTSTRAP_ADMIN_EMAILS });
};

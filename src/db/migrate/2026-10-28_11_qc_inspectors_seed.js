'use strict';

// Seed the one QC agency we use today (AQI Service; its reports arrive from
// aqiservice.com, see src/services/front-qc-import.js). INSERT IGNORE on
// uq_qc_inspectors_name: a re-run, or an AQI row edited since, is left alone.

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO qc_inspectors (name, kind, company, notes, created_by_email)
         VALUES ('AQI', 'agency', 'AQI Service', 'Reports arrive from aqiservice.com', 'migration')`
    );
    log(r.affectedRows ? 'AQI inspector created' : 'AQI inspector already existed');
};

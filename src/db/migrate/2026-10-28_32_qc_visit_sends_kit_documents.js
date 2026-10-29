'use strict';

// qc_visit_sends.kit_documents_json, for databases where 2026-10-28_30 was
// applied before it was edited (that first version carried brief_id instead).
// The runner never re-runs an applied file, so on those databases the column
// never arrived and every QC visit email failed AFTER Front had sent it
// ("Unknown column 'kit_documents_json'"), leaving no send row.
//
// Reads the live columns first, so it is a no-op on a fresh database (where
// 2026-10-28_30 already has kit_documents_json and no brief_id) and safe to
// re-run. A leftover brief_id is made nullable: nothing writes it any more,
// and the send INSERT does not name it.
//
// The same history applies to qc_visit_lines: an early 2026-10-28_10 carried
// kit_version_id and extra_checks_json, which the code no longer writes. Where
// they survive as NOT NULL they would break every line insert, so they are
// made nullable too (type kept from the live column).

exports.up = async (conn, { log }) => {
    const [cols] = await conn.query(
        `SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'qc_visit_sends'`
    );
    if (!cols.length) {
        log('qc_visit_sends does not exist; nothing to do');
        return;
    }
    const byName = new Map(cols.map(c => [c.COLUMN_NAME, c]));
    if (!byName.has('kit_documents_json')) {
        await conn.query('ALTER TABLE qc_visit_sends ADD COLUMN kit_documents_json JSON NULL AFTER sent_to');
        log('added qc_visit_sends.kit_documents_json');
    }
    const brief = byName.get('brief_id');
    if (brief && brief.IS_NULLABLE !== 'YES') {
        await conn.query('ALTER TABLE qc_visit_sends MODIFY COLUMN brief_id INT NULL');
        log('made qc_visit_sends.brief_id nullable');
    }

    const [lineCols] = await conn.query(
        `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'qc_visit_lines'
            AND COLUMN_NAME IN ('kit_version_id', 'extra_checks_json')`
    );
    for (const c of lineCols) {
        if (c.IS_NULLABLE === 'YES') continue;
        // COLUMN_TYPE is interpolated, so accept only plain types such as
        // `int`, `int(11)`, `int unsigned` or `json`.
        if (!/^[a-z]+(\(\d+\))?( unsigned)?$/i.test(c.COLUMN_TYPE)) {
            throw new Error(`qc_visit_lines.${c.COLUMN_NAME} has unexpected type ${c.COLUMN_TYPE}`);
        }
        await conn.query(`ALTER TABLE qc_visit_lines MODIFY COLUMN \`${c.COLUMN_NAME}\` ${c.COLUMN_TYPE} NULL`);
        log(`made qc_visit_lines.${c.COLUMN_NAME} nullable`);
    }
};

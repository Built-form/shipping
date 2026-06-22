'use strict';
// Inspect the Replenishment HangerWorld project + the section from the URL,
// surfacing the HW0461 / B001VYUZZE task(s) with the fields the API flows use.
//
//   node tools/peek-hw0461.js
require('dotenv').config();
const axios = require('axios');

const ASANA_BASE = 'https://app.asana.com/api/1.0';
const PROJECT = '1214307738471512'; // Replenishment HangerWorld
const SECTION = '1214307852230030'; // section from the URL
const NEEDLE_CODE = 'HW0461';
const NEEDLE_ASIN = 'B001VYUZZE';

const OPT = 'name,completed,created_at,modified_at,memberships.section.name,memberships.section.gid,memberships.project.name,custom_fields.name,custom_fields.display_value';

(async () => {
    const pat = process.env.ASANA_PAT;
    if (!pat) { console.error('ASANA_PAT not set'); process.exit(1); }
    const headers = { Authorization: `Bearer ${pat}` };

    // Section name
    try {
        const s = await axios.get(`${ASANA_BASE}/sections/${SECTION}`, { headers, params: { opt_fields: 'name,project.name' } });
        console.log(`Section ${SECTION} = "${s.data.data?.name}" in "${s.data.data?.project?.name}"`);
    } catch (e) { console.log(`Section lookup failed: ${e.response?.status}`); }

    // Full project sweep (matches flow #2 fetchProjectTasks)
    const tasks = [];
    let offset = null;
    do {
        const params = { limit: 100, opt_fields: OPT };
        if (offset) params.offset = offset;
        const { data } = await axios.get(`${ASANA_BASE}/projects/${PROJECT}/tasks`, { headers, params });
        tasks.push(...(data.data || []));
        offset = data.next_page?.offset || null;
    } while (offset);
    console.log(`\nProject ${PROJECT} has ${tasks.length} tasks total.`);

    const code = t => (t.custom_fields || []).find(c => c.name === 'JF / HW Code')?.display_value || null;
    const asin = t => (t.custom_fields || []).find(c => c.name === 'ASIN')?.display_value || null;

    const hits = tasks.filter(t =>
        (code(t) || '').toUpperCase().includes(NEEDLE_CODE) ||
        (t.name || '').toUpperCase().includes(NEEDLE_CODE) ||
        asin(t) === NEEDLE_ASIN
    );
    console.log(`\nMatches for ${NEEDLE_CODE} / ${NEEDLE_ASIN}: ${hits.length}`);
    for (const t of hits) {
        const m = (t.memberships || []).find(x => x.section) || {};
        console.log(`  ${t.gid}  "${t.name}"`);
        console.log(`     code=${code(t)}  asin=${asin(t)}  completed=${t.completed}  created=${t.created_at}`);
        console.log(`     section="${m.section?.name}" (${m.section?.gid})`);
    }
})().catch(e => {
    console.error('Fatal:', e.response?.status, JSON.stringify(e.response?.data || e.message));
    process.exit(1);
});

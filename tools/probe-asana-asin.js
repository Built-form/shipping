'use strict';
// Probe the Asana portfolio + search flow used by GET /stock-snapshots/asana-tasks
// against an arbitrary ASIN, printing each step so we can see what's filtering.
//
//   node tools/probe-asana-asin.js B001AG78X0

require('dotenv').config();
const axios = require('axios');

const ASANA_BASE = 'https://app.asana.com/api/1.0';
const ASANA_PORTFOLIO_GID = '1207527576522409';
const ASANA_PROJECT_BY_COMPANY = {
    JFA: '1214003962428764',
    HANGERWORLD: '1214307738471512',
};

(async () => {
    const asin = (process.argv[2] || 'B001AG78X0').trim();
    const pat = process.env.ASANA_PAT;
    if (!pat) { console.error('ASANA_PAT not set in env'); process.exit(1); }
    const headers = { Authorization: `Bearer ${pat}` };

    console.log('== Portfolio metadata ==');
    const meta = await axios.get(`${ASANA_BASE}/portfolios/${ASANA_PORTFOLIO_GID}`, {
        headers, params: { opt_fields: 'workspace.gid,name' },
    });
    const workspaceGid = meta.data.data?.workspace?.gid;
    console.log({ name: meta.data.data?.name, workspaceGid });

    console.log('\n== Portfolio items ==');
    const items = await axios.get(`${ASANA_BASE}/portfolios/${ASANA_PORTFOLIO_GID}/items`, {
        headers, params: { opt_fields: 'gid,name,resource_type' },
    });
    const portfolioProjects = (items.data.data || []).filter(i => i.resource_type === 'project');
    console.log(`Found ${portfolioProjects.length} projects:`);
    for (const p of portfolioProjects) console.log(`  ${p.gid}  ${p.name}`);

    const portfolioIdSet = new Set(portfolioProjects.map(p => p.gid));
    const extraIds = Object.values(ASANA_PROJECT_BY_COMPANY).filter(id => !portfolioIdSet.has(id));
    const extras = extraIds.length === 0 ? [] : (await Promise.all(
        extraIds.map(id => axios.get(`${ASANA_BASE}/projects/${id}`, { headers, params: { opt_fields: 'gid,name' } }))
    )).map(r => r.data.data);
    console.log(`\n== Extra (company-routing) projects merged in: ${extras.length} ==`);
    for (const p of extras) console.log(`  ${p.gid}  ${p.name}`);

    const allProjects = [...portfolioProjects, ...extras];

    console.log(`\n== Custom field settings on ${allProjects[0].name} (${allProjects[0].gid}) ==`);
    const cfs = await axios.get(`${ASANA_BASE}/projects/${allProjects[0].gid}/custom_field_settings`, {
        headers, params: { opt_fields: 'custom_field.name,custom_field.gid,custom_field.type' },
    });
    const settings = cfs.data.data || [];
    for (const s of settings) console.log(`  ${s.custom_field?.name} (${s.custom_field?.gid}) [${s.custom_field?.type}]`);
    const asinSetting = settings.find(s => s.custom_field?.name === 'ASIN');
    if (!asinSetting) { console.error('ASIN custom field not found.'); process.exit(2); }
    console.log(`\nASIN field GID: ${asinSetting.custom_field.gid}`);

    const projectIds = allProjects.map(p => p.gid);

    console.log(`\n== Search workspace ${workspaceGid} for ASIN=${asin} across ${projectIds.length} projects ==`);
    let resp;
    try {
        resp = await axios.get(`${ASANA_BASE}/workspaces/${workspaceGid}/tasks/search`, {
            headers,
            params: {
                'projects.any': projectIds.join(','),
                [`custom_fields.${asinSetting.custom_field.gid}.value`]: asin,
                opt_fields: 'name,completed,memberships.section.name,memberships.project.gid,memberships.project.name,custom_fields.name,custom_fields.display_value',
                limit: 100,
            },
        });
    } catch (err) {
        console.error('Search failed:', err.response?.status, JSON.stringify(err.response?.data || err.message, null, 2));
        process.exit(3);
    }
    const tasks = resp.data.data || [];
    console.log(`Search returned ${tasks.length} tasks.`);
    for (const t of tasks) {
        const asinCf = (t.custom_fields || []).find(cf => cf.name === 'ASIN');
        console.log(`  ${t.gid}  ${t.name}  asin_cf=${asinCf?.display_value}`);
        for (const m of (t.memberships || [])) {
            console.log(`    proj=${m.project?.name} (${m.project?.gid})  section=${m.section?.name}`);
        }
    }

    // Also try without the projects filter to see if the ASIN exists anywhere in the workspace.
    console.log(`\n== Workspace-wide ASIN search (no projects filter) ==`);
    try {
        const wide = await axios.get(`${ASANA_BASE}/workspaces/${workspaceGid}/tasks/search`, {
            headers,
            params: {
                [`custom_fields.${asinSetting.custom_field.gid}.value`]: asin,
                opt_fields: 'name,memberships.project.gid,memberships.project.name',
                limit: 100,
            },
        });
        const wt = wide.data.data || [];
        console.log(`Workspace-wide hits: ${wt.length}`);
        for (const t of wt) {
            const projs = (t.memberships || []).map(m => `${m.project?.name}(${m.project?.gid})`).join(', ');
            console.log(`  ${t.gid}  ${t.name}  in: ${projs}`);
        }
    } catch (err) {
        console.error('Wide search failed:', err.response?.status, JSON.stringify(err.response?.data || err.message, null, 2));
    }
})().catch(err => {
    console.error('Fatal:', err.response?.status, JSON.stringify(err.response?.data || err.message, null, 2));
    process.exit(1);
});

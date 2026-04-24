require('dotenv').config();
const https = require('https');

const ASANA_PAT = process.env.ASANA_PAT;
const SOURCE_PROJECT = '1207505242228155'; // Goods in / Goods out Schedule
const TARGET_PROJECT = '1210568539171010'; // Goods on sea

function asanaRequest(method, path, body) {
    return new Promise((resolve, reject) => {
        const bodyStr = body ? JSON.stringify(body) : null;
        const options = {
            hostname: 'app.asana.com',
            path: `/api/1.0${path}`,
            method,
            headers: {
                'Authorization': `Bearer ${ASANA_PAT}`,
                ...(bodyStr ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
            },
        };
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (err) {
                    reject(new Error(`JSON parse error: ${data.slice(0, 200)}`));
                }
            });
        });
        req.on('error', reject);
        if (bodyStr) req.write(bodyStr);
        req.end();
    });
}

async function fetchAllPages(path) {
    let results = [];
    let currentPath = path;
    while (true) {
        const response = await asanaRequest('GET', currentPath);
        if (response.errors) {
            console.error('API Error:', response.errors);
            break;
        }
        if (response.data) results = results.concat(response.data);
        if (response.next_page && response.next_page.offset) {
            const sep = path.includes('?') ? '&' : '?';
            currentPath = `${path}${sep}offset=${response.next_page.offset}`;
        } else {
            break;
        }
    }
    return results;
}

async function withConcurrency(items, limit, fn) {
    const results = [];
    let i = 0;
    async function worker() {
        while (i < items.length) {
            const item = items[i++];
            results.push(await fn(item));
        }
    }
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
    return results;
}

async function main() {
    // 1. Get sections from source project
    console.log('Fetching sections from Goods in / Goods out Schedule...');
    const sourceSections = await fetchAllPages(`/projects/${SOURCE_PROJECT}/sections`);
    console.log(`  Found ${sourceSections.length} sections:`, sourceSections.map(s => s.name).join(', '));

    const seaFreightSection = sourceSections.find(s => s.name.toLowerCase().includes('sea freight'));
    if (!seaFreightSection) {
        console.error('Could not find Sea Freight section!');
        return;
    }
    console.log(`  Using section: "${seaFreightSection.name}" (${seaFreightSection.gid})`);

    // 2. Get tasks from Sea Freight section with custom fields
    console.log('\nFetching tasks from Sea Freight section...');
    const seaFreightTasks = await fetchAllPages(
        `/sections/${seaFreightSection.gid}/tasks?opt_fields=name,custom_fields,custom_fields.name,custom_fields.display_value,custom_fields.date_value,custom_fields.type`
    );
    console.log(`  Found ${seaFreightTasks.length} tasks`);

    // Build name -> ETA map (ShipsGoETA preferred, ETA to Port fallback)
    // Build name -> Delivery Date map
    const etaMap = {};
    const deliveryDateMap = {};
    for (const task of seaFreightTasks) {
        const shipsGoField = task.custom_fields?.find(f =>
            f.name && f.name.toLowerCase().replace(/[\s_]/g, '') === 'shipsgoeta'
        );
        const etaField = task.custom_fields?.find(f =>
            f.name && f.name.toLowerCase() === 'eta to port'
        );
        const deliveryField = task.custom_fields?.find(f =>
            f.name && f.name.toLowerCase() === 'delivery date'
        );

        let dateVal = null;
        let source = null;

        // Primary: ShipsGoETA
        if (shipsGoField) {
            if (shipsGoField.date_value && shipsGoField.date_value.date) {
                dateVal = shipsGoField.date_value.date;
                source = 'ShipsGoETA';
            } else if (shipsGoField.display_value) {
                dateVal = shipsGoField.display_value.slice(0, 10);
                source = 'ShipsGoETA';
            }
        }

        // Fallback: ETA to Port
        if (!dateVal && etaField) {
            if (etaField.date_value && etaField.date_value.date) {
                dateVal = etaField.date_value.date;
                source = 'ETA to Port';
            } else if (etaField.display_value) {
                dateVal = etaField.display_value.slice(0, 10);
                source = 'ETA to Port';
            }
        }

        if (dateVal) {
            etaMap[task.name.trim()] = dateVal;
            console.log(`  ${task.name}: ETA = ${dateVal} (${source})`);
        } else {
            console.log(`  ${task.name}: no ETA value`);
        }

        // Delivery Date
        let deliveryVal = null;
        if (deliveryField) {
            if (deliveryField.date_value && deliveryField.date_value.date) {
                deliveryVal = deliveryField.date_value.date;
            } else if (deliveryField.display_value) {
                deliveryVal = deliveryField.display_value.slice(0, 10);
            }
        }
        if (deliveryVal) {
            deliveryDateMap[task.name.trim()] = deliveryVal;
            console.log(`  ${task.name}: Delivery Date = ${deliveryVal}`);
        }
    }

    const matchCount = Object.keys(etaMap).length;
    const deliveryCount = Object.keys(deliveryDateMap).length;
    console.log(`\nBuilt ETA map with ${matchCount} entries`);
    console.log(`Built Delivery Date map with ${deliveryCount} entries`);
    if (matchCount === 0) {
        console.log('No ETAs found, nothing to update.');
        return;
    }

    // 3. Get sections from target project (Goods on sea)
    console.log('\nFetching sections from Goods on sea...');
    const targetSections = await fetchAllPages(`/projects/${TARGET_PROJECT}/sections`);
    console.log(`  Found ${targetSections.length} sections:`, targetSections.map(s => s.name).join(', '));

    // 4. Discover the ETA to Port and Delivery Date custom field GIDs
    console.log('\nDiscovering custom fields from target project...');
    const sampleResp = await asanaRequest('GET',
        `/projects/${TARGET_PROJECT}/tasks?opt_fields=custom_fields.name,custom_fields.gid,custom_fields.type&limit=1`
    );
    let etaFieldGid = null;
    let deliveryDateFieldGid = null;
    if (sampleResp.data && sampleResp.data.length > 0) {
        const fields = sampleResp.data[0].custom_fields || [];
        for (const f of fields) {
            console.log(`  Field: "${f.name}" (${f.gid}) type=${f.type}`);
            if (f.name && f.name.toLowerCase() === 'eta to port') {
                etaFieldGid = f.gid;
            }
            if (f.name && f.name.toLowerCase() === 'delivery date') {
                deliveryDateFieldGid = f.gid;
            }
        }
    }

    if (!etaFieldGid) {
        console.error('\nCould not find "ETA to Port" custom field! Listed all fields above.');
        return;
    }
    console.log(`\nUsing ETA to Port field GID: ${etaFieldGid}`);

    if (!deliveryDateFieldGid) {
        console.warn('\nCould not find "Delivery Date" custom field in target project! Will skip delivery date updates.');
    } else {
        console.log(`Using Delivery Date field GID: ${deliveryDateFieldGid}`);
    }

    // 5. Match sections and update tasks
    let totalUpdated = 0;
    let totalFailed = 0;
    for (const section of targetSections) {
        const sectionName = section.name.trim();
        const etaValue = etaMap[sectionName];
        const deliveryValue = deliveryDateMap[sectionName];
        if (!etaValue && !deliveryValue) continue;

        if (etaValue) console.log(`\nSection "${sectionName}" -> ETA to Port = ${etaValue}`);
        if (deliveryValue) console.log(`${etaValue ? '' : '\n'}Section "${sectionName}" -> Delivery Date = ${deliveryValue}`);

        const tasks = await fetchAllPages(`/sections/${section.gid}/tasks?opt_fields=name`);
        console.log(`  ${tasks.length} tasks to update`);

        await withConcurrency(tasks, 10, async (task) => {
            try {
                const customFields = {};
                if (etaValue) {
                    customFields[etaFieldGid] = { date: etaValue };
                }
                if (deliveryValue && deliveryDateFieldGid) {
                    customFields[deliveryDateFieldGid] = { date: deliveryValue };
                }

                const resp = await asanaRequest('PUT', `/tasks/${task.gid}`, {
                    data: { custom_fields: customFields }
                });
                if (resp.errors) {
                    console.error(`  FAIL "${task.name}": ${JSON.stringify(resp.errors)}`);
                    totalFailed++;
                } else {
                    console.log(`  OK "${task.name}"`);
                    totalUpdated++;
                }
            } catch (err) {
                console.error(`  FAIL "${task.name}": ${err.message}`);
                totalFailed++;
            }
        });
    }

    console.log(`\nDone! Updated: ${totalUpdated}, Failed: ${totalFailed}`);
}

// ── Lambda Handler ─────────────────────────────────────────────────────────
exports.handler = async (event) => {
    try {
        await main();
        return { statusCode: 200, body: JSON.stringify({ message: 'ETA to Port and Delivery Date updated' }) };
    } catch (err) {
        console.error('Fatal:', err.message);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// ── CLI Runner ─────────────────────────────────────────────────────────────
if (require.main === module) {
    exports.handler({}).then(result => {
        console.log(JSON.stringify(result, null, 2));
    }).catch(err => {
        console.error('Fatal:', err);
    });
}

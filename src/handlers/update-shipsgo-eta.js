require('dotenv').config();
const https = require('https');

const ASANA_PAT = process.env.ASANA_PAT;
const SHIPSGO_API_KEY = process.env.SHIPSGO_API_KEY;
const SOURCE_PROJECT = '1207505242228155'; // Goods in / Goods out Schedule


// ── Asana ──────────────────────────────────────────────────────────────────
function asanaRequest(method, path, body) {
    return new Promise((resolve, reject) => {
        const bodyStr = body ? JSON.stringify(body) : null;
        const options = {
            hostname: 'app.asana.com',
            path: `/api/1.0${path}`,
            method,
            headers: {
                'Authorization': `Bearer ${ASANA_PAT}`,
                'Content-Type': 'application/json',
                ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
            },
        };
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); }
                catch (err) { reject(new Error(`JSON parse error: ${data.slice(0, 200)}`)); }
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
        if (response.errors) { console.error('Asana API Error:', response.errors); break; }
        if (response.data) results = results.concat(response.data);
        if (response.next_page && response.next_page.offset) {
            const sep = path.includes('?') ? '&' : '?';
            currentPath = `${path}${sep}offset=${response.next_page.offset}`;
        } else break;
    }
    return results;
}

// ── ShipsGo ────────────────────────────────────────────────────────────────
function shipsgoRequest(method, path, body) {
    return new Promise((resolve, reject) => {
        const bodyStr = body ? JSON.stringify(body) : null;
        const options = {
            hostname: 'api.shipsgo.com',
            path: `/v2${path}`,
            method,
            headers: {
                'X-Shipsgo-User-Token': SHIPSGO_API_KEY,
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
            },
        };
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
                catch { resolve({ status: res.statusCode, body: data }); }
            });
        });
        req.on('error', reject);
        if (bodyStr) req.write(bodyStr);
        req.end();
    });
}

async function getShipsGoETA(containerNumber) {
    // Try listing first
    const listResp = await shipsgoRequest('GET', `/ocean/shipments?filters[container_number]=eq:${containerNumber}`);
    if (listResp.status === 200) {
        const shipments = listResp.body.shipments || listResp.body.data || [];
        if (Array.isArray(shipments) && shipments.length > 0) {
            return await fetchShipmentETA(shipments[0].id);
        }
    }

    // Not found — register it
    console.log(`  Registering ${containerNumber} with ShipsGo...`);
    const createResp = await shipsgoRequest('POST', '/ocean/shipments', { container_number: containerNumber });

    if (createResp.status === 409) {
        const id = createResp.body.shipment?.id;
        return await fetchShipmentETA(id);
    }
    if (createResp.status === 200 || createResp.status === 201) {
        const id = createResp.body.shipment?.id || createResp.body.id;
        return await fetchShipmentETA(id);
    }
    if (createResp.status === 422) {
        // Invalid container number format (e.g. road/trailer references) — skip silently
        console.log(`  Skipping — not a valid sea container number`);
        return { eta: 'SKIP' };
    }

    console.error(`  ShipsGo error (${createResp.status}):`, JSON.stringify(createResp.body));
    return { eta: null };
}

async function fetchShipmentETA(id) {
    const resp = await shipsgoRequest('GET', `/ocean/shipments/${id}`);
    if (resp.status !== 200) {
        console.error(`  Failed to fetch shipment ${id}:`, JSON.stringify(resp.body));
        return { eta: null, checkedAt: null };
    }
    const shipment = resp.body.shipment || resp.body;
    const eta = shipment.route?.port_of_discharge?.date_of_discharge;
    return { eta: eta ? eta.slice(0, 10) : null };
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
    // 1. Get Sea Freight section from source project
    console.log('Fetching sections from Goods in / Goods out Schedule...');
    const sections = await fetchAllPages(`/projects/${SOURCE_PROJECT}/sections`);
    const seaSection = sections.find(s => s.name.toLowerCase().includes('sea freight'));
    if (!seaSection) { console.error('Sea Freight section not found!'); return; }
    console.log(`  Using: "${seaSection.name}"`);

    // 2. Fetch tasks with custom fields
    console.log('\nFetching Sea Freight tasks...');
    const tasks = await fetchAllPages(
        `/sections/${seaSection.gid}/tasks?opt_fields=name,gid,custom_fields,custom_fields.gid,custom_fields.name,custom_fields.display_value,custom_fields.type`
    );
    console.log(`  Found ${tasks.length} tasks`);

    // 3. Discover field GIDs from tasks
    let shipsGoEtaGid = null;
    let etaChangedGid = null;
    let containerNoGid = null;
    for (const task of tasks) {
        for (const f of (task.custom_fields || [])) {
            const key = f.name?.toLowerCase().replace(/[\s_.]/g, '');
            if (key === 'shipsgoeta') shipsGoEtaGid = f.gid;
            if (key === 'containerno') containerNoGid = f.gid;
            if (key === 'shipsgoetachanged' || key === 'etachanged') etaChangedGid = f.gid;
        }
        if (shipsGoEtaGid && containerNoGid) break;
    }

    if (!shipsGoEtaGid) { console.error('Could not find ShipsGoETA custom field!'); return; }
    if (!containerNoGid) { console.error('Could not find Container No. custom field!'); return; }
    console.log(`\nShipsGoETA field GID    : ${shipsGoEtaGid}`);
    console.log(`Container No. field GID : ${containerNoGid}`);
    console.log(`ETA Changed field GID   : ${etaChangedGid || 'not found — create a date field named "ShipsGoETA Changed"'}`);

    // 4. Process each task
    let updated = 0, skipped = 0, failed = 0;
    for (const task of tasks) {
        const containerField = task.custom_fields?.find(f => f.gid === containerNoGid);
        const containerNumber = containerField?.display_value?.trim();

        if (!containerNumber) {
            console.log(`\nTask "${task.name}": no container number, skipping`);
            skipped++;
            continue;
        }

        console.log(`\nTask "${task.name}" — container: ${containerNumber}`);
        const { eta } = await getShipsGoETA(containerNumber);

        if (eta === 'SKIP') {
            skipped++;
            continue;
        }
        if (!eta) {
            console.log(`  No ETA available`);
            failed++;
            continue;
        }

        // Check if ETA has changed since last run
        const currentEtaField = task.custom_fields?.find(f => f.gid === shipsGoEtaGid);
        const currentEta = currentEtaField?.display_value
            ? new Date(currentEtaField.display_value).toISOString().slice(0, 10)
            : null;
        const etaChanged = currentEta && currentEta !== eta;

        // Check if ETA Changed field is currently empty
        const currentEtaChangedField = task.custom_fields?.find(f => f.gid === etaChangedGid);
        const etaChangedEmpty = etaChangedGid && !currentEtaChangedField?.display_value;

        if (!etaChanged && !etaChangedEmpty) {
            console.log(`  ETA: ${eta} (unchanged)`);
            skipped++;
            continue;
        }

        const customFields = { [shipsGoEtaGid]: { date: eta } };
        if (etaChangedGid) {
            if (etaChanged) {
                console.log(`  ETA CHANGED: ${currentEta} -> ${eta}`);
                customFields[etaChangedGid] = { date: new Date().toISOString().slice(0, 10) };
            } else if (etaChangedEmpty) {
                console.log(`  ETA: ${eta} (new) — setting baseline`);
                customFields[etaChangedGid] = { date: new Date().toISOString().slice(0, 10) };
            }
        } else {
            console.log(`  ETA: ${eta} (new) — updating Asana...`);
        }

        const resp = await asanaRequest('PUT', `/tasks/${task.gid}`, {
            data: { custom_fields: customFields }
        });

        if (resp.errors) {
            console.error(`  FAIL: ${JSON.stringify(resp.errors)}`);
            failed++;
        } else {
            console.log(`  OK`);
            updated++;
        }
    }

    console.log(`\nDone! Updated: ${updated}, Skipped: ${skipped}, Failed: ${failed}`);
}

exports.handler = async () => {
    try {
        await main();
        return { statusCode: 200, body: JSON.stringify({ message: 'ShipsGo ETAs updated' }) };
    } catch (err) {
        console.error('Fatal:', err.message);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

if (require.main === module) {
    exports.handler().catch(err => console.error('Fatal:', err));
}

require('dotenv').config();
const https = require('https');

const API_KEY = process.env.SHIPSGO_API_KEY;
const BASE_HOST = 'api.shipsgo.com';
const BASE_PATH = '/v2';

function shipsgoRequest(method, path, body) {
    return new Promise((resolve, reject) => {
        const bodyStr = body ? JSON.stringify(body) : null;
        const options = {
            hostname: BASE_HOST,
            path: `${BASE_PATH}${path}`,
            method,
            headers: {
                'X-Shipsgo-User-Token': API_KEY,
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
            },
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                console.log(`  HTTP ${res.statusCode}`);
                try {
                    resolve({ status: res.statusCode, body: JSON.parse(data) });
                } catch {
                    resolve({ status: res.statusCode, body: data });
                }
            });
        });
        req.on('error', reject);
        if (bodyStr) req.write(bodyStr);
        req.end();
    });
}

async function getContainerETA(containerNumber) {
    console.log(`\nChecking if "${containerNumber}" is already tracked...`);
    const listResp = await shipsgoRequest('GET', `/ocean/shipments?filters[container_number]=eq:${containerNumber}`);

    if (listResp.status !== 200) {
        console.error('Failed to list shipments:', JSON.stringify(listResp.body, null, 2));
        return;
    }

    const shipments = listResp.body.data || listResp.body;
    const existing = Array.isArray(shipments) && shipments.length > 0 ? shipments[0] : null;

    if (existing) {
        console.log(`  Already tracked (id: ${existing.id})`);
        return await fetchAndPrintShipment(existing.id);
    }

    console.log(`  Not tracked yet — registering...`);
    const createResp = await shipsgoRequest('POST', '/ocean/shipments', {
        container_number: containerNumber,
    });

    if (createResp.status === 409) {
        // Already exists — fetch by the returned id
        const id = createResp.body.shipment && createResp.body.shipment.id;
        console.log(`  Already exists (id: ${id}) — fetching details...`);
        return await fetchAndPrintShipment(id);
    }

    if (createResp.status !== 200 && createResp.status !== 201) {
        console.error('Failed to register container:', JSON.stringify(createResp.body, null, 2));
        return;
    }

    console.log(`  Registered successfully`);
    const shipment = createResp.body.data || createResp.body;
    printETA(shipment);
    return shipment;
}

async function fetchAndPrintShipment(id) {
    const resp = await shipsgoRequest('GET', `/ocean/shipments/${id}`);
    if (resp.status !== 200) {
        console.error('Failed to fetch shipment:', JSON.stringify(resp.body, null, 2));
        return;
    }
    printETA(resp.body);
    return resp.body.shipment || resp.body;
}

function printETA(shipment) {
    // Unwrap if nested under 'shipment' key
    const s = shipment.shipment || shipment;

    const eta = s.route?.port_of_discharge?.date_of_discharge;
    const container = s.container_number || (s.containers && s.containers[0]?.number);
    const status = s.status;
    const carrier = s.carrier?.name;
    const pod = s.route?.port_of_discharge?.location?.name;

    console.log(`\nContainer : ${container}`);
    console.log(`Carrier   : ${carrier}`);
    console.log(`Status    : ${status}`);
    console.log(`ETA (${pod}): ${eta ? eta.slice(0, 10) : 'unknown'}`);

    return { container, carrier, status, eta: eta ? eta.slice(0, 10) : null };
}

// ── Lambda Handler ─────────────────────────────────────────────────────────
exports.handler = async (event) => {
    const containerNumber = (event && event.containerNumber) || 'MRKU4285754';
    try {
        const result = await getContainerETA(containerNumber);
        return { statusCode: 200, body: JSON.stringify(result) };
    } catch (err) {
        console.error('Fatal:', err.message);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// ── CLI Runner ─────────────────────────────────────────────────────────────
if (require.main === module) {
    const container = process.argv[2] || 'MRKU4285754';
    exports.handler({ containerNumber: container }).catch(err => console.error('Fatal:', err));
}

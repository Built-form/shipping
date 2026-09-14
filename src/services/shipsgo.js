require('dotenv').config();
const https = require('https');

const API_KEY = process.env.SHIPSGO_API_KEY;
const BASE_HOST = 'api.shipsgo.com';
const BASE_PATH = '/v2';

// ShipsGo rate-limits per account across ALL endpoints (~100 req/min; the reply
// is a bare 429 "Too Many Attempts"). The container/air sync loops fire 3 calls
// per shipment back-to-back, so an unthrottled run of 40 shipments burned the
// budget partway through and every remaining shipment 429'd — always the same
// tail of the list, which therefore never got a `containers` row at all.
// So: serialise requests behind a minimum gap, and retry a 429 after a wait.
const MIN_REQUEST_GAP_MS = 700;
const MAX_429_RETRIES = 3;
const RETRY_429_WAIT_MS = 20000;

const sleep = ms => new Promise(r => setTimeout(r, ms));

let requestChain = Promise.resolve();
let lastRequestAt = 0;

// Queues onto a single chain so concurrent callers can't bypass the gap.
function throttle() {
    const next = requestChain.then(async () => {
        const wait = MIN_REQUEST_GAP_MS - (Date.now() - lastRequestAt);
        if (wait > 0) await sleep(wait);
        lastRequestAt = Date.now();
    });
    requestChain = next.catch(() => {});
    return next;
}

async function shipsgoRequest(method, path, body, attempt = 0) {
    await throttle();
    const resp = await rawShipsgoRequest(method, path, body);

    if (resp.status === 429 && attempt < MAX_429_RETRIES) {
        const retryAfter = Number(resp.headers?.['retry-after']);
        const wait = Number.isFinite(retryAfter) && retryAfter > 0
            ? retryAfter * 1000
            : RETRY_429_WAIT_MS * (attempt + 1);
        console.log(`  429 — backing off ${Math.round(wait / 1000)}s (attempt ${attempt + 1}/${MAX_429_RETRIES})`);
        await sleep(wait);
        return shipsgoRequest(method, path, body, attempt + 1);
    }
    return resp;
}

function rawShipsgoRequest(method, path, body) {
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
                    resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(data) });
                } catch {
                    resolve({ status: res.statusCode, headers: res.headers, body: data });
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

// ── High-level fetch + parse for `containers` table population ────────────
// Resolves a container number to a ShipsGo shipment ID, registering it if
// not yet tracked, then fetches both the shipment detail AND the GeoJSON
// route (which is where the vessel's current lat/lng lives — the standard
// shipment endpoint doesn't expose them).
async function getShipmentByContainer(containerNumber, { mapPoint = true, geojson = true } = {}) {
    const cn = encodeURIComponent(containerNumber);
    const listResp = await shipsgoRequest('GET', `/ocean/shipments?filters[container_number]=eq:${cn}`);

    // v2 wraps results under `shipments` ("data" was never present). Reading the
    // wrong key made every lookup look untracked, so we POSTed a registration for
    // every container on every run and ShipsGo 429'd the tail of the list.
    let shipmentId = null;
    if (listResp.status === 200) {
        const list = listResp.body.shipments || listResp.body.data || listResp.body;
        if (Array.isArray(list) && list.length > 0) shipmentId = list[0].id;
    }

    if (!shipmentId) {
        const createResp = await shipsgoRequest('POST', '/ocean/shipments', { container_number: containerNumber });
        if (createResp.status === 409) {
            shipmentId = createResp.body.shipment?.id || createResp.body.data?.id || null;
        } else if (createResp.status === 200 || createResp.status === 201) {
            shipmentId = createResp.body.data?.id || createResp.body.id || createResp.body.shipment?.id || null;
        } else {
            throw new Error(`ShipsGo register failed (${createResp.status}): ${JSON.stringify(createResp.body).slice(0, 300)}`);
        }
    }
    if (!shipmentId) throw new Error(`Could not resolve ShipsGo shipment for ${containerNumber}`);

    const detailPath = mapPoint
        ? `/ocean/shipments/${shipmentId}?mapPoint=true`
        : `/ocean/shipments/${shipmentId}`;
    const detailResp = await shipsgoRequest('GET', detailPath);
    if (detailResp.status !== 200) {
        throw new Error(`ShipsGo fetch failed (${detailResp.status}): ${JSON.stringify(detailResp.body).slice(0, 300)}`);
    }
    const shipment = detailResp.body.shipment || detailResp.body.data || detailResp.body;

    // Optional GeoJSON fetch — it's the only place ShipsGo exposes coords.
    // Failure here is non-fatal: we'd rather store the shipment without
    // position than lose the whole row.
    let geo = null;
    if (geojson) {
        try {
            const geoResp = await shipsgoRequest('GET', `/ocean/shipments/${shipmentId}/geojson`);
            if (geoResp.status === 200) {
                geo = geoResp.body.geojson || geoResp.body.data || geoResp.body || null;
            }
        } catch { /* swallow; position stays null */ }
    }

    return { shipment, geojson: geo };
}

// Parses ShipsGo v2 /ocean/shipments/{id} responses against the real schema:
//
//   { id, container_number, booking_number, reference, status, checked_at,
//     carrier: { name, scac },
//     route: {
//       co2_emission, transit_time, transit_percentage, ts_count,
//       port_of_loading:  { location: { code, name, country, timezone },
//                           date_of_loading, date_of_loading_initial },
//       port_of_discharge: { location, date_of_discharge, date_of_discharge_initial },
//     },
//     containers: [{ size, type, number, status,
//                    movements: [{ event, status: ACT|EST, vessel, voyage,
//                                  location, timestamp }] }],
//     tags, ... }
//
// Fields the basic v2 endpoint does NOT include — left as null in the table
// so other carriers / future API extensions can populate them: bl_number,
// bl_type, service_name, seal_number, holds, free_time_info, demurrage_info,
// por_*, final_delivery_*, discharge_terminal, current_lat/lng.
function parseShipment(rawOrBundle) {
    // Accept both legacy raw shipments and the new { shipment, geojson } bundle.
    const isBundle = rawOrBundle
        && typeof rawOrBundle === 'object'
        && 'shipment' in rawOrBundle
        && 'geojson' in rawOrBundle;
    const raw = isBundle ? rawOrBundle.shipment : rawOrBundle;
    const geojson = isBundle ? rawOrBundle.geojson : null;
    const s = raw?.shipment || raw || {};
    const tri = v => (v === true ? 1 : v === false ? 0 : null);
    const dt = v => (v ? String(v).replace('T', ' ').slice(0, 19) : null);

    // Pull current vessel position out of the GeoJSON FeatureCollection.
    // The "CURRENT" LineString feature represents the in-progress leg; its
    // properties carry the live position. Field names vary slightly across
    // ShipsGo plans, so check several plausible paths.
    let currentLat = null;
    let currentLng = null;
    if (geojson?.features) {
        const currentLine = geojson.features.find(f =>
            f.geometry?.type === 'LineString'
            && (f.properties?.status || f.properties?.Status) === 'CURRENT'
        );
        if (currentLine) {
            const p = currentLine.properties || {};
            const cp = p.current || p.current_position || p.currentPosition || p.position || {};
            const coords = cp.coordinates || cp.coords;
            if (Array.isArray(coords) && coords.length >= 2) {
                currentLng = Number(coords[0]);
                currentLat = Number(coords[1]);
            } else if (cp.lat != null && (cp.lng != null || cp.lon != null)) {
                currentLat = Number(cp.lat);
                currentLng = Number(cp.lng ?? cp.lon);
            } else if (typeof cp.index === 'number' && Array.isArray(currentLine.geometry.coordinates)) {
                // Some responses point at an index inside the LineString's
                // coordinate array rather than emitting a separate point.
                const pt = currentLine.geometry.coordinates[cp.index];
                if (Array.isArray(pt) && pt.length >= 2) {
                    currentLng = Number(pt[0]);
                    currentLat = Number(pt[1]);
                }
            }
        }
    }
    const locodeOf = loc => loc?.code || loc?.locode || loc?.unlocode || null;
    const countryOf = loc => {
        const c = loc?.country;
        if (c && typeof c === 'object' && typeof c.code === 'string') return c.code.slice(0, 2).toUpperCase();
        if (typeof c === 'string' && c.length === 2) return c.toUpperCase();
        const direct = loc?.country_code;
        if (typeof direct === 'string' && direct.length >= 2) return direct.slice(0, 2).toUpperCase();
        const lc = locodeOf(loc);
        if (typeof lc === 'string' && lc.length >= 2) return lc.slice(0, 2).toUpperCase();
        return null;
    };

    const polWrap = s.route?.port_of_loading || {};
    const podWrap = s.route?.port_of_discharge || {};
    const polLoc = polWrap.location || {};
    const podLoc = podWrap.location || {};

    const c0 = s.containers?.[0] || {};
    const movements = c0.movements || [];

    // Movements → milestones (one row per discrete event, with the IsActual
    // flag derived from m.status: ACT = actual, EST = estimated).
    const milestones = movements.map(m => ({
        event: m.event || null,
        is_actual: m.status === 'ACT',
        timestamp: dt(m.timestamp),
        port: {
            locode: locodeOf(m.location),
            name: m.location?.name || null,
            country: countryOf(m.location),
        },
        vessel: m.vessel ? { imo: m.vessel.imo || null, name: m.vessel.name || null } : null,
        voyage: m.voyage || null,
    }));

    // "Current" vessel: most-recent ACT event with a vessel attached.
    // Fallback: first upcoming EST event's vessel (which usually matches).
    const lastAct = [...movements].reverse().find(m => m.status === 'ACT' && m.vessel);
    const firstEst = movements.find(m => m.status === 'EST' && m.vessel);
    const cur = lastAct || firstEst || {};
    const curVessel = cur.vessel || null;

    // Transshipments: ports visited between POL and POD. Aggregate ARRV +
    // DEPA at each non-POL/POD location, preserving order of first arrival.
    const polCode = locodeOf(polLoc);
    const podCode = locodeOf(podLoc);
    const tsByCode = new Map();
    for (const m of movements) {
        const code = locodeOf(m.location);
        if (!code || code === polCode || code === podCode) continue;
        if (!tsByCode.has(code)) {
            tsByCode.set(code, {
                port: m.location?.name || null,
                locode: code,
                country: countryOf(m.location),
                arrival: null, arrival_is_actual: null,
                departure: null, departure_is_actual: null,
            });
        }
        const t = tsByCode.get(code);
        if (m.event === 'ARRV') { t.arrival = dt(m.timestamp); t.arrival_is_actual = m.status === 'ACT'; }
        if (m.event === 'DEPA') { t.departure = dt(m.timestamp); t.departure_is_actual = m.status === 'ACT'; }
    }
    const transshipments = [...tsByCode.values()];

    // Departure / arrival actuality is derived from the matching movement
    // at the POL / POD location, not from any top-level flag.
    const polDepa = movements.find(m => m.event === 'DEPA' && locodeOf(m.location) === polCode);
    const podArrv = movements.find(m => m.event === 'ARRV' && locodeOf(m.location) === podCode);
    const departureIsActual = polDepa ? polDepa.status === 'ACT' : null;
    const arrivalIsActual = podArrv ? podArrv.status === 'ACT' : null;

    const podDate = podWrap.date_of_discharge || null;
    const podDateInitial = podWrap.date_of_discharge_initial || null;

    // Combine size (40) + type ("HC") into "40HC" when both are available.
    const sizeType = (c0.size != null && c0.type) ? `${c0.size}${c0.type}` : (c0.type || null);

    return {
        container_number: s.container_number || c0.number || null,
        bl_number: null,
        booking_ref: s.booking_number || null,
        vessel_imo: curVessel?.imo || null,
        vessel_name: curVessel?.name || null,
        voyage: cur.voyage || null,
        shipping_line: s.carrier?.name || null,
        carrier_scac: s.carrier?.scac || null,
        carrier_ref_number: s.reference || null,
        bl_type: null,
        service_name: null,

        pol_name: polLoc.name || null,
        pol_locode: locodeOf(polLoc),
        pol_country: countryOf(polLoc),
        pod_name: podLoc.name || null,
        pod_locode: locodeOf(podLoc),
        pod_country: countryOf(podLoc),
        por_name: null,
        por_locode: null,
        final_delivery_name: null,
        final_delivery_locode: null,
        discharge_terminal: null,
        transshipments,

        current_lat: currentLat,
        current_lng: currentLng,

        departure_date: dt(polWrap.date_of_loading),
        departure_is_actual: tri(departureIsActual),
        arrival_date: dt(podDate),
        arrival_is_actual: tri(arrivalIsActual),
        // ETA = current estimate when not yet arrived; ATA = same date when
        // it has been confirmed actual.
        eta: arrivalIsActual === false ? dt(podDate) : null,
        ata: arrivalIsActual === true ? dt(podDate) : null,
        eta_initial: dt(podDateInitial),
        total_transit_days: s.route?.transit_time ?? null,
        carrier_last_updated: dt(s.checked_at),

        milestones,
        co2_emissions: s.route?.co2_emission ?? null,
        delay_status: s.status || null,
        container_size_type: sizeType,
        seal_number: null,
        holds: null,
        free_time_info: null,
        tags: Array.isArray(s.tags) && s.tags.length ? s.tags : null,
        demurrage_info: null,
        shipsgo_id: s.id || null,
        route_geojson: geojson || null,
        raw: isBundle ? { shipment: raw, geojson } : raw,
    };
}

// ── AIR (AWB) tracking ─────────────────────────────────────────────────────
// The ShipsGo v2 Air API mirrors the Ocean API used above: same host, auth
// header and request conventions. Differences: shipments are keyed by AWB
// number (not container), carriers are airlines (IATA code, not SCAC),
// locations are airports (3-letter IATA, not UN/LOCODE), and movements carry
// a flight number instead of vessel/voyage. Air event codes:
//   RCS Received from shipper · MAN Manifested · DEP Departed ·
//   ARR Arrived · RCF Received from flight · DLV Delivered
// Resolves an AWB to a ShipsGo air shipment id, registering it if not yet
// tracked, then fetches the shipment detail plus the GeoJSON route.
async function getShipmentByAwb(awbNumber, { geojson = true } = {}) {
    const awb = encodeURIComponent(awbNumber);
    const listResp = await shipsgoRequest('GET', `/air/shipments?filters[awb_number]=eq:${awb}`);

    // Same wrapper as ocean: results live under `shipments`.
    let shipmentId = null;
    if (listResp.status === 200) {
        const list = listResp.body.shipments || listResp.body.data || listResp.body;
        if (Array.isArray(list) && list.length > 0) shipmentId = list[0].id;
    }

    if (!shipmentId) {
        const createResp = await shipsgoRequest('POST', '/air/shipments', { awb_number: awbNumber });
        if (createResp.status === 409) {
            shipmentId = createResp.body.shipment?.id || createResp.body.data?.id || null;
        } else if (createResp.status === 200 || createResp.status === 201) {
            shipmentId = createResp.body.data?.id || createResp.body.id || createResp.body.shipment?.id || null;
        } else {
            throw new Error(`ShipsGo air register failed (${createResp.status}): ${JSON.stringify(createResp.body).slice(0, 300)}`);
        }
    }
    if (!shipmentId) throw new Error(`Could not resolve ShipsGo air shipment for ${awbNumber}`);

    const detailResp = await shipsgoRequest('GET', `/air/shipments/${shipmentId}`);
    if (detailResp.status !== 200) {
        throw new Error(`ShipsGo air fetch failed (${detailResp.status}): ${JSON.stringify(detailResp.body).slice(0, 300)}`);
    }
    const shipment = detailResp.body.shipment || detailResp.body.data || detailResp.body;

    let geo = null;
    if (geojson) {
        try {
            const geoResp = await shipsgoRequest('GET', `/air/shipments/${shipmentId}/geojson`);
            if (geoResp.status === 200) {
                geo = geoResp.body.geojson || geoResp.body.data || geoResp.body || null;
            }
        } catch { /* swallow; position stays null */ }
    }

    return { shipment, geojson: geo };
}

// Parses ShipsGo v2 /air/shipments/{id} responses into the flat shape the
// `air_shipments` table stores. Mirrors parseShipment() for ocean.
function parseAirShipment(rawOrBundle) {
    const isBundle = rawOrBundle
        && typeof rawOrBundle === 'object'
        && 'shipment' in rawOrBundle
        && 'geojson' in rawOrBundle;
    const raw = isBundle ? rawOrBundle.shipment : rawOrBundle;
    const geojson = isBundle ? rawOrBundle.geojson : null;
    const s = raw?.shipment || raw || {};

    const tri = v => (v === true ? 1 : v === false ? 0 : null);
    const dt = v => (v ? String(v).replace('T', ' ').slice(0, 19) : null);
    // reference / checked_at are nullable-or-string in the spec; coerce to string|null.
    const str = v => (typeof v === 'string' && v ? v : null);
    const iataOf = loc => loc?.iata || null;
    const countryOf = loc => {
        const c = loc?.country;
        if (c && typeof c === 'object' && typeof c.code === 'string') return c.code.slice(0, 2).toUpperCase();
        if (typeof c === 'string' && c.length === 2) return c.toUpperCase();
        return null;
    };

    const route = s.route || {};
    const origin = route.origin || {};
    const dest = route.destination || {};
    const originLoc = origin.location || {};
    const destLoc = dest.location || {};
    const originIata = iataOf(originLoc);
    const destIata = iataOf(destLoc);

    const movements = Array.isArray(s.movements) ? s.movements : [];
    const milestones = movements.map(m => ({
        event: m.event || null,
        is_actual: m.status === 'ACT',
        timestamp: dt(m.timestamp),
        airport: {
            iata: iataOf(m.location),
            name: m.location?.name || null,
            country: countryOf(m.location),
        },
        flight: str(m.flight),
    }));

    // Departure actuality from the DEP movement at origin; arrival from the
    // RCF (received from flight) — or ARR — movement at destination.
    const depMove = movements.find(m => m.event === 'DEP' && iataOf(m.location) === originIata)
        || movements.find(m => m.event === 'DEP');
    const arrMove = movements.find(m => (m.event === 'RCF' || m.event === 'ARR') && iataOf(m.location) === destIata)
        || movements.find(m => m.event === 'RCF' || m.event === 'ARR');
    const departureIsActual = depMove ? depMove.status === 'ACT' : null;
    const arrivalIsActual = arrMove ? arrMove.status === 'ACT' : null;

    // Transshipments: airports touched between origin and destination.
    const tsByIata = new Map();
    for (const m of movements) {
        const iata = iataOf(m.location);
        if (!iata || iata === originIata || iata === destIata) continue;
        if (!tsByIata.has(iata)) {
            tsByIata.set(iata, {
                airport: m.location?.name || null,
                iata,
                country: countryOf(m.location),
                arrival: null, arrival_is_actual: null,
                departure: null, departure_is_actual: null,
            });
        }
        const t = tsByIata.get(iata);
        if (m.event === 'ARR' || m.event === 'RCF') { t.arrival = dt(m.timestamp); t.arrival_is_actual = m.status === 'ACT'; }
        if (m.event === 'DEP') { t.departure = dt(m.timestamp); t.departure_is_actual = m.status === 'ACT'; }
    }
    const transshipments = [...tsByIata.values()];

    // Current position out of the GeoJSON FeatureCollection (best-effort —
    // field shapes vary across ShipsGo plans; null when not found).
    let currentLat = null;
    let currentLng = null;
    if (geojson?.features) {
        const currentLine = geojson.features.find(f =>
            f.geometry?.type === 'LineString'
            && (f.properties?.status || f.properties?.Status) === 'CURRENT'
        );
        const f = currentLine
            || geojson.features.find(ft => ft.geometry?.type === 'Point' && (ft.properties?.status || ft.properties?.Status) === 'CURRENT');
        if (f) {
            const p = f.properties || {};
            const cp = p.current || p.current_position || p.currentPosition || p.position || {};
            const coords = cp.coordinates || cp.coords || (f.geometry?.type === 'Point' ? f.geometry.coordinates : null);
            if (Array.isArray(coords) && coords.length >= 2) {
                currentLng = Number(coords[0]);
                currentLat = Number(coords[1]);
            } else if (cp.lat != null && (cp.lng != null || cp.lon != null)) {
                currentLat = Number(cp.lat);
                currentLng = Number(cp.lng ?? cp.lon);
            }
        }
    }

    const rcfDate = dest.date_of_rcf || null;
    const depDate = origin.date_of_dep || null;

    return {
        awb_number: s.awb_number || null,
        reference: str(s.reference),
        airline_name: s.airline?.name || null,
        airline_iata: s.airline?.iata || null,
        status: s.status || null,

        origin_name: originLoc.name || null,
        origin_iata: originIata,
        origin_country: countryOf(originLoc),
        destination_name: destLoc.name || null,
        destination_iata: destIata,
        destination_country: countryOf(destLoc),
        transshipments,

        current_lat: currentLat,
        current_lng: currentLng,

        departure_date: dt(depDate),
        departure_is_actual: tri(departureIsActual),
        departure_initial: dt(origin.date_of_dep_initial),
        arrival_date: dt(rcfDate),
        arrival_is_actual: tri(arrivalIsActual),
        // ETA = current estimate when not yet arrived; ATA = same date once actual.
        eta: arrivalIsActual === false ? dt(rcfDate) : null,
        ata: arrivalIsActual === true ? dt(rcfDate) : null,
        eta_initial: dt(dest.date_of_rcf_initial),
        total_transit_time: route.transit_time ?? null,
        transit_percentage: route.transit_percentage ?? null,
        ts_count: route.ts_count ?? null,

        milestones,
        tags: Array.isArray(s.tags) && s.tags.length ? s.tags.map(t => (t && t.name) || t) : null,
        checked_at: dt(str(s.checked_at)),
        shipsgo_id: s.id != null ? String(s.id) : null,
        route_geojson: geojson || null,
        raw: isBundle ? { shipment: raw, geojson } : raw,
    };
}

exports.getShipmentByContainer = getShipmentByContainer;
exports.parseShipment = parseShipment;
exports.getShipmentByAwb = getShipmentByAwb;
exports.parseAirShipment = parseAirShipment;

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

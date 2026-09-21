'use strict';

// ── Front email → order linker (the "email index") ───────────────────────────
//
// Scheduled sibling of front-status-import.js and front-qc-import.js. Where the
// status importer asks Gemini to INFER a status change from supplier mail, this
// job is purely DETERMINISTIC and much broader: it walks EVERY recent Front
// conversation (no sender filter), scans the thread text for known order
// identifiers — PO number, container / AWB number, SKU (jf_code), ASIN, lot —
// and writes a LINK row per (conversation, order) it matches. No Gemini, no
// status changes, no human approval: it just builds a searchable index so the
// UI can answer "show me every email about this order" (GET /orders/:id/emails).
//
// WHY DETERMINISTIC: the user wants the email⇄order graph, keyed on identifiers
// that already live on the order row. Matching is a string/token lookup against
// an in-memory index of all live orders' identifiers — cheap, exact, and
// repeatable. A whole run over a quiet window is a Front search + a few SELECTs.
//
// PRECISION (learned from the live data — see the per-type filters below):
//   • orders.container_number is a JUNK field here ("103", "UPS", "test",
//     "112. Air Freight"); the real shipment refs are in
//     external_container_number (ISO MRKU5353914, UPS 1Z…, AWB 999-93393786).
//     usableShipRef() keeps only real-looking refs and drops the labels.
//   • po_number ("YOHO-102", "TAM-123") is alpha-prefixed and order-specific →
//     normalized-substring match (floor on length + must contain a letter).
//   • jf_code / asin are PRODUCT identifiers (one SKU → many historical orders).
//     A bare SKU mention must NOT fan an email out to every order of that SKU,
//     so product matches are SUPPRESSED whenever a strong (PO/container/AWB) or
//     batch (jf+lot) identifier already pins specific orders, and are capped
//     when they're the only signal.
//   • lot_number is a 7-digit batch — far too generic alone, so it only counts
//     as supporting evidence on an order already matched by its jf_code.
//
// TIERS recorded on each link (so the API can rank strong-first):
//   strong  — PO / container / AWB matched (identifies the shipment/order)
//   batch   — jf_code + its lot_number both present (identifies the batch)
//   product — only a SKU/ASIN matched (whole-product, lower confidence)
//
// IDEMPOTENT: UNIQUE(conversation_id, order_id) upsert refreshes metadata and
// adds newly-appearing links; a per-conversation ledger (front_email_index) +
// an incremental watermark (front_email_scan_state) mean re-runs only re-walk
// conversations with activity since the last fully-drained run. Links are
// append-only facts ("this thread referenced this order") and are never
// auto-removed — even if the order later changes its container.

const log = require('../lib/logger');
const {
    searchConversations,
    listMessages,
    messageFromEmail,
    emailInDomain,
    messageText,
    cleanSubject,
    frontWebUrl,
} = require('./front-qc-import');
const { extractOrderCluesFromEmail } = require('./order-email-extract');

const DEFAULT_SINCE_DAYS = 3;          // first-ever run (no watermark yet)
const DEFAULT_OVERLAP_HOURS = 6;       // re-scan a little before the last run (late mail)
const DEFAULT_MAX_LOOKBACK_DAYS = 30;  // incremental outage cap — a long gap can't trigger a giant scan
// Absolute hard ceiling on how far back ANY run reaches — including a manual
// `sinceDays` backfill. We never index email older than six months.
const DEFAULT_MAX_IMPORT_DAYS = 180;
// Hard ceiling on conversations walked per run so a wide backfill window can't
// blow the Lambda timeout on listMessages calls.
const MAX_CONVERSATIONS = Number(process.env.EMAIL_INDEX_MAX_CONVERSATIONS) || 4000;
// Per-conversation text fed to the matcher (all messages concatenated, capped).
const BLOB_CAP = 120000;
// Full thread body STORED on each link (the whole email, not just a preview) so
// the UI can show it without re-fetching from Front. MEDIUMTEXT holds 16 MB;
// half a megabyte of text covers any realistic thread.
const BODY_STORE_CAP = Number(process.env.EMAIL_INDEX_BODY_CAP) || 500000;
// A bare-SKU email (no strong/batch id) must not fan out to an unbounded number
// of historical orders of that SKU. Above this we still record the conversation
// but skip the product links and flag it ambiguous.
const MAX_PRODUCT_FANOUT = Number(process.env.EMAIL_INDEX_MAX_PRODUCT_FANOUT) || 12;

// ── Gemini Flash fallback (gated, capped) ────────────────────────────────────
// Only fires on conversations the deterministic matcher couldn't link. The gate
// (business sender OR order-ish keywords) + per-run cap keep LLM spend bounded.
const DEFAULT_MAX_GEMINI = Number(process.env.EMAIL_INDEX_MAX_GEMINI) || 25;
const DEFAULT_GEMINI_CONF_MIN = 0.55;
// Conservative cap on the no-identifier supplier+product fuzzy path.
const FUZZY_CAP = Number(process.env.EMAIL_INDEX_FUZZY_CAP) || 3;
// Freight forwarders (not in the suppliers tables) — order-related senders.
const FORWARDER_DOMAINS = ['dcglogistics.com', 'savinodelbene.com'];
// Cheap "is this plausibly about an order/shipment?" gate before a Gemini call.
const ORDER_HINT_RE = /produc|manufactur|\bready\b|ship|vessel|sail|depart|\betd\b|\beta\b|load|dispatch|deliver|consign|container|booking|inspect|\bqc\b|airway|\bawb\b|bill of lading|\bb\/?l\b|cargo|forwarder|pick.?up|collect|invoice|proforma|\bp\/?o\b|purchase order|\border\b|\blot\b|batch|carton|pallet|freight|customs|packing list/i;

function envNum(name, fallback) {
    const n = Number(process.env[name]);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

// Uppercase + strip every non-alphanumeric. Used for PO / ship-ref / lot
// matching against an identically-normalized email blob (so "YOHO-102",
// "YOHO 102" and "YOHO102" all compare equal).
function normAlnum(s) {
    return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

// Is this *_container_number / awb value a real shipment reference worth
// matching, or internal junk ("103", "UPS", "test", "112. Air Freight")?
// Returns the normalized ref to match on, or null to ignore the value.
function usableShipRef(raw) {
    const s = String(raw || '').trim();
    if (!s || /\s/.test(s)) return null;          // labels with spaces ("112. Air Freight")
    const norm = normAlnum(s);
    if (norm.length < 8) return null;             // "103", "282", "UPS", "TEST"
    if (/^[A-Z]{4}\d{7}$/.test(norm)) return norm;  // ISO 6346 container (MRKU5353914)
    if (/^1Z[A-Z0-9]{16}$/.test(norm)) return norm; // UPS tracking (1ZG07Y…)
    if (/^\d{11}$/.test(norm)) return norm;         // air waybill (999-93393786, dashless too)
    // A booking / BL / SCAC-style ref: a letter block then a digit block. We do
    // NOT accept arbitrary long alphanumerics — external_container_number is a
    // junk-prone free-text field, so an interleaved value like "HANGER2024BLK"
    // or "INVOICE2024A" must NOT be treated as a shipment reference.
    if (/^[A-Z]{3,}\d{6,}$/.test(norm)) return norm;
    return null;
}

// PO is order-specific; keep it only if it's distinctive enough to token-match
// safely: ≥5 normalized chars AND contains a letter (rejects bare numbers).
function usablePo(raw) {
    const norm = normAlnum(raw);
    if (norm.length < 5 || !/[A-Z]/.test(norm)) return null;
    return norm;
}

// Base SKU for a jf_code ("JF0130_FQC" → "JF0130"); the FQC twin is a separate
// order of the SAME product, so both should match a "JF0130" mention.
function baseJf(raw) {
    const m = String(raw || '').toUpperCase().match(/JF\d{4,}/);
    return m ? m[0] : null;
}
function baseAsin(raw) {
    const m = String(raw || '').toUpperCase().match(/B0[A-Z0-9]{8}/);
    return m ? m[0] : null;
}

// ── Order identifier index (built once per run) ──────────────────────────────
// Loads every live order's identifiers into reverse maps so matching a
// conversation is a handful of lookups, not a query per email.
async function buildOrderIndex(conn) {
    const [rows] = await conn.query(`
        SELECT id, status, po_number, jf_code, asin, lot_number, supplier, product_name,
               container_number, external_container_number, awb_number
          FROM orders
         WHERE deleted_at IS NULL`);

    const orders = new Map();                 // id -> { id, status, jf, lot, ... }
    const poList = [];                        // [{ id, norm, raw }]
    const shipRefs = new Map();               // normRef -> { type, raw, ids:Set }
    const jf = new Map();                     // baseJf -> Set(id)
    const asin = new Map();                   // baseAsin -> Set(id)

    const addShip = (raw, type, id) => {
        const norm = usableShipRef(raw);
        if (!norm) return;
        let e = shipRefs.get(norm);
        if (!e) { e = { type, raw: String(raw).trim(), ids: new Set() }; shipRefs.set(norm, e); }
        e.ids.add(id);
    };
    const addSet = (map, key, id) => {
        if (!key) return;
        let s = map.get(key); if (!s) { s = new Set(); map.set(key, s); } s.add(id);
    };

    for (const r of rows) {
        const lotNorm = normAlnum(r.lot_number);
        orders.set(r.id, {
            id: r.id, status: r.status, supplier: r.supplier || null,
            productName: r.product_name || null,
            jf: r.jf_code || null,
            lotNorm: lotNorm.length >= 5 ? lotNorm : null, // ignore tiny/blank lots
            lotRaw: r.lot_number || null,
        });
        const po = usablePo(r.po_number);
        if (po) poList.push({ id: r.id, norm: po, raw: String(r.po_number).trim() });
        addShip(r.container_number, 'container', r.id);
        addShip(r.external_container_number, 'container', r.id);
        addShip(r.awb_number, 'awb', r.id);
        addSet(jf, baseJf(r.jf_code), r.id);
        addSet(asin, baseAsin(r.asin), r.id);
    }
    return { orders, poList, shipRefs, jf, asin, orderCount: rows.length };
}

// Build the set of identifier candidates present in the text. We match on EXACT
// alphanumeric TOKENS (not substrings of a whitespace-stripped blob), so a short
// identifier can't match inside a longer run ("YOHO102" must NOT match
// "12345YOHO1029999") or across unrelated words. To still catch identifiers
// written with internal punctuation ("YOHO-102", "PO_00333J", "999-93393786"),
// adjacent tokens joined by a SINGLE non-whitespace separator are also merged
// into a candidate — but we never merge across a space/newline, so "PI 5582"
// (label + unrelated quantity) cannot fabricate the PO "PI5582".
//   • candidates  — single tokens + tight-merged runs (for PO / ship refs)
//   • tokens      — single tokens only (for lot: a lot is always one number)
function buildCandidates(rawUpper) {
    const tokens = new Set();
    const candidates = new Set();
    const toks = [];
    const re = /[A-Z0-9]+/g;
    let m;
    while ((m = re.exec(rawUpper))) toks.push({ t: m[0], start: m.index, end: re.lastIndex });
    for (let i = 0; i < toks.length; i++) {
        tokens.add(toks[i].t);
        candidates.add(toks[i].t);
        let merged = toks[i].t;
        for (let j = i + 1; j < toks.length && j <= i + 3; j++) {
            const gap = rawUpper.slice(toks[j - 1].end, toks[j].start);
            if (gap.length === 1 && !/\s/.test(gap)) { merged += toks[j].t; candidates.add(merged); }
            else break;
        }
    }
    return { tokens, candidates };
}

// ── Match one conversation's text against the order index ─────────────────────
// Returns { links: [{ orderId, tier, bases:[{type,value}] }], ambiguous, signals }.
// Strong/batch matches suppress product fan-out; product-only matches are capped.
function matchConversation(idx, blobRaw) {
    const rawUpper = String(blobRaw || '').toUpperCase();
    const { tokens, candidates } = buildCandidates(rawUpper);

    const strong = new Map();   // id -> [{type,value}]
    const batch = new Map();    // id -> [{type,value}]  (jf + its lot)
    const product = new Map();  // id -> [{type,value}]  (jf/asin only)
    const addBasis = (map, id, basis) => {
        let a = map.get(id); if (!a) { a = []; map.set(id, a); } a.push(basis);
    };

    // Strong: PO + ship refs, matched as exact (possibly punctuation-joined) tokens.
    for (const p of idx.poList) {
        if (candidates.has(p.norm)) addBasis(strong, p.id, { type: 'po', value: p.raw });
    }
    for (const [norm, e] of idx.shipRefs) {
        if (candidates.has(norm)) {
            for (const id of e.ids) addBasis(strong, id, { type: e.type, value: e.raw });
        }
    }

    // Product: SKU + ASIN tokens (boundary-anchored so "MYJF0130X" can't match).
    const jfHits = new Set((rawUpper.match(/(?<![A-Z0-9])JF\d{4,}(?![A-Z0-9])/g) || []));
    for (const code of jfHits) {
        const ids = idx.jf.get(code);
        if (!ids) continue;
        for (const id of ids) {
            const o = idx.orders.get(id);
            // Lot upgrade: jf + its lot (as an exact token) both present → batch.
            // (No leading-zero stripping — a 6-digit substring collides with dates.)
            const lotPresent = o && o.lotNorm && tokens.has(o.lotNorm);
            if (lotPresent) {
                addBasis(batch, id, { type: 'jf_code', value: code });
                addBasis(batch, id, { type: 'lot', value: o.lotRaw });
            } else {
                addBasis(product, id, { type: 'jf_code', value: code });
            }
        }
    }
    const asinHits = new Set((rawUpper.match(/(?<![A-Z0-9])B0[A-Z0-9]{8}(?![A-Z0-9])/g) || []));
    for (const code of asinHits) {
        const ids = idx.asin.get(code);
        if (!ids) continue;
        for (const id of ids) addBasis(product, id, { type: 'asin', value: code });
    }

    const links = [];
    const pinned = new Set([...strong.keys(), ...batch.keys()]);

    // A strong/batch match pins specific orders → emit those, folding in any
    // product/lot evidence for the SAME order, and SUPPRESS unrelated product
    // fan-out (the email is about these orders, not the whole SKU history).
    if (pinned.size) {
        for (const id of strong.keys()) {
            const bases = [...strong.get(id), ...(batch.get(id) || []), ...(product.get(id) || [])];
            links.push({ orderId: id, tier: 'strong', bases: dedupeBases(bases) });
        }
        for (const id of batch.keys()) {
            if (strong.has(id)) continue;
            links.push({ orderId: id, tier: 'batch', bases: dedupeBases([...batch.get(id), ...(product.get(id) || [])]) });
        }
        return { links, ambiguous: false, signals: { strong: strong.size, batch: batch.size, product: product.size, suppressed: product.size } };
    }

    // Product-only: cap the fan-out so a bare-SKU email can't link to dozens of
    // historical orders. Above the cap, record the conversation but link nothing.
    const productIds = [...product.keys()];
    if (productIds.length > MAX_PRODUCT_FANOUT) {
        return { links: [], ambiguous: true, signals: { strong: 0, batch: 0, product: productIds.length, suppressed: productIds.length } };
    }
    for (const id of productIds) {
        links.push({ orderId: id, tier: 'product', bases: dedupeBases(product.get(id)) });
    }
    return { links, ambiguous: false, signals: { strong: 0, batch: 0, product: productIds.length, suppressed: 0 } };
}

function dedupeBases(bases) {
    const seen = new Set();
    const out = [];
    for (const b of bases) {
        const k = `${b.type}:${String(b.value || '').toUpperCase()}`;
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(b);
    }
    return out;
}

// ── Gemini fallback helpers ──────────────────────────────────────────────────
// Business senders (suppliers + forwarders): an email from one of these is
// order-related enough to be worth a Gemini look even with no keyword hit.
async function loadBusinessSenders(conn) {
    const domains = new Set(FORWARDER_DOMAINS);
    const emails = new Set();
    try {
        const [rows] = await conn.query(`
            SELECT email FROM supplier_emails
             WHERE deleted_at IS NULL AND email IS NOT NULL AND email <> ''
            UNION
            SELECT contact_email AS email FROM suppliers
             WHERE deleted_at IS NULL AND contact_email IS NOT NULL AND contact_email <> ''`);
        for (const r of rows) {
            const a = String(r.email).trim().toLowerCase();
            if (!/^[^@\s]+@[^@\s]+\.[a-z]{2,}$/.test(a)) continue;
            emails.add(a);
        }
    } catch (e) {
        log.warn('[front-email-index] business sender lookup failed', { error: e.message });
    }
    return { domains: [...domains], emails };
}

function senderIsBusiness(from, biz) {
    if (!from || !biz) return false;
    const a = String(from).toLowerCase();
    return biz.emails.has(a) || biz.domains.some(d => emailInDomain(a, d));
}

function tokenizeWords(s) {
    return [...new Set(String(s || '').toLowerCase().match(/[a-z]{4,}/g) || [])];
}

// No-identifier path: a supplier name + product description with no usable
// identifier. Conservatively map to the most-recent orders of that supplier
// whose product name shares a word with the description. Capped + ambiguous.
function resolveFuzzy(idx, clues) {
    const sup = normAlnum(clues.supplierName);
    const prodWords = tokenizeWords(clues.productSummary);
    // Require BOTH a supplier (≥5 normalized chars — short clues over-match) and a
    // product signal — supplier alone is too broad.
    if (sup.length < 5 || !prodWords.length) {
        return { links: [], ambiguous: false, signals: { fuzzy: 0 } };
    }
    const cands = [];
    for (const o of idx.orders.values()) {
        const osup = normAlnum(o.supplier);
        // Prefix match (either direction), not bare substring: a 5+ char supplier
        // clue must START one of the names (so "SUNMED" ~ "SUNMEDLTD"), not merely
        // sit inside it ("ABC" inside "FABRICABCLTD").
        if (!osup || !(osup.startsWith(sup) || sup.startsWith(osup))) continue;
        if (!o.productName) continue;
        const pn = o.productName.toLowerCase();
        if (!prodWords.some(w => pn.includes(w))) continue;
        cands.push(o);
    }
    cands.sort((a, b) => b.id - a.id);
    const top = cands.slice(0, FUZZY_CAP);
    const links = top.map(o => ({
        orderId: o.id, tier: 'product',
        bases: dedupeBases([
            { type: 'supplier', value: clues.supplierName },
            ...(clues.productSummary ? [{ type: 'product', value: clues.productSummary }] : []),
        ]),
    }));
    return { links, ambiguous: cands.length > top.length, signals: { fuzzy: cands.length } };
}

// Turn Gemini's extracted clues into links by feeding them back through the
// deterministic index, so a hallucinated value can't link to a real order.
//
// IMPORTANT scoping: this grounding guarantee is strong for ORDER-UNIQUE
// identifiers (PO / container / AWB) but NOT for PRODUCT identifiers (jf_code /
// asin) — a single real SKU maps to many orders, so letting Gemini emit a bare
// SKU would fan an email out to the whole product history (exactly the links the
// deterministic pass already declined). So we only let a Gemini SKU through when
// it is accompanied by a lot, where matchConversation's jf+lot rule promotes it
// to the order-specific `batch` tier. A bare Gemini SKU is dropped; the no-id
// tail is handled by the conservative, capped resolveFuzzy (supplier+product).
function resolveClues(idx, clues) {
    const parts = [...clues.poNumbers, ...clues.containerNumbers, ...clues.awbNumbers];
    if (clues.lotNumbers.length) parts.push(...clues.skus, ...clues.lotNumbers);
    // Each clue is a Gemini-ISOLATED identifier, so collapse its internal
    // separators to one token ("YOHO 102" → "YOHO102", "MRKU 5353914" →
    // "MRKU5353914") before re-grounding. The bounded matcher won't merge across
    // spaces in free email text (to avoid "PI 5582" false positives), but here
    // Gemini has already decided the span is a single identifier — and this also
    // lets the fallback recover space-separated refs the deterministic pass missed.
    const clueBlob = parts.map(p => normAlnum(p)).filter(Boolean).join('\n');
    let res = matchConversation(idx, clueBlob);
    if (!res.links.length) res = resolveFuzzy(idx, clues);
    return res;
}

async function getLastRunMs(conn) {
    const [rows] = await conn.query(`SELECT last_run_ts FROM front_email_scan_state WHERE id = 1`);
    return rows.length && rows[0].last_run_ts ? Number(rows[0].last_run_ts) * 1000 : null;
}
async function setLastRunTs(conn, sec) {
    await conn.query(
        `INSERT INTO front_email_scan_state (id, last_run_ts) VALUES (1, ?)
         ON DUPLICATE KEY UPDATE last_run_ts = VALUES(last_run_ts)`,
        [sec]
    );
}

// ── Conversation → text + participants ───────────────────────────────────────
// Concatenate every message's text (capped) for matching, and derive the
// conversation's display metadata (subject, external participant, direction,
// message timestamps).
function summarizeConversation(convo, messages) {
    // Order messages oldest→newest so the stored body reads as a real thread.
    const ordered = [...messages].sort((a, b) => (a.created_at || 0) - (b.created_at || 0));
    let firstTs = null, lastTs = null, lastMsg = null;
    const handles = new Set();
    const parts = [];
    let bodyLen = 0;
    for (const m of ordered) {
        if (bodyLen < BODY_STORE_CAP) {
            // One readable block per message: who/when header + its text.
            const ts = m.created_at ? new Date(m.created_at * 1000).toISOString() : '';
            const dir = m.is_inbound ? 'IN ' : 'OUT';
            const header = `\n──── [${dir}] ${messageFromEmail(m) || ''} ${ts} ────\n`;
            const chunk = header + messageText(m);
            parts.push(chunk);
            bodyLen += chunk.length;
        }
        const ts = m.created_at || 0;
        if (ts) {
            if (firstTs == null || ts < firstTs) firstTs = ts;
            if (lastTs == null || ts >= lastTs) { lastTs = ts; lastMsg = m; }
        }
        const from = messageFromEmail(m);
        if (from) handles.add(from.toLowerCase());
        for (const r of (m.recipients || [])) {
            if (r && r.handle && (r.role === 'to' || r.role === 'from')) handles.add(String(r.handle).toLowerCase());
        }
    }
    const bodyFull = parts.join('\n').slice(0, BODY_STORE_CAP);
    const toDate = (ts) => (ts ? new Date(ts * 1000) : null);
    return {
        subject: cleanSubject(convo.subject) || null,
        // Matching reads the first BLOB_CAP chars; the full body is stored separately.
        blob: bodyFull.slice(0, BLOB_CAP),
        bodyFull,
        preview: bodyFull.replace(/[ \t]+/g, ' ').replace(/\n{2,}/g, '\n').trim().slice(0, 1000),
        fromEmail: lastMsg ? messageFromEmail(lastMsg) : null,
        participants: [...handles].slice(0, 12),
        direction: lastMsg ? (lastMsg.is_inbound ? 'inbound' : 'outbound') : null,
        messageCount: messages.length,
        firstMessageAt: toDate(firstTs),
        lastMessageAt: toDate(lastTs),
        lastTs,
        // Full-precision epoch ms for the dedup compare (host-TZ-independent,
        // no DATETIME round-trip, no same-second truncation).
        lastMs: lastTs != null ? Math.round(lastTs * 1000) : null,
    };
}

// Tier strength order for the monotonic upgrade guard below.
const TIER_RANK_SQL = "'product','batch','strong'";

async function upsertLink(conn, { orderId, conversationId, link, meta, source, confidence }) {
    const conf = confidence == null ? null : Math.round(Math.min(1, Math.max(0, confidence)) * 1000) / 1000;
    // On a re-scan, only let the classification (tier/basis/source/confidence)
    // change when the NEW tier is at least as strong as the stored one — a later
    // weaker re-match must never downgrade a 'strong' link. Display fields
    // (subject/preview/body/participants/timestamps) always refresh.
    const stronger = `FIELD(VALUES(match_tier), ${TIER_RANK_SQL}) >= FIELD(match_tier, ${TIER_RANK_SQL})`;
    await conn.query(
        `INSERT INTO order_emails
            (order_id, conversation_id, subject, preview, from_email, participants, direction,
             front_url, match_tier, match_basis, source, confidence,
             message_count, first_message_at, last_message_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            subject = VALUES(subject), preview = VALUES(preview), from_email = VALUES(from_email),
            participants = VALUES(participants), direction = VALUES(direction), front_url = VALUES(front_url),
            match_tier = IF(${stronger}, VALUES(match_tier), match_tier),
            match_basis = IF(${stronger}, VALUES(match_basis), match_basis),
            source = IF(${stronger}, VALUES(source), source),
            confidence = IF(${stronger}, VALUES(confidence), confidence),
            message_count = VALUES(message_count), first_message_at = VALUES(first_message_at),
            last_message_at = VALUES(last_message_at)`,
        [
            orderId, conversationId, meta.subject ? meta.subject.slice(0, 512) : null,
            meta.preview || null, meta.fromEmail ? String(meta.fromEmail).slice(0, 255) : null,
            JSON.stringify(meta.participants || []),
            meta.direction, frontWebUrl(conversationId), link.tier, JSON.stringify(link.bases || []),
            source || 'rule', conf,
            meta.messageCount || 0, meta.firstMessageAt, meta.lastMessageAt,
        ]
    );
}

async function recordConversation(conn, { conversationId, meta, result, geminiUsed = false, geminiConfidence = null }) {
    const conf = geminiConfidence == null ? null : Math.round(Math.min(1, Math.max(0, geminiConfidence)) * 1000) / 1000;
    // Store the full body ONLY for conversations linked to an order — an
    // unmatched thread's body is never surfaced, so don't waste storage on it.
    const body = (result.links && result.links.length) ? (meta.bodyFull || null) : null;
    await conn.query(
        `INSERT INTO front_email_index
            (conversation_id, subject, from_email, front_url, body_full,
             message_count, last_message_at, last_message_ms,
             matched_order_count, order_ids, ambiguous, gemini_used, gemini_confidence, signals)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            subject = VALUES(subject), from_email = VALUES(from_email), front_url = VALUES(front_url),
            body_full = VALUES(body_full),
            message_count = VALUES(message_count), last_message_at = VALUES(last_message_at),
            last_message_ms = VALUES(last_message_ms),
            matched_order_count = VALUES(matched_order_count), order_ids = VALUES(order_ids),
            ambiguous = VALUES(ambiguous), gemini_used = VALUES(gemini_used),
            gemini_confidence = VALUES(gemini_confidence), signals = VALUES(signals)`,
        [
            conversationId, meta.subject ? meta.subject.slice(0, 512) : null,
            meta.fromEmail ? String(meta.fromEmail).slice(0, 255) : null,
            frontWebUrl(conversationId), body,
            meta.messageCount || 0, meta.lastMessageAt, meta.lastMs,
            result.links.length, JSON.stringify(result.links.map(l => l.orderId)),
            result.ambiguous ? 1 : 0, geminiUsed ? 1 : 0, conf, JSON.stringify(result.signals || {}),
        ]
    );
}

// ── Orchestrator ─────────────────────────────────────────────────────────────
async function indexEmailsFromFront(conn, {
    sinceDays = null,   // null => incremental (since last run); a number forces a fixed window
    dryRun = false,
    force = false,      // re-scan conversations even if already in the ledger
    verbose = false,
    useGemini = true,   // Flash fallback on the deterministic residual
    maxGemini = DEFAULT_MAX_GEMINI,
    geminiConfMin = DEFAULT_GEMINI_CONF_MIN,
    geminiModel,
} = {}) {
    if (!process.env.FRONT_API_TOKEN) {
        const e = new Error('FRONT_API_TOKEN is not configured.');
        e.code = 'NOT_CONFIGURED';
        throw e;
    }

    // The Flash fallback is best-effort: only on if explicitly wanted, keyed, AND
    // given a positive budget. maxGemini<=0 means "Gemini off" (NOT "cap at zero",
    // which would otherwise stall the very first residual).
    const geminiOn = !!useGemini && !!process.env.GEMINI_API_KEY && maxGemini > 0;
    const biz = geminiOn ? await loadBusinessSenders(conn) : null;

    const overlapHours = envNum('EMAIL_INDEX_OVERLAP_HOURS', DEFAULT_OVERLAP_HOURS);
    const maxLookbackDays = envNum('EMAIL_INDEX_MAX_LOOKBACK_DAYS', DEFAULT_MAX_LOOKBACK_DAYS);
    const runStartMs = Date.now();

    let cutoffMs, windowMode;
    if (sinceDays != null && Number(sinceDays) > 0) {
        cutoffMs = runStartMs - Number(sinceDays) * 86400000;
        windowMode = `sinceDays=${sinceDays}`;
    } else {
        const lastRunMs = await getLastRunMs(conn);
        const floorMs = runStartMs - maxLookbackDays * 86400000;
        if (lastRunMs) {
            cutoffMs = Math.max(lastRunMs - overlapHours * 3600000, floorMs);
            windowMode = `incremental(-${overlapHours}h overlap)`;
        } else {
            cutoffMs = runStartMs - DEFAULT_SINCE_DAYS * 86400000;
            windowMode = `first-run(${DEFAULT_SINCE_DAYS}d)`;
        }
    }

    // Hard six-month ceiling: clamp the cutoff so NO run — not even a manual
    // `sinceDays` backfill — ever reaches further back than MAX_IMPORT_DAYS.
    const maxImportDays = envNum('EMAIL_INDEX_MAX_IMPORT_DAYS', DEFAULT_MAX_IMPORT_DAYS);
    const ceilingMs = runStartMs - maxImportDays * 86400000;
    if (cutoffMs < ceilingMs) {
        cutoffMs = ceilingMs;
        windowMode += ` capped@${maxImportDays}d`;
    }
    const afterTs = Math.floor(cutoffMs / 1000);

    const idx = await buildOrderIndex(conn);
    const summary = {
        window: windowMode, windowFrom: new Date(cutoffMs).toISOString().slice(0, 16),
        orders: idx.orderCount, shipRefs: idx.shipRefs.size, pos: idx.poList.length,
        scanned: 0, matched: 0, linksUpserted: 0, ambiguous: 0, capped: false,
        failed: 0, fetchFailed: 0, processFailed: 0,
        geminiOn, geminiCalls: 0, geminiMatched: 0, geminiBudgetSkipped: 0,
        byTier: { strong: 0, batch: 0, product: 0 }, bySource: { rule: 0, gemini: 0 },
    };

    log.info('[front-email-index] start', {
        window: windowMode, windowFrom: summary.windowFrom,
        orders: idx.orderCount, shipRefs: idx.shipRefs.size, pos: idx.poList.length,
        dryRun, force, geminiOn, maxGemini,
    });

    const query = `after:${afterTs}`;
    const seenConvo = new Set();
    let walked = 0;

    try {
        for await (const convo of searchConversations(query)) {
            if (seenConvo.has(convo.id)) continue;
            seenConvo.add(convo.id);
            if (++walked > MAX_CONVERSATIONS) {
                summary.capped = true;
                log.warn('[front-email-index] conversation cap hit', { cap: MAX_CONVERSATIONS });
                break;
            }

            // Dedup: skip threads with no new activity since we last scanned them
            // (unless force). The `after:` search re-surfaces a thread whenever it
            // gets a new message, so an unchanged last message means nothing new.
            // Compare in epoch MILLISECONDS (host-TZ-independent, full precision —
            // no DATETIME round-trip, no same-second truncation).
            const convoTs = convo.last_message ? (convo.last_message.created_at || 0)
                : (convo.last_activity || convo.created_at || 0);
            if (!force && convoTs) {
                const [prev] = await conn.query(
                    `SELECT last_message_ms FROM front_email_index WHERE conversation_id = ? LIMIT 1`,
                    [convo.id]
                );
                if (prev.length && prev[0].last_message_ms != null
                    && Number(prev[0].last_message_ms) >= Math.round(convoTs * 1000)) {
                    continue; // already scanned, unchanged
                }
            }

            let messages;
            try {
                messages = await listMessages(convo.id);
            } catch (e) {
                // Unrecorded failure: hold the watermark so this thread is re-tried
                // next run (it has no ledger row, so dedup can't skip it). Bounded by
                // the 30-day incremental floor.
                summary.failed += 1;
                summary.fetchFailed += 1;
                log.error('[front-email-index] listMessages failed', { conversationId: convo.id, error: e.message });
                continue;
            }
            if (!messages.length) continue;

            const meta = summarizeConversation(convo, messages);
            summary.scanned += 1;

            // Matching through persistence is isolated per conversation: one bad
            // insert / parse must not abort the whole scan (which would otherwise
            // poison the watermark for every later thread in the window).
            try {
                // 1) Deterministic identifier match (the primary path). Match
                // against the SUBJECT + body — many threads carry the PO/container
                // only in the subject line ("Signed PI – PO_00333J") with a
                // boilerplate body, so a body-only scan misses them.
                let result = matchConversation(idx, `${meta.subject || ''}\n${meta.blob}`);
                let source = 'rule';
                let geminiUsed = false;
                let geminiConfidence = null;

                // 2) Gemini Flash fallback — ONLY on the residual the rules missed,
                // and only for plausibly order-related mail (business sender or
                // keywords), under a per-run budget. Gemini's clues are resolved
                // back through the SAME index; for order-UNIQUE ids (PO/container/
                // AWB) a hallucinated value therefore can't link (see resolveClues).
                if (geminiOn && !result.links.length) {
                    const orderish = senderIsBusiness(meta.fromEmail, biz)
                        || ORDER_HINT_RE.test(`${meta.subject || ''}\n${meta.blob}`);
                    if (orderish && summary.geminiCalls >= maxGemini) {
                        // Over the per-run Gemini budget: index this thread
                        // deterministically anyway (so the window drains and the
                        // watermark advances) and skip only the Flash pass. Surfaced
                        // via the counter + a one-time log — never a silent cap. Raise
                        // --max-gemini to Gemini-check more residuals in one run.
                        if (summary.geminiBudgetSkipped === 0) {
                            log.warn('[front-email-index] Gemini per-run budget reached; remaining residuals indexed deterministically only', { maxGemini });
                        }
                        summary.geminiBudgetSkipped += 1;
                    } else if (orderish) {
                        try {
                            summary.geminiCalls += 1;
                            const clues = await extractOrderCluesFromEmail(meta.blob, {
                                subject: meta.subject, fromEmail: meta.fromEmail, model: geminiModel,
                            });
                            geminiUsed = true;
                            geminiConfidence = clues.confidence;
                            if (clues.orderRelated && clues.confidence >= geminiConfMin) {
                                const gres = resolveClues(idx, clues);
                                if (gres.links.length) {
                                    result = gres;
                                    source = 'gemini';
                                    summary.geminiMatched += 1;
                                } else if (gres.ambiguous) {
                                    result.ambiguous = true; // record the unresolved tail
                                }
                            }
                        } catch (e) {
                            summary.failed += 1;
                            log.error('[front-email-index] gemini fallback failed', { conversationId: convo.id, error: e.message });
                        }
                    }
                }

                if (verbose) {
                    log.info('[front-email-index] examined', {
                        subject: (meta.subject || '').slice(0, 70), msgs: messages.length,
                        links: result.links.length, source, ambiguous: result.ambiguous, signals: result.signals,
                    });
                }

                if (result.ambiguous) summary.ambiguous += 1;
                if (result.links.length) {
                    summary.matched += 1;
                    summary.bySource[source] = (summary.bySource[source] || 0) + 1;
                    for (const link of result.links) {
                        summary.byTier[link.tier] = (summary.byTier[link.tier] || 0) + 1;
                        if (!dryRun) {
                            await upsertLink(conn, {
                                orderId: link.orderId, conversationId: convo.id, link, meta,
                                source, confidence: source === 'gemini' ? geminiConfidence : null,
                            });
                        }
                        summary.linksUpserted += 1;
                    }
                    log.info('[front-email-index] LINKED', {
                        convo: convo.id, subject: (meta.subject || '').slice(0, 60), source,
                        orders: result.links.map(l => `${l.orderId}:${l.tier}`), dryRun,
                    });
                }
                if (!dryRun) await recordConversation(conn, { conversationId: convo.id, meta, result, geminiUsed, geminiConfidence });
            } catch (e) {
                // Per-conversation persistence/processing error. Leave it unrecorded
                // (retried next run) and hold the watermark via processFailed.
                summary.failed += 1;
                summary.processFailed += 1;
                log.error('[front-email-index] process/persist failed', { conversationId: convo.id, error: e.message });
            }
        }
    } catch (e) {
        // A mid-run Front failure shouldn't lose the work already persisted; just
        // don't advance the watermark (the window re-scans next run, dedup-skipping
        // what we already recorded).
        summary.capped = true;
        summary.error = e.message;
        log.error('[front-email-index] scan aborted', { error: e.message });
    }

    // Advance the watermark only on a fully-drained, persisted run with NO
    // unrecorded conversations. A listMessages/persist failure leaves a thread
    // with no ledger row; holding the watermark re-scans it next run (bounded by
    // the 30-day incremental floor) rather than letting it age out unindexed.
    summary.watermarkAdvanced = !dryRun && !summary.capped
        && summary.fetchFailed === 0 && summary.processFailed === 0;
    if (summary.watermarkAdvanced) await setLastRunTs(conn, Math.floor(runStartMs / 1000));

    return summary;
}

module.exports = {
    indexEmailsFromFront,
    buildOrderIndex,
    matchConversation,
    resolveClues,
    resolveFuzzy,
    loadBusinessSenders,
    senderIsBusiness,
    summarizeConversation,
    usableShipRef,
    usablePo,
    baseJf,
    baseAsin,
    normAlnum,
    getLastRunMs,
    setLastRunTs,
};

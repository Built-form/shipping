// AI Suggestions — types, API client, and the data hook.
// Drop into ShipLine (e.g. src/features/aiSuggestions/). Adjust the `authedFetch`
// import + `Order` type to your codebase. See docs/ai-status-suggestions-frontend*.md.

// ── Types ────────────────────────────────────────────────────────────────────
export type OrderStatus =
  | 'SCHEDULED' | 'PO_SENT' | 'IN_PRODUCTION' | 'READY_FOR_QC' | 'READY'
  | 'CONSOLIDATED' | 'ON_SEA' | 'ON_AIR' | 'ARRIVED_AT_WAREHOUSE'
  | 'RECEIVED' | 'PARTIALLY_RECEIVED' | 'DESTROYED';

export type SuggestionCategory =
  | 'forward' | 'multi_step' | 'lateral' | 'backward_qc_failed' | 'data_update';

export type AlertStatus = 'pending' | 'snoozed' | 'dismissed' | 'resolved';

export interface FieldUpdate { field: string; from: string | number | null; to: string | number; }
export interface BlockedField { field: string; value: string | number; reason: string; }

export interface StatusSuggestionMeta {
  orderId: number;
  poNumber: string | null;
  extractedPo: string | null;
  currentStatus: OrderStatus;
  suggestedStatus: OrderStatus;     // === currentStatus for data_update
  milestone: OrderStatus | null;
  category: SuggestionCategory;
  remainingPath: OrderStatus[];
  isFqc: boolean;
  hasQcReport: boolean;
  gate: { required: string[]; missing: string[] };
  fieldUpdates: FieldUpdate[];
  blockedFields: BlockedField[];
  confidence: number;
  rationale: string;
  model: string;
  reasoning: string | null;   // Pass-2 order-aware justification (why panel); null if not refined
  refined: boolean;           // true when the order-context refine pass produced this
  refineModel: string | null;
  jfCode: string | null;
  productName: string | null;
  supplier: string | null;
  source: {
    messageId: string; conversationId: string;
    fromEmail: string | null; subject: string | null;
    frontUrl: string | null; receivedAt: string | null;
    bodyFull: string | null;   // full email text Gemini read (≤20k chars)
    highlights: string[];      // verbatim quotes to bold inside bodyFull
  };
  sourceRef: string;
}

export interface StatusSuggestion {
  id: number;
  type: 'status_suggestion';
  severity: 'info';
  status: AlertStatus;
  eventDate: string;
  title: string;
  body: string | null;
  entityType: 'order';
  entityId: string;
  meta: StatusSuggestionMeta;
  lastAction: 'approve' | 'acknowledge' | 'deny' | 'snooze' | 'dismiss' | 'restore' | null;
  actionNote: string | null;
  actionedBy: string | null;
  actionedAt: string | null;
  createdAt: string;
}

export interface AlertsListResponse<T> { data: T[]; counts: { returned: number; pending: number }; }
export interface ApproveSuggestionResponse {
  order: unknown;                   // your Order type
  alert: StatusSuggestion;
  appliedStatus: OrderStatus;
  appliedFields: { field: string; value: string | number }[];
  skippedFields: { field: string; reason: string }[];
}

// True when the suggestion changes data only, with no pipeline move.
export const isDataUpdate = (s: StatusSuggestion) =>
  s.meta.category === 'data_update' || s.meta.suggestedStatus === s.meta.currentStatus;

// ── Display helpers ──────────────────────────────────────────────────────────
export const STATUS_LABEL: Record<string, string> = {
  SCHEDULED: 'Scheduled', PO_SENT: 'Purchase Order Sent', IN_PRODUCTION: 'Under Production',
  READY_FOR_QC: 'Ready for QC', READY: 'Ready at Factory', CONSOLIDATED: 'Consolidated',
  ON_SEA: 'On Sea', ON_AIR: 'On Air', ARRIVED_AT_WAREHOUSE: 'Arrived at Warehouse',
  RECEIVED: 'Received', PARTIALLY_RECEIVED: 'Partially Received', DESTROYED: 'Destroyed',
};
export const statusLabel = (s: string) => STATUS_LABEL[s] ?? s;

export const FIELD_LABEL: Record<string, string> = {
  poDate: 'PO date', supplier: 'Supplier', unitPrice: 'Unit price',
  artworkConfirmedDate: 'Artwork confirmed', lotNumber: 'Lot number',
  mfgDate: 'Mfg date', expDate: 'Expiry', estimatedReadyDate: 'Estimated ready date',
  actualReadyDate: 'Actual ready date', qcStatus: 'QC status', qcDate: 'QC date',
  qcInvoiceNumber: 'QC invoice', containerNumber: 'Container', externalContainerNumber: 'Container',
  vesselName: 'Vessel',
  eta: 'ETA', estimatedDepartureDate: 'ETD', shippedDate: 'Shipped date',
  deliveryDate: 'Delivery date', arrivedDate: 'Arrived date',
};
export const fieldLabel = (f: string) => FIELD_LABEL[f] ?? f;
export const fmtVal = (v: string | number | null) =>
  v == null || v === '' ? '—' : String(v);

// Split `bodyFull` into segments, flagging the spans that match any `highlights`
// quote so the card can <mark> the evidence (the date/PO/milestone phrasing) in
// place. Verbatim, case-insensitive, longest-first so overlapping quotes nest
// cleanly. Returns [{ text, hit }] for the card to map to <mark> vs plain text.
export function highlightSegments(
  body: string | null,
  highlights: string[],
): Array<{ text: string; hit: boolean }> {
  if (!body) return [];
  const quotes = [...new Set((highlights || []).map(h => h.trim()).filter(Boolean))]
    .sort((a, b) => b.length - a.length);
  if (!quotes.length) return [{ text: body, hit: false }];
  const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`(${quotes.map(esc).join('|')})`, 'gi');
  return body.split(re).filter(s => s !== '').map((text, i) => ({ text, hit: i % 2 === 1 }));
}

// ── API client ───────────────────────────────────────────────────────────────
// Replace with your authed fetch wrapper. It must attach the Google JWT, parse
// JSON, and on a non-2xx THROW an error carrying { status, body } so the hook can
// branch on 409/422.
import { authedFetch } from '../../api'; // <-- adjust path

const BASE = '/api/v1';

export function listStatusSuggestions(
  status: 'pending' | 'dismissed' | 'resolved' | 'all' = 'pending',
  limit = 200,
): Promise<AlertsListResponse<StatusSuggestion>> {
  const q = new URLSearchParams({ type: 'status_suggestion', status, limit: String(limit) });
  return authedFetch(`${BASE}/alerts?${q.toString()}`);
}

export function approveSuggestion(
  id: number,
  opts: { statusOverride?: OrderStatus; fields?: Record<string, string | number>; note?: string } = {},
): Promise<ApproveSuggestionResponse> {
  const body: Record<string, unknown> = {};
  if (opts.statusOverride) body.status = opts.statusOverride;
  if (opts.fields) body.fields = opts.fields;
  if (opts.note) body.note = opts.note;
  return authedFetch(`${BASE}/alerts/${id}/approve`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: Object.keys(body).length ? JSON.stringify(body) : undefined,
  });
}

export function acknowledgeSuggestion(id: number, note?: string): Promise<StatusSuggestion> {
  return authedFetch(`${BASE}/alerts/${id}/acknowledge`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: note ? JSON.stringify({ note }) : undefined,
  });
}

export function denySuggestion(id: number, note?: string): Promise<StatusSuggestion> {
  return authedFetch(`${BASE}/alerts/${id}/deny`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: note ? JSON.stringify({ note }) : undefined,
  });
}

// ── Hook ─────────────────────────────────────────────────────────────────────
import { useCallback, useEffect, useState } from 'react';

const POLL_MS = 60_000;

export function useStatusSuggestions() {
  const [items, setItems] = useState<StatusSuggestion[]>([]);
  const [pending, setPending] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const res = await listStatusSuggestions('pending');
      setItems(res.data);
      setPending(res.counts.pending);
      setError(null);
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load suggestions');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, POLL_MS);
    return () => clearInterval(t);
  }, [refresh]);

  // Optimistic remove; 409 (already actioned / stale) or 422 (gate-blocked) → refresh.
  const act = useCallback(async (id: number, run: () => Promise<unknown>) => {
    const prev = items;
    setItems(list => list.filter(s => s.id !== id));
    setPending(n => Math.max(0, n - 1));
    try {
      return await run();
    } catch (e: any) {
      if (e?.status === 409) { refresh(); return; }
      setItems(prev);
      setPending(prev.filter(s => s.status === 'pending').length);
      throw e; // 422 gate-blocked etc. — surface to the caller for a toast
    }
  }, [items, refresh]);

  const approve = useCallback(
    (id: number, opts?: { statusOverride?: OrderStatus; fields?: Record<string, string | number> }) =>
      act(id, () => approveSuggestion(id, opts ?? {})),
    [act],
  );
  const acknowledge = useCallback((id: number, note: string) => act(id, () => acknowledgeSuggestion(id, note)), [act]);
  const deny = useCallback((id: number, note?: string) => act(id, () => denySuggestion(id, note)), [act]);

  return { items, pending, loading, error, refresh, approve, acknowledge, deny };
}

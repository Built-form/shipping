# AI Status Suggestions — ShipLine implementation handoff

Build-ready spec for the **"AI Suggestions"** view: a human-in-the-loop approval
queue where an operator approves/denies order status changes that an AI inferred
from supplier emails. This is the *implementation* companion to the API contract
in [ai-status-suggestions-frontend.md](ai-status-suggestions-frontend.md) — read
that for the full field reference; this doc gives you copy-paste types, an API
client, and the components.

**Backend is live-tested and ready.** No new endpoints to wait on — suggestions
reuse the existing authed alerts API (Google JWT, same as `/api/v1/orders`), plus
two new actions (`/approve`, `/deny`) already covered by the existing
`PATCH /api/v1/alerts/{proxy+}` route.

---

## 1. What to build (scope)

A dedicated **AI Suggestions** view (NOT folded into the daily-alerts slider — we
keep AI guesses separate so they don't erode trust in the deterministic alerts):

- A nav entry **"AI Suggestions"** with a count badge (number of `pending`).
- A list of **suggestion cards**, each proposing `currentStatus → suggestedStatus`
  for one order line, with the AI's confidence + the email phrase it quoted, and
  a deep-link to the original email in Front.
- Per card: **Approve** (applies the change to the order + audits it),
  **Acknowledge** (resolve *without* changing the order — capture a note on what
  you did instead), and **Deny** (reject/dismiss). Optional: a status dropdown to
  *correct* the AI before approving.
- One PO can produce several cards (one per affected order line) — actioned
  independently.

Maps to the acceptance criteria: list of pending AI changes ✓, Approve/Deny ✓,
approve updates `orders.status` + audit log ✓ (server-side), deny dismisses ✓ —
plus **Acknowledge** for "handled it manually, here's what I did."

---

## 2. Types (`src/types.ts`)

```ts
// Live DB status vocabulary (the doc's UNDER_PRODUCTION/READY_AT_FACTORY are
// display aliases for IN_PRODUCTION/READY).
export type OrderStatus =
  | 'SCHEDULED' | 'PO_SENT' | 'IN_PRODUCTION' | 'READY_FOR_QC' | 'READY'
  | 'CONSOLIDATED' | 'ON_SEA' | 'ON_AIR' | 'ARRIVED_AT_WAREHOUSE'
  | 'RECEIVED' | 'PARTIALLY_RECEIVED' | 'DESTROYED';

export type SuggestionCategory =
  | 'forward' | 'multi_step' | 'lateral' | 'backward_qc_failed'
  | 'data_update'; // no status change — fieldUpdates only (e.g. estimated ready date)

export type AlertStatus = 'pending' | 'snoozed' | 'dismissed' | 'resolved';

export interface FieldUpdate { field: string; from: string | number | null; to: string | number; }
export interface BlockedField { field: string; value: string | number; reason: string; }

export interface StatusSuggestionMeta {
  orderId: number;
  poNumber: string | null;       // the order's PO as stored
  extractedPo: string | null;    // raw PO reference the AI read from the email
  currentStatus: OrderStatus;    // order status when the suggestion was made
  suggestedStatus: OrderStatus;  // the ONE valid next step Approve applies (unless overridden)
  milestone: OrderStatus | null; // the stage the email actually evidenced
  category: SuggestionCategory;  // forward | multi_step | lateral | backward_qc_failed
  remainingPath: OrderStatus[];  // for multi_step: stages still to traverse after this one
  isFqc: boolean;                // FQC sample (can't ship past READY)
  hasQcReport: boolean;          // QC report attached (READY hard-gate)
  gate: { required: string[]; missing: string[] };  // §3 gate fields + which are still absent
  fieldUpdates: FieldUpdate[];   // values the email provided that Approve will also set
  blockedFields: BlockedField[]; // values NOT applied + why (expiry_in_past, lot_expiry_conflict)
  confidence: number;            // 0..1
  rationale: string;             // the email phrase the AI quoted
  model: string;
  jfCode: string | null;
  productName: string | null;
  supplier: string | null;
  source: {
    messageId: string;
    conversationId: string;
    fromEmail: string | null;
    subject: string | null;
    frontUrl: string | null;     // deep-link to the email in Front
    receivedAt: string | null;   // ISO
  };
  sourceRef: string;
}

export interface StatusSuggestion {
  id: number;
  type: 'status_suggestion';
  severity: 'info';
  status: AlertStatus;           // 'pending' until approved (->'resolved') or denied (->'dismissed')
  eventDate: string;             // YYYY-MM-DD
  title: string;                 // "Suggested: IN_PRODUCTION → READY — JF1372"
  body: string | null;
  entityType: 'order';
  entityId: string;              // order id as a string
  meta: StatusSuggestionMeta;
  lastAction: 'approve' | 'acknowledge' | 'deny' | 'snooze' | 'dismiss' | 'restore' | null;
  actionNote: string | null;   // free-text note the operator left (esp. on acknowledge)
  actionedBy: string | null;   // who actioned it (email / username) — shown in history
  actionedAt: string | null;
  createdAt: string;
}

export interface AlertsListResponse<T> {
  data: T[];
  counts: { returned: number; pending: number };
}

export interface ApproveSuggestionResponse {
  order: Order;                  // your existing full Order type
  alert: StatusSuggestion;       // now status: 'resolved'
  appliedStatus: OrderStatus;
  appliedFields: { field: string; value: string | number }[];   // fields actually set
  skippedFields: { field: string; reason: string }[];           // fields rejected at apply time
}
```

---

## 3. API client (`src/api.ts`)

Assumes you already have an `authedFetch` (attaches the Google JWT, parses JSON,
throws on non-2xx). Adapt names to your wrapper.

```ts
import {
  StatusSuggestion, AlertsListResponse, ApproveSuggestionResponse, OrderStatus,
} from './types';

const BASE = '/api/v1';

/** The queue. status defaults to 'pending'. */
export async function listStatusSuggestions(
  status: 'pending' | 'dismissed' | 'resolved' | 'all' = 'pending',
  limit = 200,
): Promise<AlertsListResponse<StatusSuggestion>> {
  const q = new URLSearchParams({ type: 'status_suggestion', status, limit: String(limit) });
  return authedFetch(`${BASE}/alerts?${q.toString()}`);
}

/**
 * Approve: applies the change to the order (status + audit) and resolves the
 * suggestion. Pass `statusOverride` to apply a corrected status instead of the
 * AI's suggestion (must be a valid OrderStatus).
 */
export async function approveSuggestion(
  id: number,
  opts: { statusOverride?: OrderStatus; fields?: Record<string, string | number>; note?: string } = {},
): Promise<ApproveSuggestionResponse> {
  const body: Record<string, unknown> = {};
  if (opts.statusOverride) body.status = opts.statusOverride;
  if (opts.fields) body.fields = opts.fields;   // override meta.fieldUpdates
  if (opts.note) body.note = opts.note;
  return authedFetch(`${BASE}/alerts/${id}/approve`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: Object.keys(body).length ? JSON.stringify(body) : undefined,
  });
}

/**
 * Acknowledge: the operator handled it their own way — resolve WITHOUT changing
 * the order. `note` records what they did instead (shown in history). Returns
 * the resolved suggestion (lastAction: 'acknowledge').
 */
export async function acknowledgeSuggestion(id: number, note?: string): Promise<StatusSuggestion> {
  return authedFetch(`${BASE}/alerts/${id}/acknowledge`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: note ? JSON.stringify({ note }) : undefined,
  });
}

/** Deny: dismiss permanently (reject the suggestion). Optional `note` = why. */
export async function denySuggestion(id: number, note?: string): Promise<StatusSuggestion> {
  return authedFetch(`${BASE}/alerts/${id}/deny`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: note ? JSON.stringify({ note }) : undefined,
  });
}
```

> **Handle `409`** on approve/deny: the body is `{ error, alert }`. Two cases,
> both handled the same way (drop the card + refetch): (a) someone else already
> actioned it; (b) on approve only, the order has since reached a
> received/warehouse state so the suggestion is stale ("Order already RECEIVED;
> suggestion is stale."). Your `authedFetch` should expose the status code (or
> throw a typed error carrying `status` + parsed `body`) so the UI can branch on
> 409 vs a real error, and ideally surface the `error` message as a toast.

---

## 4. Components

### `useStatusSuggestions` hook

```tsx
import { useCallback, useEffect, useState } from 'react';
import { listStatusSuggestions, approveSuggestion, acknowledgeSuggestion, denySuggestion } from './api';
import type { StatusSuggestion, OrderStatus } from './types';

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

  // Optimistic remove; on 409 (already actioned) just refresh; on error, refresh to restore.
  const act = useCallback(async (id: number, run: () => Promise<unknown>) => {
    const prev = items;
    setItems(list => list.filter(s => s.id !== id));
    setPending(n => Math.max(0, n - 1));
    try {
      await run();
    } catch (e: any) {
      if (e?.status === 409) { refresh(); return; }
      setItems(prev);                 // restore the card
      setPending(prev.filter(s => s.status === 'pending').length);
      throw e;
    }
  }, [items, refresh]);

  const approve = useCallback(
    (id: number, override?: OrderStatus, fields?: Record<string, string | number>) =>
      act(id, () => approveSuggestion(id, { statusOverride: override, fields })),
    [act],
  );
  const acknowledge = useCallback(
    (id: number, note: string) => act(id, () => acknowledgeSuggestion(id, note)),
    [act],
  );
  const deny = useCallback((id: number, note?: string) => act(id, () => denySuggestion(id, note)), [act]);

  return { items, pending, loading, error, refresh, approve, acknowledge, deny };
}
```

### `SuggestionCard`

```tsx
import { useState } from 'react';
import type { StatusSuggestion, OrderStatus, InferredStatus } from './types';
import { INFERRABLE_STATUSES } from './types';

export function SuggestionCard({
  s, onApprove, onAcknowledge, onDeny,
}: {
  s: StatusSuggestion;
  onApprove: (id: number, override?: OrderStatus) => Promise<void>;
  onAcknowledge: (id: number, note: string) => Promise<void>;
  onDeny: (id: number) => Promise<void>;
}) {
  const m = s.meta;
  const [override, setOverride] = useState<InferredStatus>(m.suggestedStatus);
  const [busy, setBusy] = useState(false);
  const pct = Math.round((m.confidence ?? 0) * 100);

  const run = async (fn: () => Promise<void>) => {
    setBusy(true);
    try { await fn(); } finally { setBusy(false); }
  };

  return (
    <div className="suggestion-card">
      <div className="suggestion-head">
        <StatusChip status={m.currentStatus} />
        <span aria-hidden>→</span>
        {/* let the operator correct the AI before approving */}
        <select value={override} disabled={busy}
                onChange={e => setOverride(e.target.value as InferredStatus)}>
          {INFERRABLE_STATUSES.map(st => <option key={st} value={st}>{st}</option>)}
        </select>
        <ConfidenceBadge pct={pct} />
      </div>

      <div className="suggestion-body">
        <strong>{m.jfCode ?? m.productName ?? `Order ${m.orderId}`}</strong>
        {m.poNumber && <span> · PO {m.poNumber}</span>}
        {m.supplier && <span> · {m.supplier}</span>}
      </div>

      {/* The rationale is what lets the operator judge the AI rather than trust it blindly. */}
      {m.rationale && <blockquote className="suggestion-rationale">“{m.rationale}”</blockquote>}

      <div className="suggestion-foot">
        {m.source.frontUrl &&
          <a href={m.source.frontUrl} target="_blank" rel="noreferrer">View email in Front ↗</a>}
        <div className="suggestion-actions">
          <button disabled={busy} className="btn-secondary"
                  onClick={() => run(() => onDeny(s.id))}>Deny</button>
          {/* Acknowledge: handled it manually — capture WHAT they did. Swap the
              window.prompt for a proper inline note field / modal in real UI. */}
          <button disabled={busy} className="btn-secondary"
                  onClick={() => {
                    const note = window.prompt('What did you do instead? (recorded in history)');
                    if (note != null) run(() => onAcknowledge(s.id, note));
                  }}>Acknowledge…</button>
          <button disabled={busy} className="btn-primary"
                  onClick={() => run(() => onApprove(s.id, override === m.suggestedStatus ? undefined : override))}>
            Approve → {override}
          </button>
        </div>
      </div>
    </div>
  );
}
```

### `AiSuggestionsView`

```tsx
import { useStatusSuggestions } from './useStatusSuggestions';
import { SuggestionCard } from './SuggestionCard';

export function AiSuggestionsView() {
  const { items, loading, error, approve, acknowledge, deny } = useStatusSuggestions();

  if (loading) return <Spinner />;
  if (error) return <ErrorBanner message={error} />;
  if (!items.length) return <EmptyState title="No AI suggestions" subtitle="Supplier emails are scanned hourly." />;

  return (
    <div className="ai-suggestions">
      <header>
        <h2>AI Suggestions</h2>
        <p className="muted">Order status changes inferred from supplier emails — approve to apply, or deny.</p>
      </header>
      <div className="suggestion-list">
        {items.map(s => (
          <SuggestionCard key={s.id} s={s} onApprove={approve} onAcknowledge={acknowledge} onDeny={deny} />
        ))}
      </div>
    </div>
  );
}
```

### Nav badge

```tsx
// Wherever your nav lives — reuse the same hook (or a lighter count-only fetch).
const { pending } = useStatusSuggestions();
<NavItem to="/ai-suggestions" label="AI Suggestions" badge={pending || undefined} />
```

---

## 5. UX / edge cases

- **Status chips**: render `currentStatus → suggestedStatus` prominently; colour
  the suggested chip distinctly (it's a proposal, not a fact).
- **Confidence**: show the % (e.g. a small badge; amber < 75%, green ≥ 75%). The
  backend already filters out anything below the configured floor (default 60%),
  so everything shown cleared that bar.
- **Override**: default the dropdown to `suggestedStatus`; only send `{ status }`
  when the operator changes it. Backend applies whatever you send (no transition
  validation — the human is the control), so a backward move is allowed if chosen.
- **Acknowledge**: for "I saw it and handled it my own way." Resolves the card
  **without** changing the order. **Prompt for a note** ("what did you do
  instead?") — it's stored as `actionNote` and shown in history. Use this (not
  Deny) when the suggestion was reasonable but already dealt with; use Deny when
  the suggestion was wrong.
- **Multi-line PO**: several cards may share a `poNumber` with different
  `orderId`/`entityId`. That's expected — each is independent. Optionally group
  cards by `poNumber` visually.
- **Optimistic + 409**: remove on action; on `409` (already actioned elsewhere)
  silently refresh; on other errors restore the card and toast.
- **Empty / first run**: suppliers are scanned hourly, so an empty queue is normal.
- **History**: `GET /api/v1/alerts/history`, filter `type === 'status_suggestion'`,
  show `lastAction` (`approve`/`acknowledge`/`deny`), `actionNote`, and
  `actionedBy`/`actionedAt` — the full record of who resolved each suggestion and
  how.

---

## 6. Quick manual test against the API

```bash
# list pending suggestions
curl -H "Authorization: Bearer <jwt>" \
  "$API/api/v1/alerts?type=status_suggestion&status=pending"

# approve (apply the AI's suggested status)
curl -X PATCH -H "Authorization: Bearer <jwt>" "$API/api/v1/alerts/<id>/approve"

# approve with an override
curl -X PATCH -H "Authorization: Bearer <jwt>" -H "Content-Type: application/json" \
  -d '{"status":"ON_SEA"}' "$API/api/v1/alerts/<id>/approve"

# deny
curl -X PATCH -H "Authorization: Bearer <jwt>" "$API/api/v1/alerts/<id>/deny"
```

To generate test data without waiting for the hourly schedule, ops can invoke the
`frontStatusImport` Lambda with `{ "sinceDays": 14 }` (or `{ "dryRun": true }` to
preview without writing alerts).

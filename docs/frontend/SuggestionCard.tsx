import { useState } from 'react';
import {
  StatusSuggestion, OrderStatus, SuggestionCategory,
  isDataUpdate, statusLabel, fieldLabel, fmtVal,
} from './aiSuggestions';

// Statuses the AI can target — used for the optional "correct the status" dropdown.
const INFERRABLE: OrderStatus[] = [
  'PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY', 'CONSOLIDATED', 'ON_SEA', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE',
];

const CATEGORY_BADGE: Record<SuggestionCategory, { label: string; tone: string } | null> = {
  forward: null,
  multi_step: { label: 'multi-step', tone: 'amber' },
  lateral: { label: 'sea/air', tone: 'blue' },
  backward_qc_failed: { label: 'QC failed', tone: 'red' },
  data_update: { label: 'data', tone: 'slate' },
};

export function SuggestionCard({
  s, onApprove, onAcknowledge, onDeny,
}: {
  s: StatusSuggestion;
  onApprove: (id: number, opts?: { statusOverride?: OrderStatus }) => Promise<unknown>;
  onAcknowledge: (id: number, note: string) => Promise<unknown>;
  onDeny: (id: number, note?: string) => Promise<unknown>;
}) {
  const m = s.meta;
  const dataUpdate = isDataUpdate(s);
  const [override, setOverride] = useState<OrderStatus>(m.suggestedStatus);
  const [busy, setBusy] = useState(false);
  const pct = Math.round((m.confidence ?? 0) * 100);
  const sku = m.jfCode ?? m.productName ?? `Order ${m.orderId}`;

  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    try { await fn(); }
    catch (e: any) {
      // 422 gate-blocked etc. — show the server message
      window.alert(e?.body?.error ?? e?.message ?? 'Action failed');
    } finally { setBusy(false); }
  };
  const acknowledge = () => {
    const note = window.prompt('What did you do instead? (recorded in history)');
    if (note != null) run(() => onAcknowledge(s.id, note));
  };

  const badge = CATEGORY_BADGE[m.category];

  return (
    <div className="suggestion-card">
      {/* ── Header: a field-change for data_update, a status move otherwise ── */}
      <div className="suggestion-head">
        {dataUpdate ? (
          <span className="su-chip su-chip-data">{statusLabel(m.currentStatus)} · data update</span>
        ) : (
          <>
            <span className="su-chip">{statusLabel(m.currentStatus)}</span>
            <span aria-hidden>→</span>
            <select value={override} disabled={busy} onChange={e => setOverride(e.target.value as OrderStatus)}>
              {INFERRABLE.map(st => <option key={st} value={st}>{statusLabel(st)}</option>)}
            </select>
          </>
        )}
        {badge && <span className={`su-tag su-tag-${badge.tone}`}>{badge.label}</span>}
        {m.isFqc && <span className="su-tag su-tag-slate">FQC</span>}
        <span className={`su-conf ${pct >= 75 ? 'is-green' : 'is-amber'}`}>{pct}% CONF</span>
      </div>

      {/* ── Identity ── */}
      <div className="suggestion-body">
        <strong>{sku}</strong>
        {m.poNumber && <span> · PO {m.poNumber}</span>}
        {m.supplier && <span> · {m.supplier}</span>}
      </div>

      {/* ── Field changes (the heart of a data_update; "also sets" on a move) ── */}
      {m.fieldUpdates.length > 0 && (
        <div className="suggestion-fields">
          {!dataUpdate && <div className="su-fields-label">Also sets:</div>}
          <ul>
            {m.fieldUpdates.map(f => (
              <li key={f.field}>
                <span className="su-field-name">{fieldLabel(f.field)}</span>
                {f.from != null && <span className="su-field-from"> {fmtVal(f.from)} →</span>}
                <strong> {fmtVal(f.to)}</strong>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* ── Warnings ── */}
      {m.gate.missing.length > 0 && (
        <div className="suggestion-warn">Needs first: {m.gate.missing.join(', ')}</div>
      )}
      {m.category === 'multi_step' && m.remainingPath.length > 0 && (
        <div className="suggestion-note">
          Email implies further stages — only the next step is proposed. Remaining: {m.remainingPath.map(statusLabel).join(' → ')}
        </div>
      )}
      {m.blockedFields.length > 0 && (
        <div className="suggestion-muted">
          Not applied: {m.blockedFields.map(b => `${fieldLabel(b.field)} (${b.reason})`).join(', ')}
        </div>
      )}

      {/* ── Rationale (lets the operator judge the AI) ── */}
      {m.rationale && <blockquote className="suggestion-rationale">“{m.rationale}”</blockquote>}

      {/* ── Footer / actions ── */}
      <div className="suggestion-foot">
        {m.source.frontUrl && (
          <a href={m.source.frontUrl} target="_blank" rel="noreferrer">View email in Front ↗</a>
        )}
        <div className="suggestion-actions">
          <button disabled={busy} className="btn-secondary" onClick={() => run(() => onDeny(s.id))}>Deny</button>
          <button disabled={busy} className="btn-secondary" onClick={acknowledge}>Acknowledge…</button>
          <button
            disabled={busy}
            className="btn-primary"
            onClick={() => run(() => onApprove(
              s.id,
              dataUpdate || override === m.suggestedStatus ? undefined : { statusOverride: override },
            ))}
          >
            {dataUpdate ? 'Approve update' : `Approve → ${statusLabel(override)}`}
          </button>
        </div>
      </div>
    </div>
  );
}

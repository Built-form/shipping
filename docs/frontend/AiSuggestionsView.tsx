import { useMemo } from 'react';
import { useStatusSuggestions, StatusSuggestion } from './aiSuggestions';
import { SuggestionCard } from './SuggestionCard';

// Group suggestions by PO so one supplier email (which can touch many order lines)
// reads as a single block instead of N loose cards.
function groupByPo(items: StatusSuggestion[]) {
  const groups = new Map<string, StatusSuggestion[]>();
  for (const s of items) {
    const key = s.meta.poNumber ?? `order:${s.meta.orderId}`;
    (groups.get(key) ?? groups.set(key, []).get(key)!).push(s);
  }
  return [...groups.entries()];
}

export function AiSuggestionsView() {
  const { items, loading, error, approve, acknowledge, deny } = useStatusSuggestions();
  const groups = useMemo(() => groupByPo(items), [items]);

  if (loading) return <div className="ai-suggestions-loading">Loading…</div>;
  if (error) return <div className="ai-suggestions-error">{error}</div>;
  if (!items.length) {
    return (
      <div className="ai-suggestions-empty">
        <h2>AI Suggestions</h2>
        <p className="muted">No pending suggestions. Supplier emails are scanned hourly.</p>
      </div>
    );
  }

  return (
    <div className="ai-suggestions">
      <header>
        <h2>AI Suggestions <span className="count">{items.length}</span></h2>
        <p className="muted">Order updates inferred from supplier emails — approve to apply, acknowledge if you handled it, or deny.</p>
      </header>

      {groups.map(([po, group]) => (
        <section key={po} className="suggestion-group">
          {group.length > 1 && <div className="suggestion-group-head">PO {po} · {group.length} lines</div>}
          {group.map(s => (
            <SuggestionCard
              key={s.id}
              s={s}
              onApprove={approve}
              onAcknowledge={acknowledge}
              onDeny={deny}
            />
          ))}
        </section>
      ))}
    </div>
  );
}

// Nav badge: reuse the hook (or a lighter count-only fetch) wherever your nav lives.
//   const { pending } = useStatusSuggestions();
//   <NavItem to="/ai-suggestions" label="AI Suggestions" badge={pending || undefined} />

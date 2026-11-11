-- Info bubbles on ShipLine's captions
-- (/api/v1/ui-hints — src/services/ui-hint-routes.js, rules in src/lib/ui-hints.js,
-- frontend guide docs/ui-hints-frontend.md).
--
-- A hint is a short plain-text explanation a person writes on a caption (a
-- column head, a field label, a section heading) for everyone to read, plus
-- one optional link to another ShipLine page. It hangs on a key the page
-- builds from where the caption sits, e.g. payments.ready/col.supplier; one
-- row per key. Every signed-in user reads; admin and standard users write;
-- the author or an admin deletes.
--
-- row_version starts at 1 and goes up on every change: an editor saves
-- against the version it read, so two people saving at once get one success
-- and one 409. The audit log keeps the history (entity_type 'ui_hint').
--
-- Named for ShipLine: this schema is shared with other apps' APIs. No FKs, no
-- ENUMs, UTC timestamps.
CREATE TABLE IF NOT EXISTS shipline_ui_hints (
    id          INT NOT NULL AUTO_INCREMENT,
    hint_key    VARCHAR(160) NOT NULL,
    body        TEXT NOT NULL,
    link_path   VARCHAR(200) NULL,
    link_label  VARCHAR(80) NULL,
    created_by  VARCHAR(255) NOT NULL,
    updated_by  VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    row_version INT NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    UNIQUE KEY uk_hint_key (hint_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

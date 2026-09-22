-- Container photos (src/services/container-photo-routes.js): photos uploaded
-- against one container — booked, or a draft — each with a description.
-- Addressed exactly like packing_lists, so a draft's photos follow it into
-- the booked container it becomes. The file lives in the PO docs bucket
-- under container-photos/<uuid>/<filename>.
--
-- `preset` is the description picked from the dropdown (a copy of the label,
-- not an id, so deleting an option never changes an old photo); `description`
-- is the text shown, which starts as the preset and may be edited.
CREATE TABLE IF NOT EXISTS container_photos (
    id INT NOT NULL AUTO_INCREMENT,
    container_kind VARCHAR(16) NOT NULL DEFAULT 'booked',
    container_number VARCHAR(100) NULL,
    draft_container_id INT NULL,
    container_name VARCHAR(100) NULL,
    filename VARCHAR(200) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    content_type VARCHAR(100) NULL,
    file_size INT NULL,
    preset VARCHAR(100) NULL,
    description VARCHAR(1000) NULL,
    uploaded_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    updated_by_email VARCHAR(255) NULL,
    deleted_at DATETIME NULL,
    deleted_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_s3_key (s3_key),
    KEY idx_container (container_number),
    KEY idx_draft_container (draft_container_id),
    KEY idx_created_at (created_at)
);

-- The pre-filled descriptions offered in the dropdown. Editable from the UI;
-- removing one soft-deletes it (re-adding the same label revives it).
CREATE TABLE IF NOT EXISTS container_photo_presets (
    id INT NOT NULL AUTO_INCREMENT,
    label VARCHAR(100) NOT NULL,
    sort_order INT NOT NULL DEFAULT 100,
    created_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_label (label)
);

INSERT IGNORE INTO container_photo_presets (label, sort_order) VALUES
    ('Proof of cleaning', 10),
    ('Empty container before loading', 20),
    ('Loading in progress', 30),
    ('Fully loaded', 40),
    ('Doors closed', 50),
    ('Seal number', 60),
    ('Container number', 70),
    ('Cartons and shipping marks', 80),
    ('Damage', 90);

CREATE TABLE IF NOT EXISTS inspection_group (
    id BIGINT NOT NULL,
    PRIMARY KEY (id))

CREATE TABLE IF NOT EXISTS inspection_region (
    id BIGINT NOT NULL,
    group_id BIGINT,
    PRIMARY KEY (id))

ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_x FLOAT;
ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS coord_y FLOAT;
ALTER TABLE inspection_region ADD COLUMN IF NOT EXISTS category INTEGER;

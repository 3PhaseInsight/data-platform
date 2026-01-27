BEGIN;

-- Schema
CREATE SCHEMA IF NOT EXISTS lv;

-- Enum
CREATE TYPE lv.node_type AS ENUM ('LvFeeder','Cabinet');

-- =========================
-- Core hierarchy (assets)
-- =========================
CREATE TABLE IF NOT EXISTS lv.secondary_substation (
    id            BIGINT PRIMARY KEY,
    zip_code      integer
);

CREATE TABLE IF NOT EXISTS lv.transformer (
    id             BIGINT PRIMARY KEY,
    substation_id  BIGINT NOT NULL REFERENCES lv.secondary_substation(id) ON DELETE CASCADE,
    capacity_kva   integer
);
CREATE INDEX IF NOT EXISTS transformer_substation_idx ON lv.transformer (substation_id);

CREATE TABLE IF NOT EXISTS lv.feeder (
    id             BIGINT PRIMARY KEY,
    transformer_id BIGINT NOT NULL REFERENCES lv.transformer(id) ON DELETE CASCADE,
    fuse_size_amps integer
);
CREATE INDEX IF NOT EXISTS lv_feeder_transformer_idx ON lv.feeder (transformer_id);

CREATE TABLE IF NOT EXISTS lv.cabinet (
    id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS lv.delivery_point (
    id                       BIGINT PRIMARY KEY,
    cabinet_id               BIGINT NOT NULL REFERENCES lv.cabinet(id) ON DELETE RESTRICT,
    service_fuse_size_amps   integer
);
CREATE INDEX IF NOT EXISTS delivery_point_cabinet_idx ON lv.delivery_point (cabinet_id);

CREATE TABLE IF NOT EXISTS lv.meter (
    id                BIGINT PRIMARY KEY,
    delivery_point_id BIGINT NOT NULL REFERENCES lv.delivery_point(id) ON DELETE RESTRICT,
    has_heat_pump     boolean,
    has_solar_panel   boolean,
    solar_capacity_kw numeric
);
CREATE INDEX IF NOT EXISTS meter_delivery_point_idx ON lv.meter (delivery_point_id);

-- =========================
-- Version catalog (snapshots)
-- =========================
CREATE TABLE IF NOT EXISTS lv.topology_version (
    version     integer PRIMARY KEY,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    is_current  boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS lv_topology_version_current_uniq
    ON lv.topology_version (is_current) WHERE is_current;

-- =========================
-- Versioned topology graph
-- =========================

-- Nodes (versioned)
CREATE TABLE IF NOT EXISTS lv.node(
    version    integer      NOT NULL REFERENCES lv.topology_version(version) ON DELETE CASCADE ,      -- snapshot number
    id    BIGSERIAL    NOT NULL,
    node_type  lv.node_type NOT NULL,
    feeder_id  BIGINT REFERENCES lv.feeder(id) ON DELETE CASCADE,
    cabinet_id BIGINT REFERENCES lv.cabinet(id)  ON DELETE CASCADE,

    PRIMARY KEY (version, id),

    -- Only one node per physical asset per version
    UNIQUE (version, feeder_id),
    UNIQUE (version, cabinet_id),

    -- Exactly one of feeder_id/cabinet_id set, matching type
    CONSTRAINT lv_node_exactly_one_id_chk CHECK (
    (node_type = 'LvFeeder' AND feeder_id IS NOT NULL AND cabinet_id IS NULL) OR
    (node_type = 'Cabinet'  AND cabinet_id IS NOT NULL AND feeder_id IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS lv_node_by_type_idx    ON lv.node (version, node_type);
CREATE INDEX IF NOT EXISTS lv_node_feeder_fk_idx  ON lv.node (version, feeder_id);
CREATE INDEX IF NOT EXISTS lv_node_cabinet_fk_idx ON lv.node (version, cabinet_id);

-- Logical edges (versioned) – use edge_id (not cable_id) for clarity
CREATE TABLE IF NOT EXISTS lv.edge (
    version  INT NOT NULL REFERENCES lv.topology_version(version) ON DELETE CASCADE,
    id  BIGSERIAL NOT NULL,
    node1_id BIGINT    NOT NULL,
    node2_id BIGINT    NOT NULL,

    PRIMARY KEY (version, id),
    CHECK (node1_id <> node2_id),

    -- Endpoints must exist in the SAME version
    FOREIGN KEY (version, node1_id) REFERENCES lv.node (version, id) ON DELETE CASCADE,
    FOREIGN KEY (version, node2_id) REFERENCES lv.node (version, id) ON DELETE CASCADE
);

-- One logical edge per undirected node pair per version (drop if you want parallel edges)
CREATE UNIQUE INDEX IF NOT EXISTS lv_edge_undirected_uniq
    ON lv.edge (version, LEAST(node1_id, node2_id), GREATEST(node1_id, node2_id));

CREATE INDEX IF NOT EXISTS lv_edge_node1_ver_idx ON lv.edge (version, node1_id);
CREATE INDEX IF NOT EXISTS lv_edge_node2_ver_idx ON lv.edge (version, node2_id);

-- Physical cables (versioned)
CREATE TABLE IF NOT EXISTS lv.cable (
    version         integer NOT NULL REFERENCES lv.topology_version(version) ON DELETE CASCADE,
    cable_id        BIGINT NOT NULL,
    cable_type      text,
    cable_length_m  numeric,
    phase_size      numeric,
    phase_material  text,
    capacity_a      integer,
    resistance_ohm  numeric,
    reactance_ohm   numeric,
    PRIMARY KEY (version, cable_id)
);
CREATE INDEX IF NOT EXISTS cable_by_type_idx ON lv.cable (version, cable_type);

-- Edge ↔ Cable mapping (ordered)
CREATE TABLE IF NOT EXISTS lv.edge_cable (
    version  integer NOT NULL REFERENCES lv.topology_version(version) ON DELETE CASCADE,
    edge_id  BIGINT  NOT NULL,
    cable_id BIGINT  NOT NULL,
    seq_no   integer NOT NULL DEFAULT 1,

    PRIMARY KEY (version, edge_id, cable_id),

    FOREIGN KEY (version, edge_id)  REFERENCES lv.edge  (version, id)  ON DELETE CASCADE,
    FOREIGN KEY (version, cable_id) REFERENCES lv.cable    (version, cable_id) ON DELETE CASCADE,

    UNIQUE (version, edge_id, seq_no)
);
CREATE INDEX IF NOT EXISTS lv_edge_cable_edge_idx  ON lv.edge_cable (version, edge_id);
CREATE INDEX IF NOT EXISTS lv_edge_cable_cable_idx ON lv.edge_cable (version, cable_id);

-- =========================
-- Convenience views
-- =========================
CREATE OR REPLACE VIEW lv.node_current AS
SELECT n.*
FROM lv.node n
WHERE n.version = (
    SELECT version
    FROM lv.topology_version
    WHERE is_current
);

CREATE OR REPLACE VIEW lv.edge_current AS
SELECT e.*
FROM lv.edge e
WHERE e.version = (
    SELECT version
    FROM lv.topology_version
    WHERE is_current
);


CREATE OR REPLACE VIEW lv.edge_current_with_totals AS
SELECT e.version, e.id, e.node1_id, e.node2_id,
       SUM(c.cable_length_m) AS total_length_m,
       SUM(c.resistance_ohm) AS total_resistance_ohm,
       SUM(c.reactance_ohm)  AS total_reactance_ohm,
       MIN(c.capacity_a)     AS min_capacity_a
FROM lv.edge e
         JOIN lv.topology_version v ON v.version=e.version AND v.is_current
         JOIN lv.edge_cable ec ON ec.version=e.version AND ec.edge_id=e.id
         JOIN lv.cable c ON c.version=ec.version AND c.cable_id=ec.cable_id
GROUP BY e.version, e.id, e.node1_id, e.node2_id;

COMMIT;

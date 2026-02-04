-- Deploy 3phi-db:06_tables_run_results to pg

BEGIN;

    -- option to scope run result to topology version
    ALTER TABLE :meta_schema.run_result
        ADD COLUMN topology_version integer
        REFERENCES :lv_schema.topology_version(version);

    -- drop meter_id NOT NULL constraint
    ALTER TABLE :meta_schema.run_result
        ALTER COLUMN meter_id DROP NOT NULL;

    -- drop phase NOT NULL constraint
    ALTER TABLE :meta_schema.run_result
        ALTER COLUMN phase DROP NOT NULL;

    -- versioned graph entity pointers
    ALTER TABLE :meta_schema.run_result
        ADD COLUMN node_id  bigint, -- lv_feeder's, cabinet's or delivery_point's node_id (from :lv_schema.node)
        ADD COLUMN edge_id  bigint,
        ADD COLUMN cable_id bigint;

    -- composite foreign keys, ON DELETE RESTRICT to prevent topology version deletes if results reference it
    ALTER TABLE :meta_schema.run_result
        ADD CONSTRAINT run_result_node_fk
            FOREIGN KEY (topology_version, node_id)
            REFERENCES :lv_schema.node(version, id)
            ON DELETE RESTRICT,
        ADD CONSTRAINT run_result_edge_fk
            FOREIGN KEY (topology_version, edge_id)
            REFERENCES :lv_schema.edge(version, id)
            ON DELETE RESTRICT,
        ADD CONSTRAINT run_result_cable_fk
            FOREIGN KEY (topology_version, cable_id)
            REFERENCES :lv_schema.cable(version, cable_id)
            ON DELETE RESTRICT;

    -- make sure EITHER meter_id OR versioned graph entity is specified
    ALTER TABLE :meta_schema.run_result
        ADD CONSTRAINT run_result_exactly_one_target_chk CHECK (
        (meter_id IS NOT NULL)::int
        + (node_id  IS NOT NULL)::int
        + (edge_id  IS NOT NULL)::int
        + (cable_id IS NOT NULL)::int
        = 1
        );

    -- ensure version is set when result of versioned graph entity is stored
    ALTER TABLE :meta_schema.run_result
        ADD CONSTRAINT run_result_version_required_for_graph_chk CHECK (
            (meter_id IS NOT NULL AND topology_version IS NULL)
            OR
            (meter_id IS NULL AND topology_version IS NOT NULL)
        );

    -- indexes for faster lookups
    CREATE INDEX IF NOT EXISTS run_result_node_idx
    ON :meta_schema.run_result (topology_version, node_id)
    WHERE node_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS run_result_edge_idx
    ON :meta_schema.run_result (topology_version, edge_id)
    WHERE edge_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS run_result_cable_idx
    ON :meta_schema.run_result (topology_version, cable_id)
    WHERE cable_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS run_result_meter_idx
    ON :meta_schema.run_result (meter_id)
    WHERE meter_id IS NOT NULL;

COMMIT;

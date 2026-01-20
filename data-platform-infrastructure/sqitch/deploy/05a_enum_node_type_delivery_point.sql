-- Deploy 3phi-db:05a_enum_node_type_delivery_point to pg

ALTER TYPE lv.node_type ADD VALUE IF NOT EXISTS 'DeliveryPoint';

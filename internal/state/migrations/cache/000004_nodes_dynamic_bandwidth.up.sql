ALTER TABLE nodes_dynamic ADD COLUMN last_bandwidth_probe_attempt_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes_dynamic ADD COLUMN last_bandwidth_update_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes_dynamic ADD COLUMN bandwidth_mbps REAL NOT NULL DEFAULT 0;

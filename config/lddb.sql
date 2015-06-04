CREATE TABLE IF NOT EXISTS lddb (
    id text not null primary key,
    data jsonb not null,
    entry jsonb not null,
    ts timestamp with time zone not null default now(),
    deleted boolean not null default false
);

CREATE INDEX lddb_idx_alive ON lddb (id) WHERE deleted IS NOT true;
CREATE INDEX lddb_idx_graphs ON lddb USING GIN ((data->'@graph') jsonb_path_ops);
CREATE INDEX lddb_idx_entry ON lddb USING GIN (entry jsonb_path_ops);
CREATE INDEX lddb_idx_ts ON lddb (ts);
CREATE INDEX lddb_idx_dataset ON lddb USING GIN ((entry->'dataset') jsonb_path_ops);

CREATE TABLE IF NOT EXISTS lddb__versions (
    pk serial,
    id text not null primary key,
    checksum char(32) not null,
    data jsonb not null,
    entry jsonb not null,
    ts timestamp with time zone not null,
    unique (id, checksum)
);

CREATE INDEX lddb__versions_idx_id ON lddb__versions (id);
CREATE INDEX lddb__versions_idx_checksum ON lddb__versions (checksum);
CREATE INDEX lddb__versions_idx_entry ON lddb__versions USING GIN (entry jsonb_path_ops);
CREATE INDEX lddb__versions_idx_ts ON lddb__versions (ts);
CREATE INDEX lddb__versions_idx_dataset ON lddb__versions USING GIN ((entry->'dataset') jsonb_path_ops);


-- Archival information for each domain
CREATE TYPE archival_info (
  enabled       boolean,
  retentionDays int,
);

ALTER TABLE domains_by_name ADD archival_info frozen<archival_info>;
ALTER TABLE domains_by_name_v2 ADD archival_info frozen<archival_info>;
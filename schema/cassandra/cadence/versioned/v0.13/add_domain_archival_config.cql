-- Archival configuration for each domain
CREATE TYPE domain_archival_config (
  enabled        boolean,
  retention_days int,
);

ALTER TABLE domains_by_name ADD archival_config frozen<domain_archival_config>;
ALTER TABLE domains_by_name_v2 ADD archival_config frozen<domain_archival_config>;
ALTER TYPE domain_config ADD archival_enabled boolean;
ALTER TYPE domain_config ADD archival_bucket_name text;
ALTER TYPE domain_config ADD archival_retention_days int;
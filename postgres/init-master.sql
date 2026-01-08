CREATE USER replicator REPLICATION LOGIN PASSWORD 'clickstream';
SELECT pg_create_physical_replication_slot('replica_slot');

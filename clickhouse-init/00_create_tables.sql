CREATE DATABASE IF NOT EXISTS clickstream;



CREATE TABLE IF NOT EXISTS clickstream.events_cleansed
(
    event_id String,
    type String,
    created_at DateTime,
    received_at DateTime,
    session_id String,
    user_id UInt64,
    ip String,
    url String,
    referrer String,
    device_type String,
    user_agent String,
    event_title String,
    element_id String,
    x Int32,
    y Int32,
    payload String,
    source String
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY event_id;

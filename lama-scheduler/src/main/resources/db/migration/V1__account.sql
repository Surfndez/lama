CREATE TYPE sync_status as ENUM(
    'registered',
    'unregistered',
    'published',
    'synchronized',
    'sync_failed',
    'deleted',
    'delete_failed'
);

CREATE TYPE coin_family as ENUM(
    'bitcoin'
);

CREATE TYPE coin as ENUM(
    'btc'
);

CREATE TABLE account_info(
    account_id UUID PRIMARY KEY,
    key VARCHAR NOT NULL,
    coin_family coin_family NOT NULL,
    coin coin NOT NULL,
    sync_frequency INTERVAL NOT NULL,
    UNIQUE (key, coin_family, coin)
);

CREATE TABLE account_sync_event(
    id SERIAL PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES account_info(account_id),
    sync_id UUID NOT NULL,
    status sync_status NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated TIMESTAMP NOT NULL DEFAUlT CURRENT_TIMESTAMP
);

CREATE INDEX account_sync_event_updated_index ON account_sync_event(updated);

CREATE VIEW account_sync_status AS (
    SELECT DISTINCT ON (account_id)
        account_id,
        "key",
        coin_family,
        coin,
        sync_frequency,
        sync_id,
        status,
        payload,
        updated
    FROM account_info JOIN account_sync_event USING (account_id)
    ORDER BY account_id, updated DESC
);



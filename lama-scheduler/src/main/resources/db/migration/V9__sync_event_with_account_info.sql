ALTER TABLE account_sync_event
    ADD COLUMN "key" VARCHAR NOT NULL DEFAULT '',
    ADD COLUMN coin_family coin_family NOT NULL DEFAULT 'bitcoin',
    ADD COLUMN coin coin NOT NULL DEFAULT 'btc',
    ADD COLUMN group_col VARCHAR NOT NULL DEFAULT 'group'
;
ALTER TABLE account_info ADD COLUMN label VARCHAR NULL ;

CREATE OR REPLACE VIEW account_sync_status AS (
    SELECT DISTINCT ON (account_id)
        account_id,
        "key",
        coin_family,
        coin,
        sync_frequency,
        sync_id,
        status,
        payload,
        updated,
        label
    FROM account_info JOIN account_sync_event USING (account_id)
    ORDER BY account_id, updated DESC
);

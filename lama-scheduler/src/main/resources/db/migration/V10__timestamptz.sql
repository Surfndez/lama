DROP VIEW account_sync_status;
DROP VIEW workable_event;

ALTER TABLE account_sync_event
ALTER COLUMN updated TYPE timestamp with time zone;

CREATE VIEW account_sync_status AS (
    SELECT DISTINCT ON (account_id)
        account_id,
        label,
        account_info."key",
        account_info.coin_family,
        account_info.coin,
        account_info.group_col,
        sync_frequency,
        sync_id,
        status,
        "cursor",
        "error",
        updated
    FROM account_info JOIN account_sync_event USING (account_id)
    ORDER BY account_id, updated DESC
);

CREATE VIEW workable_event AS (
WITH e AS (
    SELECT
        account_id,
        account_info."key",
        account_info.coin_family,
        account_info.coin,
        account_info.group_col,
        sync_frequency,
        sync_id,
        status,
        cursor,
        error,
        updated,
        COUNT(*) OVER (PARTITION BY sync_id) as nb_events
    FROM account_info JOIN account_sync_event USING (account_id)
)
SELECT *
FROM e
WHERE nb_events = 1
AND status IN ('registered', 'unregistered')
ORDER BY updated
);
CREATE VIEW workable_event AS
WITH e AS (
    SELECT
        account_id,
        "key",
        coin_family,
        coin,
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

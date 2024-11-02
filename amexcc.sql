SELECT
    substr(a.unique_hash, 1, 5) || '...' || substr(a.unique_hash, -4) AS unique_hash,
    'amexcc' AS Account,
    a."Date" AS tx_date,
    a.Description AS tx_merchant,
    -a.Amount AS tx_amount,
    COALESCE(m.category, a.Category) AS tx_category,
    ax.tx_merchant as xxx,
    date(a."Date") as adate,
    date(ax.tx_date) as axdate,
    -a.Amount as aAmount,
    ax.tx_amount as axAmount,
    CASE
        -- Enrich the tx_note if the category is 'AMAZON'
        WHEN COALESCE(m.category, a.Category) = 'AMAZON' THEN
            a."Extended Details" || ' - ' || ax.tx_merchant
        ELSE
            a."Extended Details"
    END AS tx_note
FROM amexcc a
LEFT JOIN amazon_view ax ON
    date(a."Date", 'unixepoch') = date(ax.tx_date, 'unixepoch') AND  -- Compare date part only
    a.Amount = -ax.tx_amount
LEFT JOIN merchant m ON a.Description = m.merchant_id  -- Adjusted join to include merchant table
WHERE (a.Tags != 'duplicate' OR a.Tags IS NULL) AND a.Description="AMAZON MARKEPLACE NA PA"
limit 10;

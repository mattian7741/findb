CREATE VIEW amexsv_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'amexsv' AS Account,                                                          -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    Type AS tx_merchant,                                                          -- Use Type as merchant
    Amount AS tx_amount,                                                          -- Use Amount as transaction amount
    NULL AS tx_category,                                                          -- Set tx_category to NULL
    NULL AS tx_note                                                               -- Set tx_note to NULL
FROM amexsv
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW venmo_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'venmo' AS Account,                                                           -- Static account name
    date(Datetime, 'unixepoch', '-4 hours') AS tx_date,                           -- Convert to EDT format (UTC-4) for the date only
    IFNULL(
        CASE
            WHEN CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) < 0 THEN "To"
            WHEN CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) >= 0 THEN "From"
            ELSE NULL
        END,
        "Destination"
    ) AS tx_merchant,                                                             -- Conditional tx_merchant with default to "Funding Source" if NULL
    CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) AS tx_amount,             -- Remove $ and cast to numeric for total amount
    Type AS tx_category,                                                          -- Use Type as transaction category
    Note AS tx_note                                                               -- Include Note column as tx_note
FROM venmo
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW paypal_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'paypal' AS Account,                                                          -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    IFNULL(Name, Type) AS tx_merchant,                                            -- Use Name as merchant, default to Type if Name is NULL
    CAST(REPLACE(Amount, '$', '') AS NUMERIC) AS tx_amount,                       -- Remove $ and cast to numeric for amount
    Type AS tx_category,                                                          -- Use Type as transaction category
    NULL AS tx_note                                                               -- Set tx_note to NULL
FROM paypal
WHERE Tags != 'duplicate' OR Tags IS NULL;



CREATE VIEW psecuch_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'psecuch' AS Account,                                                         -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    "Transaction Description" AS tx_merchant,                                     -- Use Transaction Description as merchant
    Amount AS tx_amount,                                                          -- Use Amount as transaction amount
    Category AS tx_category,                                                      -- Use Category as transaction category
    Note AS tx_note                                                               -- Use Note column as tx_note
FROM psecuch
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW psecucc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'psecucc' AS Account,                                                         -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    "Transaction Description" AS tx_merchant,                                     -- Use Transaction Description as merchant
    -(Principal + Interest + Fees) AS tx_amount,                                  -- Invert tx_amount by multiplying by -1
    Category AS tx_category,                                                      -- Use Category as transaction category
    Note AS tx_note                                                               -- Use Note column as tx_note
FROM psecucc
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW chasecc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'chasecc' AS Account,                                                         -- Static account name
    date("Transaction Date", 'unixepoch', '-4 hours') AS tx_date,                 -- Convert to EDT format (UTC-4) for the date only
    Description AS tx_merchant,                                                   -- Use Description as merchant
    Amount AS tx_amount,                                                          -- Use Amount as transaction amount
    Category AS tx_category,                                                      -- Use Category as transaction category
    Memo AS tx_note                                                               -- Use Memo column as tx_note
FROM chasecc
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW citicc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'citicc' AS Account,                                                          -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    Description AS tx_merchant,                                                   -- Use Description as merchant
    CASE
        WHEN Debit != 0 THEN -Debit                                               -- Use Debit as a negative amount
        ELSE -Credit                                                              -- Use Credit as a positive amount
    END AS tx_amount,                                                             -- Conditional tx_amount based on Debit or Credit
    NULL AS tx_category,                                                          -- Set tx_category to NULL
    NULL AS tx_note                                                               -- Set tx_note to NULL
FROM citicc
WHERE Tags != 'duplicate' OR Tags IS NULL;


CREATE VIEW all_transactions AS
SELECT 
    a.unique_hash,
    a.Account,
    a.tx_date,
    a.tx_merchant,
    a.tx_amount,
    COALESCE(m.category, m.tx_category, a.tx_category) AS tx_category,
    a.tx_note
FROM (
    SELECT * FROM amexcc_view
    UNION ALL
    SELECT * FROM amexsv_view
    UNION ALL
    SELECT * FROM psecuch_view
    UNION ALL
    SELECT * FROM psecucc_view
    UNION ALL
    SELECT * FROM paypal_view
    UNION ALL
    SELECT * FROM venmo_view
    UNION ALL
    SELECT * FROM chasecc_view
    UNION ALL
    SELECT * FROM citicc_view
) a
LEFT JOIN merchant m ON a.tx_merchant = m.merchant_id
ORDER BY a.tx_date;


CREATE VIEW amexcc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    'amexcc' AS Account,                                                          -- Static account name
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    Description AS tx_merchant,                                                   -- Use Description as merchant
    -Amount AS tx_amount,                                                         -- Invert Amount by multiplying by -1
    Category AS tx_category,                                                      -- Use Category as transaction category
    "Extended Details" AS tx_note                                                 -- Use Extended Details as tx_note
FROM amexcc
WHERE Tags != 'duplicate' OR Tags IS NULL;


DROP VIEW amexsv_view;
CREATE VIEW amexsv_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'amexsv' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    Type AS tx_merchant,
    Amount AS tx_amount,
    NULL AS tx_category,
    NULL AS tx_note
FROM amexsv
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW venmo_view;
CREATE VIEW venmo_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'venmo' AS Account,
    date(Datetime, 'unixepoch', '-4 hours') AS tx_date,
    IFNULL(
        CASE
            WHEN CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) < 0 THEN "To"
            WHEN CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) >= 0 THEN "From"
            ELSE NULL
        END,
        "Destination"
    ) AS tx_merchant, 
    CAST(REPLACE([Amount (total)], '$', '') AS NUMERIC) AS tx_amount,
    Type AS tx_category,
    Note AS tx_note
FROM venmo
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW paypal_view;
CREATE VIEW paypal_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'paypal' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    IFNULL(Name, Type) AS tx_merchant,
    CAST(REPLACE(Amount, '$', '') AS NUMERIC) AS tx_amount,
    Type AS tx_category,
    NULL AS tx_note
FROM paypal
WHERE Tags != 'duplicate' OR Tags IS NULL;


DROP VIEW psecuch_view;
CREATE VIEW psecuch_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'psecuch' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    "Transaction Description" AS tx_merchant,
    Amount AS tx_amount,
    Category AS tx_category,
    Note AS tx_note
FROM psecuch
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW psecucc_view;
CREATE VIEW psecucc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'psecucc' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    "Transaction Description" AS tx_merchant,
    -(Principal + Interest + Fees) AS tx_amount,
    Category AS tx_category,
    Note AS tx_note
FROM psecucc
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW chasecc_view;
CREATE VIEW chasecc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'chasecc' AS Account,
    date("Transaction Date", 'unixepoch', '-4 hours') AS tx_date,
    Description AS tx_merchant,
    Amount AS tx_amount,
    Category AS tx_category,
    Memo AS tx_note
FROM chasecc
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW citicc_view;
CREATE VIEW citicc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'citicc' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    Description AS tx_merchant,
    CASE
        WHEN Debit != 0 THEN -Debit
        ELSE -Credit
    END AS tx_amount,
    NULL AS tx_category,
    NULL AS tx_note
FROM citicc
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW amexcc_view;
CREATE VIEW amexcc_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'amexcc' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    Description AS tx_merchant,
    -Amount AS tx_amount,
    Category AS tx_category,
    "Extended Details" AS tx_note
FROM amexcc
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW psecupe_view;
CREATE VIEW psecupe_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'psecuch' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    "Transaction Description" AS tx_merchant,
    Amount AS tx_amount,
    Category AS tx_category,
    Note AS tx_note
FROM psecupe
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW psecuxd_view;
CREATE VIEW psecuxd_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'psecuxd' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    "Transaction Description" AS tx_merchant,
    Amount AS tx_amount,
    Category AS tx_category,
    Note AS tx_note
FROM psecuxd
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW psecudr_view;
CREATE VIEW psecudr_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'psecudr' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    "Transaction Description" AS tx_merchant,
    Amount AS tx_amount,
    Category AS tx_category,
    Note AS tx_note
FROM psecudr
WHERE Tags != 'duplicate' OR Tags IS NULL;
DROP VIEW chasemo_view;
CREATE VIEW chasemo_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'chasemo' AS Account,
    date(Date, 'unixepoch', '-4 hours') AS tx_date,
    Description AS tx_merchant,
    Amount AS tx_amount,
    NULL AS tx_category,
    NULL AS tx_note
FROM chasemo
WHERE Tags != 'duplicate' OR Tags IS NULL;

DROP VIEW IF EXISTS mint_view;

CREATE VIEW mint_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,  -- Shortened hash format
    "Account Name" AS Account,                                                    -- Use "Account Name" from mint as Account
    date(Date, 'unixepoch', '-4 hours') AS tx_date,                               -- Convert to EDT format (UTC-4) for the date only
    "Original Description" AS tx_merchant,                                        -- Use "Original Description" as merchant
    CASE
        WHEN "Transaction Type" = "debit" THEN -Amount                            -- Negate Amount for debit transactions
        ELSE Amount                                                               -- Use Amount directly for credit transactions
    END AS tx_amount,                                                             -- Transaction amount based on type
    Category AS tx_category,                                                      -- Use Category as transaction category
    TRIM(
        CASE WHEN Description IS NOT NULL AND Description != '' THEN Description ELSE '' END ||
        CASE WHEN Labels IS NOT NULL AND Labels != '' THEN 
            CASE WHEN Description IS NOT NULL AND Description != '' THEN ', ' ELSE '' END || Labels 
        ELSE '' END ||
        CASE WHEN Notes IS NOT NULL AND Notes != '' THEN 
            CASE WHEN (Description IS NOT NULL AND Description != '') OR (Labels IS NOT NULL AND Labels != '') THEN ', ' ELSE '' END || Notes 
        ELSE '' END
    ) AS tx_note                                                                  -- Concatenate Description, Labels, Notes for tx_note
FROM mint
WHERE Tags != 'duplicate' OR Tags IS NULL;



DROP VIEW all_transactions;
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
    UNION ALL
    SELECT * FROM chasemo_view
    UNION ALL
    SELECT * FROM psecupe_view
    UNION ALL
    SELECT * FROM psecuxd_view
    UNION ALL
    SELECT * FROM psecudr_view
    UNION ALL
    SELECT * FROM mint_view
) a
LEFT JOIN merchant m ON a.tx_merchant = m.merchant_id
ORDER BY a.tx_date;
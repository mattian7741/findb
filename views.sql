
DROP VIEW amexsv_view;
CREATE VIEW amexsv_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'amexsv' AS Account,
    "Date" AS tx_date,
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
    "Datetime" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Transaction Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,
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
    "Date" AS tx_date,                                                            -- Retaining epoch seconds for consistency
    "Original Description" AS tx_merchant,                                        -- Use "Original Description" as merchant
    CASE
        WHEN "Transaction Type" = 'debit' THEN -"Amount"                          -- Negate Amount for debit transactions
        ELSE "Amount"                                                             -- Use Amount directly for credit transactions
    END AS tx_amount,                                                             -- Transaction amount based on type
    "Category" AS tx_category,                                                    -- Use Category as transaction category
    TRIM(
        COALESCE("Description", '') ||
        CASE WHEN "Labels" IS NOT NULL AND "Labels" != '' THEN ', ' || "Labels" ELSE '' END ||
        CASE WHEN "Notes" IS NOT NULL AND "Notes" != '' THEN ', ' || "Notes" ELSE '' END
    ) AS tx_note  -- Concatenate Description, Labels, Notes for tx_note
FROM mint
WHERE "Tags" != 'duplicate' OR "Tags" IS NULL;


DROP VIEW IF EXISTS amazon_digital_view;
CREATE VIEW amazon_digital_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'amazon_digital' AS Account,
    OrderDate AS tx_date,
    ProductName AS tx_merchant,
    OurPrice AS tx_amount,
    NULL AS tx_category, -- Assuming there's no direct category mapping available
    "ASIN" || ', ' || "OrderId" || ', ' || COALESCE("GiftMessage", '') AS tx_note
FROM amazon
WHERE Tags != 'duplicate' OR Tags IS NULL;


DROP VIEW IF EXISTS amazon_view;
CREATE VIEW amazon_view AS
SELECT
    substr(unique_hash, 1, 5) || '...' || substr(unique_hash, -4) AS unique_hash,
    'amazon' AS Account,
    "Order Date" AS tx_date,
    "Product Name" AS tx_merchant,
    "Total Owed" AS tx_amount,
    "AMAZON" AS tx_category,  -- No direct category mapping available
    "Purchase Order Number" || ', ' || "Order ID" AS tx_note  -- Combine Purchase Order Number and Order ID for notes
FROM amazon
WHERE Tags != 'duplicate' OR Tags IS NULL;


DROP VIEW all_transactions;
CREATE VIEW all_transactions AS
SELECT 
    a.unique_hash,
    a.Account,
    CASE
        WHEN strftime('%m', a.tx_date, 'unixepoch') IN ('04', '05', '06', '07', '08', '09', '10') THEN date(a.tx_date, 'unixepoch', '-4 hours') -- EDT months
        ELSE date(a.tx_date, 'unixepoch', '-5 hours') -- EST months
    END AS tx_date,
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
    UNION ALL
    SELECT * FROM amazon_view
) a
LEFT JOIN merchant m ON a.tx_merchant = m.merchant_id
ORDER BY a.tx_date;

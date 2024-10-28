Bugs and Features BACKLOG:

- [ ] Currently ingestion interprets dates from source csv files as UTC time.  This not correct behavior.  It should interpret csv dates as Eastern Time, and store them as EPOCH UTC seconds.
- [ ] When the merchant table update (merchant.py) runs, it should not modify existing merchants in the merchant table.   The new merchants should be determined by doing a diff between the all_transactions.tx_merchant and the merchant.mechant_id and only the new merchants should be written to the merchant table.
- [ ] primary keys to tables and indexes for speed
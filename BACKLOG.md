Bugs and Features BACKLOG:

- [x] Currently ingestion interprets dates from source csv files as UTC time.  This not correct behavior.  It should interpret csv dates as Eastern Time, and store them as EPOCH UTC seconds.
- [x] When the merchant table update (merchant.py) runs, it should not modify existing merchants in the merchant table.   The new merchants should be determined by doing a diff between the all_transactions.tx_merchant and the merchant.mechant_id and only the new merchants should be written to the merchant table.
- [ ] primary keys to tables and indexes for speed
- [ ] merchant names need to be normalized - some have double spaces and other have single spaces for the same merchant causing transactions to look different that are the same. AMEX
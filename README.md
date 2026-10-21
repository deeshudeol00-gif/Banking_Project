Banking ETL Demo with Databricks and PySpark
A small batch data pipeline that validates fictional banking records and produces account-level transaction summaries. The project uses Bronze, Silver, and Gold Delta tables, retains rejected transactions with reasons, and verifies the results through automated checks.

The implementation is in [Banking_ETLs.ipynb](Banking_ETLs.ipynb). Place this README beside the notebook in your repository.
Project objective
Convert raw customer, account, and transaction records into reliable account summaries while detecting duplicates, invalid values, and broken account references.

The pipeline demonstrates:
- Parsing CSV-formatted sample records with an explicit schema.
- Saving raw records in Bronze Delta tables.
- Cleaning text and converting identifiers, dates, timestamps, and amounts.
- Validating master data and transaction relationships.
- Removing exact duplicate transactions and rejecting conflicting transaction IDs.
- Separating invalid transactions with rejection reasons and original values.
- Calculating account-level credits, debits, transaction counts, and balances.
- Reconciling records and validating expected results.
Technology
Component	Purpose
Databricks	Notebook execution and compute
Python / PySpark	Transformations and validation logic
Spark SQL	SQL expressions and report queries
Delta Lake	Persistent project tables
Python decimal.Decimal	Exact expected monetary values in tests


Data flow
Stage	Processing	Output
Source	Parse fictional CSV text embedded in the notebook	Customer, account, and transaction DataFrames
Bronze	Preserve raw business-field values and add ingestion timestamps	Three raw Delta tables
Silver	Clean, cast, validate, and deduplicate	Three validated Delta tables and a rejection table
Gold	Join account/customer details and aggregate valid transactions	One summary row per account
Validation	Compare actual results with fixture-specific expectations	Test results table and a final assertion


Source scope: The CSV records are embedded in the notebook. This version does not ingest external files, databases, or live banking feeds.
Sample data
Dataset	Rows	Fields
Customers	5	customer_id, customer_name, city, join_date
Accounts	5	account_id, customer_id, account_type, balance, account_status
Transactions	14	transaction_id, account_id, transaction_timestamp, transaction_type, amount, channel


The transactions contain eight valid records, one additional exact duplicate, and five invalid records.

Relationships are validated in code: accounts reference customers through customer_id, and transactions reference accounts through account_id. Database key constraints are not declared by the notebook.
Run the project
Prerequisites
- A Databricks workspace with suitable compute, such as Serverless.
- A writable catalog and schema, or permission to create the project schema.
- Permission to create and overwrite the project tables.
No external datasets, credentials, or additional Python package installations are required for this demo in Databricks. The notebook uses Databricks-provided spark and display; it is not intended to run unchanged in a plain local Jupyter environment.
Setup
1. Import Banking_ETLs.ipynb into a Databricks workspace folder.
2. Open it and select compute.
3. Review the configuration in the first cell:
   CATALOG = spark.sql("SELECT current_catalog()").first()[0]
   SCHEMA = "banking_etl_demo"
   CATALOG uses the session's current catalog. Change the configuration if your workspace requires another writable location.
4. Run all eight Python cells in order.
5. Inspect the rejected transactions, Gold summary, and validation output.
6. Confirm that the final validation cell prints:
   All ETL checks passed.
Write behavior: Every run overwrites the named project tables and allows their schemas to be replaced. Use a dedicated demo schema. Do not target existing production tables.
Notebook cells
Cell	Responsibility
1	Configure catalog/schema and define read/write helpers
2	Define and parse embedded CSV records
3	Save Bronze tables
4	Clean and validate customer/account master data
5	Deduplicate, validate, and separate transactions
6	Build the Gold account summary
7	Execute automated checks and save test results
8	Query and display the saved report


Tables created
All tables are written under <catalog>.banking_etl_demo unless configuration is changed.
Table	Purpose
bronze_customers	Raw customer fields plus ingested_at
bronze_accounts	Raw account fields plus ingested_at
bronze_transactions	All raw transaction rows plus ingested_at
silver_customers	Cleaned and validated customers
silver_accounts	Cleaned accounts with opening_balance
silver_transactions	Accepted, typed transaction records
rejected_transactions	Invalid transactions with raw fields and rejection reasons
gold_account_summary	Account/customer details and calculated totals
etl_test_results	Check name, expected value, actual value, and PASS/FAIL status


Validation rules
Customer and account master data
Master-data assertions stop processing when the implemented checks fail. They verify required IDs, customer names and dates, unique customer/account IDs, nonnegative opening balances, supported account statuses, and valid account-to-customer references.
Transactions
- Remove exact duplicate rows across raw transaction business columns, excluding ingestion metadata.
- After parsing, reject all rows with a conflicting transaction ID rather than selecting an arbitrary version.
- Reject missing or unparseable IDs, unknown accounts, invalid timestamps, unsupported transaction types, and missing or nonpositive amounts.
- Allow only CREDIT and DEBIT after text normalization.
- Retain raw fields and allow multiple rejection reasons per record.
Rejected transaction	Reason in the sample
5009	Unknown account 9999
5010	Negative amount
5011	Missing amount
5012	Invalid timestamp
5013	Unsupported transaction type Transfer


Business assumptions
- All amounts represent INR. No currency conversion is implemented.
- The source balance is interpreted as the opening balance before the supplied transactions and renamed opening_balance.
- Credit increases the customer account balance; Debit decreases it.
- Monetary inputs use DECIMAL(18,2).
- Accounts with no accepted transactions remain in Gold through left joins; missing totals are filled with zero.
- An inactive account with accepted transactions is flagged with review_required = true. Current status alone does not establish its status at transaction time, so those historical records are not rejected automatically.
Calculated balance = Opening balance + Total credits - Total debits
Expected results
Record reconciliation
14 raw transactions = 8 accepted + 5 rejected + 1 duplicate removed
Account summary
Account	Customer	Opening balance	Credits	Debits	Transactions	Calculated balance
1001	Amit	25,000.00	5,000.00	1,900.00	3	28,100.00
1002	Priya	85,000.00	25,000.00	15,000.00	2	95,000.00
1003	Rahul	12,000.00	8,000.00	0.00	1	20,000.00
1004	Neha	45,000.00	0.00	2,000.00	1	43,000.00
1005	Vikas	95,000.00	0.00	50,000.00	1	45,000.00
Total		262,000.00	38,000.00	68,900.00	8	231,100.00


Account 1004 is flagged for review.
Automated checks
The notebook validates row counts and reconciliation, accepted transaction ID uniqueness, valid account references, Gold account/transaction counts, monetary totals, individual account summaries, expected rejected IDs, and the inactive-account review flag.

Results are saved to etl_test_results before the final assertion. If any recorded comparison fails, the notebook raises an assertion error.

These expectations are specific to the supplied sample. Update them when changing the input data. The sample does not exercise every implemented branch; conflicting transaction IDs and accounts without transactions need additional fixtures.
Query the outputs
For a run using the workspace catalog and default schema, use these queries in Databricks SQL Editor or a SQL notebook cell. Replace the catalog/schema if configured differently.
SELECT *
FROM workspace.banking_etl_demo.gold_account_summary
ORDER BY account_id;
SELECT transaction_id, raw_account_id, raw_amount, rejection_reason
FROM workspace.banking_etl_demo.rejected_transactions
ORDER BY transaction_id;
SELECT *
FROM workspace.banking_etl_demo.etl_test_results
WHERE status = 'FAIL';
The last query should return no rows after a successful run with the supplied sample.
Reruns and troubleshooting
Situation	Action or behavior
Permission error	Select a writable catalog/schema and check table privileges
Undefined variable	Run all cells in order; later cells depend on earlier definitions
Failed master assertion	Correct the source master records before continuing
Failed final validation	Inspect etl_test_results and compare the actual values with the expected fixture
Same input is rerun	Business counts and totals remain unchanged; ingestion timestamps change
A later stage fails	Earlier table writes may remain committed; the notebook does not roll back the entire pipeline


Limitations and future improvements
This is a full-refresh batch learning project, not a production banking ledger. Each table write is separate; there is no transaction covering the whole notebook. Bronze is overwritten, so durable ingestion/audit history is not implemented.

Future improvements include external ingestion, incremental loads, rejected-record replay, effective-dated account status, batch audit IDs, scheduling, monitoring, access controls, and more boundary/recovery tests. Running balances, overdraft decisions, reversals, and multiple currencies are outside the current scope.

For larger datasets, review repeated count actions, avoid collecting large tables into Python memory, and evaluate join/shuffle behavior. The present notebook collects only small account-summary results for validation.

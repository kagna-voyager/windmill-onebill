# Windmill OneBill Sample Data

This repository is used to store sample CSV files that Windmill jobs can read directly from GitHub.

## Objective

- Keep a stable set of sample OneBill-like datasets in version control.
- Let Windmill workflows use the files for development, testing, and demos.

## Data location

Sample files are stored in the `Sample Data/` folder:

- `1B_Contact_Details.csv`
- `1B_Invoice_Detail.csv`
- `1B_Partner.csv`
- `1B_Product_List.csv`
- `1B_Product_PricePlan.csv`
- `1B_Subscriber.csv`
- `1B_Subscription.csv`
- `1B_Tax_Details.csv`

## Using files from Windmill

Use GitHub raw URLs in Windmill scripts:

```text
https://raw.githubusercontent.com/kagna-voyager/windmill-onebill/main/Sample%20Data/<file-name>.csv
```

Example:

```text
https://raw.githubusercontent.com/kagna-voyager/windmill-onebill/main/Sample%20Data/1B_Subscription.csv
```

## Notes

- `Extracts/` and `Data Source/` are local working folders and are excluded from git.
- Keep sample files small and non-sensitive.

## Windmill script example

Use `windmill_load_sample_csv.py` as a Windmill Python script.

What it does:

- Downloads one CSV from this repo's `Sample Data/` folder using GitHub Raw.
- Parses rows with `csv.DictReader`.
- Returns `url`, `row_count`, `columns`, and `rows`.

Input parameters:

- `file_name` (default: `1B_Subscription.csv`)
- `limit` (default: `100`)

Example return shape:

```json
{
	"url": "https://raw.githubusercontent.com/kagna-voyager/windmill-onebill/main/Sample%20Data/1B_Subscription.csv",
	"row_count": 100,
	"columns": ["..."],
	"rows": [{"...": "..."}]
}
```

## Windmill flow: read, preview, write to database

Create three Python scripts in Windmill using files from this repository.

Step 1 script:

- Source file: `windmill_step1_load_csvs.py`
- Function: `main`
- Purpose: read CSV files from GitHub raw URLs.

Recommended inputs:

- `file_names`: array, for example:
	- `1B_Subscription.csv`
	- `1B_Invoice_Detail.csv`
- `row_limit`: `1000`

Step 2 script:

- Source file: `windmill_step2_prepare_preview.py`
- Function: `main`
- Purpose: clean rows and return preview output for display in job results.

Recommended inputs:

- `load_result`: output from Step 1
- `preview_rows`: `5`

Step 3 script:

- Source file: `windmill_step3_write_database.py`
- Function: `main`
- Purpose: write prepared datasets to a SQL database.

Recommended inputs:

- `prepared_result`: output from Step 2
- `database_url`: secure variable in Windmill
- `table_prefix`: `sample_`
- `write_mode`: `append` or `replace`
- `batch_size`: `500`

Example database URLs:

- PostgreSQL: `postgresql+psycopg://user:pass@host:5432/dbname`
- MySQL: `mysql+mysqlconnector://user:pass@host:3306/dbname`
- SQL Server: `mssql+pyodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server`
- SQLite: `sqlite:///tmp/windmill_sample.db`

Flow wiring in Windmill:

1. Create flow.
2. Add Step 1 and set file list.
3. Add Step 2 with `load_result` mapped from Step 1 output.
4. Add Step 3 with `prepared_result` mapped from Step 2 output.
5. Store `database_url` as a Windmill secret and map it into Step 3.

Expected result:

- Step 2 displays cleaned preview rows in the job output.
- Step 3 creates one table per CSV and inserts the data.

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

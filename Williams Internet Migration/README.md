# OneBill Migration Module

One notebook per function, sharing config/auth/HTTP helpers via
`onebill_common.py` and handing data off between steps through a
`migration_data/` folder of CSVs (created automatically on first run).

## Files

| File | Purpose |
|---|---|
| `onebill_common.py` | Shared config, OAuth token managers (OneBill + Dataverse), HTTP session, address-parsing, account-routing, and all the reusable "verb" functions (`create_onebill_account`, `add_address_to_account`, `create_onebill_order`, `fetch_all_products`, `fetch_product_detail`, `get_contacts`, ...). **Edit this, not the notebooks**, when a shared constant or endpoint needs to change. |
| `00_Environment_Check.ipynb` | Smoke-tests `.env`, OneBill auth, Dataverse auth, and MySQL connectivity. Run first whenever you point at a new environment. |
| `01_Fetch_Contacts.ipynb` | Pulls Dataverse contacts. Saves both the full per-contact rows (for 04's multi-contact account payloads) and a one-row-per-account primary-contact summary (for 07's optional order enrichment). |
| `02_Fetch_Products.ipynb` | Pulls the OneBill product catalog + every product's price-plan detail. |
| `03_Match_Plan_Codes.ipynb` | Builds a template of every distinct vBill plan code in use, with a reference copy of the OneBill catalog, for a manual product/price-plan mapping — **there's no reliable automatic match here**, see the notebook's intro cell. Validates your edits against the real catalog before finalizing. |
| `04_Create_Accounts.ipynb` | Mirrors the original bulk per-customer account-creation notebook — pulls every vBill account from MySQL, cleans names, classifies type, attaches contacts, creates idempotently in parallel. The two Williams "bucket" accounts are two manually-defined rows (`MANUAL_BUCKET_ACCOUNTS`) that go through the same pipeline — add more there any time (e.g. a second "Managed by Williams" account). |
| `05_Fetch_Subscriptions.ipynb` | Pulls every subscription, routes each to one of the two accounts based on `Reference`, and parses an address out of the subscription label. Pure data prep — no API writes. |
| `06_Create_Addresses.ipynb` | POSTs each subscription's parsed address onto its target account, records the returned `ship_add_id`. |
| `07_Create_Subscription_Orders.ipynb` | Resolves each subscription's plan (step 03's mapping, or a static fallback), optionally attaches a contact summary (step 01), builds the order payload per the field mapping, and creates the order. |
| `08_Migration_Results.ipynb` | Consolidated results/failures across every step above — read-only, safe to re-run any time. |

## Run order

```
00 -> 01 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07 -> 08
```

01 and 02/03 don't depend on each other and can run in either order, but
everything before 07 must have run at least once before 07, and 07 must
run before 08 has anything to report on. Every notebook after 00 reads its
inputs from `migration_data/*.csv` rather than from in-memory notebook
state, so **you can close and reopen notebooks between steps**, or re-run
just one step, without re-running everything before it.

## `migration_data/` — the hand-off folder

| File | Written by | Read by |
|---|---|---|
| `01_contacts_raw.csv`, `01_contacts_by_account.csv` | 01 | 04, 07 (optional enrichment) |
| `02_available_priceplans.csv`, `02_unavailable_products.csv`, `02_failed_product_lookups.csv` | 02 | 03 |
| `03_plan_code_mapping_template.csv`, `03_onebill_catalog_reference.csv` -> (you edit the template by hand) -> `03_plan_code_mapping.csv` | 03 | 07 |
| `04_account_creation_results.csv` | 04 | 08 |
| `05_subscriptions_resolved.csv` | 05 | 06, 07, 08 |
| `06_address_creation_results.csv` | 06 | 07, 08 |
| `07_order_creation_results.csv` | 07 | 08 |

## Required `.env` keys

Same as before, plus two new optional ones:

```
CRM_TENANT_ID=...
CRM_CLIENT_ID=...
CRM_CLIENT_SECRET=...
CRM_ENVIRONMENT_URL=...

DB_USERNAME=...
DB_PASSWORD=...
DB_HOST=...

CLIENT_ID=...
CLIENT_SECRET=...
API_USERNAME=...
API_PASSWORD=...

CREATION_PROXY_ACCOUNT_NUMBER=...

# New — set once you know the real OneBill account numbers for the two
# target accounts (or edit onebill_common.TARGET_ACCOUNTS directly):
MANAGED_BY_WILLIAMS_ACCOUNT_NUMBER=...
WILLIAMS_CORPORATION_ACCOUNT_NUMBER=...

# Optional — appended to every bulk-migrated AccountCode when 04_Create_Accounts.ipynb
# builds the OneBill accountNumber. Useful for test runs so you don't clash
# with real account numbers. Leave unset for a real/final run. NOT applied
# to the two Williams bucket accounts.
ACCOUNT_NUMBER_SUFFIX=...
```

## Things flagged as assumptions — check before a production run

Called out inline (search for `TODO`) but the important ones:

1. **Target account numbers** — `TARGET_ACCOUNTS` in `onebill_common.py`
   (placeholders / env vars) is only used as a *fallback*. The real source
   of truth is `04_Create_Accounts.ipynb`'s actual results:
   `05_Fetch_Subscriptions.ipynb` calls `load_real_target_account_numbers()`,
   which reads `04_account_creation_results.csv` and uses whatever
   `AccountCode_Batch` each bucket account (`AccountKey` column) actually
   got created with. Run `04_Create_Accounts.ipynb` first — if it hasn't
   run, or a bucket account failed, 05 logs a warning and falls back to
   the `TARGET_ACCOUNTS` placeholder for that key.
2. **`Reference` column name** — `SUBSCRIPTION_REFERENCE_COLUMN` in
   `onebill_common.py` is set to `"CustomerSuppliedReference"` (confirmed
   against the real subscriptions table — update it if that changes).
3. **`PlanCode` column name** — same idea, `SUBSCRIPTION_PLANCODE_COLUMN`.
   Plan mapping itself is **manual by design** (see `03_Match_Plan_Codes.ipynb`'s
   intro cell) — there's no reliable way to auto-match a vBill plan code to
   a OneBill product/price-plan unless the two catalogs share codes. If
   your subscriptions table has a plan name/description column, add it to
   `SUBSCRIPTION_PLAN_CONTEXT_COLUMNS` in `onebill_common.py` so it shows
   up in the mapping template as a hint.
4. **Address parsing** (`parse_address_from_label`) — handles
   `"28roadname@..."` → `"28 roadname road"` and `"5.28roadname@..."` →
   `"5/28 roadname road"`, always appending the literal word " road".
   Anything that doesn't match `[unit.]number+name` is left unparsed and
   flagged for manual review (`06_Create_Addresses.ipynb` marks these
   `"skipped"`). City/postcode aren't available per-subscription either,
   so those are currently hardcoded placeholders (`Christchurch` / `1234`)
   in `add_address_to_account`.
5. **"Subscription Username" / "Imported Subscription USN" field names** —
   sent as `orderElement.subscriptionUsername` /
   `orderElement.importedSubscriptionUsn` by default. Flip
   `USE_ATTRIBUTE_STYLE_FOR_USN_FIELDS = True` in `onebill_common.py` if
   OneBill actually wants these as `orderElementAttribute` entries instead.
6. **Contacts have no natural home** — every subscription lands on one of
   two *shared* accounts, so there's no per-customer account to attach a
   contact to the way the original per-customer migration did. Contacts
   are instead attached as an informational summary (name/email/phone) on
   each subscription's *order*, keyed by the subscription's original
   vBill `AccountCode`. Set `ATTACH_CONTACT_SUMMARY_TO_ORDER = False` in
   `onebill_common.py` to turn this off, or point it somewhere else if
   there's a better fit.
7. **`STATIC_FALLBACK_PLAN`** — the get-it-importing default plan used
   when a subscription's plan code has no match. Currently reuses the one
   plan seen in the reference notebook — swap for your team's real
   "needs review" placeholder.

None of these needed a live OneBill/MySQL/Dataverse connection to write,
so they haven't been run against real data — start with
`00_Environment_Check.ipynb`, then test with `TEST_ROW_LIMIT` (top of
`05_Fetch_Subscriptions.ipynb`) before a full run.

"""
onebill_common.py
==================
Shared configuration, auth, HTTP "verbs", and small pure-Python helpers used
by every notebook in the OneBill migration module:

    00_Environment_Check.ipynb        -> sanity-check all external connections
    01_Fetch_Contacts.ipynb           -> pull Dataverse contacts
    02_Fetch_Products.ipynb           -> pull OneBill product/price-plan catalog
    03_Match_Plan_Codes.ipynb         -> match vBill plan codes to OneBill plans
    04_Create_Accounts.ipynb          -> create the two target OneBill accounts
    05_Fetch_Subscriptions.ipynb      -> pull subscriptions, route + parse addresses
    06_Create_Addresses.ipynb         -> POST each subscription's address
    07_Create_Subscription_Orders.ipynb -> POST each subscription's order
    08_Migration_Results.ipynb        -> consolidated results/failures

Each notebook reads its inputs from, and writes its outputs to, the shared
`migration_data/` folder (see MIGRATION_FILES below) — that's the hand-off
mechanism between notebooks instead of notebook-to-notebook coupling.

Every notebook starts with:

    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path.cwd()))
    from onebill_common import *

--------------------------------------------------------------------------
ASSUMPTIONS / TODOs  (confirm against your Postman collection + DB schema
before a production run — each is flagged again at its point of use)
--------------------------------------------------------------------------
1. TARGET_ACCOUNTS holds the two destination OneBill account numbers —
   placeholders until you set the env vars / edit the constants below.
2. SUBSCRIPTION_REFERENCE_COLUMN / SUBSCRIPTION_PLANCODE_COLUMN — set these
   to whatever your MySQL query actually returns.
3. "Subscription Username" / "Imported Subscription USN" — sent as
   orderElement-level fields by default; flip
   USE_ATTRIBUTE_STYLE_FOR_USN_FIELDS if OneBill wants them as
   orderElementAttribute entries instead.
4. RECURRING_FROM_DATE is fixed at 2026-08-01 per spec.
5. Contacts (01_Fetch_Contacts.ipynb) are keyed by the *original* vBill
   AccountCode, not by the two bucket accounts — there's no natural
   per-customer contact slot once every subscription lands on one of two
   shared accounts. 07_Create_Subscription_Orders.ipynb optionally attaches
   a primary-contact summary onto each order as orderElementAttribute
   entries (see ATTACH_CONTACT_SUMMARY_TO_ORDER).
"""

import os
import re
import json
import time
import random
import logging
import threading
import urllib.parse
import pathlib
from datetime import datetime, timedelta
from collections import defaultdict

import requests
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Shared data folder — the hand-off mechanism between notebooks
# ---------------------------------------------------------------------------
MIGRATION_DATA_DIR = pathlib.Path("migration_data")
MIGRATION_DATA_DIR.mkdir(exist_ok=True)

MIGRATION_FILES = {
    "contacts":                MIGRATION_DATA_DIR / "01_contacts_by_account.csv",
    "contacts_raw":            MIGRATION_DATA_DIR / "01_contacts_raw.csv",
    "products_available":     MIGRATION_DATA_DIR / "02_available_priceplans.csv",
    "products_unavailable":   MIGRATION_DATA_DIR / "02_unavailable_products.csv",
    "products_failed":        MIGRATION_DATA_DIR / "02_failed_product_lookups.csv",
    "plan_mapping":             MIGRATION_DATA_DIR / "03_plan_code_mapping.csv",
    "plan_mapping_template":    MIGRATION_DATA_DIR / "03_plan_code_mapping_template.csv",
    "plan_mapping_reference":   MIGRATION_DATA_DIR / "03_onebill_catalog_reference.csv",
    "account_results":         MIGRATION_DATA_DIR / "04_account_creation_results.csv",
    "subscriptions_resolved":  MIGRATION_DATA_DIR / "05_subscriptions_resolved.csv",
    "address_results":         MIGRATION_DATA_DIR / "06_address_creation_results.csv",
    "order_results":           MIGRATION_DATA_DIR / "07_order_creation_results.csv",
}


def save_df(key: str, df: pd.DataFrame) -> None:
    path = MIGRATION_FILES[key]
    df.to_csv(path, index=False)
    print(f"Saved {len(df):,} rows -> {path}")


def load_df(key: str, dtype=None) -> pd.DataFrame:
    path = MIGRATION_FILES[key]
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run the notebook that produces it first "
            f"(see the table in README.md)."
        )
    return pd.read_csv(path, dtype=dtype)


def try_load_df(key: str, dtype=None) -> pd.DataFrame | None:
    path = MIGRATION_FILES[key]
    return pd.read_csv(path, dtype=dtype) if path.exists() else None


# Columns in subscriptions_resolved.csv that look numeric but MUST stay strings —
# a bare pd.read_csv would otherwise infer float64 for the decimal-looking
# TargetAccountNumber/AccountCode_Batch (silently reformatting/corrupting it) and
# would strip leading zeros from postcode-like values (e.g. "0001" -> 1). Extra
# keys for columns that aren't present in a given run are silently ignored by
# pd.read_csv, so it's safe to list all of these unconditionally.
SUBSCRIPTIONS_RESOLVED_DTYPES = {
    "SubscriptionUSN":           str,
    "AccountCode":               str,
    "TargetAccountKey":          str,
    "TargetAccountNumber":       str,
    "SupplierServiceID":         str,
    "CustomerSuppliedReference": str,
    "ParsedAddress_location_id": str,
    "ParsedAddress_radius_user": str,
    "ParsedAddress_postcode":    str,
    "ParsedAddress_region_iso":  str,
    "ParsedAddress_region_code_raw": str,
}


def load_subscriptions_resolved() -> pd.DataFrame:
    """load_df("subscriptions_resolved") with ID/code-like columns pinned to str
    dtype (see SUBSCRIPTIONS_RESOLVED_DTYPES) — use this instead of a bare
    load_df("subscriptions_resolved") in 06_Create_Addresses.ipynb and
    07_Create_Subscription_Orders.ipynb."""
    return load_df("subscriptions_resolved", dtype=SUBSCRIPTIONS_RESOLVED_DTYPES)


# ---------------------------------------------------------------------------
# Shared random run suffix
#
# A plain `random.randint(...)` at import time would give every notebook a
# DIFFERENT number, since each notebook has its own kernel and re-imports
# this module fresh. Caching the value to a file in migration_data/ makes
# it genuinely shared: whichever notebook runs first generates it, every
# notebook after that (in this run, in any kernel) reads the same value.
#
# Delete migration_data/run_suffix.txt to force a new number on the next
# import (e.g. starting a fresh test run).
# ---------------------------------------------------------------------------
RUN_SUFFIX_FILE = MIGRATION_DATA_DIR / "run_suffix.txt"


def get_run_suffix() -> str:
    """Random 4-digit string (zero-padded), generated once and cached to
    migration_data/run_suffix.txt so every notebook in this run sees the
    same value."""
    if RUN_SUFFIX_FILE.exists():
        return RUN_SUFFIX_FILE.read_text().strip()
    suffix = f"{random.randint(0, 9999):04d}"
    RUN_SUFFIX_FILE.write_text(suffix)
    return suffix


RUN_SUFFIX = get_run_suffix()  # e.g. "0417" — same value in every notebook until run_suffix.txt is deleted


# ---------------------------------------------------------------------------
# Dataverse (Dynamics CRM) — contacts
# ---------------------------------------------------------------------------
CRM_TENANT_ID       = os.environ.get("CRM_TENANT_ID")
CRM_CLIENT_ID       = os.environ.get("CRM_CLIENT_ID")
CRM_CLIENT_SECRET   = os.environ.get("CRM_CLIENT_SECRET")
CRM_ENVIRONMENT_URL = os.environ.get("CRM_ENVIRONMENT_URL")

# {extra_condition} is filled in per-page with a `contactid gt <last_seen>` clause
FETCHXML_TEMPLATE = """
<fetch version="1.0" output-format="xml-platform" mapping="logical" no-lock="false" count="5000">
  <entity name="contact">
    <attribute name="fullname"/>
    <attribute name="emailaddress1"/>
    <attribute name="telephone1"/>
    <attribute name="contactid"/>
    <attribute name="vgr_contacttypes"/>
    <attribute name="mobilephone"/>
    <attribute name="firstname"/>
    <attribute name="lastname"/>
    <attribute name="vgr_contactcode"/>
    <order attribute="contactid" descending="false"/>
    <filter type="and">
        <condition attribute="parentcustomerid" operator="ne" value="7378af87-be17-eb11-a813-000d3a7940d5" uiname="Portal Default Account" uitype="account"/>
        {extra_condition}
    </filter>
    <link-entity name="account" from="accountid" to="parentcustomerid" link-type="inner" alias="AccountCode">
      <attribute name="accountnumber"/>
      <filter type="and">
        <condition attribute="vgr_datasource" operator="eq" value="vBill"/>
      </filter>
    </link-entity>
  </entity>
</fetch>
""".strip()

LINKED_ACCOUNTNUMBER_COL = "AccountCode.accountnumber"

CONTACT_TYPE_MAP = {
    "287790000": "Billing",
    "287790001": "Technical",
    "287790002": "Outage - Email",
    "287790009": "Outage - SMS",
    "287790003": "Primary",
    "287790004": "Technical - Data",
    "287790005": "Technical - Voice",
    "287790006": "Commercial",
    "287790008": "Communication",
    "287790007": "Voyager Staff",
}


def get_dataverse_token() -> str:
    from msal import ConfidentialClientApplication
    app = ConfidentialClientApplication(
        client_id=CRM_CLIENT_ID,
        client_credential=CRM_CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{CRM_TENANT_ID}",
    )
    result = app.acquire_token_for_client(scopes=[f"{CRM_ENVIRONMENT_URL}/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Token acquisition failed: {result.get('error_description')}")
    return result["access_token"]


def get_contacts(token: str, max_pages: int = 50) -> pd.DataFrame:
    """Fetch all Dataverse contacts using keyset pagination on contactid."""
    headers = {
        "Authorization":    f"Bearer {token}",
        "OData-MaxVersion": "4.0",
        "OData-Version":    "4.0",
        "Accept":           "application/json",
        "Prefer":           "odata.maxpagesize=5000",
    }

    all_records: list[dict] = []
    last_contactid: str | None = None
    page = 1

    while True:
        extra_condition = (
            "" if last_contactid is None
            else f'<condition attribute="contactid" operator="gt" value="{last_contactid}"/>'
        )
        fetch = FETCHXML_TEMPLATE.format(extra_condition=extra_condition)
        url = f"{CRM_ENVIRONMENT_URL}/api/data/v9.2/contacts?fetchXml={urllib.parse.quote(fetch)}"

        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        records = response.json().get("value", [])

        if not records:
            break

        all_records.extend(records)
        new_last = records[-1]["contactid"]
        print(f"Contacts page {page}: fetched {len(records):,} (total so far: {len(all_records):,})")

        if len(records) < 5000 or new_last == last_contactid:
            break

        last_contactid = new_last
        page += 1
        if page > max_pages:
            print(f"Hit max_pages safety limit ({max_pages})")
            break

    return pd.DataFrame(all_records)


def parse_contact_types(raw) -> list[str]:
    """Parse a vgr_contacttypes value into an ordered list of OneBill labels."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    codes = [c.strip() for c in str(raw).split(",") if c.strip()]
    return [CONTACT_TYPE_MAP[c] for c in codes if c in CONTACT_TYPE_MAP]


def index_contacts_by_account(df: pd.DataFrame) -> dict[str, list[dict]]:
    """Group contact rows by AccountCode; BILLING- contact placed first within each account."""
    by_account: dict[str, list[dict]] = defaultdict(list)
    for _, row in df.iterrows():
        acct = row.get("AccountCode")
        if pd.isna(acct) or acct in (None, ""):
            continue
        by_account[str(acct)].append(row.to_dict())
    for acct, contacts in by_account.items():
        contacts.sort(key=lambda c: not bool(c.get("BillingContact", False)))
    return dict(by_account)


# ---------------------------------------------------------------------------
# Account payload builder (SubscriberService) — used by 04_Create_Accounts.ipynb
#
# Mirrors the original per-customer OneBill_Customer_Migration.ipynb: every
# Dataverse contact on an account becomes a full contact block (name, type,
# billing/primary flags, communication points, Dynamics Contact Types
# attribute); accounts with no contacts get one safe placeholder contact.
# ---------------------------------------------------------------------------
def build_communication_points(contact: dict) -> list[dict]:
    """Emit Email + Phone communication points, skipping blanks. Mobile preferred over work phone."""
    points: list[dict] = []

    email = clean(contact.get("EmailAddresses"))
    if email is not None:
        points.append({"type": "EMAIL", "value": str(email)})

    phone = clean(contact.get("PhoneMobile")) or clean(contact.get("PhoneWork"))
    if phone is not None:
        points.append({"type": "Phone", "value": str(phone)})

    return points


def build_contact_block(contact: dict) -> dict:
    """Build a single OneBill contact entry from a Dynamics contact row (see 01_Fetch_Contacts.ipynb)."""
    block = {
        "firstName":          contact["FirstName"],
        "lastName":           contact["LastName"],
        "contactType":        contact.get("OneBill_ContactType"),
        "primaryContact":     contact.get("BillingContact", False),
        "billingContact":     contact.get("BillingContact", False),
        "communicationPoint": build_communication_points(contact),
    }

    types = parse_contact_types(contact.get("Dynamics_ContactTypes"))
    if types:
        block["contactAttributes"] = [
            {
                "key":                   "Dynamics Contact Types",
                "value":                 types[0],
                "multipleEntriesConfig": "ENABLED",
                "attributeValuesInfo": {
                    "associateValues": [
                        {"value": t, "sequence": i + 1} for i, t in enumerate(types)
                    ],
                },
            },
            {"key": "Contact Code", "value": contact.get("ContactCode")},
        ]

    return block


def build_placeholder_contact() -> dict:
    """A single safe contact used when an account has zero Dynamics contacts."""
    return {
        "firstName": DEFAULT_FIRST_NAME,
        "lastName":  DEFAULT_LAST_NAME,
        "communicationPoint": [
            {"type": "EMAIL", "value": DEFAULT_EMAIL},
        ],
    }


def _serialize_date(value, fmt: str | None = None):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.strftime(fmt) if fmt else value.isoformat()
    if fmt:
        try:
            return datetime.strptime(str(value), "%Y-%m-%d").strftime(fmt)
        except ValueError:
            return str(value)
    return str(value)


def build_account_payload(row: dict, contacts_by_account: dict[str, list[dict]]) -> str:
    """Build the OneBill account-creation payload for one account row.

    `row` is expected to have (at minimum): AccountName_Cleaned, AccountCode,
    AccountCode_Batch, OneBill_AccountType, Address1, City, Postcode,
    CreatedDate. Optional: Address2, AccountType, DateOfBirth,
    AccountName_Original, AccountName_Unique (used for accountingDisplayName —
    falls back to AccountName_Cleaned if missing). Works the same whether `row`
    came from the bulk MySQL query or was hand-built (e.g. the two Williams
    bucket accounts in 04_Create_Accounts.ipynb) — as long as it has these fields.
    """
    row = {k: clean(v) if not isinstance(v, (list, dict)) else v for k, v in dict(row).items()}

    extra_contacts = contacts_by_account.get(str(row.get("AccountCode")), [])
    contact_list = [build_contact_block(c) for c in extra_contacts]
    if not contact_list:
        contact_list = [build_placeholder_contact()]

    address_block = {
        "addLine1":        row.get("Address1"),
        "addLine2":        row.get("Address2"),
        "county":          "",
        "city":            row.get("City"),
        "state":           None,
        "country":         "New Zealand",
        "zip":             str(row["Postcode"]) if row.get("Postcode") is not None else None,
        "defaultShipping": True,
        "defaultBilling":  True,
    }

    created_date = row.get("CreatedDate")
    activation_start_date = (
        created_date.strftime("%Y-%m-%d") if hasattr(created_date, "strftime")
        else (str(created_date) if created_date else datetime.now().strftime("%Y-%m-%d"))
    )

    payload = {
        "accountType":           row.get("OneBill_AccountType", "1002"),
        "accountName":           row.get("AccountName_Cleaned"),
        "accountNumber":         row.get("AccountCode_Batch"),
        "accountingDisplayName": row.get("AccountName_Unique") or row.get("AccountName_Cleaned"),
        "activationStartDate":   activation_start_date,
        "address":               [address_block],
        "contact":               contact_list,
        "accountAttribute": [
            {"key": "vBill Account Types",  "value": row.get("AccountType")},
            {"key": "Date Of Birth",        "value": _serialize_date(row.get("DateOfBirth"), "%d/%m/%Y")},
            {"key": "vBill Account Name",   "value": row.get("AccountName_Original")},
        ],
    }

    return json.dumps(payload)


# ---------------------------------------------------------------------------
# OneBill endpoints
# ---------------------------------------------------------------------------
ONEBILL_BASE_URL = os.environ.get("ONEBILL_BASE_URL", "https://sandbox-sg.onebillsoftware.com")

ONEBILL_TOKEN_URL          = f"{ONEBILL_BASE_URL}/oauth/token"
ONEBILL_ACCOUNT_URL        = f"{ONEBILL_BASE_URL}/rest/SubscriberService/v1/subscriber"          # POST -> create account
ONEBILL_SUBSCRIBER_URL     = f"{ONEBILL_BASE_URL}/rest/SubscriberService/v1/subscribers"          # + /{accountcode}  (GET detail, PUT -> add address)
ONEBILL_ORDER_URL          = f"{ONEBILL_BASE_URL}/rest/OrderService/v1/order"                     # POST -> create subscription/order
ONEBILL_PRODUCT_LIST_URL   = f"{ONEBILL_BASE_URL}/rest/ProductService/v1/products"                # GET  -> list of products
ONEBILL_PRODUCT_DETAIL_URL = f"{ONEBILL_BASE_URL}/rest/ProductService/v1/products"                # + /{code} (GET -> single product incl. pricePlanInfos)

ONEBILL_PROXY_ACCT = os.environ.get("CREATION_PROXY_ACCOUNT_NUMBER", "")

TOKEN_TTL_FALLBACK = 3500  # seconds, used only if OAuth response omits expires_in
MAX_WORKERS         = 10

# ---------------------------------------------------------------------------
# Voyager address lookup (circuits -> address-search) — used by
# 05_Fetch_Subscriptions.ipynb to resolve each subscription's real address
# and radius username, replacing the old label-parsing heuristic.
# ---------------------------------------------------------------------------
VOYAGER_CCP_KEY     = os.environ.get("VOYAGER_CCP_KEY")
VOYAGER_PARTNER_ID  = os.environ.get("VOYAGER_PARTNER_ID")

VOYAGER_CIRCUITS_URL       = "https://api.voyager.nz/fibre/v1/circuits"            # + /{supplierServiceId}
VOYAGER_ADDRESS_SEARCH_URL = "https://api.voyager.nz/address-search/v3/addresses/id"  # + /{locationId}

# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------
BI_DATASTORE_URL = (
    f"mysql+mysqlconnector://{os.environ.get('DB_USERNAME')}:{os.environ.get('DB_PASSWORD')}"
    f"@{os.environ.get('DB_HOST')}/bi_datastore"
) if os.environ.get("DB_USERNAME") else None

# TODO(assumption #2): confirm these column names against the real query result.
SUBSCRIPTION_REFERENCE_COLUMN = "CustomerSuppliedReference"
SUBSCRIPTION_PLANCODE_COLUMN  = "PlanCode"

# Optional: extra columns from reporting_subscription that help a human
# recognize what a vBill PlanCode actually is when building the manual
# mapping (e.g. a plan name or description column, if one exists). Leave
# empty if there's nothing beyond the bare code.
SUBSCRIPTION_PLAN_CONTEXT_COLUMNS: list[str] = []

# ---------------------------------------------------------------------------
# NZ region name -> 3-character ISO code (from NZ_Regions.xlsx: "Code","Region")
#
# The Voyager address-search response's region_name comes back like
# "CANTERBURY REGION" — the trailing "REGION" word is stripped before
# matching against the spreadsheet's "Canterbury" so the lookup actually hits.
# ---------------------------------------------------------------------------
NZ_REGIONS_FILE = pathlib.Path("NZ_Regions.xlsx")


def _load_region_iso_map() -> dict[str, str]:
    if not NZ_REGIONS_FILE.exists():
        return {}
    df = pd.read_excel(NZ_REGIONS_FILE)
    return {
        str(row["Region"]).strip().upper(): str(row["Code"]).strip()
        for _, row in df.iterrows()
        if pd.notna(row.get("Region")) and pd.notna(row.get("Code"))
    }


REGION_ISO_MAP = _load_region_iso_map()  # {} if NZ_Regions.xlsx isn't next to the notebooks

if not REGION_ISO_MAP:
    print(
        f"WARNING: NZ_Regions.xlsx not found at {NZ_REGIONS_FILE.resolve()} (or it loaded empty) — "
        f"region_name_to_iso() will return None for everything, so every address's region/state will "
        f"fall back to the raw region_code from Voyager (see get_voyager_address)."
    )


def _normalize_region_name(region_name) -> str:
    """'CANTERBURY REGION' -> 'CANTERBURY'. Drops the word REGION (any case,
    anywhere in the string) and collapses whitespace, so the API's region_name
    matches the spreadsheet's plain region names."""
    if region_name is None or (isinstance(region_name, float) and pd.isna(region_name)):
        return ""
    name = re.sub(r"\bREGION\b", "", str(region_name), flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", name).strip().upper()


def region_name_to_iso(region_name) -> str | None:
    """Map a Voyager region_name (e.g. 'CANTERBURY REGION') to its 3-char ISO code
    (e.g. 'CAN') via NZ_Regions.xlsx. Returns None if there's no match."""
    key = _normalize_region_name(region_name)
    if not key:
        return None
    return REGION_ISO_MAP.get(key)

# ---------------------------------------------------------------------------
# Target accounts (TODO(assumption #1): replace placeholders with real
# OneBill account numbers once known / once 04_Create_Accounts.ipynb has run)
# ---------------------------------------------------------------------------
TARGET_ACCOUNTS = {
    "managed_by_williams": {
        "account_number": os.environ.get("MANAGED_BY_WILLIAMS_ACCOUNT_NUMBER", "MANAGED-BY-WILLIAMS"),
        "account_name":   "Managed by Williams",
    },
    "williams_corporation": {
        "account_number": os.environ.get("WILLIAMS_CORPORATION_ACCOUNT_NUMBER", "WILLIAMS-CORPORATION"),
        "account_name":   "Williams Corporation",
    },
}

# Marker matched case-insensitively against the subscription's Reference
# field. Anything that does NOT match goes to "williams_corporation".
MANAGED_BY_WILLIAMS_MARKER = "managed by williams"

# Optional suffix appended to every bulk-migrated AccountCode when building
# the OneBill accountNumber (AccountCode_Batch) — handy for test runs so you
# don't clash with real account numbers. Leave "" for a real/final run.
# NOT applied to the manually-defined bucket accounts in 04_Create_Accounts.ipynb.
ACCOUNT_NUMBER_SUFFIX = os.environ.get("ACCOUNT_NUMBER_SUFFIX", "")

# ---------------------------------------------------------------------------
# Field-mapping constants
# ---------------------------------------------------------------------------
RECURRING_FROM_DATE = "2026-08-01T00:00:00"   # static, per spec — not "beginning of this month"
DEFAULT_ORDER_STATE = "1005"
DEFAULT_ACTION_TYPE  = "New"
DEFAULT_QUANTITY     = 1

USE_ATTRIBUTE_STYLE_FOR_USN_FIELDS = True        # confirmed against working Postman payload —
                                                   # OneBill wants these as orderElementAttribute
                                                   # entries ("Imported Subscription USN" /
                                                   # "Subscription Username"), not as top-level
                                                   # orderElement fields (importedSubscriptionUsn /
                                                   # subscriptionUsername), which it silently ignores
ATTACH_CONTACT_SUMMARY_TO_ORDER    = True        # see assumption #5 above

# Used when a subscription's plan code has no match in the product/price-plan
# lookup table (03_Match_Plan_Codes.ipynb output) — just enough to get the
# record importing; MUST be reviewed/corrected later.
STATIC_FALLBACK_PLAN = {
    "productName":   "Wholesale Fibre BS2 (Chorus)",              # TODO: pick a real always-valid product
    "priceplanName": "WS Tail+Data - BS2 Res (Chorus) - 100/20",  # TODO: pick a real always-valid price plan
}

# ---------------------------------------------------------------------------
# Placeholder contact defaults (used for the two "bucket" accounts, and for
# any subscription whose account has no real contact info)
# ---------------------------------------------------------------------------
DEFAULT_FIRST_NAME = "John"
DEFAULT_LAST_NAME  = "Doe"
DEFAULT_EMAIL      = "someone@example.com"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    log_filename = f'{name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh = logging.FileHandler(log_filename)
        fh.setFormatter(fmt)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger


# Module-level logger for onebill_common.py's own internal functions (e.g. the
# Voyager retry wrapper below). Notebooks create their own logger via
# get_logger(name) for their own log lines — this is separate and only used
# by code that lives inside this file, since a bare `logger` reference inside
# a function defined here resolves against THIS module's globals, not
# whatever `from onebill_common import *` happened to pull into the notebook.
logger = get_logger("onebill_common")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def clean(value):
    """Return None for blanks/NaN; pass everything else through unchanged."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def to_iso_midnight(value) -> str | None:
    """Format a date/datetime/str value as OneBill's 'YYYY-MM-DDT00:00:00'. None-safe."""
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    return ts.strftime("%Y-%m-%dT00:00:00")


# ---------------------------------------------------------------------------
# Supplier -> OneBill "Vendor" attribute mapping
#
# `Supplier` (from bi_datastore.billing_subscription) needs mapping onto
# OneBill's Vendor picklist for the "Vendor" orderElementAttribute. Only
# Chorus / Enable / UFF show up in MySQL for the accounts migrated so far,
# so only those are mapped below. UFF maps to OneBill's "tff" value — UFF
# was renamed to Tuatahi First Fibre and OneBill still calls it "tff".
#
# Full OneBill Vendor picklist, for reference when new suppliers turn up:
#   chorus (1067), enable (1066), tff (1065) [was UFF], litNetworks (1061),
#   networkTasman (1060), northpower (1068), chorusWireline (1064),
#   oneNz (1063), 2degrees (1062)
# ---------------------------------------------------------------------------
SUPPLIER_TO_VENDOR = {
    "chorus": "chorus",
    "enable": "enable",
    "uff":    "tff",
}


def resolve_vendor(supplier) -> str | None:
    """Map a MySQL `Supplier` value onto OneBill's Vendor picklist value.

    Returns None if `supplier` is blank or not a supplier we have a mapping
    for yet — callers should decide whether that's worth a warning.
    """
    supplier = clean(supplier)
    if supplier is None:
        return None
    return SUPPLIER_TO_VENDOR.get(str(supplier).strip().lower())


def months_between(start, end) -> int:
    """Whole calendar months between two dates (end - start), floored, never negative.

    e.g. 2026-07-21 -> 2027-01-15 is 5 whole months (not 6, since the 15th is
    before the 21st). Used for followOnTermDetails.term, which OneBill expects
    as a month count when termMode == 'M'.
    """
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    months = (end_ts.year - start_ts.year) * 12 + (end_ts.month - start_ts.month)
    if end_ts.day < start_ts.day:
        months -= 1
    return max(months, 0)


def check_validation(data: dict) -> tuple[bool, str | None]:
    """Inspect a OneBill response body's validationResponse block.

    Returns (successful, message). message is None when successful.
    """
    validation = data.get("validationResponse", {})
    if not validation.get("successful", True):
        errors = validation.get("validationErrorInfo", [])
        messages = "; ".join(e.get("message", "") for e in errors)
        return False, (messages or "validationResponse.successful = false")
    return True, None


def validation_error_codes(data: dict) -> list[str]:
    """All validationErrorInfo[].code values from a response body (may be empty)."""
    validation = data.get("validationResponse", {})
    return [e.get("code") for e in validation.get("validationErrorInfo", []) if e.get("code")]


def _raise_for_status_with_body(response: requests.Response) -> None:
    """Like response.raise_for_status(), but folds the response BODY into the exception
    message. requests' default HTTPError message is just the status line (e.g.
    "401 Client Error:  for url: ...") — the actual reason (an auth/permission message,
    or a validationResponse error) is in the body, which raise_for_status() discards.
    """
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = (response.text or "").strip()
        if len(body) > 1000:
            body = body[:1000] + "... (truncated)"
        raise requests.exceptions.HTTPError(f"{e} — response body: {body or '<empty>'}", response=response) from None


# ---------------------------------------------------------------------------
# OAuth token manager (thread-safe, proactive refresh)
# ---------------------------------------------------------------------------
class TokenManager:
    """Thread-safe bearer token cache with proactive refresh."""

    def __init__(self):
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: datetime = datetime.min

    def get_token(self) -> str:
        with self._lock:
            if datetime.now() >= self._expires_at:
                self._refresh()
            return self._token

    def force_refresh(self) -> str:
        """Get a brand-new token regardless of what the client-side expiry clock says.

        OneBill's sandbox can invalidate a token server-side before our own countdown
        runs out (e.g. logging in again elsewhere invalidates the older token) — a
        request then comes back 401 "invalid_token" even though get_token() thought
        the cached one was still good. Callers use this to recover from exactly that.
        """
        with self._lock:
            self._refresh()
            return self._token

    def _refresh(self) -> None:
        token_data = {
            "grant_type":    "password",
            "client_id":     os.environ["CLIENT_ID"],
            "client_secret": os.environ["CLIENT_SECRET"],
            "username":      os.environ["API_USERNAME"],
            "password":      os.environ["API_PASSWORD"],
        }
        response = requests.post(
            ONEBILL_TOKEN_URL,
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        ttl = payload.get("expires_in", TOKEN_TTL_FALLBACK)
        self._expires_at = datetime.now() + timedelta(seconds=ttl - 100)


token_manager = TokenManager()


def new_session(max_workers: int = MAX_WORKERS) -> requests.Session:
    """A requests.Session pre-configured with the proxy-account header + pool sizing."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount("https://", adapter)
    session.headers.update({
        "proxy_accountNumber": ONEBILL_PROXY_ACCT,
        "Content-Type":        "application/json",
    })
    return session


def auth_headers() -> dict:
    return {"Authorization": f"Bearer {token_manager.get_token()}"}


def onebill_request(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    """session.<post|put|get>(url, headers=auth_headers(), **kwargs), with ONE automatic
    retry using a force-refreshed token if the first attempt comes back 401.

    Why: TokenManager's own expiry clock can say a token is still good while OneBill
    has already invalidated it server-side — that shows up as a 401 with
    WWW-Authenticate: error="invalid_token", not as an expiry we could have predicted.
    Retrying once with a forced-fresh token covers that transparently so every
    OneBill-calling function doesn't need to reimplement this.
    """
    response = getattr(session, method)(url, headers=auth_headers(), **kwargs)
    if response.status_code == 401:
        token_manager.force_refresh()
        response = getattr(session, method)(url, headers=auth_headers(), **kwargs)
    return response


def new_voyager_session(max_workers: int = MAX_WORKERS) -> requests.Session:
    """A plain pooled requests.Session for Voyager calls (no OneBill-specific
    headers — those are set per-request in fetch_voyager_circuit/fetch_voyager_address)."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount("https://", adapter)
    return session


# ---------------------------------------------------------------------------
# Voyager rate limiting — every subscription does 2 sequential Voyager calls
# (circuits, then address-search), and the API returns 429s well before any
# reasonable worker count would suggest. Two mechanisms, both global (shared
# across all threads, not per-worker):
#   1. A minimum spacing between any two Voyager requests, so N workers don't
#      just recreate the same burst by firing in a tight loop.
#   2. Retry-with-backoff specifically for 429, honoring Retry-After if the
#      API sends one, otherwise exponential backoff with jitter.
# Tune VOYAGER_MIN_REQUEST_INTERVAL_SECONDS down if 429s stop appearing, or
# up if they persist even after this change.
# ---------------------------------------------------------------------------
VOYAGER_MIN_REQUEST_INTERVAL_SECONDS = 0.5
VOYAGER_MAX_RETRIES = 6
VOYAGER_BACKOFF_BASE_SECONDS = 1.0
VOYAGER_5XX_MAX_RETRIES = 3
VOYAGER_5XX_BACKOFF_BASE_SECONDS = 2.0
VOYAGER_RETRYABLE_5XX = {500, 502, 503, 504}

_voyager_throttle_lock = threading.Lock()
_voyager_last_request_at = 0.0


def _voyager_throttle():
    """Block the calling thread until at least VOYAGER_MIN_REQUEST_INTERVAL_SECONDS
    has elapsed since the last Voyager request from ANY thread."""
    global _voyager_last_request_at
    with _voyager_throttle_lock:
        now = time.monotonic()
        wait = VOYAGER_MIN_REQUEST_INTERVAL_SECONDS - (now - _voyager_last_request_at)
        if wait > 0:
            time.sleep(wait)
        _voyager_last_request_at = time.monotonic()


def _voyager_get_with_retry(session: requests.Session, url: str, headers: dict) -> requests.Response:
    """GET with global throttling + retry-with-backoff on 429 and transient 5xx.
    404s and other 4xx are NOT retried (retrying won't make a missing circuit
    exist) — those raise immediately via raise_for_status().

    429: honors Retry-After if Voyager sends one, otherwise exponential backoff,
    up to VOYAGER_MAX_RETRIES attempts (Voyager's own Retry-After values have
    been observed up to ~60s, so this can legitimately take a while).

    5xx (500/502/503/504): no Retry-After to honor here, so a shorter
    exponential backoff with fewer attempts (VOYAGER_5XX_MAX_RETRIES) — enough
    to ride out a transient blip without masking a persistently broken circuit
    behind minutes of retries.
    """
    last_exc = None
    attempt_429 = 0
    attempt_5xx = 0

    while True:
        _voyager_throttle()
        response = session.get(url, headers=headers, timeout=30)

        if response.status_code == 429:
            if attempt_429 >= VOYAGER_MAX_RETRIES:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = VOYAGER_BACKOFF_BASE_SECONDS * (2 ** attempt_429)
            else:
                delay = VOYAGER_BACKOFF_BASE_SECONDS * (2 ** attempt_429)
            delay += random.uniform(0, 0.5)  # jitter, so parallel workers don't retry in lockstep
            logger.warning(f"Voyager 429 on {url} — retry {attempt_429 + 1}/{VOYAGER_MAX_RETRIES} in {delay:.1f}s")
            attempt_429 += 1
            time.sleep(delay)
            continue

        if response.status_code in VOYAGER_RETRYABLE_5XX:
            if attempt_5xx >= VOYAGER_5XX_MAX_RETRIES:
                response.raise_for_status()
            delay = VOYAGER_5XX_BACKOFF_BASE_SECONDS * (2 ** attempt_5xx) + random.uniform(0, 0.5)
            logger.warning(
                f"Voyager {response.status_code} on {url} — retry {attempt_5xx + 1}/{VOYAGER_5XX_MAX_RETRIES} in {delay:.1f}s"
            )
            attempt_5xx += 1
            time.sleep(delay)
            continue

        response.raise_for_status()
        return response


# ---------------------------------------------------------------------------
# Address parsing from the subscription (vBill) label — SUPERSEDED
#
# Kept only as a fallback/reference. 05_Fetch_Subscriptions.ipynb now uses
# get_voyager_address() below (circuits -> address-search) to get a real,
# validated address instead of guessing one out of the label text.
#
# Observed patterns:
#   "28roadname@internet.com"      -> "28 roadname road"
#   "5.28roadname@internet.com"    -> "5/28 roadname road"   (unit 5, number 28)
#
# i.e. local-part = [unit "."] number name...  The literal word " road" is
# appended per the example payload — flip ADDRESS_SUFFIX to "" or something
# else if that turns out to be wrong for a given record.
# ---------------------------------------------------------------------------
ADDRESS_SUFFIX = " road"

_ADDRESS_LOCAL_PART_RE = re.compile(
    r"^(?:(?P<unit>\d+)\.)?(?P<number>\d+)(?P<name>[A-Za-z][A-Za-z0-9\- ]*)$"
)


def parse_address_from_label(label: str) -> dict:
    """Best-effort parse of a vBill subscription label into an address line 1.

    Returns a dict:
        {
            "addLine1": str | None,
            "unit": str | None,
            "number": str | None,
            "name": str | None,
            "parsed_ok": bool,
            "raw_local_part": str,
        }

    When parsing fails, addLine1 is None and parsed_ok is False — these rows
    need manual review.
    """
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return {"addLine1": None, "unit": None, "number": None, "name": None,
                "parsed_ok": False, "raw_local_part": ""}

    local_part = str(label).split("@", 1)[0].strip()
    match = _ADDRESS_LOCAL_PART_RE.match(local_part)

    if not match:
        return {"addLine1": None, "unit": None, "number": None, "name": None,
                "parsed_ok": False, "raw_local_part": local_part}

    unit   = match.group("unit")
    number = match.group("number")
    name   = match.group("name").strip()

    street = f"{unit}/{number}" if unit else number
    add_line1 = f"{street} {name}{ADDRESS_SUFFIX}".strip()

    return {
        "addLine1":       add_line1,
        "unit":           unit,
        "number":         number,
        "name":           name,
        "parsed_ok":      True,
        "raw_local_part": local_part,
    }


# ---------------------------------------------------------------------------
# Voyager address lookup — used by 05_Fetch_Subscriptions.ipynb
#
# Two-step lookup per subscription, keyed on SupplierServiceID:
#   1. GET {VOYAGER_CIRCUITS_URL}/{supplierServiceId}
#        headers: X-Api-Key, X-Partner-Id
#        -> {"serviceId", "vendor", "locationId", "radiusUsers": [...]}
#   2. GET {VOYAGER_ADDRESS_SEARCH_URL}/{locationId}
#        headers: Accept: application/json
#        -> full address record (street_address, locality_name, town_name,
#           postcode_zone, region_name, ...)
# ---------------------------------------------------------------------------
def fetch_voyager_circuit(session: requests.Session, supplier_service_id: str) -> dict:
    """GET the circuit detail for one SupplierServiceID."""
    url = f"{VOYAGER_CIRCUITS_URL}/{supplier_service_id}"
    headers = {"X-Api-Key": VOYAGER_CCP_KEY, "X-Partner-Id": VOYAGER_PARTNER_ID}
    response = _voyager_get_with_retry(session, url, headers)
    return response.json()


def fetch_voyager_address(session: requests.Session, location_id: str) -> dict:
    """GET the full address record for one locationId."""
    url = f"{VOYAGER_ADDRESS_SEARCH_URL}/{location_id}"
    response = _voyager_get_with_retry(session, url, {"accept": "application/json"})
    return response.json()


def _title(value) -> str | None:
    """Title-case a possibly-None/NaN/all-caps string; None-safe."""
    value = clean(value)
    return str(value).title() if value is not None else None


def get_voyager_address(session: requests.Session, supplier_service_id) -> dict:
    """Resolve one subscription's real address + radius username via Voyager.

    Returns a dict (always these keys, so downstream code doesn't need to
    guard for missing keys):
        {
            "addLine1":    str | None,   # e.g. "39 Chester Street West"
            "addLine2":    str | None,   # e.g. "Christchurch Central" (locality_name)
            "city":        str | None,   # e.g. "Christchurch" (town_name)
            "postcode":    str | None,   # e.g. "8013" (postcode_zone)
            "region_name": str | None,   # raw region_name from the API, e.g. "CANTERBURY REGION"
            "region_iso":  str | None,   # 3-char ISO: NZ_Regions.xlsx match on region_name,
                                          # falling back to the API's own region_code if that misses
            "region_code_raw": str | None,  # the API's region_code field, untouched — for auditing
            "radius_user": str | None,   # radiusUsers[0] from the circuits call
            "location_id": str | None,
            "parsed_ok":   bool,
            "error":       str | None,
        }
    """
    result = {
        "addLine1": None, "addLine2": None, "city": None, "postcode": None,
        "region_name": None, "region_iso": None, "region_code_raw": None,
        "radius_user": None, "location_id": None,
        "parsed_ok": False, "error": None,
    }

    if supplier_service_id is None or pd.isna(supplier_service_id) or not str(supplier_service_id).strip():
        result["error"] = "no SupplierServiceID on this subscription"
        return result

    supplier_service_id = str(supplier_service_id).strip()

    try:
        circuit = fetch_voyager_circuit(session, supplier_service_id)
    except Exception as e:
        result["error"] = f"circuits lookup failed: {e}"
        return result

    radius_users = circuit.get("radiusUsers") or []
    result["radius_user"] = radius_users[0] if radius_users else None

    location_id = circuit.get("locationId")
    result["location_id"] = location_id
    if not location_id:
        result["error"] = "circuit response had no locationId"
        return result

    try:
        address = fetch_voyager_address(session, location_id)
    except Exception as e:
        result["error"] = f"address-search lookup failed: {e}"
        return result

    result["addLine1"]         = _title(address.get("street_address"))
    result["addLine2"]         = _title(address.get("locality_name"))
    result["city"]             = _title(address.get("town_name"))
    result["postcode"]         = clean(address.get("postcode_zone"))
    result["region_name"]      = address.get("region_name")
    result["region_code_raw"]  = clean(address.get("region_code"))
    # Primary: match NZ_Regions.xlsx on the normalized region_name ("CANTERBURY REGION" -> "Canterbury" -> "CAN").
    # Fallback: if that doesn't hit (missing/blank REGION_ISO_MAP, or a region_name the spreadsheet doesn't
    # have), use the API's own region_code as-is — it's already a 3-char ISO code straight from Voyager.
    result["region_iso"] = region_name_to_iso(address.get("region_name")) or result["region_code_raw"]

    result["parsed_ok"] = result["addLine1"] is not None
    if not result["parsed_ok"]:
        result["error"] = "address-search response had no street_address"

    return result


# Matches a 5xx status code appearing anywhere in a ParsedAddress_error string,
# e.g. "circuits lookup failed: 500 Server Error: ..." or "... 503 ...".
# Deliberately does NOT match 404 ("Not Found" — permanent, not transient) or
# "no SupplierServiceID on this subscription" (a data problem, not an API one).
_VOYAGER_5XX_ERROR_PATTERN = re.compile(r"\b(500|502|503|504)\b")


def voyager_second_pass(
    df: pd.DataFrame,
    session: requests.Session,
    delay_seconds: float = 90.0,
    max_workers: int = 2,
) -> pd.DataFrame:
    """Retry Voyager lookups for rows that failed with a transient 5xx on the
    first pass, after waiting `delay_seconds`.

    Intended to run as a SEPARATE, later pass after the main Voyager lookup in
    05_Fetch_Subscriptions.ipynb. get_voyager_address()/_voyager_get_with_retry()
    already retry a 5xx up to VOYAGER_5XX_MAX_RETRIES times with a short backoff
    (a few seconds each) — but some backend errors are only transient over a
    longer horizon (minutes) than that. This is that longer, second attempt.

    404s and "no SupplierServiceID" failures are intentionally NOT retried
    here — those are permanent (a missing circuit or missing source data isn't
    going to appear because we waited longer).

    Mutates `df` in place (only overwrites ParsedAddress_* columns for rows
    that get retried; rows are otherwise untouched) and also returns it, so
    it drops straight into `df_subscriptions = voyager_second_pass(df_subscriptions, ...)`.
    """
    needs_retry = df[
        (~df["ParsedAddress_parsed_ok"])
        & (df["ParsedAddress_error"].fillna("").str.contains(_VOYAGER_5XX_ERROR_PATTERN))
    ]

    if needs_retry.empty:
        logger.info("Voyager second pass: nothing to retry (no transient-5xx failures from the first pass).")
        return df

    logger.info(
        f"Voyager second pass: {len(needs_retry):,} subscriptions failed with a transient 5xx on the "
        f"first pass — waiting {delay_seconds:.0f}s before retrying..."
    )
    time.sleep(delay_seconds)

    def _lookup(supplier_service_id):
        return get_voyager_address(session, supplier_service_id)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        retry_results = list(executor.map(_lookup, needs_retry["SupplierServiceID"]))

    retry_parts = pd.DataFrame(retry_results, index=needs_retry.index).add_prefix("ParsedAddress_")
    df.update(retry_parts)  # only touches matching (index, column) cells — everything else is untouched

    now_resolved = int(retry_parts["ParsedAddress_parsed_ok"].sum())
    logger.info(f"Voyager second pass: {now_resolved:,} / {len(needs_retry):,} resolved on retry.")

    still_unparsed = df[~df["ParsedAddress_parsed_ok"]]
    if not still_unparsed.empty:
        error_summary = (
            still_unparsed["ParsedAddress_error"].value_counts(dropna=False)
            .rename_axis("error").reset_index(name="count")
        )
        logger.info(f"{len(still_unparsed):,} subscriptions still unresolved after second pass:\n" + error_summary.to_string(index=False))

    return df


def resolve_target_account(reference) -> dict:
    """Given a subscription's Reference value, return the TARGET_ACCOUNTS entry it belongs to."""
    ref = "" if reference is None or (isinstance(reference, float) and pd.isna(reference)) else str(reference)
    if MANAGED_BY_WILLIAMS_MARKER in ref.lower():
        return TARGET_ACCOUNTS["managed_by_williams"]
    return TARGET_ACCOUNTS["williams_corporation"]


def load_real_target_account_numbers() -> dict[str, str]:
    """The AUTHORITATIVE account_number for each bucket account key, sourced from
    04_Create_Accounts.ipynb's actual results — not from TARGET_ACCOUNTS placeholders,
    which can drift from what's really in OneBill (unset env var, etc.).

    Returns {account_key: AccountCode_Batch}, e.g. {"managed_by_williams": "...", "williams_corporation": "..."}.
    Only includes keys whose account actually created successfully ("created" or "exists").

    Raises FileNotFoundError if 04_Create_Accounts.ipynb hasn't been run yet
    (via load_df — same contract as everywhere else in this module).
    """
    # dtype=str is important: AccountCode_Batch looks like a decimal (e.g. "99965692.1234"),
    # so a bare pd.read_csv silently parses it as float64 and can lose trailing zeros.
    df_results = load_df("account_results", dtype={"AccountCode": str, "AccountCode_Batch": str})
    bucket_rows = df_results[df_results["AccountKey"].notna() & df_results["status"].isin(["created", "exists"])]
    return dict(zip(bucket_rows["AccountKey"], bucket_rows["AccountCode_Batch"]))


def load_account_code_batch_map() -> dict[str, str]:
    """{AccountCode: AccountCode_Batch} for every account that actually exists in OneBill
    (status "created" or "exists"), sourced from 04_Create_Accounts.ipynb's results.

    This covers BOTH the bulk-migrated real vBill accounts (AccountKey is blank,
    AccountCode_Batch = AccountCode + "." + BATCH_NUMBER) and the two manual bucket
    rows (AccountKey set) — it's a straight AccountCode -> AccountCode_Batch map
    across every row in account_results.

    Used by 05_Fetch_Subscriptions.ipynb to route each subscription onto its OWN
    real account (the actual OneBill accountNumber) rather than one of the two
    shared Williams buckets. Raises FileNotFoundError if 04_Create_Accounts.ipynb
    hasn't been run yet (via load_df).
    """
    # dtype=str for the same reason as load_real_target_account_numbers() above —
    # AccountCode_Batch (e.g. "99965692.1234") must not be parsed as float64.
    df_results = load_df("account_results", dtype={"AccountCode": str, "AccountCode_Batch": str})
    ok = df_results[df_results["status"].isin(["created", "exists"])].copy()
    ok = ok.drop_duplicates(subset="AccountCode", keep="last")
    return dict(zip(ok["AccountCode"], ok["AccountCode_Batch"]))


def validate_plan_mapping(df_mapping: pd.DataFrame, df_catalog: pd.DataFrame) -> pd.DataFrame:
    """Cross-check hand-filled (product_name, priceplan_name) pairs against the real
    OneBill catalog fetched in 02_Fetch_Products.ipynb. Since 07_Create_Subscription_Orders.ipynb
    does an exact string match, a typo here silently produces a rejected order later —
    this catches that at mapping time instead.

    Returns df_mapping with an added "catalog_match" column: True / False / "blank".
    """
    valid_pairs = set(
        zip(df_catalog["product_name"].fillna(""), df_catalog["priceplan_name"].fillna(""))
    )

    def _norm_cell(value) -> str:
        """NaN-safe: pd.read_csv turns a blank cell into float NaN, not ''."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip()

    def _check(row):
        product = _norm_cell(row.get("product_name"))
        plan = _norm_cell(row.get("priceplan_name"))
        if not product and not plan:
            return "blank"
        return (product, plan) in valid_pairs

    df_mapping = df_mapping.copy()
    df_mapping["catalog_match"] = df_mapping.apply(_check, axis=1)
    return df_mapping


# ---------------------------------------------------------------------------
# Product / price-plan lookup (ProductService)
# ---------------------------------------------------------------------------
PRODUCT_NOT_AVAILABLE_CODE = "10PR1126"  # "Product is not available for the user."


def fetch_all_products(session: requests.Session) -> pd.DataFrame:
    """GET /rest/ProductService/v1/products -> DataFrame of every product's name/category/code/id/status."""
    response = onebill_request(session, "get", ONEBILL_PRODUCT_LIST_URL, timeout=30)
    _raise_for_status_with_body(response)
    data = response.json()
    products = data.get("product", [])
    return pd.DataFrame(products)


def fetch_product_detail(session: requests.Session, code: str) -> tuple[str, dict | None, str | None]:
    """GET /rest/ProductService/v1/products/{code}.

    Returns (status, data, message):
        status "available"     -> data is the product object itself (NOT wrapped
                                   in {"product": [...]} the way the list endpoint
                                   is) — includes pricePlanInfos directly.
        status "unavailable"   -> "Product is not available for the user." (or similar) — expected/normal, not an error
        status "failed"        -> any other error
    """
    url = f"{ONEBILL_PRODUCT_DETAIL_URL}/{code}"
    try:
        response = onebill_request(session, "get", url, timeout=30)
        _raise_for_status_with_body(response)
        data = response.json()
    except Exception as e:
        return "failed", None, str(e)

    ok, message = check_validation(data)
    if ok:
        return "available", data, None

    if PRODUCT_NOT_AVAILABLE_CODE in validation_error_codes(data) or "not available" in (message or "").lower():
        return "unavailable", data, message

    return "failed", data, message


# ---------------------------------------------------------------------------
# Account creation (SubscriberService) — used by 04_Create_Accounts.ipynb
# ---------------------------------------------------------------------------
ACCOUNT_ALREADY_EXISTS_MARKER = "already exist"  # matches both "...exists." and "...exist." (OneBill isn't consistent)
ACCOUNT_NAME_DUPLICATE_CODE   = "12CM1066"        # "Accounting Name [X] already exist." — a DIFFERENT account already has this name


def create_onebill_account(session: requests.Session, payload: str) -> tuple[str, dict | None, str | None]:
    """POST one account to OneBill. Returns (status, data, message).

    status: "created" | "exists" | "duplicate_name" | "failed".
        "exists"         -> NOT an error — this exact account number already exists.
        "duplicate_name" -> a genuine problem — a DIFFERENT account number already holds
                             this Accounting Name. OneBill enforces name uniqueness
                             account-wide, so any later PUT to add an address on this
                             account will also fail until the duplicate is resolved.
                             Almost always caused by BATCH_NUMBER changing between runs
                             (same underlying vBill customer, new account number each
                             time — see AccountCode_Batch above), so the account-number
                             dedupe check below never gets a chance to fire.
    """
    response = onebill_request(session, "post", ONEBILL_ACCOUNT_URL, data=payload, timeout=30)
    _raise_for_status_with_body(response)
    data = response.json()

    ok, message = check_validation(data)
    if ok:
        return "created", data, None
    if ACCOUNT_NAME_DUPLICATE_CODE in validation_error_codes(data):
        return "duplicate_name", data, message
    if ACCOUNT_ALREADY_EXISTS_MARKER in (message or "").lower():
        return "exists", data, message
    return "failed", data, message


# ---------------------------------------------------------------------------
# Address creation (SubscriberService) — used by 06_Create_Addresses.ipynb
# ---------------------------------------------------------------------------
LOCATION_ID_DUPLICATE_MARKER = "location id already exists"  # "Custom attribute - Location Id already exists."

# The idempotency check inside add_address_to_account (GET -> "does this Location Id
# already exist?" -> PUT if not) is NOT atomic. 06_Create_Addresses.ipynb runs with
# several parallel workers, so if two subscriptions resolve to the same physical
# location (same Voyager locationId), two workers can both run the GET before either
# one's PUT has committed — both see "not found" and both create a duplicate address.
# These locks serialize creation per (account_number, location_id) so that can't
# happen, while still allowing full parallelism across DIFFERENT locations.
_address_creation_locks: dict[tuple[str, str], threading.Lock] = {}
_address_creation_locks_guard = threading.Lock()


def _get_location_lock(account_number: str, location_id: str) -> threading.Lock:
    key = (str(account_number), str(location_id))
    with _address_creation_locks_guard:
        if key not in _address_creation_locks:
            _address_creation_locks[key] = threading.Lock()
        return _address_creation_locks[key]


def add_address_to_account(
    session: requests.Session,
    account_number: str,
    add_line1: str,
    location_id: str,
    address2: str | None = None,
    city: str = "Christchurch",   # fallback only — used if the Voyager lookup didn't return a city
    zip_code: str = "1234",       # fallback only — used if the Voyager lookup didn't return a postcode
    region_iso: str | None = None,
) -> tuple[str, str | None, str | None]:
    """PUT one address onto an account. Returns (status, address_id, error).

    city/zip_code default to the old placeholders only as a last resort — pass
    the real values from get_voyager_address() (05_Fetch_Subscriptions.ipynb)
    whenever they're available.

    Every address is created with defaultShipping=true, so each new address
    supersedes the previous default — the account's default service address
    ends up being whichever address was created *last*.

    CAUTION: 06_Create_Addresses.ipynb runs create_address_for_subscription
    through a ThreadPoolExecutor (MAX_WORKERS workers), so "last" here means
    whichever PUT happens to land last on OneBill's side — NOT necessarily the
    last row for that account in df_subscriptions. If a specific subscription's
    address needs to deterministically end up as the default (rather than
    "whichever one wins the race"), either run 06 with max_workers=1, or add
    logic that only sets defaultShipping=true for that specific subscription.

    status: "created" | "exists" | "failed". "exists" means an address with this
    location_id was already on the account — the PUT is skipped entirely, so
    re-running this notebook against an account that's already been processed
    won't create duplicate addresses.
    """
    # clean() turns NaN/blank into None so the "or" fallbacks below actually work —
    # NaN is truthy in Python, so an un-cleaned NaN silently skips the fallback and
    # ends up in the JSON payload, which `requests` then refuses to serialize
    # ("Out of range float values are not JSON compliant: nan").
    add_line1   = clean(add_line1)
    address2    = clean(address2)
    city        = clean(city) or "Christchurch"
    zip_code    = clean(zip_code) or "1234"
    region_iso  = clean(region_iso)
    location_id = clean(location_id)

    with _get_location_lock(account_number, location_id):
        return _put_address_locked(
            session, account_number, add_line1, location_id, address2, city, zip_code, region_iso,
        )


def _put_address_locked(
    session: requests.Session,
    account_number: str,
    add_line1: str | None,
    location_id: str | None,
    address2: str | None,
    city: str,
    zip_code: str,
    region_iso: str | None,
) -> tuple[str, str | None, str | None]:
    """The actual check-then-create body of add_address_to_account, run while
    holding that (account_number, location_id)'s lock — see _get_location_lock."""

    # Idempotency check: this endpoint always APPENDS a new address rather than
    # upserting by Location Id, and doesn't reject a genuine duplicate the way
    # "already exists" errors do elsewhere in this module — so without this
    # check, re-running against an already-processed account creates a second,
    # duplicate address every time.
    existing_status, existing_id, existing_error = _find_address_id_by_location(session, account_number, location_id)
    if existing_status == "failed":
        return "failed", None, f"couldn't check for an existing address before creating one: {existing_error}"
    if existing_status == "found":
        return "exists", existing_id, None

    payload = {
        "address": [
            {
                "zip":             zip_code,
                "country":         "NEW ZEALAND",
                "state":           region_iso,
                "county":          "",
                "defaultBilling":  False,
                "city":            city,
                "addLine1":        add_line1,
                "addLine2":        address2 or "",
                "defaultShipping": True,
                "locationAttributes": [
                    {"key": "Location Id", "value": location_id},
                ],
            }
        ]
    }

    url = f"{ONEBILL_SUBSCRIBER_URL}/{account_number}"
    try:
        response = onebill_request(session, "put", url, json=payload, timeout=30)
        _raise_for_status_with_body(response)
        data = response.json()
    except Exception as e:
        return "failed", None, str(e)

    ok, message = check_validation(data)
    if not ok:
        if LOCATION_ID_DUPLICATE_MARKER in (message or "").lower():
            # Not a real failure — an address with this Location Id already exists on the
            # account (e.g. re-running 06 after a partial previous run). Look up its real
            # id instead of treating this as an error.
            status, existing_id, lookup_error = _find_address_id_by_location(session, account_number, location_id)
            if status == "found":
                return "exists", existing_id, message
            return "failed", None, f"{message} (and the follow-up lookup for the existing address also failed: {lookup_error or 'not found'})"
        return "failed", None, message

    # A successful PUT here does NOT echo the address back — it only returns
    # {"accountNumber": ..., "status": "OK"}. So to get the new address's id
    # (needed as shipAddId in 07_Create_Subscription_Orders.ipynb), fetch the
    # account and find the address we just added by its "Location Id" attribute.
    status, address_id, error = _find_address_id_by_location(session, account_number, location_id)
    if status == "found":
        return "created", address_id, None
    if status == "not_found":
        return "failed", None, (
            f"address PUT succeeded but no address with Location Id={location_id!r} was found on "
            f"account {account_number} afterward"
        )
    return "failed", None, f"address PUT succeeded but the follow-up GET failed: {error}"


def _find_address_id_by_location(
    session: requests.Session, account_number: str, location_id: str | None
) -> tuple[str, str | None, str | None]:
    """GET the account and look for an address whose "Location Id" addressAttribute
    matches location_id.

    Returns (status, address_id, error):
        "found"     -> address_id is the matching address's real id
        "not_found" -> no matching address exists on the account (address_id/error are None)
        "failed"    -> the GET itself failed; error has details
    """
    url = f"{ONEBILL_SUBSCRIBER_URL}/{account_number}"
    try:
        response = onebill_request(session, "get", url, timeout=30)
        _raise_for_status_with_body(response)
        data = response.json()
    except Exception as e:
        return "failed", None, str(e)

    for address in data.get("address", []):
        for attr in address.get("addressAttribute", []):
            if attr.get("key") == "Location Id" and clean(attr.get("value")) == location_id:
                return "found", str(address.get("id")), None

    return "not_found", None, None


# ---------------------------------------------------------------------------
# Order creation (OrderService) — used by 07_Create_Subscription_Orders.ipynb
# ---------------------------------------------------------------------------
def create_onebill_order(session: requests.Session, payload: dict) -> dict:
    """POST one subscription/order to OneBill. Raises ValueError on validation failure."""
    response = onebill_request(session, "post", ONEBILL_ORDER_URL, json=payload, timeout=30)
    _raise_for_status_with_body(response)
    data = response.json()
    ok, message = check_validation(data)
    if not ok:
        raise ValueError(message)
    return data

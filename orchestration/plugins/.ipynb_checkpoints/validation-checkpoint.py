"""
Validation module for ETL Week 2
Includes:
- Null checks
- Positive amounts
- Valid currency codes
"""

import pandas as pd
import logging

VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]  # extend as needed

def validate_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transactions before loading"""
    issues = []

    # Null checks
    null_cols = df.columns[df.isna().any()].tolist()
    if null_cols:
        issues.append(f"Null values in columns: {null_cols}")

    # Positive amounts
    if "amount" in df.columns:
        neg = df[df["amount"] <= 0]
        if not neg.empty:
            issues.append(f"Negative or zero amounts: {len(neg)} rows")

    # Valid currency codes
    if "currency" in df.columns:
        invalid_cur = df[~df["currency"].str.upper().isin(VALID_CURRENCIES)]
        if not invalid_cur.empty:
            issues.append(f"Invalid currencies: {invalid_cur['currency'].unique().tolist()}")

    # Mark validation result
    if issues:
        logging.warning(f"Validation issues: {issues}")
        df["validation_status"] = "failed"
    else:
        df["validation_status"] = "passed"

    return df

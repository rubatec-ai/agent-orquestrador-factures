import pandas as pd
from typing import Dict
from logging import Logger

def transform_invoices(df: pd.DataFrame, all_inputs: Dict[str, pd.DataFrame], logger:Logger) -> pd.DataFrame:
    # Copy the input DataFrames
    invoices = df.copy()
    files = all_inputs.get('files').copy()

    # Convert date columns to datetime
    invoices['date_received'] = pd.to_datetime(invoices['date_received'], errors='coerce')
    files['modified_time'] = pd.to_datetime(files['modified_time'], errors='coerce')
    files['created_time'] = pd.to_datetime(files['created_time'], errors='coerce')

    # Count occurrences of each invoice hash in Gmail and flag duplicates.
    invoices['invoice_count'] = invoices.groupby('hash')['hash'].transform('count')
    invoices['duplicated'] = invoices['invoice_count'] > 1

    # For each invoice hash, keep the row with the latest date_received.
    invoices_sorted = invoices.sort_values('date_received')
    latest_invoices = invoices_sorted.groupby('hash', as_index=False).last()

    # Prepare sets of hashes from Gmail and Drive
    gmail_hashes = set(latest_invoices['hash'])
    drive_hashes = set(files['md5Checksum'])

    # 1. New invoices: present in Gmail but not in Drive.
    new_hashes = gmail_hashes - drive_hashes
    new_invoices = latest_invoices[latest_invoices['hash'].isin(new_hashes)]

    # 2. Common invoices: those present in both Gmail and Drive.
    common_hashes = gmail_hashes & drive_hashes
    common_invoices = latest_invoices[latest_invoices['hash'].isin(common_hashes)]

    # Among common invoices, find which ones came from duplicate Gmail entries.
    # (We use the original invoices DataFrame to extract details on duplicates.)
    duplicates_info = (
        invoices.groupby('hash')
        .agg(
            count=('hash', 'size'),
            filenames=('filename', lambda x: list(x))
        )
        .reset_index()
    )
    duplicated_common = duplicates_info[
        (duplicates_info['hash'].isin(common_hashes)) & (duplicates_info['count'] > 1)
    ]

    total_gmail = len(df)
    total_drive = len(files)
    total_unique_gmail = len(gmail_hashes)
    total_new = len(new_invoices)
    total_common = len(common_hashes)
    total_dup_common = duplicated_common['count'].sum()  # total number of duplicated rows in common invoices

    logger.debug(f"Total invoices in Gmail: {total_gmail}")
    logger.debug(f"Total files in Drive: {total_drive}")
    logger.debug(f"Unique invoices in Gmail (by hash): {total_unique_gmail}")
    logger.debug(f"New invoices (in Gmail but not in Drive): {total_new}")
    logger.debug(f"Invoices present in both (common hashes): {total_common}")
    logger.debug(f"Among common invoices, duplicated rows in Gmail: {total_dup_common}")

    if not duplicated_common.empty:
        logger.debug("\nDetailed duplicate information among common invoices:")
        logger.debug("-" * 60)
        logger.debug(f"{'Hash':<40} {'Count':<5} {'Filenames'}")
        logger.debug("-" * 60)
        for _, row in duplicated_common.iterrows():
            # For a cleaner display, join the filenames with a comma and a space.
            filenames_str = ", ".join(row['filenames'])
            logger.debug(f"{row['hash']:<40} {row['count']:<5} {filenames_str}")
        logger.debug("-" * 60)

    # Return only the new invoices (or adjust to return a dict with both new and common info)
    return new_invoices

import pandas as pd
from typing import Dict
from logging import Logger

def transform_invoices(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transforms the Gmail invoices DataFrame by:
      - Converting date strings to datetime.
      - Counting duplicate invoices (by hash) on the raw data and flagging them.
      - Adding a flag 'is_latest' to indicate if the row has the latest date_received for its hash.
      - Flagging whether the invoice is new (present in Gmail but not in Drive) or common.
      - Merging duplicate information (such as multiple filenames) back into the raw data.
      - (Bonus) Flagging if, for a common invoice, the Gmail filename differs from the Drive filename.

    The returned DataFrame has the same number of rows as the original invoices DataFrame,
    so you can later filter by new_invoice and is_latest to process only the newest record per invoice.

    Additional columns added:
      - invoice_count: count of occurrences of the same hash in Gmail.
      - duplicated: True if invoice_count > 1.
      - latest_date: the maximum date_received for each hash.
      - is_latest: True if date_received equals latest_date.
      - new_invoice: True if the invoice hash is not found in Drive.
      - common_invoice: True if the invoice hash is found in both Gmail and Drive.
      - diff_filename_in_gmail: True if, within Gmail, the same hash has different filenames.
      - diff_filename_between_sources: True if for a common invoice the Gmail filename differs from the Drive filename.

    Args:
        key: A string key for the table being transformed.
        all_inputs: Dictionary containing input DataFrames, including "invoices" and "files".
        logger: Logger instance for logging messages.

    Returns:
        A DataFrame enriched with duplicate and cross-source flags.
    """
    logger.info(f"Starting transformation of table: {key}")
    # Copy the raw DataFrames
    invoices = all_inputs.get('invoices').copy()
    files = all_inputs.get('files').copy()

    # Convert date columns to datetime
    invoices['date_received'] = pd.to_datetime(invoices['date_received'], errors='coerce')
    files['modified_time'] = pd.to_datetime(files['modified_time'], errors='coerce')
    files['created_time'] = pd.to_datetime(files['created_time'], errors='coerce')

    # 1. Compute invoice_count for each row (raw count) and flag duplicates.
    invoices['invoice_count'] = invoices.groupby('hash')['hash'].transform('count')
    invoices['duplicated'] = invoices['invoice_count'] > 1

    # 2. For each hash, compute the latest date_received.
    invoices['latest_date'] = invoices.groupby('hash')['date_received'].transform('max')
    # Flag the row as the latest version if its date_received equals the latest_date.
    invoices['is_latest'] = invoices['date_received'] == invoices['latest_date']

    # 3. Determine new vs. common invoices.
    gmail_hashes = set(invoices['hash'])
    drive_hashes = set(files['md5Checksum'])
    invoices['new_invoice'] = invoices['hash'].apply(lambda h: h in (gmail_hashes - drive_hashes))
    invoices['common_invoice'] = invoices['hash'].apply(lambda h: h in (gmail_hashes & drive_hashes))

    # 4. Aggregate Gmail duplicate info (to know if same hash has different filenames).
    duplicates_info = (
        invoices.groupby('hash')
        .agg(
            count=('hash', 'size'),
            gmail_filenames=('filename', lambda x: list(x))
        )
        .reset_index()
    )
    duplicates_info['diff_filename_in_gmail'] = duplicates_info['gmail_filenames'].apply(lambda x: len(set(x)) > 1)
    # Merge these aggregated columns back to the raw invoices DataFrame.
    invoices = pd.merge(
        invoices,
        duplicates_info[['hash', 'diff_filename_in_gmail']],
        on='hash',
        how='left'
    )

    # 5. (Bonus) Check common invoices between Gmail and Drive for filename mismatches.
    common_invoices_gmail = invoices[invoices['common_invoice']][['hash', 'filename']]
    common_invoices_drive = files[files['md5Checksum'].isin(drive_hashes)][['md5Checksum', 'filename', 'relative_path']]
    merged_common = pd.merge(
        common_invoices_gmail,
        common_invoices_drive,
        left_on='hash',
        right_on='md5Checksum',
        suffixes=('_gmail', '_drive')
    )
    diff_filename_between = merged_common['filename_gmail'] != merged_common['filename_drive']
    diff_filename_mapping = merged_common[diff_filename_between].groupby('hash').size().to_dict()
    invoices['diff_filename_between_sources'] = invoices['hash'].apply(lambda h: h in diff_filename_mapping)

    # Log summary details.
    total_gmail = len(all_inputs.get('invoices'))
    total_drive = len(all_inputs.get('files'))
    total_unique_gmail = len(gmail_hashes)
    # Here, total_new and total_common can be taken from any row, but since it repeats per hash,
    # we can just sum the boolean column for rows where is_latest is True.
    total_new = invoices[invoices['is_latest']]['new_invoice'].sum()
    total_common = invoices[invoices['is_latest']]['common_invoice'].sum()
    total_dup_common = duplicates_info.loc[duplicates_info['hash'].isin(gmail_hashes & drive_hashes), 'count'].sum()

    logger.debug(f"Total invoices in Gmail: {total_gmail}")
    logger.debug(f"Total files in Drive: {total_drive}")
    logger.debug(f"Unique invoices in Gmail (by hash): {total_unique_gmail}")
    logger.debug(f"New invoices (Gmail but not in Drive) [latest only]: {total_new}")
    logger.debug(f"Invoices present in both (common hashes) [latest only]: {total_common}")
    logger.debug(f"Total duplicate rows in common invoices (Gmail): {total_dup_common}")

    # Optionally, log details of invoices missing in Drive.
    missing_invoices = invoices[invoices['is_latest'] & (~invoices['new_invoice'])]
    if not missing_invoices.empty:
        logger.debug("Invoices (latest records) that are common (i.e. not new):")
        separator = "-" * 80
        logger.debug(separator)
        logger.debug(f"{'Hash':<40} {'Filename':<30} {'Received Date'}")
        logger.debug(separator)
        for _, row in missing_invoices.iterrows():
            logger.debug(f"{row['hash']:<40} {row['filename']:<30} {row['date_received']}")
        logger.debug(separator)

    # Optionally, log details for filename mismatches between sources.
    if not merged_common[diff_filename_between].empty:
        logger.debug("Common invoices with differing filenames between Gmail and Drive:")
        separator = "-" * 102
        logger.debug(separator)
        logger.debug(f"{'Hash':<40} {'Gmail Filename':<30} {'Drive Filename':<30} {'Relative Path'}")
        logger.debug(separator)
        for _, row in merged_common[diff_filename_between].iterrows():
            logger.debug(f"{row['hash']:<40} {row['filename_gmail']:<30} {row['filename_drive']:<30} {row['relative_path']}")
        logger.debug(separator)

    # Return the enriched DataFrame (same number of rows as the original invoices).
    return invoices

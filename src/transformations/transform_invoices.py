import pandas as pd
from typing import Dict
from logging import Logger

def transform_invoices(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transforms the Gmail invoices DataFrame by:
      - Converting date strings to datetime.
      - Counting duplicate invoices (by hash) on the raw data and flagging them.
      - Adding a flag 'is_latest' to indicate if the row has the latest date_received for its hash.
      - Using register as source of truth:
          * Determining the latest register reception date ('data_recepcio').
          * Flagging invoices arriving after that date.
          * Flagging 'new_invoice' if not in register and after register date.
          * Flagging 'common_invoice' if hash already in register.
      - Merging duplicate information (such as multiple filenames) back into the raw data.
      - (Bonus) Flagging if, for a common invoice, the Gmail filename differs from the Drive filename.

    The returned DataFrame has the same number of rows as the original invoices DataFrame,
    so you can later filter by is_latest & new_invoice to process only the truly new invoices.

    Additional columns added:
      - invoice_count: count of occurrences of the same hash in Gmail.
      - duplicated: True if invoice_count > 1.
      - latest_date: the maximum date_received for each hash.
      - is_latest: True if date_received equals latest_date.
      - after_register: True if date_received > max(register.data_recepcio).
      - new_invoice: True if hash not in register and after_register.
      - common_invoice: True if hash in register.
      - diff_filename_in_gmail: True if, within Gmail, the same hash has different filenames.
      - diff_filename_between_sources: True if for a common invoice the Gmail filename differs from the Drive filename.

    Args:
        key: A string key for the table being transformed.
        all_inputs: Dictionary containing input DataFrames: 'invoices', 'files', and 'register'.
        logger: Logger instance for logging messages.

    Returns:
        A DataFrame enriched with duplicate and cross-source flags.
    """
    logger.info(f"Starting transformation of table: {key}")
    # Copy the raw DataFrames
    invoices = all_inputs.get('invoices').copy()
    files = all_inputs.get('files').copy()
    register = all_inputs.get('register').copy()

    # Convert date columns to datetime
    invoices['date_received'] = pd.to_datetime(invoices['date_received'], errors='coerce')
    register['data_recepcio'] = pd.to_datetime(register['data_recepcio'], errors='coerce')
    files['modified_time'] = pd.to_datetime(files['modified_time'], errors='coerce')
    files['created_time'] = pd.to_datetime(files['created_time'], errors='coerce')


    # Determine latest date in register (source of truth)
    max_register_date = register['data_recepcio'].max()
    if pd.isna(max_register_date):
        max_register_date = pd.Timestamp.now() - pd.DateOffset(years=42)

    logger.debug(f"Max register reception date: {max_register_date}")

    # Flag invoices arriving after last register date
    invoices['after_register'] = invoices['date_received'] > max_register_date

    # 1. Compute invoice_count for each row and flag duplicates
    invoices['invoice_count'] = invoices.groupby('hash')['hash'].transform('count')
    invoices['duplicated'] = invoices['invoice_count'] > 1

    # 2. For each hash, compute meaningful dates
    invoices['latest_date'] = invoices.groupby('hash')['date_received'].transform('max')
    invoices['is_latest'] = invoices['date_received'] == invoices['latest_date']

    invoices['earliest_date'] = invoices.groupby('hash')['date_received'].transform('min')
    invoices['is_earliest'] = invoices['date_received'] == invoices['earliest_date']

    # 3. Determine new vs common invoices based on register
    gmail_hashes = set(invoices['hash'].dropna())
    register_hashes = set(register['md5Checksum'].dropna())
    invoices['new_invoice'] = invoices.apply(
        lambda r: (r['hash'] not in register_hashes), axis=1
    )
    invoices['common_invoice'] = invoices['hash'].isin(register_hashes)

    # 4. Aggregate Gmail duplicate info for filename variations
    duplicates_info = (
        invoices.groupby('hash')
        .agg(
            count=('hash', 'size'),
            gmail_filenames=('filename', lambda x: list(x))
        )
        .reset_index()
    )
    duplicates_info['diff_filename_in_gmail'] = duplicates_info['gmail_filenames'].apply(
        lambda x: len(set(x)) > 1
    )
    invoices = pd.merge(
        invoices,
        duplicates_info[['hash', 'diff_filename_in_gmail']],
        on='hash', how='left'
    )

    # 5. (Bonus) Check common invoices between Gmail and Drive for filename mismatches
    drive_hashes = set(files['md5Checksum'].dropna())
    common_invoices_gmail = invoices[invoices['common_invoice']][['hash', 'filename']]
    common_invoices_drive = files[files['md5Checksum'].isin(drive_hashes)][
        ['md5Checksum', 'filename', 'relative_path']
    ]
    merged_common = pd.merge(
        common_invoices_gmail, common_invoices_drive,
        left_on='hash', right_on='md5Checksum', suffixes=('_gmail', '_drive')
    )
    diff_filename_between = merged_common['filename_gmail'] != merged_common['filename_drive']
    diff_filename_mapping = merged_common[diff_filename_between].groupby('hash').size().to_dict()
    invoices['diff_filename_between_sources'] = invoices['hash'].apply(
        lambda h: h in diff_filename_mapping
    )

    # Log summary details
    total_gmail = len(invoices)
    total_unique_gmail = len(gmail_hashes)
    total_new = invoices[invoices['is_earliest'] & invoices['new_invoice']]['new_invoice'].sum()

    logger.debug(f"Total invoices in Gmail: {total_gmail}")
    logger.debug(f"Unique invoices in Gmail (by hash): {total_unique_gmail}")
    logger.debug(f"New invoices to be processed (earliest  & not in register): {total_new}")

    # Detailed logging for new invoices
    new_invoices = invoices[invoices['is_earliest'] & invoices['new_invoice']]
    if not new_invoices.empty:
        logger.debug("New invoices to be processed:")
        separator = "-" * 80
        logger.debug(separator)
        logger.debug(f"{'Hash':<40} {'Filename':<30} {'Received Date'}")
        logger.debug(separator)
        for _, row in new_invoices.iterrows():
            logger.debug(f"{row['hash']:<40} {row['filename']:<30} {row['date_received']}")
        logger.debug(separator)
    else:
        logger.debug("No new invoices to process.")

    return invoices
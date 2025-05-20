import pandas as pd
from typing import Dict
from logging import Logger


def transform_register(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transforms the register DataFrame by:
      1) Checking which files from Drive (based on md5Checksum) are present in the register.
      2) Adding basic flags for existence and optionally duplicates if needed later.

    New columns added:
      - file_exists_in_drive: True if the register's md5Checksum is found in the Drive files.

    Logging:
      - Reports Drive files that match register entries (by md5Checksum and filename).
      - Summarizes total records, matches, and discrepancies.

    Args:
        key: String key for the table being transformed (e.g., "register").
        all_inputs: Dictionary containing input DataFrames, including "register" and "files".
        logger: Logger instance for logging messages.

    Returns:
        A transformed register DataFrame with existence checks.
    """
    logger.info(f"Starting transformation of register table: {key}")

    # Get the register and files DataFrames
    register = all_inputs.get('register').copy()
    files = all_inputs.get('files').copy()

    # Ensure necessary columns exist
    for col in ['md5Checksum', 'filename']:
        if col not in register.columns:
            raise KeyError(f"Column '{col}' not found in register DataFrame.")
        if col not in files.columns:
            raise KeyError(f"Column '{col}' not found in files DataFrame.")

    # 1. Check which register md5Checksums exist in Drive files
    drive_hashes = set(files['md5Checksum'].dropna())
    register['file_exists_in_drive'] = register['md5Checksum'].apply(lambda x: x in drive_hashes)

    # 2. Get Drive files that match register entries
    matched_files = pd.merge(
        register[['md5Checksum', 'filename']],
        files[['md5Checksum', 'filename']],
        on='md5Checksum',
        how='inner',
        suffixes=('_register', '_drive')
    )

    # Logging: Drive files that are in register
    if not matched_files.empty:
        logger.debug("Drive files present in register (by md5Checksum):")
        separator = "-" * 100
        logger.debug(separator)
        logger.debug(f"{'md5Checksum':<40} {'Register Filename':<30} {'Drive Filename':<30}")
        logger.debug(separator)
        for _, row in matched_files.iterrows():
            logger.debug(f"{row['md5Checksum']:<40} {row['filename_register']:<30} {row['filename_drive']}")
        logger.debug(separator)
    else:
        logger.debug("No Drive files found in register.")

    # Logging: Register entries not in Drive
    missing_in_drive = register[~register['file_exists_in_drive']]
    if not missing_in_drive.empty:
        logger.debug("Register entries not found in Drive:")
        separator = "-" * 80
        logger.debug(separator)
        logger.debug(f"{'md5Checksum':<40} {'Register Filename'}")
        logger.debug(separator)
        for _, row in missing_in_drive.iterrows():
            logger.debug(f"{row['md5Checksum']:<40} {row['filename']}")
        logger.debug(separator)

    # Summary logging
    total_register = len(register)
    total_files = len(files)
    total_matches = len(matched_files)
    total_missing = len(missing_in_drive)

    logger.debug(f"Total records in register: {total_register}")
    logger.debug(f"Total files in Drive: {total_files}")
    logger.debug(f"Drive files present in register: {total_matches}")
    logger.debug(f"Register entries missing in Drive: {total_missing}")

    return register
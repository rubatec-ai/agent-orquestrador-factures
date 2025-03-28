import pandas as pd
from typing import Dict
from logging import Logger


def transform_register(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transforms the register DataFrame by performing the following checks:
      1) Constructs a full file path from register columns 'canal_sie' and 'filename'.
      2) Flags if the register file exists in the Drive files DataFrame by comparing
         full paths (constructed as relative_path + '/' + filename in files).
      3) Flags duplicates in the register based on the md5Checksum.
      4) Merges with the files DataFrame to flag:
           a) If, for a common md5Checksum, the filename in register is different from
              the filename in files.
           b) If the same md5Checksum appears more than once in Drive.

    The enriched register DataFrame will have the following new columns:
      - full_path: constructed as canal_sie + '/' + filename (from register).
      - file_exists: True if full_path is found in the files DataFrame.
      - duplicated_in_register: True if the same md5Checksum appears more than once in register.
      - drive_file_count: Number of files in Drive with the same md5Checksum.
      - drive_duplicates: True if drive_file_count > 1.
      - diff_filename_between_sources: True if for a common md5Checksum the register filename
        differs from the drive filename.

    Args:
        key: A string key for the table being transformed (e.g., "register").
        all_inputs: Dictionary containing input DataFrames, including "register" and "files".
        logger: Logger instance for logging messages.

    Returns:
        A transformed register DataFrame enriched with the checks above.
    """
    logger.info(f"Starting transformation of register table: {key}")

    # Get the register and files DataFrames
    register = all_inputs.get('register').copy()
    files = all_inputs.get('files').copy()

    # Ensure necessary columns exist
    for col in ['filename', 'canal_sie']:
        if col not in register.columns:
            raise KeyError(f"Column '{col}' not found in register DataFrame.")
    for col in ['relative_path', 'filename', 'md5Checksum']:
        if col not in files.columns:
            raise KeyError(f"Column '{col}' not found in files DataFrame.")

    # 1. Construct full file paths in both register and files.
    register['full_path'] = register['canal_sie'].str.strip() + '/' + register['filename'].str.strip()
    files['full_path'] = files['relative_path'].str.strip() + '/' + files['filename'].str.strip()

    # 2. Flag in register if the full_path exists in the files DataFrame.
    files_full_paths = set(files['full_path'])
    register['file_exists'] = register['full_path'].apply(lambda x: x in files_full_paths)

    # 3. Flag duplicate registers based on md5Checksum.
    if 'md5Checksum' in register.columns:
        register['register_count'] = register.groupby('md5Checksum')['md5Checksum'].transform('count')
        register['duplicated_in_register'] = register['register_count'] > 1
    else:
        # If no md5Checksum exists, set flag to False.
        register['duplicated_in_register'] = False

    # 4. Merge drive information into register for cross-source checks.
    #    (Assumes files DataFrame has a unique combination per file per md5Checksum.)
    drive_info = files.groupby('md5Checksum').agg(
        drive_filename=('filename', lambda x: x.iloc[0]),
        drive_file_count=('md5Checksum', 'size')
    ).reset_index()
    # Flag if the same md5Checksum appears more than once in Drive.
    drive_info['drive_duplicates'] = drive_info['drive_file_count'] > 1

    # Merge drive info into register on md5Checksum (if available).
    if 'md5Checksum' in register.columns:
        register = register.dropna(subset=['md5Checksum'])
        register = pd.merge(
            register,
            drive_info[['md5Checksum', 'drive_filename', 'drive_file_count', 'drive_duplicates']],
            on='md5Checksum',
            how='left'
        )
        # 4a. Flag if the register filename differs from the drive filename.
        register['diff_filename_between_sources'] = register.apply(
            lambda row: (pd.notnull(row['drive_filename']) and (
                        row['filename'].strip() != row['drive_filename'].strip())),
            axis=1
        )
    else:
        register['drive_file_count'] = None
        register['drive_duplicates'] = False
        register['diff_filename_between_sources'] = False

    # --- Logging: Table of register entries missing in Drive.
    missing_files = register[~register['file_exists']]
    if not missing_files.empty:
        logger.debug("Register entries missing in Drive:")
        separator = "-" * 102
        logger.debug(separator)
        logger.debug(f"{'md5Checksum':<40} {'Register Filename':<30} {'Full Path'}")
        logger.debug(separator)
        for _, row in missing_files.iterrows():
            md5 = row.get('md5Checksum', 'N/A')
            reg_fn = row['filename']
            full_path = row['full_path']
            logger.debug(f"{str(md5):<40} {reg_fn:<30} {full_path}")
        logger.debug(separator)

    # --- Logging: Table of register entries with differing filenames between register and Drive.
    diff_filename = register[register['diff_filename_between_sources']]
    if not diff_filename.empty:
        logger.debug("Register entries with differing filenames between register and Drive:")
        separator = "-" * 100
        logger.debug(separator)
        logger.debug(f"{'md5Checksum':<40} {'Register Filename':<30} {'Drive Filename':<30}")
        logger.debug(separator)
        for _, row in diff_filename.iterrows():
            md5 = row.get('md5Checksum', 'N/A')
            reg_fn = row['filename']
            drive_fn = row.get('drive_filename', 'N/A')
            logger.debug(f"{str(md5):<40} {reg_fn:<30} {drive_fn:<30}")
        logger.debug(separator)

    # Log summary details.
    total_register = len(all_inputs.get('register'))
    total_files = len(files)
    total_missing = (~register['file_exists']).sum()
    total_dup_register = register['duplicated_in_register'].sum() if 'register_count' in register.columns else 0
    total_drive_dup = register['drive_duplicates'].sum()
    total_diff_filename = register['diff_filename_between_sources'].sum()

    logger.debug(f"Total records in register: {total_register}")
    logger.debug(f"Total files in Drive: {total_files}")
    logger.debug(f"Files missing in Drive: {total_missing}")
    logger.debug(f"Duplicate registers (by md5Checksum): {total_dup_register}")
    logger.debug(f"Registers with multiple entries in Drive: {total_drive_dup}")
    logger.debug(f"Registers with differing filenames between register and Drive: {total_diff_filename}")

    return register

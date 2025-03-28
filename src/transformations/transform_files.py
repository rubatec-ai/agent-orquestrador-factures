import pandas as pd
from typing import Dict
from logging import Logger


def transform_files(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transform and enrich the Drive files DataFrame by:
      1) Counting how many files share the same md5Checksum (indicating duplicates).
      2) Marking the newest file (based on modified_time) as last_version.
      3) Detecting if there are duplicates with different filenames.
      4) Detecting if there are duplicates stored in different subfolders (relative_path).

    Args:
        key: String key for the table being transformed (e.g. "files").
        all_inputs: Dictionary containing DataFrames, including "files".
        logger: Logger instance for debug/info messages.

    Returns:
        A DataFrame (files) with:
          - 'file_count': how many files share the same md5Checksum
          - 'duplicated': boolean indicating if the file has duplicates
          - 'last_version': boolean indicating if this row is the latest version
            among those sharing the same md5Checksum
        No columns are dropped, so you can still inspect them afterwards.
    """
    logger.info(f"Starting default transformation of table: {key}")
    files = all_inputs.get('files').copy()

    # 1) Count how many files share the same md5Checksum and mark duplicates
    files['file_count'] = files.groupby('md5Checksum')['md5Checksum'].transform('count')
    files['duplicated'] = files['file_count'] > 1

    # 2) Determine which file is the newest for each md5Checksum (by modified_time)
    #    Sort ascending by modified_time, then groupby.last() picks the final row in that order.
    files_sorted = files.sort_values('modified_time')
    latest_files = files_sorted.groupby('md5Checksum', as_index=False).last()

    # 2b) Merge back to create a boolean 'last_version'
    latest_marker = pd.merge(
        files[['md5Checksum', 'modified_time']],
        latest_files[['md5Checksum', 'modified_time']],
        on=['md5Checksum', 'modified_time'],
        how='left',
        indicator=True
    )
    files['last_version'] = (latest_marker['_merge'] == 'both')

    # 3) Build an aggregator DataFrame that collects all filenames and relative_paths per md5Checksum
    drive_duplicates = files.groupby('md5Checksum').agg(
        drive_filenames=('filename', lambda x: list(x)),
        drive_paths=('relative_path', lambda x: list(x))
    ).reset_index()

    # Mark if there are different filenames for the same hash
    drive_duplicates['diff_filename'] = drive_duplicates['drive_filenames'].apply(lambda x: len(set(x)) > 1)
    # Mark if the same hash appears in multiple subfolders
    drive_duplicates['diff_folder'] = drive_duplicates['drive_paths'].apply(lambda x: len(set(x)) > 1)

    # 3a) Log duplicates that differ by filename
    diff_drive_filename = drive_duplicates[drive_duplicates['diff_filename']]
    if not diff_drive_filename.empty:
        logger.debug("Drive duplicates with same md5Checksum but different filenames:")
        logger.debug("-" * 102)
        logger.debug(f"{'md5Checksum':<40} {'Filenames'}")
        logger.debug("-" * 102)
        for _, row in diff_drive_filename.iterrows():
            fnames = ", ".join(sorted(set(row['drive_filenames'])))
            logger.debug(f"{row['md5Checksum']:<40} {fnames}")
        logger.debug("-" * 102)

    # 3b) Log duplicates that are in different subfolders
    diff_drive_folders = drive_duplicates[drive_duplicates['diff_folder']]
    if not diff_drive_folders.empty:
        logger.debug("Drive duplicates with same md5Checksum stored in multiple subfolders:")
        logger.debug("-" * 102)
        logger.debug(f"{'md5Checksum':<40} {'Relative Paths'}")
        logger.debug("-" * 102)
        for _, row in diff_drive_folders.iterrows():
            paths = ", ".join(sorted(set(row['drive_paths'])))
            filenames = ", ".join(sorted(set(row['drive_filenames'])))
            logger.debug(f"{row['md5Checksum']:<40} {paths} {filenames} ")
        logger.debug("-" * 102)

    return files

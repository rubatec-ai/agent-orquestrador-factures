import pandas as pd
from typing import Dict
from logging import Logger

def transform_files(key: str, all_inputs: Dict[str, pd.DataFrame], logger: Logger) -> pd.DataFrame:
    """
    Transforma y enriquece el DataFrame de archivos de Drive:
      1) Cuenta cuántos archivos comparten el mismo md5Checksum (indicando duplicados).
      2) Marca el archivo más reciente (basado en modified_time) como last_version.
      3) Detecta si hay duplicados con diferentes nombres de archivo.

    Args:
        key: Clave de la tabla que se está transformando (e.g., "files").
        all_inputs: Diccionario con DataFrames, incluyendo "files".
        logger: Instancia de Logger para mensajes de debug/info.

    Returns:
        Un DataFrame con:
          - 'file_count': cuántos archivos comparten el mismo md5Checksum
          - 'duplicated': booleano que indica si el archivo tiene duplicados
          - 'last_version': booleano que indica si esta fila es la versión más reciente
    """
    logger.info(f"Starting transformation of table: {key}")
    files = all_inputs.get('files').copy()

    # 1) Contar duplicados por md5Checksum
    files['file_count'] = files.groupby('md5Checksum')['md5Checksum'].transform('count')
    files['duplicated'] = files['file_count'] > 1

    # 2) Marcar la versión más reciente por md5Checksum
    files_sorted = files.sort_values('modified_time')
    latest_files = files_sorted.groupby('md5Checksum', as_index=False).last()
    latest_marker = pd.merge(
        files[['md5Checksum', 'modified_time']],
        latest_files[['md5Checksum', 'modified_time']],
        on=['md5Checksum', 'modified_time'],
        how='left',
        indicator=True
    )
    files['last_version'] = (latest_marker['_merge'] == 'both')

    # 3) Detectar duplicados con diferentes nombres de archivo
    drive_duplicates = files.groupby('md5Checksum').agg(
        drive_filenames=('filename', lambda x: list(x))
    ).reset_index()
    drive_duplicates['diff_filename'] = drive_duplicates['drive_filenames'].apply(lambda x: len(set(x)) > 1)

    # Logging de duplicados con nombres diferentes
    diff_drive_filename = drive_duplicates[drive_duplicates['diff_filename']]
    if not diff_drive_filename.empty:
        logger.debug("Archivos duplicados en Drive con el mismo md5Checksum pero nombres diferentes:")
        logger.debug("-" * 80)
        logger.debug(f"{'md5Checksum':<40} {'Nombres de archivo'}")
        logger.debug("-" * 80)
        for _, row in diff_drive_filename.iterrows():
            fnames = ", ".join(sorted(set(row['drive_filenames'])))
            logger.debug(f"{row['md5Checksum']:<40} {fnames}")
        logger.debug("-" * 80)

    return files
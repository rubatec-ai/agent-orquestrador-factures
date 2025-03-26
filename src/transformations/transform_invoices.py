import pandas as pd
from typing import Dict

def transform_invoices(df: pd.DataFrame, all_inputs: Dict[str, pd.DataFrame]) -> pd.DataFrame:

    invoices = df.copy()
    files = all_inputs.get('files').copy()

    # Aseguramos que las fechas son datetime
    invoices['date_received'] = pd.to_datetime(invoices['date_received'], errors='coerce')
    files['modified_time'] = pd.to_datetime(files['modified_time'], errors='coerce')

    # ---- 1. Nueva factura: está en Gmail pero no en "files"
    filenames_gmail = set(invoices['filename'])
    filenames_files = set(files['filename'])

    new_filenames = filenames_gmail - filenames_files
    new_invoices = invoices[invoices['filename'].isin(new_filenames)]

    # ---- 2. Updated invoices: misma filename, distinta fecha
    common_filenames = filenames_gmail & filenames_files

    # Merge para comparar fechas
    merged = pd.merge(
        invoices[invoices['filename'].isin(common_filenames)],
        files[['filename', 'modified_time']],
        on='filename',
        suffixes=('_gmail', '_files'),
        how='inner'
    )

    updated_invoices_df = merged[
        merged['date_received'] != merged['modified_time']
    ]

    updated_filenames = updated_invoices_df['filename'].tolist()

    # ---- 3. Facturas iguales (mismo filename y misma fecha)
    same_filenames_df = merged[
        merged['date_received'] == merged['modified_time']
    ]
    duplicated_filenames = same_filenames_df['filename'].tolist()

    # ---- Resultado final
    # Nos quedamos solo con las nuevas de verdad
    invoices_final = new_invoices.copy()

    # Logs temporales por si quieres inspeccionar
    print(f"🆕 Nuevas facturas: {len(new_filenames)}")
    print(f"🛠️ Facturas actualizadas: {len(updated_filenames)}")
    print(f"✅ Facturas ya presentes sin cambios: {len(duplicated_filenames)}")

    return invoices_final

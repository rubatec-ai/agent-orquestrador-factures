MAX_RESULTS_GMAIL = 100
PAGE_SIZE_DRIVE = 1000

COLUMNS_GOOGLE_SHEET = [
    'Num Factura Proveedor', 'Proveedor', 'Fecha Recepción', 'Canal - SIE',
    'Base', 'IVA[%]', 'IVA[€]', 'Total', 'Sage - Serie', 'Sage - Num Factura',
    'Num Factura RUBATEC', 'Fecha Vencimento', 'Ruta', 'Marca Temporal OCR'
]



COLUMNS_REGISTRO = [
    'fr_proveedor','proveedor', 'data_recepcio', 'canal_sie', 'base', 'iva [%]', 'iva [€]', 'total',
    'serie', 'fr', 'fr_rubatec', 'data_venciment', 'ruta_pdf', 'marca_temporal_ocr'
]

MAPPING_RENAME_COL_REGISTRO = {
    'Num Factura Proveedor': 'fr_proveedor',
    'Proveedor': 'proveedor',
    'Fecha Recepción': 'data_recepcio',
    'Canal - SIE': 'canal_sie',
    'Base': 'base',
    'IVA [%]': 'iva[%]',
    'IVA [€]': 'iva[€]',
    'Total': 'total',
    'Sage - Serie': 'serie',
    'Sage - Num Factura': 'fr',
    'Num Factura RUBATEC': 'fr_rubatec',
    'Fecha Vencimento': 'data_venciment',
    'Ruta': 'ruta_pdf',
    'Marca Temporal OCR': 'marca_temporal_ocr'
}



# Model pricing constants (as of 2025-01-13)

# Define model costs per token
MODELS_COST = {
    "gpt-4o": {
        "input": 0.00000250,
        "output": 0.00001000
    },
    "gpt-4o-mini": {
        "input": 0.00000015,
        "output": 0.00000060
    },
    "o1": {
        "input": 0.00001500,
        "output": 0.00006000
    },
    "o1-mini": {
        "input": 0.00000300,
        "output": 0.00001200
    },
}


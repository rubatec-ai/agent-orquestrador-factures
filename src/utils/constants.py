MAX_RESULTS_GMAIL = 100
PAGE_SIZE_DRIVE = 1000

MAPPING_RENAME_COL_REGISTRO = {
    'Num Factura Proveedor': ('fr_proveedor', str),
    'Proveedor': ('proveedor', str),
    'Fecha Recepción': ('data_recepcio', 'datetime'),
    'Canal - SIE': ('canal_sie', str),
    'Base': ('base', float),
    'IVA [%]': ('iva[%]', float),
    'IVA [€]': ('iva[€]', float),
    'Total': ('total', float),
    'Sage - Serie': ('serie', str),
    'Sage - Num Factura': ('fr', str),
    'Num Factura RUBATEC': ('fr_rubatec', str),
    'Fecha Vencimento': ('data_venciment', 'date'),
    'Nom Factura': ('filename', str),
    'Marca Temporal OCR': ('marca_temporal_ocr', 'datetime'),
    'md5Checksum': ('md5Checksum', str)
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


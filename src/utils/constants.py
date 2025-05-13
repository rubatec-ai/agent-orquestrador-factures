MAX_RESULTS_GMAIL = 100
PAGE_SIZE_DRIVE = 1000
MAX_WORDS_FIRST_PAGE = 333

CRITICAL_FIELDS_LINE_ITEMS = ['line_item/amount', 'line_item/quantity', 'line_item/unit_price']

MAPPING_RENAME_COL_REGISTRO = {
    'Link': ('web_view_link', str),
    'Num Factura Proveedor': ('fr_proveedor', str),
    'Proveedor': ('proveedor', str),
    'CIF': ('cif', str),
    'Fecha Recepción': ('data_recepcio', 'datetime'),
    'Remitente': ('sender', str),
    'Nom Factura': ('filename', str),
    'canal_id': ('canal_sie', str),
    'Base': ('base', float),
    'IVA [%]': ('iva[%]', float),
    'IVA [€]': ('iva[€]', float),
    'Total': ('total', float),
    'Forma Pago': ('forma_pago', str),
    'Fecha Vencimento': ('data_venciment', 'date'),
    'Marca Temporal OCR': ('marca_temporal_ocr', 'datetime'),
    'Imagen': ('image', str),
    'Comptabilitzada': ('supervisada', str),
    'Marca Temporal Comptabilitzada': ('marca_temporal_supervisada', 'datetime'),
    'Devolver factura': ('devuelta', str),
    'Marca Temporal Devuelta': ('marca_temporal_devuelta', 'datetime'),
    'Motivo Devolución': ('motivo_devuelta', str),
}

EXTRACTED_DATA_INVOICE_PARSER = {
            # Initialize to None or empty so you can see what was / wasn't found
            "amount_due": None,
            "amount_paid_since_last_invoice": None,
            "carrier": None,
            "currency": None,
            "currency_exchange_rate": None,
            "customer_tax_id": None,
            "delivery_date": None,
            "due_date": None,
            "freight_amount": None,
            "invoice_date": None,
            "invoice_id": None,
            "net_amount": None,
            "payment_terms": None,
            "purchase_order": None,
            "receiver_address": None,
            "receiver_email": None,
            "receiver_name": None,
            "receiver_phone": None,
            "receiver_tax_id": None,
            "receiver_website": None,
            "remit_to_address": None,
            "remit_to_name": None,
            "ship_from_address": None,
            "ship_from_name": None,
            "ship_to_address": None,
            "ship_to_name": None,
            "supplier_address": None,
            "supplier_email": None,
            "supplier_iban": None,
            "supplier_name": None,
            "supplier_payment_ref": None,
            "supplier_phone": None,
            "supplier_registration": None,
            "supplier_tax_id": None,
            "supplier_website": None,
            "total_amount": None,
            "total_tax_amount": None
}


EXTRACTED_DATA_OPENAI = ['line_item', 'vat', 'marca_temporal_ocr', 'text', 'canal_sie', 'fr_proveedor', 'proveedor', 'forma_pago', 'supplier_id']

INVALID_CANAL_SIE_VALUES = ["desconocido", "9999"]

EMAIL_INVALID_CANAL_SUBJECT = "Falta número de contrato (Canal/SIE) en factura"

EMAIL_INVALID_CANAL_BODY = (
    "Estimado remitente,\n\n"
    "Hemos detectado que en el PDF de la factura no se ha identificado el campo 'Canal' o número de contrato interno (SIE), "
    "el cual debe ser un código de 4 dígitos que asigna el importe de la factura.\n\n"
    "Para corregir este problema, por favor proceda de una de las siguientes maneras:\n\n"
    "1. Reenvíe la factura adjuntando el PDF modificado en el que se incluya correctamente el campo 'Canal' (SIE).\n\n"
    "2. Responda a ESTE CORREO o haga uno nuevo con el pdf adjunto indicando el número de contrato en el asunto "
    "ó cuerpo del mensaje, siguiendo el siguiente formato:\n\n"
    "    Ejemplo de asunto: #1234 Factura resuelta\n\n"
    "    Ejemplo de cuerpo del mensaje:\n"
    "        La factura adjunta en este correo se debe asignar al canal #1234 (SIE).\n\n"
    "Donde '#' introduce a '1234' que es el número de contrato (SIE) correspondiente a la factura.\n\n"
    "Muchas gracias,\n"
)

SYSTEM_PROMPT_ANALYSIS = (
    "You are a smart OCR system designed to extract specific parameters from PDF content. "
    "Follow the detailed instructions provided and return ONLY a valid JSON object with the results, "
    "without any additional commentary or explanation."
)

PROMPT_TASK = (
    "Extract the following parameters from the provided PDF text according to the instructions below. "
    "If a parameter is not explicitly found or cannot be inferred from the context, write 'desconocido'."
)

DEFAULT_PARAMETERS_TO_SEARCH = {
    "canal_sie" : "desconocido",
    "fr_proveedor" : "desconocido",
    "proveedor" : "desconocido",
    "forma_pago" : "desconocido",
    "supplier_id" : "desconocido",
}

PARAMETERS_TO_SEARCH = {
    "canal_sie": (
        "A mandatory 4-digit internal contract identifier used for cost allocation purposes (e.g., '9999'). "
        "Must appear as a standalone number, not embedded within longer codes or identifiers. "
        "Must be found in proximity to specific labels such as 'SIE', 'Canal', or 'Contrato'. "
        "If no valid identifier is found, return 'desconocido'."
    ),
    "fr_proveedor": (
        "The supplier's own invoice identification number. "
        "It MUST NOT start with 'FR' as those are our internal reference numbers. "
        "Look for terms like 'Número Factura' near the identifier. "
        "These typically appear as numeric or alphanumeric codes in various formats depending on the supplier. "
        "Examples: 61557, F/0000340, A2425209815, 9512419314, A000172950, 25F0016633, F250260, 21250130030005999. "
        "Avoid identifiers starting with FRXX-XXX (X are digits) format as those are our internal references, not the supplier's. "
        "If not found, return 'desconocido'."
    ),
    "proveedor": (
        "RUBATEC always receives the invoice, so IS NOT the supplier! "
        "If the supplier name is not explicitly found in the invoice text, check for the supplier email as a fallback. "
        "Usually emails are like name@company_name and websites are company_name.com, so you could use this info. "
        "If neither the name nor the email is found, write 'desconocido'."
    ),
    "forma_pago": (
        "This field indicates the payment terms. Examples include 'Confirming XXX días' or 'Recibo domiciliado'. "
        "If not found, write 'desconocido'."
    ),
    "supplier_id": (
        "RUBATEC always receives the invoice, so the supplier identifier must NOT be 'A60744216'. "
        "Instead, the supplier identifier should be a valid Spanish CIF: it must start with a letter, "
        "followed by 7 digits, and end with a control character (which can be either a digit or a letter). "
        "Optionally, it may be prefixed with 'ES' to indicate that it belongs to Spain. "
        "If not found, write 'desconocido'."
    ),
}

# Model pricing constants (as of 2025-01-13)

# Define model costs per token
MODELS_COST = {
    "gpt-4.5-preview-2025-02-27": {
        "input": 0.000075,        # $75.00 / 1,000,000
        "cached_input": 0.0000375, # $37.50 / 1,000,000
        "output": 0.00015         # $150.00 / 1,000,000
    },
    "gpt-4o-2024-08-06": {
        "input": 0.0000025,      # $2.50 / 1,000,000
        "cached_input": 0.00000125,
        "output": 0.00001
    },
    "gpt-4o-audio-preview-2024-12-17": {
        "input": 0.0000025,
        "cached_input": None,     # Sin valor en la tabla
        "output": 0.00001
    },
    "gpt-4o-realtime-preview-2024-12-17": {
        "input": 0.000005,       # $5.00 / 1,000,000
        "cached_input": 0.0000025,
        "output": 0.00002
    },
    "gpt-4o-mini-2024-07-18": {
        "input": 0.00000015,     # $0.15 / 1,000,000
        "cached_input": 0.000000075,  # $0.075 / 1,000,000
        "output": 0.0000006      # $0.60 / 1,000,000
    },
    "gpt-4o-mini-audio-preview-2024-12-17": {
        "input": 0.00000015,
        "cached_input": None,
        "output": 0.0000006
    },
    "gpt-4o-mini-realtime-preview-2024-12-17": {
        "input": 0.0000006,      # $0.60 / 1,000,000
        "cached_input": 0.0000003,    # $0.30 / 1,000,000
        "output": 0.0000024      # $2.40 / 1,000,000
    },
    "o1-2024-12-17": {
        "input": 0.000015,       # $15.00 / 1,000,000
        "cached_input": 0.0000075,
        "output": 0.00006
    },
    "o1-pro-2025-03-19": {
        "input": 0.00015,        # $150.00 / 1,000,000
        "cached_input": None,
        "output": 0.0006         # $600.00 / 1,000,000
    },
    "o3-mini-2025-01-31": {
        "input": 0.0000011,      # $1.10 / 1,000,000
        "cached_input": 0.00000055,
        "output": 0.0000044      # $4.40 / 1,000,000
    },
    "o1-mini-2024-09-12": {
        "input": 0.0000011,      # $1.10 / 1,000,000
        "cached_input": 0.00000055,
        "output": 0.0000044
    },
    "gpt-4o-mini-search-preview-2025-03-11": {
        "input": 0.00000015,
        "cached_input": None,
        "output": 0.0000006
    },
    "gpt-4o-search-preview-2025-03-11": {
        "input": 0.0000025,
        "cached_input": None,
        "output": 0.00001
    },
    "computer-use-preview-2025-03-11": {
        "input": 0.000003,       # $3.00 / 1,000,000
        "cached_input": None,
        "output": 0.000012       # $12.00 / 1,000,000
    }
}



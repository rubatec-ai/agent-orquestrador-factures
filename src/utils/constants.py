MAX_RESULTS_GMAIL = 100
PAGE_SIZE_DRIVE = 1000
MAX_WORDS_FIRST_PAGE = 333

# Mapping of valid 4-digit SIE codes to their descriptions
SIE_MAP = {
    "1924": "Entorns Escoles Lot 5",
    "1934": "Sot.Linies Sarria Horta Lots 1-2-3",
    "1992": "Millora Luminic.Hospitalet Ll",
    "2020": "Pla Endreça Lot 1",
    "2021": "Pla Endreça Lot 5",
    "1860": "Mant.Col·lectors 2020-23",
    "1899": "Mant.Obra Civil BSM",
    "1919": "Mant. Mobiliari Urba BCN",
    "1936": "Mant.Via Public.El Masnou Lot 1",
    "1937": "Motors Obres 1",
    "1942": "Mant.Via Public. Mollet del Valles",
    "1959": "Mant.Plaques Carrers BCN",
    "1961": "F.O.Agramunt",
    "1963": "F.O.Peramola",
    "1964": "F.O.Ulldecona-Alcanar",
    "1967": "F.O. Collbato",
    "1968": "F.O.Estamariu - Bellver de Cerdanya",
    "1974": "Claveguerons Sant Cugat del Valles",
    "1978": "Gestio Operativa ciutat de Barcelona",
    "1982": "Menors Infraest. 2024",
    "1998": "F.O. Tragsa Lot 5",
    "2000": "F.O. Tragsa Lot 6",
    "2010": "Revestim.Cunetes Lot 2",
    "2011": "F.O. Sanaüja",
    "2012": "Mant.Via Publ.St.Just Lot 1",
    "2013": "Mant.Edificis St.Just Lot 2",
    "2014": "Mantenim. Platges BCN",
    "2035": "Menors Infraest.2025",
    "2036": "Soterrament Linies Lot 2",
    "1870": "Menors Infraestruct. 2021",
    "1914": "Obres PMI LOT 3",
    "1933": "F.O. Borges Blanques",
    "1935": "Menors Infraest.2023",
    "1962": "F.O.Capmany-Garriguella",
    "1969": "F.O. Tarragona-Plana LLeida(Fase 10)",
    "1834": "Manteniment Horta Lot7",
    "1865": "Ponts Diputació 2019-21",
    "1877": "Ute Rossello",
    "1878": "Obres Edar Garcia",
    "1921": "Reurb.C/Abat Escarre El Papiol",
    "1929": "Xarxa ATL Zona Nord",
    "1938": "Motors Obres 2",
    "1948": "Xarxa ATL Zona Sud",
    "1957": "Carrer Loreto",
    "1975": "Reurbanització Camp de l'Arpa",
    "1976": "Estacions Bicing",
    "1981": "Abastament del Cardener",
    "1993": "ATL Cabalimetres 2 Lots",
    "1997": "Emergencia ATL Sant Celoni",
    "2009": "Talus Elisa Moragas Lot-3",
    "2017": "Ute Urbanització Segur Calafell",
    "2019": "Jocs Jardins Celestina Vigneaux",
    "2039": "Obres Edar Esponellà",
    "1001": "Simmar",
    "1791": "UTE Edars Segria",
    "1854": "Edar Sta Coloma de Farnés",
    "1876": "Edar Martorell",
    "1888": "Edars Ribera d'Ebre (2022-25)",
    "1889": "Edars Priorat (2022-25)",
    "1890": "Edars Terra Alta (2022-25)",
    "1983": "Millora EDARU Valls",
    "2002": "Edar Mas Pins",
    "2026": "Serveis de Sanejament Vallirana",
    "1895": "Reurb.Carrer Romans",
    "1922": "Millora Parc de Can Buxeres",
    "1714": "Neteja Horta Esportiva",
    "1835": "Reclamacions Netej.FGC",
    "1851": "Mant.Agenc.Salut Publica BCN",
    "1857": "Mant. Ins.Cat.Salut BCN",
    "1881": "IES Ventura Gassol Bdn",
    "1926": "Mant.Menors 2023",
    "1939": "Nau Cornellà",
    "1953": "Mant. Castelldefels",
    "1960": "Manteniment MUTUAM",
    "1970": "Manteniment A.C.P.C 2023",
    "1973": "Nau Viladecans",
    "1985": "Dep. Manteniment",
    "1991": "Mant. ADIF Tarragona-Lleida",
    "1996": "BSM A.Marc Instal.lacions",
    "2004": "Clima Inst.Mpal.Mercats BCN",
    "2005": "Inst.Comiss.Mossos Lleida",
    "2008": "Mant. Dep. Presidència",
    "2018": "P.Recàrrega Aparcaments BSM",
    "2025": "ICS Centres Atenció Primaria 2024",
    "2027": "ACPC Clima MHC",
    "2042": "Mant.Menors preventiu 2025",
    "1764": "Enll. La Roca del Vallès",
    "1780": "Enllumenat Sentmenat",
    "1783": "Enllumenat Calafell",
    "1792": "Enllumenat Begues",
    "1795": "UTE Enllumenat BCN",
    "1855": "Enllumenat Tercers 2020",
    "1882": "Ute Enllumenat BCN 2",
    "1892": "Enllumenat Cornellà 2022-25",
    "1911": "Enllumenat Cubelles",
    "1925": "Enllumenat Tercers 2023",
    "1941": "Llumeneres LED Montornès",
    "1954": "Lluminàries Sta Coloma Gramanet",
    "1966": "Enllumenat Sitges",
    "1987": "Dep. Enllumenat",
    "2028": "Enllumenat Tercers 2025",
    "2038": "PRI Enllumenat 2025 Lot5",
    "2721": "Enllumenat Cornella",
    "2722": "Enllumenat Salou",
    "2727": "UTE Instal·lacions Anoia",
    "1875": "Plaques Fotovoltaiques",
    "1905": "ICS Plaques Fotovoltaiques",
    "1946": "Plaques Fotov.Fase II",
    "1956": "Acord Marc ACM",
    "1965": "UTE Esco Fotovoltaiques",
    "1971": "F.V  Biblioteca Martorell",
    "1979": "Tercers F.V",
    "1980": "Fotolineres AMB",
    "1986": "Dep. Fotovoltaica",
    "1990": "F.V. Ajuntament Batea",
    "2006": "Mant. Punts Recàrrega",
    "2007": "F.V Garrigues",
    "2030": "FV C-32 Santa Susana",
    "2033": "FV Costa Brava",
    "2037": "Tercers FV 2025",
    "2040": "Manteniments FV 2025",
    "1850": "Mant.Parc Rec.Biomedica BCN",
    "1880": "Obres RAM Infraestructures.cat",
    "1900": "Instal.lac. Tercers 2022",
    "1906": "Punts Recàrrega BSM",
    "1907": "P. Recàrrega bases FMB",
    "1916": "Paleteria ZAL Port",
    "1927": "Obres REACT Infraestructues",
    "1950": "Reforma Cap Rambla",
    "1972": "Manteniment AHC 2023",
    "1984": "Dep. Edificació",
    "2022": "Obres CAP Can Gibert Girona",
    "2023": "Obres CAP Chafarinas BCN",
    "2024": "Obres CAP Sant Quirze",
    "1775": "Manteniment Port Vell",
    "1822": "Net.ADIF Tarragona-Lleida",
    "1823": "Neteja I.C.U.B.",
    "1849": "Net.ADIF Sants-Pº.GRacia-França",
    "1864": "Mant.Serv. Center CILSA",
    "1871": "Mant.Ins.Elèctriques APB",
    "1884": "Net. Museu Martítim BCN",
    "1886": "Enllumenat ZAL Port",
    "1891": "Mant. Port Olímpic",
    "1893": "Equip.Infraestructures.cat",
    "1947": "Neteja ACPC",
    "1988": "Dep. Facilities",
    "2016": "Neteja edificis APB",
    "2031": "Manteniment Parkings BSM",
    "2034": "Infras D.A i Treball",
    "1995": "ACM Enllumenat",
    "2015": "Enllumenat Vielha",
    "2029": "Millora Enllum.Parc Tarragona",
    "1759": "Direcc.Gral.d'acció Civica I Comun.",
    "1796": "Mantenim. Menors 2017",
    "1815": "Manteniment A.C.P.C.",
    "1908": "Ventilació c/Elisabets",
    "1904": "Obres Enllum.Vilanova",
    "1912": "Obres Enllumenat  TMB",
    "1955": "Obres Enllum. Sabadell",
    "1958": "Obres Enllum. Reus",
    "1989": "Obres Enll. Calaf",
    "2003": "Obres Enllumenat Viladecavalls",
    "1952": "Fotovoltaiques Castellnou Bages",
    "1913": "P.Recàrrega Badalona",
    "1847": "Ute Instal.Tunel Glories",
    "1915": "Ute Manteniment Rondes 2022",
    "1944": "Ute Tunel Borras",
    "1977": "Ute Ponts Ronda Litoral",
    "1994": "Ute Complex Central",
    "2032": "Tunel Amadeu Torner",
    "2041": "Dep. Ctes. Singulars",
    "1930": "Central Rbt",
    "8900": "Estructura"
}

# Build a newline-separated snippet from the mapping for use in prompts
MAPPING_SNIPPET = "\n".join([f"{code}: {desc}" for code, desc in SIE_MAP.items()])

VALID_CANAL_SIE = list(SIE_MAP.keys())

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

EXTRACTED_DATA_OPENAI = ['line_item', 'vat', 'marca_temporal_ocr', 'text', 'canal_sie', 'fr_proveedor', 'proveedor',
                         'forma_pago', 'supplier_id']

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
    "canal_sie": "desconocido",
    "fr_proveedor": "desconocido",
    "proveedor": "desconocido",
    "forma_pago": "desconocido",
    "supplier_id": "desconocido",
}

PARAMETERS_TO_SEARCH = {
    "canal_sie": (
        "Extract the four-digit internal contract identifier (canal_sie) from the invoice.\n"
        "1. WHERE TO LOOK:\n"
        "   - Project or work descriptions\n"
        "   - Near keywords: 'Obra', 'Manteniment', 'Referencia'\n"
        "   - Sections: 'Observaciones', 'Comentarios', 'Descripción', 'Description'\n"
        "   - Any part of the invoice text\n"
        "\n"
        "3. IDENTIFICATION CRITERIA:\n"
        "   - Must be exactly 4 digits\n"
        "   - Must appear as a standalone sequence (not part of a larger number)\n"
        "   - Use regex: '\\b\\d{4}\\b' to find standalone four-digit sequences\n"
        "\n"
        "4. EXTRACTION EXAMPLES:\n"
        "   - 'OBRA 2020 PLAN ENDRESA LOTE 1' → canal_sie: '2020'\n"
        "   - 'O.2010 REVESTIMENT DE CUNETES BV-2433' → canal_sie: '2010'\n"
        "   - 'OBRA 2021 MANTENIMENT DE LA SUPERFICIE EN ESPAI PUBLIC DE BARCELONA' → canal_sie: '2021'\n"
        "   - 'Obra: 2023 CAP CHAFARINAS BCN' → canal_sie: '2023'\n"
        "   - '2014 MANTENIM. PLATGES BCN' → canal_sie: '2014'\n"
        "   - 'Obra: 1926-MENORS' → canal_sie: '1926'\n"
        "   - 'Referència Client Contrato 23100216 - 1972 AHC' → canal_sie: '1972'\n"
        "   - 'Referencia: RF-18376 CONTRATO 1916-ZAL' → canal_sie: '1916'\n"
        "   - 'S/Pedido: 1960/ MANTENIMENT MUTUAM' → canal_sie: '1960'\n"
        "If no valid four-digit identifier is found, return 'desconocido'."
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

PROMPT_SIE = (
    "OBJECTIVE: Extract the four-digit internal contract identifier (canal_sie) from the invoice.\n"
    "\n"
    "INSTRUCTIONS:\n"
    "1. Search for valid SIE codes using this reference mapping:\n"
    f"{MAPPING_SNIPPET}\n"
    "\n"
    "2. WHERE TO LOOK:\n"
    "   - Project or work descriptions\n"
    "   - Near keywords: 'Obra', 'Manteniment', 'Referencia'\n"
    "   - Sections: 'Observaciones', 'Comentarios', 'Descripción', 'Description'\n"
    "   - Any part of the invoice text\n"
    "\n"
    "3. IDENTIFICATION CRITERIA:\n"
    "   - Must be exactly 4 digits\n"
    "   - Must appear as a standalone sequence (not part of a larger number)\n"
    "   - Must match one of the valid codes from the mapping\n"
    "   - Use regex: '\\b\\d{4}\\b' to find standalone four-digit sequences\n"
    "\n"
    "4. EXTRACTION EXAMPLES:\n"
    "   - 'OBRA 2020 PLAN ENDRESA LOTE 1' → canal_sie: '2020'\n"
    "   - 'O.2010 REVESTIMENT DE CUNETES BV-2433' → canal_sie: '2010'\n"
    "   - 'OBRA 2021 MANTENIMENT DE LA SUPERFICIE EN ESPAI PUBLIC DE BARCELONA' → canal_sie: '2021'\n"
    "   - 'Obra: 2023 CAP CHAFARINAS BCN' → canal_sie: '2023'\n"
    "   - '2014 MANTENIM. PLATGES BCN' → canal_sie: '2014'\n"
    "   - 'Obra: 1926-MENORS' → canal_sie: '1926'\n"
    "   - 'Referència Client Contrato 23100216 - 1972 AHC' → canal_sie: '1972'\n"
    "   - 'Referencia: RF-18376 CONTRATO 1916-ZAL' → canal_sie: '1916'\n"
    "   - 'S/Pedido: 1960/ MANTENIMENT MUTUAM' → canal_sie: '1960'\n"
    "\n"
    "5. RESPONSE FORMAT:\n"
    "   Return ONLY a valid JSON object:\n"
    "   {\"canal_sie\": \"####\"}\n"
    "\n"
    "IMPORTANT: If no valid code is found, return {\"canal_sie\": desconocido}\n"
)

# Model pricing constants (as of 2025-01-13)

# Define model costs per token
MODELS_COST = {
    "gpt-4.5-preview-2025-02-27": {
        "input": 0.000075,  # $75.00 / 1,000,000
        "cached_input": 0.0000375,  # $37.50 / 1,000,000
        "output": 0.00015  # $150.00 / 1,000,000
    },
    "gpt-4o-2024-08-06": {
        "input": 0.0000025,  # $2.50 / 1,000,000
        "cached_input": 0.00000125,
        "output": 0.00001
    },
    "gpt-4o-audio-preview-2024-12-17": {
        "input": 0.0000025,
        "cached_input": None,  # Sin valor en la tabla
        "output": 0.00001
    },
    "gpt-4o-realtime-preview-2024-12-17": {
        "input": 0.000005,  # $5.00 / 1,000,000
        "cached_input": 0.0000025,
        "output": 0.00002
    },
    "gpt-4o-mini-2024-07-18": {
        "input": 0.00000015,  # $0.15 / 1,000,000
        "cached_input": 0.000000075,  # $0.075 / 1,000,000
        "output": 0.0000006  # $0.60 / 1,000,000
    },
    "gpt-4o-mini-audio-preview-2024-12-17": {
        "input": 0.00000015,
        "cached_input": None,
        "output": 0.0000006
    },
    "gpt-4o-mini-realtime-preview-2024-12-17": {
        "input": 0.0000006,  # $0.60 / 1,000,000
        "cached_input": 0.0000003,  # $0.30 / 1,000,000
        "output": 0.0000024  # $2.40 / 1,000,000
    },
    "o1-2024-12-17": {
        "input": 0.000015,  # $15.00 / 1,000,000
        "cached_input": 0.0000075,
        "output": 0.00006
    },
    "o1-pro-2025-03-19": {
        "input": 0.00015,  # $150.00 / 1,000,000
        "cached_input": None,
        "output": 0.0006  # $600.00 / 1,000,000
    },
    "o3-mini-2025-01-31": {
        "input": 0.0000011,  # $1.10 / 1,000,000
        "cached_input": 0.00000055,
        "output": 0.0000044  # $4.40 / 1,000,000
    },
    "o1-mini-2024-09-12": {
        "input": 0.0000011,  # $1.10 / 1,000,000
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
        "input": 0.000003,  # $3.00 / 1,000,000
        "cached_input": None,
        "output": 0.000012  # $12.00 / 1,000,000
    }
}
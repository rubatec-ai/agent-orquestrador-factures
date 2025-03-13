import json
import os

import pdfplumber
import logging
import camelot
import pandas as pd
import re

import spacy
from langdetect import detect, LangDetectException

from src.config import ConfigurationManager
from typing import Tuple, Dict, List

from src.utils.embeddings import preprocess_text, calculate_embeddings, ensure_array
from src.utils.utils import replace_nan_values


def extract_paragraphs_from_pdf(pdf_path: str,
                                pdf_name: str,
                                logger: logging.Logger,
                                shy_log_interval: int = 10) -> List[Dict]:
    """
    Extract paragraphs from a PDF, page by page.
      - No section or heading detection.
      - Splits on double newlines to define paragraphs.
      - Cleans extra spaces to avoid weird spacing.
      - Logs only every 'shy_log_interval' pages.

    Returns a list of dicts with keys:
        'path', 'page', 'paragraph'
    """

    paragraphs_data = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"Started processing PDF '{pdf_name}' with {total_pages} pages.")

            for page_number, page in enumerate(pdf.pages, start=1):

                # Minimal logging to show progress
                if page_number % shy_log_interval == 0:
                    logger.info(f"[{pdf_name}] Processed {page_number}/{total_pages} pages...")

                # Extract text from page
                text = page.extract_text() or ""

                # Split into paragraphs by double newlines
                raw_paragraphs = text.split("\n\n")

                # Clean and store
                for para in raw_paragraphs:
                    # Remove extra spaces, trim ends
                    clean_para = re.sub(r"\s+", " ", para).strip()

                    if clean_para:  # Skip if empty
                        paragraphs_data.append({
                            'name': pdf_name,
                            'page': page_number,
                            'paragraph': clean_para
                        })

            logger.info(f"Finished processing PDF '{pdf_name}'.")

    except Exception as e:
        logger.error(f"Error reading {pdf_path}: {e}")

    return paragraphs_data


def extract_tables_from_pdf(pdf_path: str,
                            logger: logging.Logger,
                            flavor="lattice",
                            pages="all",
                            min_fill_ratio=0.5,
                            merge_tolerance=10.0) -> dict:
    """
    1) Extract all tables from 'pdf_path' using Camelot.
    2) Remove tables that are mostly empty (< min_fill_ratio).
    3) Attempt to merge tables that are on the same page and share similar columns,
       if their bounding boxes are close vertically (within merge_tolerance).
    4) Return a dict { "table_1": df, "table_2": df, ... }.

    :param pdf_path: Path to the PDF
    :param logger: Python logger
    :param flavor: 'lattice' or 'stream' (Camelot extraction mode)
    :param pages: pages to parse (default = 'all')
    :param min_fill_ratio: e.g. 0.7 means at least 70% of cells are non-empty
    :param merge_tolerance: vertical gap threshold for merging consecutive tables (same page)
    :return: dictionary of final tables as Pandas DataFrames
    """
    tables_dict = {}

    try:
        # 1) Extract tables with Camelot
        all_tables = camelot.read_pdf(pdf_path, pages=pages, flavor=flavor)
        logger.info(f"Found {len(all_tables)} raw tables in {pdf_path} using {flavor} flavor.")

        if len(all_tables) == 0:
            return {}

        # 2) Convert them into a list of objects with more info (page, df, bbox)
        #    Camelot Table object has table.page, table.df, table._bbox, etc.
        table_objects = []
        for i, table in enumerate(all_tables):
            df_table = table.df.copy()
            page_num = table.page
            bbox = table._bbox  # (x1, y1, x2, y2)

            # Build a small structure
            table_objects.append({
                "original_idx": i,
                "page": page_num,
                "df": df_table,
                "bbox": bbox  # bounding box can help us figure out merges
            })

        # 3) Group by page
        from collections import defaultdict
        tables_by_page = defaultdict(list)
        for t in table_objects:
            tables_by_page[t["page"]].append(t)

        # 4) Within each page, sort by the bounding box's top coordinate (y2)
        #    Camelot's bbox = (x1, y1, x2, y2), typically y2 is top, y1 is bottom
        #    But confirm with your version. We'll assume y2 is top and smaller = higher up
        for page_num in tables_by_page:
            tables_by_page[page_num].sort(key=lambda x: x["bbox"][3], reverse=True)
            # So now the tables go from top to bottom

        # 5) Merge consecutive tables if they have the same (or nearly the same) number of columns
        #    and their vertical gap is within merge_tolerance
        merged_tables = []
        for page_num, tlist in tables_by_page.items():
            merged_list_for_page = []
            current_block = None

            for table_info in tlist:
                df_candidate = table_info["df"]
                candidate_bbox = table_info["bbox"]
                n_cols_candidate = df_candidate.shape[1]

                if current_block is None:
                    # start a new block
                    current_block = table_info
                else:
                    # check if we can merge with current_block
                    current_df = current_block["df"]
                    current_bbox = current_block["bbox"]
                    n_cols_current = current_df.shape[1]

                    # compute vertical gap: distance from bottom of the current block to top of the candidate
                    # current_bbox[1] is bottom, current_bbox[3] is top
                    # We'll say gap = current_block's y1 - candidate_bbox y2 (some geometry)
                    current_bottom = current_bbox[1]
                    candidate_top = candidate_bbox[3]
                    vertical_gap = abs(current_bottom - candidate_top)

                    # check if number of columns matches (or is close)
                    # You might do a stricter check if columns are the same or if there's a small difference
                    same_cols = (n_cols_candidate == n_cols_current)

                    if same_cols and vertical_gap <= merge_tolerance:
                        # Attempt row-wise merge
                        # naive approach: just append rows
                        merged_df = pd.concat([current_df, df_candidate], axis=0, ignore_index=True)
                        # update the bounding box => the top is the same as the old top, the bottom is new
                        new_bbox = (
                            min(current_block["bbox"][0], table_info["bbox"][0]),
                            min(current_block["bbox"][1], table_info["bbox"][1]),
                            max(current_block["bbox"][2], table_info["bbox"][2]),
                            max(current_block["bbox"][3], table_info["bbox"][3]),
                        )
                        current_block = {
                            "page": page_num,
                            "df": merged_df,
                            "bbox": new_bbox
                        }
                    else:
                        # can't merge => finalize the old block and start a new one
                        merged_list_for_page.append(current_block)
                        current_block = table_info

            # after the loop ends, flush the last block
            if current_block is not None:
                merged_list_for_page.append(current_block)

            merged_tables.extend(merged_list_for_page)

        # 6) Now filter out tables that have < min_fill_ratio
        #    We'll define "fill ratio" = #non-empty cells / total cells
        final_tables = []
        for t in merged_tables:
            df_ = t["df"]
            total_cells = df_.size  # rows*columns
            non_empty = df_.astype(bool).sum().sum()  # simplistic: counts non-empty strings
            fill_ratio = non_empty / total_cells if total_cells > 0 else 0

            if fill_ratio < min_fill_ratio:
                logger.debug(f"Dropping table on page {t['page']} with fill_ratio={fill_ratio:.2f}.")
                continue

            # (Optional) remove all-empty rows/columns
            df_ = _clean_table(df_)

            # if after cleaning, it's too small, skip
            if df_.shape[0] == 0 or df_.shape[1] == 0:
                continue

            final_tables.append(t)

        # 7) Build a final dictionary
        # We’ll enumerate them as table_1, table_2, ...
        for i, t in enumerate(final_tables, start=1):
            tables_dict[f"table_{i}"] = t["df"]

    except Exception as e:
        logger.error(f"Error extracting tables from {pdf_path}: {e}")

    return tables_dict

def table_to_json(table_df: pd.DataFrame, table_id: str, pdf_name: str) -> str:
    """
    Converts a table DataFrame into a structured JSON format to improve LLM processing.

    Args:
        table_df (pd.DataFrame): The table to convert.
        table_id (str): Identifier for the table (e.g., "table_1").
        pdf_name (str): Name of the PDF file.

    Returns:
        str: A JSON string representation of the table.
    """
    table_data = {
        "document": pdf_name,
        "table_id": table_id,
        "columns": list(table_df.columns),
        "rows": []
    }

    for row_idx, row in table_df.iterrows():
        row_dict = {"row_index": row_idx + 1}
        for col_name, value in row.items():
            row_dict[col_name] = str(value)  # Convert all values to string for uniformity
        table_data["rows"].append(row_dict)

    return json.dumps(table_data, indent=4, ensure_ascii=False)


def _clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all-empty rows and all-empty columns from the DataFrame.
    """
    # Convert any empty strings or whitespace to NaN
    df = df.replace(r"^\s*$", None, regex=True)

    # Drop rows that are all NaN
    df = df.dropna(axis=0, how='all')
    # Drop columns that are all NaN
    df = df.dropna(axis=1, how='all')

    # Optionally reset index
    df = df.reset_index(drop=True)
    return df


def create_master_dataframe_and_tables(df: pd.DataFrame,
                                       config: ConfigurationManager,
                                       logger: logging.Logger
                                       ) -> pd.DataFrame:
    all_paragraphs = {}
    pdf_tables = {}

    for idx, row in df.iterrows():
        pdf_path = row['path']
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]

        # (1) Extract paragraphs
        paragraphs_data = extract_paragraphs_from_pdf(
            pdf_path=pdf_path,
            pdf_name=pdf_name,
            logger=logger
        )
        all_paragraphs[pdf_name] = paragraphs_data

        # (2) Extract or skip tables
        if config.etl_param_use_tables:
            # Use the robust version:
            tables_dict = extract_tables_from_pdf(
                pdf_path=pdf_path,
                logger=logger,
                flavor='lattice',  # or 'stream'
                min_fill_ratio=0.7,
                merge_tolerance=25.0
            )
            pdf_tables[pdf_name] = tables_dict
        else:
            pdf_tables[pdf_name] = {}

    # Flatten paragraphs into one master list
    master_list = []
    for pdf_name, paragraphs_data in all_paragraphs.items():
        master_list.extend(paragraphs_data)

    # Add table data to the master list
    for pdf_name, tables_dict in pdf_tables.items():
        for table_id, table_df in tables_dict.items():
            # Convert the table to text
            table_text = table_to_json(table_df, table_id, pdf_name=pdf_name)  # Adjust page number as needed
            master_list.append({
                'name': pdf_name,
                'page': -1,
                'paragraph': table_text
            })

    # Create the master DataFrame
    master_df = pd.DataFrame(master_list, columns=['name', 'page', 'paragraph'])

    return master_df


def quality_check_master_df(df: pd.DataFrame,
                            nlp_ca: spacy.Language,
                            nlp_es: spacy.Language,
                            config: ConfigurationManager,
                            logger: logging.Logger) -> pd.DataFrame:
    """
    Processes each PDF (grouped by 'name') separately.
    For each row in the PDF subset (with columns like 'name', 'page', 'paragraph'):
      1) Auto-detect language (Catalan or Spanish), store it as 'detected_lang'.
      2) Parse with spaCy -> store 'token_count'.
      3) If 'token_count' < short_limit, merge with previous row (if any).
      4) If 'token_count' > token_threshold, split into chunks (never splitting sentences).
      5) Otherwise, keep as is.

    After building the final list of rows, we remove any rows that remain below 'short_limit'.

    Returns a new DataFrame that respects merges/splits, ensuring we never merge paragraphs
    from different PDFs, storing language and token_count in one pass for performance.
    """

    token_threshold = config.etl_param_chunk_tokens  # e.g. 200
    short_limit = config.etl_param_min_chunk_tokens  # e.g. 10

    logger.info(f"[quality_check_master_df] token_threshold={token_threshold}, short_limit={short_limit}")

    final_rows = []
    pdf_names = df['name'].unique()

    for pdf_name in pdf_names:
        logger.info(f"Processing PDF group: '{pdf_name}'")

        # Subset for this PDF, sort by page
        pdf_df = df[df['name'] == pdf_name].sort_values(by='page').reset_index(drop=True)

        processed_rows = []
        total_rows = len(pdf_df)

        for idx, row in pdf_df.iterrows():
            page_number = row.get('page', None)

            if page_number<0:
                processed_rows.append({
                    'name': pdf_name,
                    'page': page_number,
                    'paragraph': row.get('paragraph', ""),
                    'detected_lang': "json",
                    'token_count': -1
                })
                logger.debug(f"Skipping NLP processing for JSON paragraph in '{pdf_name}'.")

            else:
                paragraph_text = (row.get('paragraph', "") or "").strip()

                if (idx + 1) % 50 == 0:
                    logger.info(f"  -> Processed {idx + 1}/{total_rows} paragraphs for PDF '{pdf_name}'...")

                if not paragraph_text:
                    # Empty text, store zero tokens or skip
                    processed_rows.append({
                        'name': pdf_name,
                        'page': page_number,
                        'paragraph': paragraph_text,
                        'detected_lang': "unknown",
                        'token_count': 0
                    })
                    continue

                # 1) Detect language
                try:
                    detected_lang = detect(paragraph_text[:300])
                except LangDetectException:
                    detected_lang = "unknown"

                # 2) Choose spaCy model
                if detected_lang == "ca":
                    nlp = nlp_ca
                    lang_str = "ca"
                elif detected_lang == "es":
                    nlp = nlp_es
                    lang_str = "es"
                else:
                    # fallback to Catalan
                    nlp = nlp_ca
                    lang_str = f"ca"  # fallback

                doc = nlp(paragraph_text)
                num_tokens = len(doc)

                logger.debug(f"[{pdf_name} row={idx}] lang={lang_str}, tokens={num_tokens}")

                # === MAIN LOGIC ===
                if num_tokens < short_limit and len(processed_rows) > 0:
                    # A) Merge short paragraph with the previous row's paragraph
                    logger.debug(f"Paragraph has only {num_tokens} tokens, merging with previous row.")
                    last_idx = len(processed_rows) - 1
                    merged_text = processed_rows[last_idx]['paragraph'] + " " + paragraph_text
                    merged_text = merged_text.strip()

                    # Remove the last row
                    popped = processed_rows.pop()
                    # We keep the same 'lang' from the popped row if you prefer
                    # or you can re-detect. We'll keep parent's language for simplicity:
                    # If you want to re-detect, uncomment below:
                    #   try:
                    #       detected_lang = detect(merged_text[:300])
                    #   except LangDetectException:
                    #       detected_lang = "unknown"
                    #   # etc.

                    merged_doc = nlp(merged_text)
                    merged_tokens = len(merged_doc)

                    # If merged text now is above threshold => split
                    if merged_tokens > token_threshold:
                        logger.debug(f"Merged paragraph => {merged_tokens} tokens, splitting.")
                        split_rows = _split_large_paragraph(
                            merged_text,
                            pdf_name,
                            page_number,
                            token_threshold,
                            nlp,
                            lang_str
                        )
                        processed_rows.extend(split_rows)
                    else:
                        # Reinsert as one chunk
                        processed_rows.append({
                            'name': pdf_name,
                            'page': page_number,
                            'paragraph': merged_text,
                            'detected_lang': popped['detected_lang'],  # or lang_str
                            'token_count': merged_tokens
                        })

                elif num_tokens <= token_threshold:
                    # B) Keep as is
                    processed_rows.append({
                        'name': pdf_name,
                        'page': page_number,
                        'paragraph': paragraph_text,
                        'detected_lang': lang_str,
                        'token_count': num_tokens
                    })

                else:
                    # C) Split large paragraph
                    logger.debug(f"Paragraph => {num_tokens} tokens, exceeding threshold => splitting.")
                    split_rows = _split_large_paragraph(
                        paragraph_text,
                        pdf_name,
                        page_number,
                        token_threshold,
                        nlp,
                        lang_str
                    )
                    processed_rows.extend(split_rows)

        # End for each paragraph in this PDF
        final_rows.extend(processed_rows)

    # Build the DataFrame with columns including 'detected_lang' and 'token_count'
    output_df = pd.DataFrame(
        final_rows,
        columns=['name', 'page', 'paragraph', 'detected_lang', 'token_count']
    )

    logger.info(
        f"[quality_check_master_df] merges/splits done. Now removing leftover short paragraphs (< {short_limit} tokens).")

    # === FINAL FILTER: Remove rows that remain < short_limit tokens ===
    filtered_df = output_df[(output_df['token_count'] >= short_limit) | (output_df['token_count'] == -1)].copy()
    logger.info(f"[quality_check_master_df] completed: original={len(df)}, new={len(filtered_df)} rows.")

    return filtered_df


def _split_large_paragraph(paragraph_text: str,
                           pdf_name: str,
                           page_number: int,
                           token_threshold: int,
                           nlp: spacy.Language,
                           lang_code: str) -> List[Dict]:
    """
    Helper to split a paragraph that exceeds the token threshold.
    Splits by spaCy sentences, never splitting a sentence.
    We parse the chunk with the same spaCy model (no re-detect language).

    Each chunk row includes 'detected_lang' = lang_code and 'token_count'.

    Returns a list of dicts:
      [{ 'name':..., 'page':..., 'paragraph':..., 'detected_lang':..., 'token_count':... }, ...]
    """
    doc = nlp(paragraph_text)
    big_rows = []
    current_chunk_sents = []
    current_chunk_token_count = 0

    def flush_chunk(sents):
        text = " ".join(sents).strip()
        # parse it to get token_count
        chunk_doc = nlp(text)
        return text, len(chunk_doc)

    for sent in doc.sents:
        sent_token_count = len(sent)
        # If adding this sentence exceeds threshold, flush current chunk
        if (current_chunk_token_count + sent_token_count) > token_threshold and current_chunk_sents:
            chunk_text, chunk_tokens = flush_chunk(current_chunk_sents)
            big_rows.append({
                'name': pdf_name,
                'page': page_number,
                'paragraph': chunk_text,
                'detected_lang': lang_code,
                'token_count': chunk_tokens
            })
            # reset
            current_chunk_sents = []
            current_chunk_token_count = 0

        current_chunk_sents.append(sent.text)
        current_chunk_token_count += sent_token_count

    # Flush leftover if any
    if current_chunk_sents:
        chunk_text, chunk_tokens = flush_chunk(current_chunk_sents)
        big_rows.append({
            'name': pdf_name,
            'page': page_number,
            'paragraph': chunk_text,
            'detected_lang': lang_code,
            'token_count': chunk_tokens
        })

    return big_rows


def transform_paragraph(
    df: pd.DataFrame,
    nlp_ca: spacy.Language,
    nlp_es: spacy.Language,
    config: ConfigurationManager,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Transforms a DataFrame by creating a unified 'master_df' of paragraphs/tables.
    If semantic_search_cosine_similarity_filter is enabled, applies quality checks
    and calculates embeddings; otherwise, skips these steps.
    """
    df = df.copy()

    # 1. Generate master DataFrame (combining raw text paragraphs + table data)
    master_df = create_master_dataframe_and_tables(
        df=df,
        config=config,
        logger=logger
    )

    # 2. If cosine similarity is enabled, perform the quality check
    if config.semantic_search_cosine_similarity_filter:
        master_df = quality_check_master_df(
            df=master_df,
            nlp_ca=nlp_ca,
            nlp_es=nlp_es,
            config=config,
            logger=logger
        )

    # 3. Assign a per-page index to each paragraph (useful for referencing)
    master_df["page_index"] = master_df.groupby("page").cumcount()

    # 4. Replace any NaN values (e.g., paragraph or table fields) with something uniform
    master_df = replace_nan_values(master_df, logger)

    # 5. If cosine similarity filter is enabled, preprocess text and compute embeddings
    if config.semantic_search_cosine_similarity_filter:
        master_df["input"] = master_df["paragraph"].apply(
            lambda x: preprocess_text(
                text=x,
                nlp_catalan=nlp_ca,
                nlp_spanish=nlp_es,
                n=config.etl_param_embedding_tokens
            )
        )

        # Compute embeddings (and ensure array format)
        master_df = calculate_embeddings(
            df=master_df,
            column="input",
            logger=logger,
            config=config
        )
        master_df["embedding"] = master_df["embedding"].apply(ensure_array)

    else:
        # If not using cosine similarity, we skip embeddings entirely
        master_df["detected_lang"] = "ca"
        master_df["input"] = None
        master_df["embedding"] = None

    return master_df


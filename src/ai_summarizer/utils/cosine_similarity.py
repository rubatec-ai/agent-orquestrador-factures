import logging
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Dict

def compute_cosine_similarity(
    scope_objects: List,
    master_objects: List,
    logger: logging.Logger
) -> List[Dict]:
    """
    Single-process function to compute cosine similarity between each Scope object
    and each Master object, selecting the correct language embedding (Catalan vs. Spanish)
    based on the Master object's `lang` field.

    Logs progress at debug level so you can see how far along the computation is.

    Args:
        scope_objects (List): List of Scope objects, each having .embedding_ca and .embedding_es.
        master_objects (List): List of Master objects, each with .embedding and .lang.
        logger (logging.Logger): Logger for logging debug info about progress.

    Returns:
        List[Dict]: A list of dictionaries with structure:
            {
                "scope_id": <scope parameter or ID>,
                "master_id": <master ID>,
                "similarity": <cosine similarity float>
            }
    """
    results = []

    # Calculate total number of comparisons:
    total_comparisons = len(scope_objects) * len(master_objects)
    comparisons_done = 0

    for i, scope in enumerate(scope_objects, 1):
        for master in master_objects:
            # Pick the matching language embedding
            if master.lang == 'ca':
                scope_embedding = scope.embedding_ca
            elif master.lang == 'es':
                scope_embedding = scope.embedding_es
            else:
                # Default to Catalan embedding if "lang" not recognized
                scope_embedding = scope.embedding_ca

            # Reshape to 2D for sklearn
            scope_vec = scope_embedding.reshape(1, -1)
            master_vec = master.embedding.reshape(1, -1)

            # Compute cosine similarity
            sim = cosine_similarity(scope_vec, master_vec)[0][0]

            # Add to results
            results.append({
                "parameter": scope.parameter,
                "master_id": master.id,
                "paragraph":master.paragraph,
                "similarity": sim
            })

            comparisons_done += 1
            # Every N comparisons, log progress. Adjust N to your preference
            if comparisons_done % 100 == 0:
                logger.debug(
                    f"Processed {comparisons_done}/{total_comparisons} comparisons..."
                )
    # At the end, you can log a final message
    logger.debug("Finished all cosine similarity computations.")
    return results

import os

import streamlit as st

from streamlitapp.utils.loaders import load_json, save_json
from streamlitapp.utils.utils import add_logo

# Main
add_logo()
st.title("Orquestador de Factures")
st.header("Configuration")
st.info("This page enables parameter configuration", icon="ℹ️")

# Check if JSON file exists
if not os.path.exists(st.session_state["json_file_path"]):
    st.error(f"File {st.session_state['json_file_path']} not found.")
else:
    data = load_json(st.session_state["json_file_path"])

    # Scenario Name
    st.markdown("#### Scenario Setup")
    data["directories"]["scenario_name"] = st.text_input(
        label="Scenario Name",
        value=data["directories"]["scenario_name"],
        help="Identificador o títol del projecte o escenari"
    )

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "**Files and agent**",
        "**ETL Parameters**",
        "**Agent Parameters**",
        "**Other Parameters**"
    ])

    ############################################################################
    # TAB 1 (NO CHANGES) - Keep your existing code as is
    ############################################################################
    with tab1:
        st.subheader("Files and agent")
        directories = data["directories"]

        directories["main_path"] = st.text_input(
            "Main Path",
            value=directories.get("main_path", ""),
            help="Ruta principal on es troba la informació (ex. workspace/agent-admin-pdfs/01_Data)"
        )
        directories["data_directory"] = st.text_input(
            "Data Folder",
            value=directories["data_directory"],
            help="Carpeta on es troben les dades d'entrada a Google Drive"
        )
        directories["logs_directory"] = st.text_input(
            "Logs Folder",
            value=directories["logs_directory"],
            help="Carpeta on es guarden els logs"
        )
        directories["transform_export_directory"] = st.text_input(
            "Transform Export Folder",
            value=directories["transform_export_directory"],
            help="Carpeta per guardar dades transformades"
        )
        directories["export_directory"] = st.text_input(
            "Export Folder",
            value=directories["export_directory"],
            help="Carpeta de sortida per resultats finals"
        )

    ############################################################################
    # TAB 2 - ETL PARAMETERS
    ############################################################################
    with tab2:
        st.subheader("ETL Parameters")
        etl_params = data["etl_param"]

        st.write(
            "Aquests paràmetres controlen la lectura i transformació de PDFs, i l'estructuració de dades abans de fer la cerca semàntica o crida a l'agent GPT.")

        # pdf_from_directory
        etl_params["pdf_from_directory"] = st.checkbox(
            "Carregar automàticament tots els PDFs des de la carpeta ./01_InputData/pdf?",
            value=etl_params.get("pdf_from_directory", True),
            help="Si està activat, buscarà tots els PDFs dins de la carpeta especificada i els processarà."
        )

        # use_debug_scope
        etl_params["use_debug_scope"] = st.checkbox(
            "Utilitzar dataset d'exemple (debug)?",
            value=etl_params.get("use_debug_scope", False),
            help="Si està activat, es farà servir un conjunt de dades de mostra en lloc de les dades reals."
        )

        # Conditionally show table extraction only if NOT in debug scope
        if not etl_params["use_debug_scope"]:
            etl_params["use_tables"] = st.checkbox(
                "Extreure taules (tables) dels PDFs?",
                value=etl_params.get("use_tables", True),
                help="Si està activat, l'ETL mirarà de llegir les taules dels PDFs a més del text."
            )

        # use_default_param
        etl_params["use_default_param"] = st.checkbox(
            "Utilitzar paràmetres predeterminats?",
            value=etl_params.get("use_default_param", True),
            help="Permet carregar paràmetres predefinits (PLIC,expedient, cpv, termini, visita, solvència...)."
        )

        # chunk_tokens
        etl_params["chunk_tokens"] = st.number_input(
            label="Mida de secció (chunk) en tokens",
            value=etl_params.get("chunk_tokens", 1000),
            min_value=200,
            step=1,
            help=(
                "Controla la mida màxima d'un paràgraf o chunk de text en tokens abans de ser embedit. "
                "Com més petit sigui el valor, més fragments generarem (i per tant, més costos d'embeddings)."
            )
        )

    ############################################################################
    # TAB 3 - AGENT PARAMETERS
    ############################################################################
    with tab3:
        st.subheader("Agent Parameters")
        st.write("Paràmetres que controlen el model i el comportament del resum/agent GPT.")

        agent_params = data["invoice_orchestrator"]["agent"]

        # GPT Model as a dropdown
        agent_params["model"] = st.selectbox(
            "GPT Model (for summarizer/agent)",
            options=["gpt-4o", "gpt-4o-mini"],
            index=["gpt-4o", "gpt-4o-mini"].index(
                agent_params.get("model", "gpt-4o")
            ),
            help="Trieu quin model GPT voleu utilitzar per a les tasques d'anàlisi o resum."
        )

        # Temperature as float input
        agent_params["temperature"] = st.number_input(
            "Temperature",
            value=float(agent_params.get("temperature", 0.15)),
            min_value=0.01,
            max_value=1.0,
            step=0.05,
            format="%.2f",
            help=(
                "Controla la 'creativitat' del model. "
                "Valors alts = respostes més variades, valors baixos = respostes més consistents."
            )
        )

    ############################################################################
    # TAB 4 - SEMANTIC SEARCH PARAMETERS
    ############################################################################
    with tab4:
        st.subheader("Semantic Search Parameters")
        st.write(
            "Aquests paràmetres defineixen com volem fer la cerca de similitud semàntica "
            "entre els diferents chunks o elements de dades."
        )
        sem_search = data["invoice_orchestrator"]["semantic_search"]

        # All against all
        sem_search["all_against_all"] = st.checkbox(
            "All-against-all comparisons?",
            value=sem_search.get("all_against_all", True),
            help=(
                "Si s'activa, es busquen tots els parèmetres dintre de totes les dades dels pdf a la vegada. "
                "Funciona per a la majoria de casos i és ho més barat."
                "Si està marcat, no utilitzarem cap filtre de cosinus similarity addicional."
            )
        )

        if not sem_search["all_against_all"]:
            # Cosine similarity filter
            sem_search["cosine_similarity_filter"] = st.checkbox(
                "Filtrar prèviament amb cosinus similarity? (recomanat per moltissímes dades, per reduir volum)",
                value=sem_search.get("cosine_similarity_filter", False),
                help=(
                    "Si està activat, primer es farà un filtre de semblança cosinus per "
                    "descartar fragments poc similars abans de la comparació més detallada."
                )
            )

            if sem_search["cosine_similarity_filter"]:
                # Similarity threshold
                sem_search["similarity_threshold"] = st.number_input(
                    "Similarity Threshold (llindar de semblança)",
                    value=float(sem_search.get("similarity_threshold", 0.7)),
                    min_value=0.01,
                    max_value=1.00,
                    step=0.05,
                    format="%.2f",
                    help="Els fragments amb una semblança inferior a aquest valor seran descartats."
                )

                # TopN similar
                sem_search["topn_similar"] = st.number_input(
                    "Top N (Similar Parts)",
                    value=int(sem_search.get("topn_similar", 10)),
                    min_value=1,
                    max_value=500,
                    step=1,
                    help="Número de fragments més semblants a retornar si no hi ha cap per sobre del llindar."
                )

    st.markdown("---")

    # Save edited JSON file
    if st.button("Save JSON"):
        save_json(data, st.session_state["json_file_path"])
        st.success(f"JSON file saved as {st.session_state['json_file_path']}")

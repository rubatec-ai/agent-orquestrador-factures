import os
import time
import re

import streamlit as st
from streamlitapp.utils.loaders import load_dfs_with_progress_bar, load_json
from streamlitapp.utils.utils import add_logo, apply_custom_styling

# Apply custom styling and add logo
apply_custom_styling()
add_logo()


def typewriter_effect(text, delay=0.001):
    """
    Simulates a typing effect by displaying text character by character.
    """
    placeholder = st.empty()
    for i in range(len(text) + 1):
        placeholder.markdown(text[:i], unsafe_allow_html=True)
        time.sleep(delay)


def display_answer(answer):
    """
    Splits the text by \[...\] blocks and displays
    each piece accordingly (plain text vs. LaTeX).
    """

    # 1. Handle None or NaN (or any other missing scenario)
    if answer is None:
        # Show nothing or a placeholder message
        st.write("No answer available.")
        return

    # 2. Convert anything non-string into a string
    if not isinstance(answer, str):
        answer = str(answer)

    # 3. Now safely use regex on the string
    pattern = re.compile(r'(\\\[.*?\\\])', re.DOTALL)
    segments = pattern.split(answer)

    for segment in segments:
        # If this segment is a LaTeX block
        if segment.startswith('\\[') and segment.endswith('\\]'):
            latex_expression = segment.replace('\\[', '').replace('\\]', '').strip()
            st.latex(latex_expression)
        else:
            # Plain text
            if segment.strip():
                typewriter_effect(segment)


# Main
st.title("Orquestador de Factures")
st.header("Results")

# Load JSON data
data = load_json(st.session_state["json_file_path"])
home_directory = os.path.expanduser('~')
output_path = os.path.join(
    home_directory,
    data["directories"]["main_path"],
    data["directories"]["export_directory"].replace("/", "\\")
)

# List folders in the output directory
dirlist = [
    filename
    for filename in os.listdir(output_path)
    if os.path.isdir(os.path.join(output_path, filename))
]

# Sidebar: Select folder
folder_to_load = st.sidebar.selectbox(label="Select folder 📁", options=dirlist)
dir_path = os.path.join(output_path, folder_to_load)

# Load results button
load_df_button = st.sidebar.button("Load results")
if load_df_button:
    load_dfs_with_progress_bar(dir_path, scenario='')

# Check if data is loaded
if "loaded_dfs" not in st.session_state:
    st.info(
        "This section will display the results of the semantic search generation and other related outputs.\n"
        "Please Load the results by clicking the button on the left (click again to reload).",
        icon="ℹ️"
    )
else:
    # Grab the solution DataFrame
    solution_df = st.session_state["loaded_dfs"]["solution"]

    # Iterate through each row in the DataFrame
    for index, row in solution_df.iterrows():
        parameter = row["parameter"]
        answer = row["answer"]

        # Display the parameter as a bold header
        st.markdown(f"**{parameter}**")

        # Display the answer with proper handling of LaTeX and plain text
        display_answer(answer)

        # Add a separator between parameter-answer pairs
        st.markdown("---")
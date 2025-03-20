import os
import sys

import streamlit as st

from PIL import Image

st.set_page_config(layout="wide")
CWD_PATH = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.join(CWD_PATH, "..")))
# Path to your JSON file
json_file_path = os.path.join(CWD_PATH, "config.json")
st.session_state["json_file_path"] = json_file_path

image_path = os.path.join(CWD_PATH, "logo.jpg")
logo = Image.open(image_path)
st.session_state["logo"] = logo

from streamlitapp.utils.utils import add_logo, apply_custom_styling

apply_custom_styling()
add_logo()
st.title("Orquestador de Factures")
st.header("Introduction")
st.write(
    """
    This application allows you to edit the configuration parameters for your semantic search in a user-friendly way. 
    Use the navigation bar on the left to switch between different sections.
    """
)

import streamlit as st

def add_logo():
    """
    Adds the FedEx logo to the Streamlit sidebar using CSS styling.
    """
    st.logo(st.session_state["logo"])
    st.markdown(
        """
        <style>
            img[data-testid="stLogo"] {
                height: 5rem
            }
        </style>
        """,
        unsafe_allow_html=True
    )


def apply_custom_styling():
    """
    Applies custom CSS styling to the Streamlit metrics.
    """
    custom_css = """
    <style>
        div[data-testid="stMetric"] {
            width: 100%;
            border-left: 5px solid #A100FF;
            padding-left: 10px;
            margin-right: 25px;
            background-color: #e6dcff;
            font-size: 20px;
        }
        div[data-testid="stMetricLabel"] > div {
            font-size: 14px;
        }
        div[data-testid="stMetricValue"] > div {
            font-size: 20px;
        }
        .metric-container {
            width: 100%;
            border-left: 0px solid #A100FF;
            padding-left: 0px;
            margin-right: 0px;
            background-color: rgba(230, 220, 255, 1);
            font-size: 6px;
            margin-bottom: 0px;
        }
        .metric-label {
            font-size: 14px;
        }
        .metric-value {
            font-size: 20px;
        }
        .tooltip {
            position: relative;
            display: inline-block;
        }
        .tooltip .tooltiptext {
            visibility: hidden;
            width: 100%;
            background-color: rgba(230, 220, 255, 0.5);
            color: #000;
            text-align: center;
            font-size: 10px;
            border-radius: 6px;
            padding: 5px;
            position: absolute;
            bottom: 110%;
            left: 0%;
            opacity: 0;
            transition: opacityrr 0.3s;
        }
        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
    </style>
    """

    # Inject the custom CSS styles
    st.markdown(custom_css, unsafe_allow_html=True)
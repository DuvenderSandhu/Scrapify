# config.py

import streamlit as st

def set_page_config():
    """Configure the Streamlit page settings"""
    st.set_page_config(
        page_title="Modern Web Scraper",
        page_icon="🕸️",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

def add_custom_styling():
    """Add custom CSS styling to the app"""
    st.markdown("""
    <style>
        /* Main container styling */
        .main {
            background-color: #f8f9fa;
        }
        
        /* Header styling */
        h1 {
            color: #2E7D32;
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }
        
        h2, h3 {
            color: #1565C0;
        }
        
        /* Progress bar styling */
        .stProgress > div > div > div > div {
            background-color: #2E7D32;
        }
        
        /* Button styling */
        .stButton button {
            font-weight: bold;
        }
        
        /* Log styling */
        .log-info {
            color: #1565C0;
            padding: 5px;
            margin: 2px 0;
            border-radius: 3px;
        }
        
        .log-success {
            color: #2E7D32;
            padding: 5px;
            margin: 2px 0;
            border-radius: 3px;
        }
        
        .log-warning {
            color: #FF8F00;
            padding: 5px;
            margin: 2px 0;
            border-radius: 3px;
        }
        
        .log-error {
            color: #C62828;
            padding: 5px;
            margin: 2px 0;
            border-radius: 3px;
        }
        
        .log-process {
            color: #6A1B9A;
            padding: 5px;
            margin: 2px 0;
            border-radius: 3px;
            font-weight: bold;
        }
        
        /* Status card styling */
        .status-card {
            background-color: #f1f8e9;
            border-radius: 5px;
            padding: 10px;
            margin-bottom: 10px;
            border-left: 4px solid #2E7D32;
        }
        
        /* URL card styling */
        .url-card {
            background-color: #e3f2fd;
            border-radius: 5px;
            padding: 5px 10px;
            margin-bottom: 5px;
            border-left: 4px solid #1565C0;
        }
        
        /* Field card styling */
        .field-card {
            background-color: #e8f5e9;
            border-radius: 5px;
            padding: 5px 10px;
            margin-bottom: 5px;
            border-left: 4px solid #2E7D32;
        }
        
        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 1px;
        }
        
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            background-color: #f8f9fa;
            border-radius: 4px 4px 0 0;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #e8f5e9;
            border-radius: 4px 4px 0 0;
            border-top: 4px solid #2E7D32;
            font-weight: bold;
        }
    </style>
    """, unsafe_allow_html=True)

# Configuration settings
SCRAPING_SETTINGS = {
    "DEFAULT_TIMEOUT": 30,  # seconds
    "USER_AGENT": "Modern Web Scraper Bot/1.0",
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 2,  # seconds
    "MAX_DEPTH": 3,
    "MAX_URLS_PER_DOMAIN": 10,
    "SUPPORTED_AI_MODELS": ["GPT-based", "Rule-based"],
    "DEFAULT_MODEL": "Rule-based"
}

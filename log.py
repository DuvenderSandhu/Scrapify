import streamlit as st
from datetime import datetime


def add_log(level, message):
    """Add a log entry"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = {
        "timestamp": timestamp,
        "level": level,
        "message": message
    }
    st.session_state.logs.append(log_entry)
    # Keep only the most recent 100 logs
    if len(st.session_state.logs) > 100:
        st.session_state.logs = st.session_state.logs[-100:]
    return log_entry

def log_info(message):
    return add_log("INFO", message)

def log_success(message):
    return add_log("SUCCESS", message)

def log_warning(message):
    return add_log("WARNING", message)

def log_error(message):
    return add_log("ERROR", message)

def log_process(message):
    return add_log("PROCESS", message)
log_process,log_error,log_warning,log_success,log_info,add_log
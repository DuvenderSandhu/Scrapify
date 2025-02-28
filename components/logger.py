# components/logger.py

from datetime import datetime

class Logger:
    """Real-time logging component for the web scraper"""
    
    def __init__(self):
        """Initialize the logger"""
        # Store logs in session state
        if 'log_data' not in st.session_state:
            st.session_state.log_data = []
        
        self.logs = st.session_state.log_data
        self.max_logs = 100  # Maximum number of logs to keep
    
    def _add_log(self, level, message):
        """Add a log entry with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        log_entry = {
            "timestamp": timestamp,
            "level": level,
            "message": message
        }
        
        self.logs.append(log_entry)
        
        # Keep only the most recent logs
        if len(self.logs) > self.max_logs:
            self.logs = self.logs[-self.max_logs:]
        
        # Update session state
        st.session_state.log_data = self.logs
    
    def info(self, message):
        """Log an informational message"""
        self._add_log("INFO", message)
    
    def success(self, message):
        """Log a success message"""
        self._add_log("SUCCESS", message)
    
    def warning(self, message):
        """Log a warning message"""
        self._add_log("WARNING", message)
    
    def error(self, message):
        """Log an error message"""
        self._add_log("ERROR", message)
    
    def process(self, message):
        """Log a process update message"""
        self._add_log("PROCESS", message)
    
    def clear(self):
        """Clear all logs"""
        self.logs = []
        st.session_state.log_data = []

# Required Streamlit import for the logger
import streamlit as st

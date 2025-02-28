# components/database.py

import streamlit as st
from datetime import datetime
from utils import generate_unique_id

class Database:
    """Simple in-memory database for storing scraping results"""
    
    def __init__(self):
        """Initialize the database"""
        # Store data in session state
        if 'database' not in st.session_state:
            st.session_state.database = {
                'raw_data': [],
                'extracted_data': []
            }
        
        self.data = st.session_state.database
    
    def save_raw_html(self, url, html_content):
        """Save raw HTML content from a URL"""
        item_id = generate_unique_id()
        
        item = {
            'id': item_id,
            'url': url,
            'html': html_content,
            'timestamp': datetime.now().isoformat(),
            'scrape_id': st.session_state.get('scrape_id', 'unknown')
        }
        
        self.data['raw_data'].append(item)
        st.session_state.database = self.data
        
        return item_id
    
    def save_extracted_data(self, raw_id, url, extracted_data):
        """Save extracted data linked to raw HTML"""
        item = {
            'id': generate_unique_id(),
            'raw_id': raw_id,
            'url': url,
            'data': extracted_data,
            'timestamp': datetime.now().isoformat(),
            'scrape_id': st.session_state.get('scrape_id', 'unknown')
        }
        
        self.data['extracted_data'].append(item)
        st.session_state.database = self.data
        
        return item['id']
    
    def save_data(self, url, extracted_data):
        """Save both raw and extracted data in one step"""
        # In a simplified version, we just save the extracted data directly
        item = {
            'id': generate_unique_id(),
            'url': url,
            'data': extracted_data,
            'timestamp': datetime.now().isoformat(),
            'scrape_id': st.session_state.get('scrape_id', 'unknown')
        }
        
        self.data['extracted_data'].append(item)
        st.session_state.database = self.data
        
        return item['id']
    
    def get_all_data(self):
        """Get all extracted data in a format suitable for display"""
        results = []
        
        for item in self.data['extracted_data']:
            # For each item in extracted data
            base_info = {
                'URL': item['url'],
                'Timestamp': item['timestamp']
            }
            
            # Add all extracted fields
            for field_name, field_value in item['data'].items():
                if isinstance(field_value, list) and field_value:
                    # Join list values for display
                    base_info[field_name] = ', '.join(field_value)
                else:
                    base_info[field_name] = field_value
            
            results.append(base_info)
        
        return results
    
    def clear(self):
        """Clear all stored data"""
        st.session_state.database = {
            'raw_data': [],
            'extracted_data': []
        }
        self.data = st.session_state.database

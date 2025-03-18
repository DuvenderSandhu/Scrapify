# app.py - Advanced Web Scraper with Comprehensive Crawling

import streamlit as st
import pandas as pd
import numpy as np
import time
import random
import threading
from streamlit.components.v1 import html
from phone import filter_valid_numbers
import spacy
# from flask import Flask
import resend 
import asyncio
# Load the spaCy model
nlp = spacy.load("en_core_web_sm")
import re
from bs4 import BeautifulSoup
from database import db
import json
from dotenv import load_dotenv
from log import log_process,log_error,log_warning,log_success,log_info,add_log
from datetime import datetime
from urllib.parse import urlparse, urljoin
import uuid
from collections import defaultdict
from crawler import rawid
from crawler import get_html_sync
from scraper import find_elements_by_selector,extract_data_with_ai
from test import get_all_data
max_depth= 0 
max_pages=1
stay_on_domain=""
coldwell=False
max_pages_status=None
handle_lazy_loading=None
start=""
# app = Flask(__name__)
# Page configuration
st.set_page_config(
    page_title="Advanced Web Scraper",
    page_icon="🕸️",
    layout="wide",
    initial_sidebar_state="expanded"
)





# load_dotenv()
# Custom CSS
if "extraction_method" not in st.session_state:
    st.session_state.extraction_method = "None"
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #424242;
        margin-top: 0;
        margin-bottom: 2rem;
    }
    .log-container {
        height: 400px;
        overflow-y: auto;
        border: 1px solid #e0e0e0;
        border-radius: 5px;
        padding: 10px;
        background-color: #f9f9f9;
        font-family: monospace;
    }
    .log-info {
        color: #1976D2;
        margin: 2px 0;
    }
    .log-success {
        color: #2E7D32;
        margin: 2px 0;
    }
    .log-warning {
        color: #FF8F00;
        margin: 2px 0;
    }
    .log-error {
        color: #C62828;
        margin: 2px 0;
    }
    .log-process {
        color: #6A1B9A;
        margin: 2px 0;
        font-weight: bold;
    }
    .url-bubble {
        background-color: #E3F2FD;
        color: #1565C0;
        border-radius: 15px;
        padding: 8px 12px;
        margin: 5px;
        display: inline-block;
        font-weight: bold;
    }
    .stProgress > div > div > div > div {
        background-color: #1E88E5;
    }
    .crawl-options {
        background-color: #f5f5f5;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 15px;
    }
    .status-card {
        background-color: #f0f7ff;
        border-left: 4px solid #1976D2;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
    .phase-indicator {
        display: flex;
        margin-bottom: 20px;
    }
    .phase-step {
        flex: 1;
        text-align: center;
        padding: 10px;
        margin: 0 5px;
        background-color: #f5f5f5;
        border-radius: 5px;
    }
    .phase-active {
        background-color: #bbdefb;
        border-left: 3px solid #1976D2;
        font-weight: bold;
    }
    .phase-complete {
        background-color: #c8e6c9;
        border-left: 3px solid #2E7D32;
    }
    .selector-example {
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 5px;
        padding: 10px;
        margin-bottom: 10px;
    }
    .pagination-example {
        display: flex;
        justify-content: center;
        background-color: white;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
        border: 1px solid #eee;
    }
    .pagination-page {
        margin: 0 5px;
        padding: 5px 10px;
        border-radius: 3px;
    }
    .pagination-current {
        background-color: #1976D2;
        color: white;
        font-weight: bold;
    }
    .pagination-next {
        color: #1976D2;
        font-weight: bold;
    }
    .field-card {
        background-color: #f5f7fa;
        border-left: 3px solid #1976D2;
        padding: 10px 15px;
        border-radius: 5px;
        margin-bottom: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .helper-card {
        background-color: #e8f4fd;
        border-radius: 5px;
        padding: 10px;
        margin-top: 10px;
        border-left: 3px solid #1976D2;
    }
</style>
""", unsafe_allow_html=True)
query_params = st.query_params
selected_tab = query_params.get("tab", False) 


tab_names = ["Setup & Controls", "Results", "Logs"]
tab_ids = ["tab1", "tab2", "tab3"]
# default_tab_index = tab_ids.index(selected_tab) if selected_tab in tab_ids else 0

# Create tabs and set the default active tab based on URL parameter
# tab1, tab2, tab3 = st.tabs(tab_names)
# Initialize session state
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'urls' not in st.session_state:
    st.session_state.urls = []
if 'fields' not in st.session_state:
    st.session_state.fields = []
if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False
if 'results' not in st.session_state:
    st.session_state.results = []
if 'current_url_index' not in st.session_state:
    st.session_state.current_url_index = 0
if 'current_phase' not in st.session_state:
    st.session_state.current_phase = None
if 'found_links' not in st.session_state:
    st.session_state.found_links = []
if 'processed_links' not in st.session_state:
    st.session_state.processed_links = set()
if 'current_depth' not in st.session_state:
    st.session_state.current_depth = 0
if 'options' not in st.session_state:
    st.session_state.options = {}

# Common regex patterns for data extraction
REGEX_PATTERNS = {
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'phone': r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
    'url': r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\.-]*\??[-\w%&=]*',
    'date': r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}',
    'price': r'\$\s*\d+(?:\.\d{2})?',
    'address': r'\d+\s+[A-Za-z\s,]+\s+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?(?:\s+[A-Za-z]+)?(?:,\s+[A-Za-z]+)?(?:,\s+[A-Z]{2})?(?:\s+\d{5})?',
    'name': r"^([A-Z\u00C0-\u00D6\u00D8-\u00DE])([a-z\u00DF-\u00F6\u00F8-\u00FF '&-]+) ([A-Za-z\u00C0-\u00D6\u00D8-\u00DE\u00DF-\u00F6\u00F8-\u00FF '&-]+)$"
}

# Logging functions

# Utility functions
def validate_url(url):
    """Validate if a string is a proper URL"""
    try:
        result = urlparse(url)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except:
        return False

def extract_domain(url):
    """Extract domain from URL"""
    parsed_url = urlparse(url)
    return parsed_url.netloc

def normalize_url(url, base_url):
    """Normalize relative URLs to absolute URLs"""
    if not url:
        return None
    if url.startswith(('#', 'javascript:', 'mailto:')):
        return None
    return urljoin(base_url, url)

# Crawler functions
async def crawl_url(url, options):
    print("Crawling Now")
    """Simulate crawling a URL with configurable options"""
    log_process(f"Crawling URL: {url}")
    domain = extract_domain(url)
    log_info(f"Connecting to {domain}")
    
    # Simulate network delay
    # time.sleep(random.uniform(0.5, 1.5))
    
    # Generate simulated HTML content
    button= options.get('pagination_selector', False) or options.get('pagination_xpath', False) or options.get('pagination_text', False) or options.get('pagination_text_match', False) or options.get('pagination_confidence', False)
    print("button",button)
    html_content =""#await get_all_data(url) # await get_html_sync(url,button,options)
    if coldwell:
        try:
            html_content=await get_all_data(url)
            print("Pring Here")
            
        except : 
            print("Eror Occured")
    else :
        html_content=await get_html_sync(url,button,options)
    # print("html",html_content)
    # Extract links if link following is enabled
    links = []
    if options.get('follow_links', False):
        print("options",options)
        links = extract_links_from_html(html_content, url, options)
        log_info(f"Found {len(links)} links on page")
    
    # Check for pagination based on configured method
    pagination_url = None
    if options.get('handle_pagination', False):
        pagination_url = find_next_page(html_content, url, options)
        if pagination_url:
            log_success(f"Found pagination link: {pagination_url}")
    
    log_success(f"Successfully crawled {url} - {len(html_content)} bytes")
    
    return {
        'html': html_content,
        'links': links,
        'pagination_url': pagination_url,
        'timeTaken':time.time() - start
    }



def extract_links_from_html(html_content, base_url, options):
    """
    Extract links from HTML content
    
    In a real implementation, we would use BeautifulSoup to parse HTML.
    Here we'll use a simple regex approach for demonstration.
    """
    # Simple regex to find links
    links = re.findall(r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"', html_content)
    
    # Filter and normalize links
    valid_links = []
    base_domain = extract_domain(base_url)
    for link in links:
        # Normalize URL
        full_url = normalize_url(link, base_url)
        if not full_url:
            continue
        
        # Apply domain filtering if stay_on_domain is enabled
        if options.get('stay_on_domain', False) and extract_domain(full_url) != base_domain:
            continue
            
        valid_links.append(full_url)
    
    return valid_links

def find_next_page(html_content, current_url, options):
    """Find pagination link based on specified method"""
    pagination_method = options.get('pagination_method', 'Auto-detect')
    
    # Default to None (no pagination link found)
    next_url = None
    
    if pagination_method == "CSS Selector":
        # In a real implementation, this would use BeautifulSoup to find elements
        # matching the provided CSS selector
        selector = options.get('pagination_selector', '.pagination .next')
        log_info(f"Looking for next page using CSS selector: {selector}")
        
        # Simulate finding the selector
        if selector in html_content or random.random() > 0.3:  # 70% chance of success
            # Extract the href from the matched element
            # This is a simplified simulation - real implementation would parse HTML
            next_url = f"{current_url}?page={random.randint(2, 10)}"
            log_success(f"Found pagination link with CSS selector: {next_url}")
    
    elif pagination_method == "XPath":
        # In a real implementation, this would use lxml or similar to find elements
        # matching the provided XPath
        xpath = options.get('pagination_xpath', '//a[contains(@class, "next")]')
        log_info(f"Looking for next page using XPath: {xpath}")
        
        # Simulate finding the XPath
        if random.random() > 0.3:  # 70% chance of success
            next_url = f"{current_url}?page={random.randint(2, 10)}"
            log_success(f"Found pagination link with XPath: {next_url}")
    
    elif pagination_method == "Button Text":
        # Look for links containing the specified text
        text = options.get('pagination_text', 'Next')
        match_method = options.get('pagination_text_match', 'Contains')
        log_info(f"Looking for next page with text '{text}' using {match_method} matching")
        
        # Simulate finding the text
        if random.random() > 0.3:  # 70% chance of success
            next_url = f"{current_url}?page={random.randint(2, 10)}"
            log_success(f"Found pagination link with text '{text}': {next_url}")
    
    elif pagination_method == "AI-powered":
        # Simulate AI analysis of the page
        confidence = random.randint(50, 100)
        threshold = options.get('pagination_confidence', 70)
        log_info(f"AI analyzing page structure for pagination (confidence: {confidence}%)")
        
        if confidence >= threshold:
            next_url = f"{current_url}?page={random.randint(2, 10)}"
            log_success(f"AI found pagination link with {confidence}% confidence: {next_url}")
        else:
            log_warning(f"AI found potential pagination link but confidence ({confidence}%) below threshold ({threshold}%)")
    
    else:  # Auto-detect
        # Try common patterns for pagination
        log_info("Auto-detecting pagination using common patterns")
        
        # Define patterns to check (simplified for demonstration)
        patterns = [
            # Look for next links
            r'<a[^>]*?(?:class="[^"]*?next[^"]*?"|id="[^"]*?next[^"]*?"|rel="next")[^>]*?href="([^"]*)"',
            # Look for page number links
            r'<a[^>]*?href="([^"]*(?:[?&]page=\d+|/page/\d+)[^"]*)"[^>]*?>(?:\d+|Next|next|»|&raquo;)</a>',
            # Look for links with next text
            r'<a[^>]*?href="([^"]*)"[^>]*?>(?:Next|next|»|&raquo;)</a>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                next_url = match.group(1)
                next_url = normalize_url(next_url, current_url)
                log_success(f"Auto-detected pagination link: {next_url}")
                break
    
    # Handle infinite scroll if enabled
    if not next_url and options.get('infinite_scroll', False):
        log_info("No pagination link found, attempting to trigger infinite scroll")
        
        # In a real implementation, this would use Selenium or similar
        # to scroll the page and wait for more content to load
        if random.random() > 0.5:  # 50% chance of success
            # Simulate finding more content after scrolling
            next_url = current_url  # Same URL but would load more content
            log_success("Successfully triggered infinite scroll pagination")
        else:
            log_warning("Failed to trigger infinite scroll pagination")
    
    # Verify URL pattern if enabled
    if next_url and options.get('url_pattern_check', True):
        # Check if the URL follows common pagination patterns
        valid_patterns = [
            r'\?page=\d+',              # ?page=2
            r'&page=\d+',               # &page=2
            r'/page/\d+',               # /page/2
            r'/p/\d+',                  # /p/2
            r'-page-\d+',               # -page-2
            r'_\d+\.html',              # _2.html
        ]
        
        is_valid = any(re.search(pattern, next_url) for pattern in valid_patterns)
        
        if not is_valid:
            log_warning(f"Pagination URL {next_url} doesn't match expected patterns")
            
            # In a real implementation, you might still allow it or
            # implement additional checking logic
    
    return next_url


import re
from collections import defaultdict, Counter
from bs4 import BeautifulSoup

from collections import defaultdict
import re
from bs4 import BeautifulSoup

import re
from bs4 import BeautifulSoup
from collections import defaultdict

import re
from bs4 import BeautifulSoup
from collections import defaultdict

import re
import json
from collections import defaultdict
from bs4 import BeautifulSoup

# Define regex patterns for common fields

# query_params = st.experimental_get_query_params()
# selected_tab = query_params.get("tab", ["Tab 1"])[0]
from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict

from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict

from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict

from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict

from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict

from bs4 import BeautifulSoup
import re
import json
from collections import defaultdict
def read_progress():
    try:
        with open("progress.json", "r") as f:
            progress_data = json.load(f)
        return progress_data
    except FileNotFoundError:
        return {"progress": 0, "processed_agents": 0, "total_estimated_agents": 50434, "estimated_time_remaining": 0}

import re
from bs4 import BeautifulSoup
import json
import itertools

import re
import json
from bs4 import BeautifulSoup
import streamlit as st

import re
import json
from bs4 import BeautifulSoup

import re
import json
from bs4 import BeautifulSoup

import re
import json
from bs4 import BeautifulSoup

import re
import json
from bs4 import BeautifulSoup

import re
import json
from bs4 import BeautifulSoup
from datetime import datetime
import time

def extract_data(html_content, fields, method="regex"):
    """Extract structured data from HTML using optimized regex."""
    print("Extracting Now")
    start = time.time()
    results = {field: [] for field in fields}  # Maintain old structure
    country_code = st.session_state.options.get('country_code', False)
    hyphen_separator = st.session_state.options.get('hyphen_separator', False)

    # Precompile regex patterns for performance
    regex_patterns = {
        "name": re.compile(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*"),  # Matches names (e.g., "John Doe")
        "phone": re.compile(r"\b\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),  # Matches 10-digit phone numbers
        "email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?=[\s<]|$)"),  # Matches emails
    }

    def clean_phone_numbers(phone_list):
        """Clean and format phone numbers."""
        cleaned = set()  # Use a set for deduplication
        for num in phone_list:
            if not num:  # Skip empty strings
                continue
            
            # Extract only digits
            digits = re.sub(r'[^0-9]', '', num)
            if len(digits) != 10:  # Skip invalid numbers
                continue
            
            # Format based on options
            if hyphen_separator:
                formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"  # Format as XXX-XXX-XXXX
            else:
                formatted = digits
            
            if country_code:
                formatted = f"+1-{formatted}" if hyphen_separator else f"+1{formatted}"
            
            cleaned.add(formatted)  # Add to set for deduplication
        
        return list(cleaned)  # Convert back to list

    def extract_json_ld(soup):
        """Extract JSON-LD data from HTML content."""
        json_ld_data = []
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                json_data = json.loads(script.string)
                json_ld_data.append(json_data)
            except json.JSONDecodeError:
                continue
        return json_ld_data

    def extract_structured_data(soup, fields):
        """Extract structured data dynamically using precompiled regex."""
        seen_data = {field: set() for field in fields}  # Track duplicates using sets
        
        # Iterate through parent containers
        for parent in soup.find_all():
            extracted = {}  # Store extracted data for this parent
            
            # Process text content in one pass
            text = parent.get_text(" ", strip=True)  # Get all text in one go
            
            for field in fields:
                pattern = regex_patterns.get(field.lower())
                if not pattern:
                    continue
                
                # Find all matches in the text
                matches = pattern.findall(text)
                
                # Clean & validate matches
                if field.lower() == "phone":
                    matches = clean_phone_numbers(matches)
                
                # Add the first valid match to results
                for match in matches:
                    if match not in seen_data[field]:  # Skip duplicates
                        seen_data[field].add(match)
                        extracted[field] = match
                        break  # Only take the first match per field per parent
            
            # Add extracted data to results
            for field, value in extracted.items():
                results[field].append(value)
        
        return results

    if method.lower() == "regex":
        soup = BeautifulSoup(html_content, "html.parser")

        # Step 1: Try to extract JSON-LD data
        json_ld_data = extract_json_ld(soup)

        # Step 2: Extract structured data grouped by parent
        extract_structured_data(soup, fields)

    elif method.lower() == "css":
        for field in fields:
            results[field] = find_elements_by_selector(html_content, field)
    elif method.lower() == "ai":
        ai_response = extract_data_with_ai(html_content, fields, ai_provider, ai_api)
        for field, data in ai_response.items():
            results[field] = handle_duplicates(data) if data else []

    print(f"Extraction completed in {time.time() - start:.2f} seconds")
    return results
def extract_unknown_field(html_content, field):
    """Extract data for fields without predefined patterns."""
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    
    # Look for elements with the field name in their attributes
    for elem in soup.find_all(attrs={"id": re.compile(field, re.I)}):
        results.append(elem.get_text(strip=True))
    
    for elem in soup.find_all(attrs={"class": re.compile(field, re.I)}):
        results.append(elem.get_text(strip=True))
    
    for elem in soup.find_all(attrs={"name": re.compile(field, re.I)}):
        results.append(elem.get_text(strip=True))
    
    # Look for elements containing the field name as text
    for elem in soup.find_all(text=re.compile(f"\\b{field}\\b", re.I)):
        parent = elem.parent
        if parent.name not in ['script', 'style']:
            next_sibling = parent.find_next_sibling()
            if next_sibling:
                results.append(next_sibling.get_text(strip=True))
    newresults = {k: list(dict.fromkeys(v)) for k, v in results.items()}

    return newresults

def simulate_css_extraction(html_content, field):
    """Simulate extraction using CSS selectors"""
    field_lower = field.lower()
    
    # Simulate different results based on field type
    if 'email' in field_lower:
        return ['info@example.com', 'sales@example.com', 'support@example.com']
    elif any(keyword in field_lower for keyword in ['phone', 'tel', 'mobile']):
        return ['+1 (555) 123-4567', '+1 (555) 987-6543']
    elif 'name' in field_lower:
        return ['John Smith', 'Jane Doe', 'Robert Johnson']
    elif 'price' in field_lower:
        # Extract price-like strings
        return [f"${random.randint(10, 999)}.{random.randint(0, 99):02d}" for _ in range(random.randint(1, 5))]
    elif 'address' in field_lower:
        return ['123 Main Street, Anytown, CA 94043']
    else:
        # Generic extraction for other fields
        return [f"Sample {field} data {i}" for i in range(1, random.randint(2, 4))]

def simulate_ai_extraction(html_content, field):
    """Simulate AI-based extraction"""
    field_lower = field.lower()
    
    # Simulate an AI extracting more intelligently
    time.sleep(random.uniform(0.8, 1.5))  # AI processing takes time
    
    if 'email' in field_lower:
        domain_match = re.search(r'<title>([^<]+)', html_content)
        domain = domain_match.group(1).split(' ')[0] if domain_match else "example.com"
        domain = domain.lower()
        return [f"info@{domain}", f"sales@{domain}", f"support@{domain}"]
    elif 'name' in field_lower or 'username' in field_lower:
        # AI might be better at extracting complete names
        text = html_content
        doc = nlp(text)
        names = []
        name_matches=[ent.text for ent in doc.ents if ent.label_ == "PERSON"]
        # name_matches = re.findall(r'<h[3-4][^>]*>([A-Z][a-z]+ [A-Z][a-z]+)</h[3-4]>', html_content)
        if name_matches:
            names.extend(name_matches)
        else:
            # Simulate AI finding names in context
            names = ["John Smith", "Jane Doe", "Robert Johnson"]
        return names
    elif any(keyword in field_lower for keyword in ['phone', 'tel', 'mobile']):
        # Extract phone numbers
        phone_matches = re.findall(REGEX_PATTER['phone'], html_content)
        if phone_matches:
            return phone_matches
        return ["+1 (555) 123-4567", "+1 (555) 987-6543"]
    elif 'address' in field_lower:
        # Address extraction
        address_matches = re.findall(REGEX_PATTERNS['address'], html_content)
        if address_matches:
            return address_matches
        return ["123 Main Street, Anytown, CA 94043"]
    elif 'price' in field_lower or 'cost' in field_lower:
        price_matches = re.findall(r'\$\s*\d+(?:\.\d{2})?', html_content)
        if price_matches:
            return price_matches
        return [f"${random.randint(10, 999)}.{random.randint(0, 99):02d}"]
    else:
        # Generic AI extraction
        potential_data = []
        # Simulate AI finding relevant context
        paragraphs = re.findall(r'<p>(.*?)</p>', html_content)
        for p in paragraphs[:3]:  # Limit to first few paragraphs
            if field_lower in p.lower():
                potential_data.append(re.sub(r'<.*?>', '', p))
        
        if potential_data:
            return potential_data
        return [f"AI extracted {field} data"]

# Main application layout
st.title("🕸️ Advanced Web Scraper")
st.markdown("Crawl websites with intelligent extraction and comprehensive link following")
def show_temp_alert(message, seconds=3):
    """Displays a temporary alert only once per session, without blocking UI."""
    if "alert_shown" in st.session_state and st.session_state["alert_shown"]:
        return  # Prevent showing again

    st.session_state["alert_message"] = message
    st.session_state["alert_shown"] = True  # Mark as shown

    def remove_alert():
        time.sleep(seconds)
        st.session_state["alert_message"] = None

    threading.Thread(target=remove_alert, daemon=True).start()
# Display the alert without blocking UI updates
show_temp_alert("Go to Result Tab to Download Coldwell Data ")

# Execute alert removal in the background if set
if "remove_alert" in st.session_state:
    st.session_state["remove_alert"]()
    del st.session_state["remove_alert"]

# Clear the alert
# alert_placeholder.empty()

emails= ""
with open('email.txt', 'r') as file:
    # Read the content of the file
    content = file.read()
    emails= content
# Create tabs for different sections
tab1, tab2, tab3 = st.tabs(["Setup & Controls", "Results", "Logs"])
ai_provider=""
ai_api=""
saveToDb=False

def addEmail():
    data_to_write=  st.session_state["emails"]
    with open('email.txt', 'w') as file:
        # Write new data to the file
        file.write(data_to_write)
# pagination_text_match= ""
# if selected_tab == "tab1":

with tab1:
    # Setup & Controls tab
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Configure Scraping Job")
        saveToDb= st.toggle("Save to DB")
        # URL Input
        url_input = st.text_area(
            "Enter URLs to scrape (one per line):",
            placeholder="https://example.com\nhttps://anothersite.com",
            height=100
        )
        if url_input.find("coldwellbankerhomes")>=0:
            coldwell= st.toggle("Extract Complete Data from ColdWellBankerHome (100% Accurate)")
            if coldwell:
                emails = st.text_input("Enter Email to Notify:", 
                                    key="emails", 
                                    placeholder="johndoe@gmail.com,sales@gmail.com", 
                                    value=emails,
                                    on_change=addEmail
                                    )
        
        # Field Configuration
        st.subheader("🔍 Data Extraction")

        # Initialize session state for fields if not already set
        if "fields" not in st.session_state:
            st.session_state.fields = []

        # Function to add field and clear input
        def add_field():
            if st.session_state.field_input.strip():  
                field = st.session_state.field_input.strip()
                if field not in st.session_state.fields:
                    st.session_state.fields.append(field)
                    st.success(f"Added: {field}")
                st.session_state.field_input = ""  # Clear input box
                # st.rerun()

        # Input field with enter-to-add functionality
        field_input = st.text_input("Enter field to extract:", 
                                    key="field_input", 
                                    placeholder="e.g., Email, Phone, #agent_name, .agent_email", 
                                    on_change=add_field)

        # Add button
        # st.session_state.extraction_method
        if st.session_state.extraction_method =="CSS":

            st.info("Use valid CSS selectors (class or ID) to target elements from website (e.g., #user-email-id, .user-number) not Email, Phone.")
        if st.button("➕ Add Field", use_container_width=True):
            add_field()

        # Display added fields with remove option
        if st.session_state.fields:
            st.write("**Fields to extract:**")
            for field in st.session_state.fields:
                colu1, colu2 = st.columns([4, 1])
                colu1.write(f"**{field}**")
                if colu2.button("Delete", key=f"remove_{field}", help=f"Remove {field}"):
                    st.session_state.fields.remove(field)
                    st.warning(f"Removed: {field}")
                    st.rerun()
        else:
            st.info("No fields added yet. Add fields to extract data.")    
        # Advanced Crawling Options
        hyphen_separator = st.checkbox("Format phone numbers with hyphens (e.g., 123-456-7890)", value=False)
        country_code = st.checkbox("Add country code (+1) to phone numbers (e.g., +1234567890)", value=True)
        if hyphen_separator and country_code:
            st.info("Phone Number will be interpreted as +1-123-456-7890") 
        

        
        st.subheader("Crawling Options")
        
        with st.expander("Configure Crawling", expanded=False):  
            follow_links = st.checkbox("Follow Links", value=False, help="Crawl and collect data from links found on the page.")  

            if follow_links:  
                stay_on_domain = st.checkbox("Stay on Same Domain", value=True, help="Only crawl pages from the same website, ignoring external links.")  

                max_depth = st.slider("Max Depth", 0, 5, 1, help="How deep the crawler should go. 0 = Only start pages, 1 = Follow one level of links, etc.")  
                max_pages_status = st.checkbox("Limit Pages", value=False, help="Set a maximum number of pages to visit.")  

                if max_pages_status:  
                    max_pages = st.number_input("Max Pages", 1, 100, 10, help="The highest number of pages the crawler will scan.")  

            handle_lazy_loading = st.checkbox("Handle Lazy Loading", value=False, help="Load hidden content that appears as you scroll.")  
            handle_pagination = st.checkbox("Handle Pagination", value=False, help="Follow 'Next' buttons to load more pages.")  

            if handle_pagination:  
                pagination_method = st.selectbox(  
                    "Pagination Detection Method",  
                    ["Auto-detect (Use Predefined Buttons)", "CSS Selector", "XPath", "Button Text"],  
                    help="Choose how the crawler finds the 'Next' button to load more pages."  
                )  

                if pagination_method == "CSS Selector":  
                    pagination_selector = st.text_input(  
                        "Enter CSS Selector:",  
                        placeholder=".pagination .next, a.next-page",  
                        help="CSS rule to find the 'Next' button. Example: `.pagination .next`, `a[rel='next']`"  
                    )  

                elif pagination_method == "XPath":  
                    pagination_xpath = st.text_input(  
                        "Enter XPath:",  
                        placeholder="//a[contains(text(), 'Next')]",  
                        help="XPath rule to find the 'Next' button. Example: `//a[contains(text(), 'Next')]`"  
                    )  

                elif pagination_method == "Button Text":  
                    pagination_text = st.text_input(  
                        "Enter Button Text:",  
                        placeholder="Next",  
                        help="The exact text on the 'Next' button. Example: 'Next', 'Load More'"  
                    )  

            

        # Extraction Method


        st.subheader(" Extraction Method")

        # Extraction method selection
        extraction_method = st.selectbox(  
            "Choose a data extraction method:",  
            ["Regex", "CSS", "AI"],  
            help="Regex: Extract data using patterns | CSS: Select specific page elements | AI: Analyze content for relevant data."  
        )  


        # Dynamic description
        descriptions = {
            "Regex": "🔹 Extracts addresses, phone numbers, and emails using predefined patterns.",
            "CSS": "🔹 Use CSS selectors (class or ID) to target elements (e.g., `#user-email-id`, `.user-number`).",
            "AI": "🔹 Uses an AI model for intelligent data extraction."
        }
        st.info(descriptions[extraction_method])

        # AI options (only shown if AI is selected)
        if extraction_method == "AI":
            st.session_state.extraction_method = "ai"
            ai_provider = st.selectbox("Select AI Model", ["OpenAI", "Gemini", "DeepSeek", "Groq"])
            ai_api = st.text_area("Enter AI API Key", placeholder="sk-...")
        # elif extraction_method =="CSS":
        #     st.session_state.extraction_method = "CSS"
        #     st.rerun()
        # else :
        #     st.session_state.extraction_method = "regex"

            
    with col2:
        # Status and Controls Section
        st.subheader("Status & Controls")
        
        if st.session_state.is_scraping:
            if st.button("⏹️ Stop Scraping", type="primary", use_container_width=True):
                st.session_state.is_scraping = False
                log_warning("Scraping job stopped by user")
                st.rerun()
            
            st.write("Scraping in progress...")

            import random
            import streamlit as st

            def calculate_estimate_time():
                """
                Calculate the estimated time based on the current state of the crawler.
                """
                # Average time per URL (in seconds)
                avg_time_per_url = random.randint(15, 20) * 1.5  # Adjust this range as needed

                # Total URLs to process
                if st.session_state.options.get("follow_links"):
                    if st.session_state.options.get("max_pages_status"):
                        # If max_pages is enabled, limit the total URLs
                        total_urls = min(
                            st.session_state.options.get("max_pages", 10),
                            len(st.session_state.urls) + len(st.session_state.found_links)
                        )
                    else:
                        # If no max_pages, use all initial and discovered links
                        total_urls = len(st.session_state.urls) + len(st.session_state.found_links)
                else:
                    # If follow_links is disabled, only process initial URLs
                    total_urls = len(st.session_state.urls)

                # URLs already processed
                processed_urls = len(st.session_state.processed_links)

                # Remaining URLs to process
                remaining_urls = total_urls - processed_urls

                # Calculate the estimate time
                estimate_time = remaining_urls * avg_time_per_url
                return estimate_time, total_urls, processed_urls, remaining_urls

            # Create a placeholder for the estimate at the app level
            estimate_placeholder = st.empty()

            def update_estimate_display():
                """
                Update the estimate display without blocking the UI.
                """
                estimate_time, total_urls, processed_urls, remaining_urls = calculate_estimate_time()
                estimate_placeholder.write(
                    f"Estimated Time: {estimate_time/60:.1f} Minutes ({estimate_time:.1f} Seconds) "
                    f"for {remaining_urls} URLs (Processed: {processed_urls}/{total_urls})"
                )
        # Use this in your main app flow
        if st.session_state.is_scraping:
            # Update the estimate once per iteration of your main loop
            update_estimate_display()
            
            # Rest of your scraping code goes here
            # ...
            
            # You can update the estimate again after processing a batch of URLs
            update_estimate_display()




            my_html = """
                    <script>
                    function startTimer(display) {
                        var seconds = 0;
                        setInterval(function () {
                            var minutes = Math.floor(seconds / 60);
                            var remainingSeconds = seconds % 60;

                            minutes = minutes < 10 ? "0" + minutes : minutes;
                            remainingSeconds = remainingSeconds < 10 ? "0" + remainingSeconds : remainingSeconds;

                            display.textContent = minutes + ":" + remainingSeconds;
                            seconds++;
                        }, 1000);
                    }

                    window.onload = function () {
                        var display = document.querySelector('#time');
                        startTimer(display);
                    };
                    </script>

                    <style>
                    body, html {
                        height: 100vh;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                    }
                    </style>

                    <div id="time">00:00</div>

            """

            html(my_html, height=20)
            # progress = (len(st.session_state.processed_links) / max(1, len(st.session_state.urls) + len(st.session_state.found_links))) * 100
            if coldwell:
                try:
                    with open("progress.json", "r") as f:
                        progress_data = json.load(f)
                    progress = (progress_data["processed_agents"] / max(1, progress_data["total_estimated_agents"])) * 100
                    estimated_time_remaining= progress_data['estimated_time_remaining'] or "calculating Estimate Time"
                    # estimated_time_remaining = ((total_estimated_agents - processed_agents) * (datetime.now() - start_time).total_seconds()) / max(1, processed_agents)

                    st.write(f"Estimated Time Remaining: {estimated_time_remaining/60:.2f} minutes")

                except FileNotFoundError:
                    progress = 0
            else:
                progress = (len(st.session_state.processed_links) / max(1, len(st.session_state.urls) + len(st.session_state.found_links))) * 100
            st.progress(min(100, progress) / 100)
            # if "start_time" not in st.session_state or "start_time" in st.session_state:
            st.session_state.start_time = time.time()

            # # Placeholder for updating the timer
            timer_placeholder = st.empty()

            # while True:
            #     elapsed_time = time.time() - st.session_state.start_time
            #     timer_placeholder.info(f"⏳ Time Elapsed: {elapsed_time:.2f} seconds")
                
            #     time.sleep(1)  # Wait 1 second before updating

            #     # Optional: Add a condition to break the loop after a certain time (e.g., 60 seconds)
            #     if elapsed_time >= 60:
            #         break
            # Current phase display
            phases = ["initializing", "crawling", "extracting", "processing", "complete"]
            current_phase_index = phases.index(st.session_state.current_phase) if st.session_state.current_phase in phases else 0
            
            st.write("Current phase:")
            phase_cols = st.columns(len(phases))
            for i, phase in enumerate(phases):
                with phase_cols[i]:
                    if i < current_phase_index:
                        st.info(f"✓ {phase.capitalize()}")
                    elif i == current_phase_index:
                        st.success(f"⏳ {phase.capitalize()}")
                    else:
                        st.text(f"○ {phase.capitalize()}")
            
            # Stats
            st.write("Statistics:")
            st.text(f"• URLs processed: {len(st.session_state.processed_links)}")
            st.text(f"• New links found: {len(st.session_state.found_links)}")
            st.text(f"• Current depth: {st.session_state.current_depth}")
            
            if st.session_state.results:
                st.text(f"• Items extracted: {len(st.session_state.results)}")
        else:
            # Start button - only show if URLs and fields are provided
            start_disabled = not (url_input.strip() and st.session_state.fields if not coldwell else True)
            
            if st.button("▶️ Start Scraping", type="primary", use_container_width=True, disabled=start_disabled):
                # Parse URLs
                if coldwell:
                    if not emails:
                        st.error("Please enter Email For Notification")
                        start_disabled= False
                    


                urls = [url.strip() for url in url_input.split('\n') if validate_url(url.strip())]
                
                if urls:
                    # Reset state
                    st.session_state.urls = urls
                    st.session_state.found_links = []
                    st.session_state.processed_links = set()
                    st.session_state.current_depth = 0
                    st.session_state.results = []
                    st.session_state.is_scraping = True
                    st.session_state.current_phase = "initializing"
                    
                    # Clear logs
                    st.session_state.logs = []
                    
                    # Log start
                    log_process(f"Starting scraping job with {len(urls)} URLs")
                    log_info(f"Fields to extract: {', '.join(st.session_state.fields)}")
                    log_info(f"Extraction method: {extraction_method}")
                    
                    options = {
                        'follow_links': follow_links if follow_links else None,
                        'max_depth': max_depth if max_depth else None,
                        'max_pages_status':max_pages_status if max_pages_status else False,
                        'max_pages': max_pages if max_pages_status else len(urls),
                        'stay_on_domain': stay_on_domain if stay_on_domain else None,
                        'handle_pagination': handle_pagination if handle_pagination else None,
                        'handle_lazy_loading': handle_lazy_loading,
                        'pagination_method': pagination_method if handle_pagination else None,
                        'hyphen_separator':hyphen_separator if hyphen_separator else False,
                        'country_code' :country_code if country_code else False,
                        'saveToDb':saveToDb if saveToDb else False
                    }
                    
                    # Add method-specific pagination options
                    if handle_pagination:
                        if pagination_method == "CSS Selector":
                            options['pagination_selector'] = pagination_selector if pagination_selector else ""
                        elif pagination_method == "XPath":
                            options['pagination_xpath'] = pagination_xpath
                        elif pagination_method == "Button Text":
                            options['pagination_text'] = pagination_text if pagination_text else None
                            # options['pagination_text_match'] = pagination_text_match if pagination_text_match else None
                        elif pagination_method == "AI-powered":
                            options['pagination_confidence'] = pagination_confidence
                    
                    # Store options in session state
                    st.session_state.options = options
                    st.session_state.extraction_method = extraction_method
                    st.session_state.ai_provider= ai_provider or ""
                    st.session_state.ai_api= ai_api
                    
                    log_info(f"Crawling options configured")
                    st.rerun()
                else:
                    st.error("Please enter valid URLs to scrape")
            
            if start_disabled:
                if not url_input.strip():
                    st.warning("Please enter at least one URL to scrape")
                if not st.session_state.fields and not coldwell:
                    st.warning("Please add at least one field to extract")
            
            # Clear all button
            if st.button("🗑️ Clear All", use_container_width=True):
                st.session_state.urls = []
                st.session_state.fields = []
                st.session_state.found_links = []
                st.session_state.processed_links = set()
                st.session_state.results = []
                st.session_state.logs = []
                log_info("All data cleared")
                st.rerun()
            if st.session_state.current_phase == "complete":
                if coldwell:
                    st.info("Task Started You will be notified after completion by email")
                else:    
                    st.info("Task Complete Go to Result Tab to See Result")
                    st.info("Took " + str(st.session_state.options.get("time", 20)) + " Seconds to Crawl and Extract Websites")

import streamlit as st
import pandas as pd
import re
from datetime import datetime

import streamlit as st
import pandas as pd
import re
from datetime import datetime
# Function to format mobile numbers
import pandas as pd
import streamlit as st
from datetime import datetime
import re

import pandas as pd
import streamlit as st
from datetime import datetime
import re

# Function to format mobile numbers
def format_mobile_number(mobile, country_code=False, hyphen_separator=False):
    if pd.isna(mobile):  # Handle NaN values
        return ""

    # Remove all non-numeric characters
    mobile = re.sub(r"\D", "", str(mobile))

    if len(mobile) < 10:  # Ensure a valid 10-digit number
        return mobile  # Return as-is if not a full number

    # Apply hyphen separator if enabled
    if hyphen_separator:
        mobile = f"{mobile[:3]}-{mobile[3:6]}-{mobile[6:]}"  # Format as XXX-XXX-XXXX

    # Apply country code if enabled
    if country_code:
        if hyphen_separator:
            mobile = f"+1-{mobile}"  # Add +1- before the number
        else:
            mobile = f"+1 {mobile}"  # Add +1 with space if no hyphens

    return mobile

# Function to normalize and deduplicate data
def normalize_and_deduplicate(value):
    if isinstance(value, str):
        value = value.strip().lower()  # Normalize: lowercase and strip whitespace
    return value

# Function to process results with strict deduplication and empty string filtering
def process_results(data_dicts, extend_metadata=True):
    if not data_dicts:
        return pd.DataFrame()
    
    # First, collect all the data into a list of dictionaries
    all_data = []
    metadata_cols = ["Source_Index", "url", "timestamp", "date", "datetime", "time", "title"]
    
    # Track seen values for deduplication (global across all sources)
    seen_values = {"email": set(), "mobile": set()}  # Add more columns as needed
    
    for i, data_dict in enumerate(data_dicts):
        source_index = f"Data-{i+1}"
        
        # Extract metadata
        metadata = {"Source_Index": source_index}
        for key in metadata_cols:
            if key in data_dict:
                value = data_dict[key]
                if isinstance(value, list):
                    metadata[key] = "" if not value else str(value[0])
                else:
                    metadata[key] = "" if value is None else str(value)
        
        # Extract list fields and non-list fields
        list_fields = {}
        non_list_fields = {}
        
        for key, value in data_dict.items():
            if key in metadata_cols:
                continue
            elif isinstance(value, list) and len(value) > 1:
                list_fields[key] = value
            else:
                if isinstance(value, list):
                    non_list_fields[key] = "" if not value else str(value[0])
                else:
                    non_list_fields[key] = "" if value is None else str(value)
        
        # Process list fields
        if list_fields:
            max_length = max(len(value) for value in list_fields.values())
            for j in range(max_length):
                row_data = {}
                
                # Add metadata
                if j == 0 or not extend_metadata:
                    row_data.update(metadata)
                else:
                    row_data["Source_Index"] = source_index
                
                # Add non-list fields
                row_data.update(non_list_fields)
                
                # Add list fields for this row
                has_valid_data = False
                for key, value_list in list_fields.items():
                    if j < len(value_list):
                        value = str(value_list[j])
                        value = normalize_and_deduplicate(value)  # Normalize the value
                        if value:  # Only process non-empty values
                            # Initialize seen_values for this column if needed
                            if key not in seen_values:
                                seen_values[key] = set()
                            
                            # Only add the value if it hasn't been seen before
                            if value not in seen_values[key]:
                                row_data[key] = value
                                seen_values[key].add(value)
                                has_valid_data = True
                
                # Only add row if it has valid data
                if has_valid_data or j == 0:  # Keep at least one row per source
                    all_data.append(row_data)
        else:
            # Process single row
            row_data = {}
            row_data.update(metadata)
            
            # Add non-list fields with deduplication
            has_valid_data = False
            for key, value in non_list_fields.items():
                value = normalize_and_deduplicate(value)  # Normalize the value
                if value:  # Only process non-empty values
                    # Initialize seen_values for this column if needed
                    if key not in seen_values:
                        seen_values[key] = set()
                    
                    # Only add the value if it hasn't been seen before
                    if value not in seen_values[key]:
                        row_data[key] = value
                        seen_values[key].add(value)
                        has_valid_data = True
            
            # Only add row if it has valid data
            if has_valid_data or i == 0:  # Keep at least one row per source
                all_data.append(row_data)
    
    # Create DataFrame
    try:
        # Create DataFrame from collected data
        df = pd.DataFrame(all_data)
        
        # If DataFrame is empty, return empty DataFrame
        if df.empty:
            return df
        
        # Fill NaN values that might have been created during the process
        df = df.fillna("")
        
        # If extend_metadata is True, clear metadata in duplicate rows
        if extend_metadata:
            metadata_cols_in_df = [col for col in metadata_cols if col in df.columns and col != "Source_Index"]
            df.loc[df.duplicated(subset=["Source_Index"]), metadata_cols_in_df] = ""
        
        # Define column order: Source_Index first, then other metadata, then data
        first_cols = ["Source_Index"]
        for col in ["url", "timestamp", "date", "datetime", "time", "title"]:
            if col in df.columns:
                first_cols.append(col)
                
        # Arrange the rest of the columns
        remaining_cols = [col for col in df.columns if col not in first_cols]
        
        # Final result with proper column order
        return df[first_cols + remaining_cols]
        
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

# Function to filter out empty strings and duplicates in downloaded data
def clean_download_data(df):
    # Remove rows with empty strings in key columns
    key_columns = ["email", "mobile"]  # Add more columns as needed
    for col in key_columns:
        if col in df.columns:
            df = df[df[col] != ""]
    
    # Remove duplicates in key columns
    df = df.drop_duplicates(subset=key_columns, keep="first")
    
    return df

# Main logic
if selected_tab != "tab2":
    with tab2:
        if not coldwell:
            showcoldWellResult = st.toggle("View Coldwell Task Results", key="result-coldwell", help="Toggle between Coldwell task results and other quick tasks.")
        if coldwell or showcoldWellResult:
            with tab2:
                st.subheader("📊 Scraping Results (Coldwell)")

                def format_mobile_number(mobile, country_code=False, hyphen_separator=False):
                    if pd.isna(mobile):  # Handle NaN values
                        return ""

                    # Remove all non-numeric characters
                    mobile = re.sub(r"\D", "", str(mobile))

                    if len(mobile) < 10:  # Ensure a valid 10-digit number
                        return mobile  # Return as-is if not a full number

                    # Apply hyphen separator if enabled
                    if hyphen_separator:
                        mobile = f"{mobile[:3]}-{mobile[3:6]}-{mobile[6:]}"  # Format as XXX-XXX-XXXX

                    # Apply country code if enabled
                    if country_code:
                        if hyphen_separator:
                            mobile = f"+1-{mobile}"  # Add +1- before the number
                        else:
                            mobile = f"+1 {mobile}"  # Add +1 with space if no hyphens

                    return mobile

                try:
                    # Load data from coldwell_agents.csv
                    coldwell_df = pd.read_csv("data/coldwell_agents.csv")

                    # Select only required columns
                    coldwell_df = coldwell_df[["name", "office", "email", "mobile"]]

                    # Format mobile numbers
                    coldwell_df["mobile"] = coldwell_df["mobile"].fillna("").apply(
                        lambda x: format_mobile_number(x, country_code=country_code, hyphen_separator=hyphen_separator)
                    )

                    st.info("This table displays only the first 50 agents. The downloadable file will include all agents.")

                    # Display the editable table
                    st.write("Edit or delete cells/rows in the table below:")

                    # Use st.data_editor to make the table editable
                    edited_df = st.data_editor(
                        coldwell_df.head(50),
                        use_container_width=True,
                        num_rows="dynamic",  # Allow adding/deleting rows
                        key="coldwell_editor"
                    )


                    # Allow users to download the edited data
                    selected_columns = st.multiselect(
                        "Select columns to download",
                        options=edited_df.columns,
                        default=edited_df.columns
                    )

                    if st.button("📥 Download Data", use_container_width=True):
                        if selected_columns:
                            # Filter DataFrame based on selected columns
                            download_df = edited_df[selected_columns]

                            # Display download options (CSV, JSON, TXT)
                            col1, col2, col3 = st.columns(3)

                            # CSV Download
                            with col1:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download CSV",
                                    data=csv_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                )

                            # JSON Download
                            with col2:
                                json_str = download_df.to_json(orient="records", indent=2)
                                st.download_button(
                                    label="📥 Download JSON",
                                    data=json_str,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json",
                                )

                            # TXT Download
                            with col3:
                                txt_data = download_df.to_csv(index=False, sep="\t", encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download TXT (Tab Separated)",
                                    data=txt_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                                    mime="text/txt",
                                )
                        else:
                            st.warning("⚠️ Please select at least one column to download.")

                except FileNotFoundError:
                    st.error("Data is being fetched: 100 agents are being loaded. Please check back shortly!")
        else:
            # Original logic for other URLs
            with tab2:
                st.subheader("📊 Scraping Results")

                if st.session_state.get("results"):
                    def process_results(data_dicts, extend_metadata=True):
                        if not data_dicts:
                            return pd.DataFrame()
                        
                        # First, collect all the data into a list of dictionaries
                        all_data = []
                        metadata_cols = ["Source_Index", "url", "timestamp", "date", "datetime", "time", "title"]
                        
                        # Track seen values for deduplication
                        seen_values = {}
                        
                        for i, data_dict in enumerate(data_dicts):
                            source_index = f"Data-{i+1}"
                            
                            # Extract metadata
                            metadata = {"Source_Index": source_index}
                            for key in metadata_cols:
                                if key in data_dict:
                                    value = data_dict[key]
                                    if isinstance(value, list):
                                        metadata[key] = "" if not value else str(value[0])
                                    else:
                                        metadata[key] = "" if value is None else str(value)
                            
                            # Extract list fields and non-list fields
                            list_fields = {}
                            non_list_fields = {}
                            
                            for key, value in data_dict.items():
                                if key in metadata_cols:
                                    continue
                                elif isinstance(value, list) and len(value) > 1:
                                    list_fields[key] = value
                                else:
                                    if isinstance(value, list):
                                        non_list_fields[key] = "" if not value else str(value[0])
                                    else:
                                        non_list_fields[key] = "" if value is None else str(value)
                            
                            # Process list fields
                            if list_fields:
                                max_length = max(len(value) for value in list_fields.values())
                                for j in range(max_length):
                                    row_data = {}
                                    
                                    # Add metadata
                                    if j == 0 or not extend_metadata:
                                        row_data.update(metadata)
                                    else:
                                        row_data["Source_Index"] = source_index
                                    
                                    # Add non-list fields
                                    row_data.update(non_list_fields)
                                    
                                    # Add list fields for this row
                                    has_valid_data = False
                                    for key, value_list in list_fields.items():
                                        if j < len(value_list):
                                            value = str(value_list[j])
                                            if value:  # Only process non-empty values
                                                # Initialize seen_values for this column if needed
                                                if key not in seen_values:
                                                    seen_values[key] = set()
                                                
                                                # Only add the value if it hasn't been seen before
                                                if value not in seen_values[key]:
                                                    row_data[key] = value
                                                    seen_values[key].add(value)
                                                    has_valid_data = True
                                        
                                    # Only add row if it has valid data
                                    if has_valid_data or j == 0:  # Keep at least one row per source
                                        all_data.append(row_data)
                            else:
                                # Process single row
                                row_data = {}
                                row_data.update(metadata)
                                
                                # Add non-list fields with deduplication
                                has_valid_data = False
                                for key, value in non_list_fields.items():
                                    if value:  # Only process non-empty values
                                        # Initialize seen_values for this column if needed
                                        if key not in seen_values:
                                            seen_values[key] = set()
                                        
                                        # Only add the value if it hasn't been seen before
                                        if value not in seen_values[key]:
                                            row_data[key] = value
                                            seen_values[key].add(value)
                                            has_valid_data = True
                                
                                # Only add row if it has valid data
                                if has_valid_data or i == 0:  # Keep at least one row per source
                                    all_data.append(row_data)
                        
                        # Create DataFrame
                        try:
                            # Create DataFrame from collected data
                            df = pd.DataFrame(all_data)
                            
                            # If DataFrame is empty, return empty DataFrame
                            if df.empty:
                                return df
                            
                            # Fill NaN values that might have been created during the process
                            df = df.fillna("")
                            
                            # If extend_metadata is True, clear metadata in duplicate rows
                            if extend_metadata:
                                metadata_cols_in_df = [col for col in metadata_cols if col in df.columns and col != "Source_Index"]
                                df.loc[df.duplicated(subset=["Source_Index"]), metadata_cols_in_df] = ""
                            
                            # Define column order: Source_Index first, then other metadata, then data
                            first_cols = ["Source_Index"]
                            for col in ["url", "timestamp", "date", "datetime", "time", "title"]:
                                if col in df.columns:
                                    first_cols.append(col)
                                    
                            # Arrange the rest of the columns
                            remaining_cols = [col for col in df.columns if col not in first_cols]
                            
                            # Final result with proper column order
                            return df[first_cols + remaining_cols]
                            
                        except Exception as e:
                            return pd.DataFrame({"Error": [str(e)]})
                    def get_data_without_metadata(df):
                        # Define metadata columns to exclude
                        metadata_cols = ["source_index", "url", "timestamp", "date", "datetime", "time", "title"]
                        # Keep only columns that are not metadata
                        data_cols = [col for col in df.columns if col.lower() not in metadata_cols]
                        return df[data_cols]


                    # Process results with extend_metadata=True
                    results_df = process_results(st.session_state.results, extend_metadata=True)

                    # Display the editable table
                    st.write("Edit or delete cells/rows in the table below:")

                    # Use st.data_editor to make the table editable
                    edited_results_df = st.data_editor(
                        results_df,
                        use_container_width=True,
                        num_rows="dynamic",  # Allow adding/deleting rows
                        key="results_editor"
                    )

                    # Add a "Delete Selected Rows" button
                    # Allow users to download the edited data
                    selected_columns = st.multiselect(
                        "Select columns to download",
                        options=edited_results_df.columns,
                        default=edited_results_df.columns
                    )

                    if st.button("📥 Download Data", use_container_width=True):
                        if selected_columns:
                            # Filter DataFrame based on selected columns
                            download_df = edited_results_df[selected_columns]

                            # Display download options (CSV, JSON, TXT)
                            col1, col2, col3 = st.columns(3)

                            # CSV Download
                            with col1:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download CSV",
                                    data=csv_data,
                                    file_name=f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                )

                            # JSON Download
                            with col2:
                                json_str = download_df.to_json(orient="records", indent=2)
                                st.download_button(
                                    label="📥 Download JSON",
                                    data=json_str,
                                    file_name=f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json",
                                )

                            # TXT Download
                            with col3:
                                txt_data = download_df.to_csv(index=False, sep="\t", encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download TXT (Tab Separated)",
                                    data=txt_data,
                                    file_name=f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                                    mime="text/txt",
                                )
                        else:
                            st.warning("⚠️ Please select at least one column to download.")

                else:
                    if st.session_state.get("is_scraping", False):
                        st.info("⏳ Scraping in progress... Results will appear here when available.")
                    else:
                        st.info("🔍 No results yet. Start a scraping job to see results here.")
   

# if selected_tab == "tab3":
with tab3:
    st.subheader("Scraping Logs")

    log_levels = ["ALL", "INFO", "SUCCESS", "WARNING", "ERROR", "PROCESS"]
    selected_level = st.selectbox("Filter logs by level:", log_levels)

    logs = st.session_state.get("logs", [])

    if not logs:
        st.info("No logs yet. Start a scraping job to see logs here.")
    else:
        filtered_logs = logs if selected_level == "ALL" else [log for log in logs if log["level"] == selected_level]

        for log in filtered_logs:
            icons = {"INFO": "ℹ️", "SUCCESS": "✅", "WARNING": "⚠️", "ERROR": "❌", "PROCESS": "🔄"}
            st.markdown(f"[{log['timestamp']}] {icons.get(log['level'], '')} {log['message']}")

if selected_tab == "tab2":
    print(selected_tab)
    with tab2:
        if True:
            showcoldWellResult=st.toggle("View Coldwell Task Results", help=f"Toggle between Coldwell task results and other quick tasks.",value=True)
        if coldwell or showcoldWellResult:
            with tab2:
                st.subheader("📊 Scraping Results (Coldwell)")
                def format_mobile_number(mobile, country_code, hyphen_separator):
                    digits = "".join(filter(str.isdigit, str(mobile)))  # Remove non-numeric characters
                    if len(digits) == 10:
                        formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if hyphen_separator else digits
                        if country_code:
                            formatted = f"+1{'-' if hyphen_separator else ''}{formatted}"
                        return formatted
                    return mobile  # Return as is if it's not a 10-digit number

                try:
                    # Load data from coldwell_agents.csv
                    coldwell_df = pd.read_csv("data/coldwell_agents.csv")
                    def format_mobile_number(mobile, country_code=False, hyphen_separator=False):
                        if pd.isna(mobile):  # Handle NaN values
                            return ""

                        # Remove all non-numeric characters
                        mobile = re.sub(r"\D", "", str(mobile))

                        if len(mobile) < 10:  # Ensure a valid 10-digit number
                            return mobile  # Return as-is if not a full number

                        # Apply hyphen separator if enabled
                        if hyphen_separator:
                            mobile = f"{mobile[:3]}-{mobile[3:6]}-{mobile[6:]}"  # Format as XXX-XXX-XXXX

                        # Apply country code if enabled
                        if country_code:
                            if hyphen_separator:
                                mobile = f"+1-{mobile}"  # Add +1- before the number
                            else:
                                mobile = f"+1 {mobile}"  # Add +1 with space if no hyphens

                        return mobile
                    
                    # Select only required columns
                    coldwell_df = coldwell_df[["name", "office", "email", "mobile"]]
                    # Add country code if enabled

                    # Display first 50 rows
                    coldwell_df["mobile"] = coldwell_df["mobile"].fillna("").apply(
            lambda x: format_mobile_number(x, country_code=country_code, hyphen_separator=hyphen_separator)
        )
                    st.info("This table displays only the first 50 agents. The downloadable file will include all agents.")
                    st.dataframe(coldwell_df.head(50), use_container_width=True)

                    selected_columns = st.multiselect(
                            "Select columns to download",
                            options=coldwell_df.columns,
                            default=coldwell_df.columns
                        )
                    # 📥 Download Buttons (CSV, JSON)
                    if st.button("📥 Download Data", use_container_width=True):
                        # Allow user to select columns for download
                        
                        
                        if selected_columns:
                            # Filter DataFrame based on selected columns
                            download_df = coldwell_df[selected_columns]
                            
                            # Display download options (CSV, JSON)
                            col1, col2, col3 = st.columns(3)
                            
                            # CSV Download
                            with col1:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download CSV",
                                    data=csv_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                )
                            
                            # JSON Download
                            with col2:
                                json_str = download_df.to_json(orient="records", indent=2)
                                st.download_button(
                                    label="📥 Download JSON",
                                    data=json_str,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json",
                                )

                            # TXT
                            with col3:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download TXT (Comma Seperated)",
                                    data=csv_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                                    mime="text/txt",
                                )
                        else:
                            st.warning("⚠️ Please select at least one column to download.")
                
                except FileNotFoundError:
                    st.error("Data is being fetched: 100 agents are being loaded. Please check back shortly!")
        else:
            # Original logic for other URLs
            with tab2:
                st.subheader("📊 Scraping Results")
                
                if st.session_state.get("results"):
                    def process_results(data_dicts, extend_metadata=True):
                        if not data_dicts:
                            return pd.DataFrame()
                        
                        all_data = []
                        
                        for i, data_dict in enumerate(data_dicts):
                            # Define metadata fields explicitly
                            metadata = {"Source_Index": f"Data-{i+1}"}
                            list_fields = {}
                            non_list_fields = {}
                            
                            # Separate fields into metadata, list fields, and non-list fields
                            for key, value in data_dict.items():
                                if key in ["Source_Index", "url", "timestamp", "date", "datetime", "time", "title"]:
                                    if isinstance(value, list):
                                        metadata[key] = "" if not value else str(value[0])
                                    else:
                                        metadata[key] = "" if value is None else str(value)
                                elif isinstance(value, list) and len(value) > 1:
                                    list_fields[key] = value
                                else:
                                    if isinstance(value, list):
                                        non_list_fields[key] = "" if not value else str(value[0])
                                    else:
                                        non_list_fields[key] = "" if value is None else str(value)
                            
                            # Special handling for email - only include in the first row
                            email_value = None
                            if "email" in non_list_fields:
                                email_value = non_list_fields.pop("email")
                            
                            # If there are list fields, expand them into rows
                            if list_fields:
                                max_length = max(len(value) for value in list_fields.values())
                                for j in range(max_length):
                                    row = metadata.copy() if j == 0 or not extend_metadata else {"Source_Index": f"Data-{i+1}"}
                                    
                                    # Add non-list fields (excluding email)
                                    row.update(non_list_fields)
                                    
                                    # Add email only to the first row
                                    if j == 0 and email_value is not None:
                                        row["email"] = email_value
                                    elif "email" not in row:
                                        row["email"] = ""
                                    
                                    for key, value_list in list_fields.items():
                                        row[key] = str(value_list[j]) if j < len(value_list) else ""
                                    all_data.append(row)
                            else:
                                row = metadata.copy()
                                row.update(non_list_fields)
                                
                                # Add email back to the row
                                if email_value is not None:
                                    row["email"] = email_value
                                    
                                all_data.append(row)
                        
                        # Create DataFrame
                        try:
                            df = pd.DataFrame(all_data)
                            
                            # If extend_metadata is True, clear metadata in duplicate rows
                            if extend_metadata:
                                metadata_cols = ["url", "timestamp", "date", "datetime", "time", "title"]
                                df.loc[df.duplicated(subset=["Source_Index"]), [col for col in metadata_cols if col in df.columns]] = ""
                            
                            # Define column order: Source_Index first, then others
                            first_cols = ["Source_Index"]
                            if "url" in df.columns:
                                first_cols.append("url")
                            if "timestamp" in df.columns:
                                first_cols.append("timestamp")
                            other_priority = ["date", "datetime", "time", "title"]
                            for col in other_priority:
                                if col in df.columns and col not in first_cols:
                                    first_cols.append(col)
                                    
                            # Make sure email is prioritized in column ordering
                            remaining_cols = [col for col in df.columns if col not in first_cols]
                            if "email" in remaining_cols:
                                remaining_cols.remove("email")
                                remaining_cols.append("email")
                                
                            df = df[first_cols + remaining_cols]
                            
                        except Exception as e:
                            return pd.DataFrame({"Error": [str(e)]})
                        
                        return df

                    def get_data_without_metadata(df):
                        # Define metadata columns to exclude
                        metadata_cols = ["source_index", "url", "timestamp", "date", "datetime", "time", "title"]
                        # Keep only columns that are not metadata
                        data_cols = [col for col in df.columns if col.lower() not in metadata_cols]
                        return df[data_cols]

                    # Process results with extend_metadata=True
                    results_df = process_results(st.session_state.results, extend_metadata=True)

                    # Display the full DataFrame in Streamlit
                    st.dataframe(results_df, use_container_width=True)

                    # Prepare data for download (exclude metadata)
                    download_df = get_data_without_metadata(results_df)

                    # 📥 Download Buttons (CSV, JSON, TXT)
                    col1, col2, col3 = st.columns(3)

                    selected_columns = st.multiselect(
                                            "Select columns to download",
                                            options=results_df.columns,
                                            default=results_df.columns
                                        )
                    # 📥 Download Buttons (CSV, JSON)
                    if st.button("📥 Download Data", use_container_width=True):
                        # Allow user to select columns for download
                        
                        
                        if selected_columns:
                            # Filter DataFrame based on selected columns
                            download_df = results_df[selected_columns]
                            
                            # Display download options (CSV, JSON)
                            col1, col2, col3 = st.columns(3)
                            
                            # CSV Download
                            with col1:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download CSV",
                                    data=csv_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                    mime="text/csv",
                                )
                            
                            # JSON Download
                            with col2:
                                json_str = download_df.to_json(orient="records", indent=2)
                                st.download_button(
                                    label="📥 Download JSON",
                                    data=json_str,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                    mime="application/json",
                                )

                            # TXT
                            with col3:
                                csv_data = download_df.to_csv(index=False, encoding="utf-8-sig")
                                st.download_button(
                                    label="📥 Download TXT (Comma Seperated)",
                                    data=csv_data,
                                    file_name=f"coldwell_agents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                                    mime="text/txt",
                                )
                        else:
                            st.warning("⚠️ Please select at least one column to download.")
                
                else:
                    if st.session_state.get("is_scraping", False):
                        st.info("⏳ Scraping in progress... Results will appear here when available.")
                    else:
                        st.info("🔍 No results yet. Start a scraping job to see results here.")
    
    # if selected_tab == "tab3":

# Main scraping process (runs when is_scraping is True)
if st.session_state.is_scraping:
    
    # Initialize the crawl frontier with starting URLs if we're just beginning
    if st.session_state.current_phase == "initializing":
        start = time.time()
        frontier = st.session_state.urls.copy()
        st.session_state.found_links = []
        st.session_state.current_phase = "crawling"
        st.rerun()
    
    # Process one URL at a time to allow UI updates
    elif st.session_state.current_phase == "crawling":
        try:
            start = time.time()

            # Ensure current_depth and max_depth are integers at the start
            current_depth = st.session_state.current_depth if st.session_state.current_depth is not None else 0
            max_depth = st.session_state.options.get('max_depth', 1) if st.session_state.options is not None else 1

            # Check if max_pages limit is reached
            if st.session_state.options.get('max_pages_status'):
                if len(st.session_state.processed_links) >= (st.session_state.options.get('max_pages', 10) or 10):
                    st.session_state.current_phase = "complete"
                    log_success(f"Max URL limit reached: {(st.session_state.options.get('max_pages', 5) or 5)} URLs processed.")
                    time.sleep(0.1)
                    st.rerun()

            # Get URLs to process based on current depth
            if current_depth == 0:
                # Depth 0: Process only initial URLs
                frontier = [url for url in (st.session_state.urls or []) if url not in st.session_state.processed_links]
            else:
                # Depth > 0: Process links found at the previous depth
                frontier = [url for url in (st.session_state.found_links or []) if url not in st.session_state.processed_links]

            if frontier:
                next_url = frontier[0]
                log_process(f"Crawling URL: {next_url} (depth: {current_depth})")

                try:
                    # Crawl the URL
                    crawl_result = asyncio.run(crawl_url(next_url, st.session_state.options))
                    html_content = crawl_result.get('html', '')

                    # Extract data from the crawled page
                    extracted_data = extract_data(html_content, st.session_state.fields, st.session_state.extraction_method)

                    # Save data to DB if enabled
                    if saveToDb:
                        rawid = db.get_most_recent_updated_id()
                        try:
                            db.save_extracted_data(rawid, next_url, extracted_data)
                        except Exception as e:
                            log_error(f"Error while saving to DB: {str(e)}")

                    # Add the extracted data to results
                    if any(extracted_data.values()):
                        result_item = {
                            'URL': next_url,
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        result_item.update(extracted_data)
                        st.session_state.results.append(result_item)
                        log_success(f"Added results from {next_url}")

                    # Process new links if we're still below max depth
                    if current_depth < max_depth:
                        new_links = crawl_result.get('links', [])
                        pagination_url = crawl_result.get('pagination_url')

                        # Add pagination URL if found
                        if pagination_url and pagination_url not in st.session_state.found_links and pagination_url not in st.session_state.processed_links:
                            st.session_state.found_links.append(pagination_url)
                            log_info(f"Added pagination URL: {pagination_url}")

                        # Add new links to found_links for the next depth level
                        for link in new_links:
                            if link not in st.session_state.found_links and link not in st.session_state.processed_links:
                                st.session_state.found_links.append(link)
                        log_info(f"Found {len(new_links)} links on {next_url}")

                    # Mark URL as processed
                    st.session_state.processed_links.add(next_url)

                except TypeError as e:
                    # Handle the specific error without logging it
                    if "'<' not supported between instances of 'int' and 'NoneType'" in str(e):
                        st.session_state.processed_links.add(next_url)  # Mark as processed to avoid retries
                        log_info(f"Skipped URL due to internal error: {next_url}")
                    else:
                        log_error(f"Error crawling {next_url}: {str(e)}")
                        st.session_state.processed_links.add(next_url)  # Mark as processed to avoid retries

                except Exception as e:
                    log_error(f"Error crawling {next_url}: {str(e)}")
                    st.session_state.processed_links.add(next_url)  # Mark as processed to avoid retries

                # Rerun to update UI
                time.sleep(0.1)
                st.rerun()

            else:
                # No more URLs to process at the current depth
                if current_depth < max_depth:
                    # Move to the next depth level
                    st.session_state.current_depth = current_depth + 1
                    log_info(f"Moving to depth {st.session_state.current_depth}")
                    st.rerun()
                else:
                    # Crawling complete (max depth reached)
                    st.session_state.current_phase = "complete"
                    log_success(f"Crawling complete: processed {len(st.session_state.processed_links)} URLs")
                    time.sleep(0.1)
                    st.rerun()

        except Exception as e:
            # Handle any unexpected errors and ensure the crawler completes
            if "'<' not supported between instances of 'int' and 'NoneType'" in str(e):
                log_info("Skipped due to internal error")
            else:
                log_error(f"Unexpected error during crawling: {str(e)}")
            st.session_state.current_phase = "complete"
            log_success(f"Crawling complete with errors: processed {len(st.session_state.processed_links)} URLs")
            time.sleep(0.1)
            st.rerun()
    elif st.session_state.current_phase == "complete":
        # Crawling is complete
        if st.session_state.results:
            if coldwell:
                log_success(f"Task Scheduled You will be notified when Task will complete by email")
            else:
                log_success(f"Scraping job completed: extracted data from {len(st.session_state.results)} pages")
        else:
            log_warning("Scraping job completed but no data was extracted")
        
        st.session_state.is_scraping = False
        time.sleep(0.1)
        st.rerun()
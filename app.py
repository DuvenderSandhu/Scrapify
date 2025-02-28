# app.py - Advanced Web Scraper with Comprehensive Crawling

import streamlit as st
import pandas as pd
import numpy as np
import time
import random
import re
import json
from log import log_process,log_error,log_warning,log_success,log_info,add_log
from datetime import datetime
from urllib.parse import urlparse, urljoin
import uuid
from crawler import get_html_sync
from scraper import find_elements_by_selector,extract_data_with_ai
# Page configuration
st.set_page_config(
    page_title="Advanced Web Scraper",
    page_icon="🕸️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
    'name': r'[A-Z][a-z]+ [A-Z][a-z]+'
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
def crawl_url(url, options):
    """Simulate crawling a URL with configurable options"""
    log_process(f"Crawling URL: {url}")
    domain = extract_domain(url)
    log_info(f"Connecting to {domain}")
    
    # Simulate network delay
    # time.sleep(random.uniform(0.5, 1.5))
    
    # Generate simulated HTML content
    button= options.get('pagination_selector', False) or options.get('pagination_xpath', False) or options.get('pagination_text', False) or options.get('pagination_text_match', False) or options.get('pagination_confidence', False)
    print("button",button)
    html_content = get_html_sync(url,button,options)
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
        'pagination_url': pagination_url
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

def extract_data(html_content, fields, method="regex"):
    """Extract data from HTML based on specified fields"""
    log_process(f"Extracting data using {method} method")
    
    results = {}
    
    for field in fields:
        log_info(f"Extracting field: {field}")
        field_lower = field.lower()
        
        # Determine extraction method by field type
        if method == "regex":
            if field_lower in REGEX_PATTERNS:
                pattern = REGEX_PATTERNS[field_lower]
                matches = re.findall(pattern, html_content)
                if matches:
                    log_success(f"Found {len(matches)} matches for {field}")
                    results[field] = list(set(matches))
                else:
                    log_warning(f"No matches found for {field}")
                    results[field] = []
            elif 'email' in field_lower:
                matches = re.findall(REGEX_PATTERNS['email'], html_content)
                results[field] = list(set(matches))
            elif any(keyword in field_lower for keyword in ['phone', 'tel', 'mobile']):
                matches = re.findall(REGEX_PATTERNS['phone'], html_content)
                formatted_numbers = ["+1"+re.sub(r'[\(\)\s\-]', '', number) for number in matches]
                results[field] = list(set(formatted_numbers))
            elif any(keyword in field_lower for keyword in ['address', 'location']):
                matches = re.findall(REGEX_PATTERNS['address'], html_content)
                results[field] = list(set(matches))
            else:
                # For unknown fields, try some heuristics
                log_warning(f"No predefined pattern for {field}, attempting generic extraction")
                results[field] = extract_unknown_field(html_content, field)
        elif method == "css":
            # In a real implementation, we would use BeautifulSoup selectors
            # Here we'll simulate CSS-based extraction
            results[field] = find_elements_by_selector(html_content,field)
            # simulate_css_extraction(html_content, field)
        elif method == "ai":
            # Simulate AI-based extraction
            print("st.session_state.ai_provider",st.session_state.ai_provider)
            ai_response = extract_data_with_ai(html_content,field,st.session_state.ai_provider,st.session_state.ai_api)
            for field, data in ai_response.items():
                results[field] = data 
            break
            # simulate_ai_extraction(html_content, field)
    
    return results

def extract_unknown_field(html_content, field):
    """Attempt to extract an unknown field using heuristics"""
    # Check for content near headings or labels that might match the field
    patterns = [
        f'<h[1-6][^>]*>.*?{field}.*?</h[1-6]>.*?<p>(.*?)</p>',
        f'<label[^>]*>{field}[^<]*</label>.*?<input[^>]*value="([^"]*)"',
        f'<dt[^>]*>{field}[^<]*</dt>.*?<dd[^>]*>(.*?)</dd>',
        f'<strong[^>]*>{field}[^<]*</strong>.*?:?\\s*(.*?)(?:<|$)',
        f'{field}\\s*:\\s*([^<\\n]+)'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
        if matches:
            return matches
    
    return []

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
    elif 'name' in field_lower or 'contact' in field_lower:
        # AI might be better at extracting complete names
        names = []
        name_matches = re.findall(r'<h[3-4][^>]*>([A-Z][a-z]+ [A-Z][a-z]+)</h[3-4]>', html_content)
        if name_matches:
            names.extend(name_matches)
        else:
            # Simulate AI finding names in context
            names = ["John Smith", "Jane Doe", "Robert Johnson"]
        return names
    elif any(keyword in field_lower for keyword in ['phone', 'tel', 'mobile']):
        # Extract phone numbers
        phone_matches = re.findall(REGEX_PATTERNS['phone'], html_content)
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

# Create tabs for different sections
tab1, tab2, tab3 = st.tabs(["Setup & Controls", "Results", "Logs"])
ai_provider=""
ai_api=""
# pagination_text_match= ""
with tab1:
    # Setup & Controls tab
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Configure Scraping Job")
        
        # URL Input
        url_input = st.text_area(
            "Enter URLs to scrape (one per line):",
            placeholder="https://example.com\nhttps://anothersite.com",
            height=100
        )
        
        # Field Configuration
        st.subheader("Data Extraction")
        
        field_col1, field_col2 = st.columns(2)
        with field_col1:
            field_input = st.text_input("Enter field to extract:", placeholder="e.g., Email, Phone, Price")
        with field_col2:
            if st.button("Add Field", use_container_width=True):
                if field_input and field_input not in st.session_state.fields:
                    st.session_state.fields.append(field_input)
                    log_info(f"Added field: {field_input}")
        
        # Display added fields with remove option
        if st.session_state.fields:
            st.write("Fields to extract:")
            for i, field in enumerate(st.session_state.fields):
                st.markdown(
                    f"""
                    <div class="field-card" style="background:rgba(0,0,0,0.5)" >
                        <span style="color:white">{field}</span>
                        # <button kind="secondary" class="remove-btn" id="remove_{i}">Remove</button>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                remove_btn = st.button(f"❌ Remove {field}", key=f"remove_{i}", help=f"Remove {field} from extraction list")
                if remove_btn:
                    st.session_state.fields.remove(field)
                    log_info(f"Removed field: {field}")
                    st.rerun()
        else:
            st.info("No fields added yet. Add fields to extract data.")
        
        # Advanced Crawling Options
        st.subheader("Crawling Options")
        
        with st.expander("Configure Crawling Behavior", expanded=True):
            crawl_col1, crawl_col2 = st.columns(2)
            
            with crawl_col1:
                follow_links = st.checkbox("Follow links", value=False, 
                                         help="Crawl links found on the page")
                
                max_depth = st.slider("Maximum crawl depth", 0, 5, 1,
                                     help="0 = only starting URLs, 1 = follow one level of links, etc.")
                
                max_pages = st.number_input("Maximum pages to crawl", 1, 100, 10,
                                          help="Limit the total number of pages crawled")
            
            with crawl_col2:
                stay_on_domain = st.checkbox("Stay on same domain", value=True,
                                           help="Only follow links on the same domain")
                
                handle_pagination = st.checkbox("Handle pagination", value=True,
                                              help="Try to find and follow 'Next Page' links")
                
                handle_lazy_loading = st.checkbox("Handle lazy loading", value=False,
                                                help="Attempt to trigger lazy loading content")
        
        # Enhanced pagination options
        if handle_pagination:
            st.subheader("Pagination Control")
            
            pagination_method = st.selectbox(
                "Next Button Detection Method:",
                ["Auto-detect", "CSS Selector", "XPath", "Button Text", "AI-powered"],
                help="Choose how to identify the 'Next' button or link on the page"
            )
            
            # Additional options based on selected method
            if pagination_method == "CSS Selector":
                pagination_selector = st.text_input(
                    "CSS Selector for next button/link:",
                    value=".pagination .next, a.next-page, [rel='next']",
                    help="Enter CSS selector that identifies the next page button or link"
                )
                
                st.markdown("""
                <div class="helper-card">
                    <strong>Example CSS Selectors:</strong>
                    <ul style="margin-bottom: 0;">
                        <li><code>.pagination .next</code> - Targets an element with class "next" inside an element with class "pagination"</li>
                        <li><code>#next-page</code> - Targets an element with ID "next-page"</li>
                        <li><code>a[rel="next"]</code> - Targets an anchor tag with attribute rel="next"</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
                
            elif pagination_method == "XPath":
                pagination_xpath = st.text_input(
                    "XPath for next button/link:",
                    value="//a[contains(@class, 'next') or contains(text(), 'Next')]",
                    help="Enter XPath expression that identifies the next page button or link"
                )
                
                st.markdown("""
                <div class="helper-card">
                    <strong>Example XPath Expressions:</strong>
                    <ul style="margin-bottom: 0;">
                        <li><code>//a[contains(text(), 'Next')]</code> - Find anchor tags containing the text "Next"</li>
                        <li><code>//div[@class='pagination']/a[last()]</code> - Find the last anchor tag inside a div with class "pagination"</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
                
            elif pagination_method == "Button Text":
                
                
                # with col1:
                pagination_text = st.text_input(
                    "Text on next button/link:",
                    value="Next",
                    help="Enter the text that appears on the next page button or link"
                )
                
                # with col2:
                pagination_text_match=""
                # pagination_text_match = st.selectbox(
                #     "Text matching method:",
                #     ["Contains", "Exact match", "Starts with", "Ends with"],
                #     help="How to match the button text"
                # )
                
            elif pagination_method == "AI-powered":
                st.info("The AI will analyze the page structure to identify pagination patterns, looking for common indicators like numbered links, 'Next' buttons, or pagination controls.")
                
                pagination_confidence = st.slider(
                    "AI confidence threshold (%):",
                    min_value=50,
                    max_value=95,
                    value=70,
                    help="Only follow pagination links when AI confidence exceeds this threshold"
                )
            
            # Visual pagination helper
            # with st.expander("Pagination Pattern Examples", expanded=False):
            #     st.markdown("""
            #     <div class="pagination-example">
            #         <span class="pagination-page">1</span>
            #         <span class="pagination-page pagination-current">2</span>
            #         <span class="pagination-page">3</span>
            #         <span class="pagination-page">4</span>
            #         <span class="pagination-page pagination-next">Next →</span>
            #     </div>
                
            #     <div class="pagination-example">
            #         <span class="pagination-page">← Previous</span>
            #         <span class="pagination-page">1</span>
            #         <span class="pagination-page pagination-current">2</span>
            #         <span class="pagination-page">3</span>
            #         <span class="pagination-page pagination-next">Next →</span>
            #     </div>
                
            #     <div class="pagination-example">
            #         <button style="background-color:#f0f0f0; border:1px solid #ccc; padding:8px 15px; border-radius:4px;">
            #             Load More Results
            #         </button>
            #     </div>
            #     """, unsafe_allow_html=True)
        
        # Extraction Method
        st.subheader("Extraction Method")
        extraction_method = st.radio(
            "Select data extraction method:",
            ["regex", "css", "ai"],
            horizontal=True,
            help="regex: Basic pattern matching, CSS: Element-based extraction, AI: Intelligent context-aware extraction"
        )
        if(extraction_method=="regex"):
            st.markdown("The regex will extract only addresses, phone numbers, and email addresses based on predefined text structure patterns. ")
        if(extraction_method =="css"):
            st.markdown("Use CSS selectors (class or ID) to define fields. For example, #user-email-id for emails or .user-number for phone numbers.")
        if extraction_method =="ai":
            ai_provider = st.selectbox("Select AI Model", ["OpenAI", "Gemini", "DeepSeek","Groq"])
            ai_api = st.text_area("Enter AI API_KEY")
            
    with col2:
        # Status and Controls Section
        st.subheader("Status & Controls")
        
        if st.session_state.is_scraping:
            if st.button("⏹️ Stop Scraping", type="primary", use_container_width=True):
                st.session_state.is_scraping = False
                log_warning("Scraping job stopped by user")
                st.rerun()
            
            st.write("Scraping in progress...")
            progress = (len(st.session_state.processed_links) / max(1, len(st.session_state.urls) + len(st.session_state.found_links))) * 100
            st.progress(min(100, progress) / 100)
            
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
            start_disabled = not (url_input.strip() and st.session_state.fields)
            
            if st.button("▶️ Start Scraping", type="primary", use_container_width=True, disabled=start_disabled):
                # Parse URLs
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
                        'follow_links': follow_links,
                        'max_depth': max_depth,
                        'max_pages': max_pages,
                        'stay_on_domain': stay_on_domain,
                        'handle_pagination': handle_pagination,
                        'handle_lazy_loading': handle_lazy_loading,
                        'pagination_method': pagination_method if handle_pagination else None
                    }
                    
                    # Add method-specific pagination options
                    if handle_pagination:
                        if pagination_method == "CSS Selector":
                            options['pagination_selector'] = pagination_selector
                        elif pagination_method == "XPath":
                            options['pagination_xpath'] = pagination_xpath
                        elif pagination_method == "Button Text":
                            options['pagination_text'] = pagination_text
                            options['pagination_text_match'] = pagination_text_match
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
                if not st.session_state.fields:
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

with tab2:
    # Results tab
    st.subheader("Scraping Results")
    
    if st.session_state.results:
        # Convert results to DataFrame for display
        results_df = pd.DataFrame(st.session_state.results)
        
        # Show the raw DataFrame
        st.dataframe(results_df, use_container_width=True)
        
        # Download options
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 Download CSV", use_container_width=True):
                # Convert lists in DataFrame to strings for CSV
                for col in results_df.columns:
                    results_df[col] = results_df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
                
                # Create download link
                csv = results_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
        
        with col2:
            if st.button("📥 Download JSON", use_container_width=True):
                # Convert DataFrame to JSON
                json_str = results_df.to_json(orient="records", indent=2)
                st.download_button(
                    label="Download JSON",
                    data=json_str,
                    file_name=f"scraping_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                )
    else:
        if st.session_state.is_scraping:
            st.info("Scraping in progress... Results will appear here when available.")
        else:
            st.info("No results yet. Start a scraping job to see results here.")

with tab3:
    # Logs tab
    st.subheader("Scraping Logs")
    
    # Log filter
    log_levels = ["ALL", "INFO", "SUCCESS", "WARNING", "ERROR", "PROCESS"]
    selected_level = st.selectbox("Filter logs by level:", log_levels)
    
    # Display logs in a container with auto-scroll
    log_container = st.container()
    
    with log_container:
        st.markdown('<div class="log-container">', unsafe_allow_html=True)
        
        if not st.session_state.logs:
            st.info("No logs yet. Start a scraping job to see logs here.")
        else:
            filtered_logs = st.session_state.logs
            if selected_level != "ALL":
                filtered_logs = [log for log in st.session_state.logs if log["level"] == selected_level]
            
            for log in filtered_logs:
                timestamp = log["timestamp"]
                level = log["level"]
                message = log["message"]
                
                if level == "INFO":
                    st.markdown(f'<div class="log-info">[{timestamp}] ℹ️ {message}</div>', unsafe_allow_html=True)
                elif level == "SUCCESS":
                    st.markdown(f'<div class="log-success">[{timestamp}] ✅ {message}</div>', unsafe_allow_html=True)
                elif level == "WARNING":
                    st.markdown(f'<div class="log-warning">[{timestamp}] ⚠️ {message}</div>', unsafe_allow_html=True)
                elif level == "ERROR":
                    st.markdown(f'<div class="log-error">[{timestamp}] ❌ {message}</div>', unsafe_allow_html=True)
                elif level == "PROCESS":
                    st.markdown(f'<div class="log-process">[{timestamp}] 🔄 {message}</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)

# Main scraping process (runs when is_scraping is True)
if st.session_state.is_scraping:
    # Initialize the crawl frontier with starting URLs if we're just beginning
    if st.session_state.current_phase == "initializing":
        frontier = st.session_state.urls.copy()
        st.session_state.found_links = []
        st.session_state.current_phase = "crawling"
        st.rerun()
    
    # Process one URL at a time to allow UI updates
    elif st.session_state.current_phase == "crawling":
        # Get URLs to process: first frDom initial URLs, then found links
        remaining_urls = [url for url in st.session_state.urls if url not in st.session_state.processed_links]
        if len(st.session_state.processed_links) >= st.session_state.options.get('max_pages', 1):
            st.session_state.current_phase = "complete"
            log_success(f"Max URL limit reached: {st.session_state.options.get('max_pages', 1)} URLs processed.")
            time.sleep(0.1)
            st.rerun()
        if not remaining_urls and st.session_state.options.get('follow_links', False):
            # If we've processed all initial URLs, move to the found links
            if st.session_state.found_links and st.session_state.current_depth < st.session_state.options.get('max_depth', 1):
                next_url = st.session_state.found_links.pop(0)
                if next_url not in st.session_state.processed_links:
                    # Crawl the URL
                    log_process(f"Crawling found link: {next_url} (depth: {st.session_state.current_depth})")
                    
                    try:
                        # Crawl the URL
                        crawl_result = crawl_url(next_url, st.session_state.options)
                        html_content = crawl_result.get('html', '')
                        
                        # Extract data from the crawled page
                        extracted_data = extract_data(html_content, st.session_state.fields, st.session_state.extraction_method)
                        
                        # Add the extracted data to results
                        if any(extracted_data.values()):
                            result_item = {
                                'URL': next_url,
                                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            # Add extracted fields
                            for field, values in extracted_data.items():
                                result_item[field] = values
                            
                            st.session_state.results.append(result_item)
                            log_success(f"Added results from {next_url}")
                        
                        # Process new links if we're still below max depth
                        new_links = crawl_result.get('links', [])
                        pagination_url = crawl_result.get('pagination_url')
                        
                        # Add pagination URL if found
                        if pagination_url and pagination_url not in st.session_state.found_links and pagination_url not in st.session_state.processed_links:
                            st.session_state.found_links.append(pagination_url)
                            log_info(f"Added pagination URL: {pagination_url}")
                        
                        # Add new links to frontier
                        for link in new_links:
                            if link not in st.session_state.found_links and link not in st.session_state.processed_links:
                                st.session_state.found_links.append(link)
                        
                        # Mark as processed
                        st.session_state.processed_links.add(next_url)
                        
                    except Exception as e:
                        log_error(f"Error crawling {next_url}: {str(e)}")
                        st.session_state.processed_links.add(next_url)  # Mark as processed to avoid retries
                    
                    # Rerun to update UI
                    time.sleep(0.1)
                    st.rerun()
                else:
                    # URL already processed, move to next
                    time.sleep(0.1)
                    st.rerun()
            else:
                # No more links to process or max depth reached
                st.session_state.current_phase = "complete"
                log_success(f"Crawling complete: processed {len(st.session_state.processed_links)} URLs")
                time.sleep(0.1)
                st.rerun()
        elif remaining_urls:
            # Process next initial URL
            next_url = remaining_urls[0]
            log_process(f"Crawling initial URL: {next_url}")
            
            try:
                # Crawl the URL
                crawl_result = crawl_url(next_url, st.session_state.options)
                html_content = crawl_result.get('html', '')
                
                # Extract data from the crawled page
                extracted_data = extract_data(html_content, st.session_state.fields, st.session_state.extraction_method)
                
                # Add the extracted data to results
                if any(extracted_data.values()):
                    result_item = {
                        'URL': next_url,
                        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    # Add extracted fields
                    for field, values in extracted_data.items():
                        result_item[field] = values
                    
                    st.session_state.results.append(result_item)
                    log_success(f"Added results from {next_url}")
                
                # Process links if link following is enabled
                if st.session_state.options.get('follow_links', False):
                    new_links = crawl_result.get('links', [])
                    pagination_url = crawl_result.get('pagination_url')
                    
                    # Add pagination URL if found
                    if pagination_url and pagination_url not in st.session_state.found_links and pagination_url not in st.session_state.processed_links:
                        st.session_state.found_links.append(pagination_url)
                        log_info(f"Added pagination URL: {pagination_url}")
                    
                    # Add new links to frontier
                    for link in new_links:
                        if link not in st.session_state.found_links and link not in st.session_state.processed_links:
                            st.session_state.found_links.append(link)
                    
                    log_info(f"Found {len(new_links)} links on {next_url}")
                
                # Mark as processed
                st.session_state.processed_links.add(next_url)
                
            except Exception as e:
                log_error(f"Error crawling {next_url}: {str(e)}")
                st.session_state.processed_links.add(next_url)  # Mark as processed to avoid retries
            
            # Rerun to update UI
            time.sleep(0.1)
            st.rerun()
        else:
            # No more URLs to process
            st.session_state.current_phase = "complete"
            log_success(f"Crawling complete: processed {len(st.session_state.processed_links)} URLs")
            time.sleep(0.1)
            st.rerun()
    
    elif st.session_state.current_phase == "complete":
        # Crawling is complete
        if st.session_state.results:
            log_success(f"Scraping job completed: extracted data from {len(st.session_state.results)} pages")
        else:
            log_warning("Scraping job completed but no data was extracted")
        
        st.session_state.is_scraping = False
        time.sleep(0.1)
        st.rerun()
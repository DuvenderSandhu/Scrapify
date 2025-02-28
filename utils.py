# utils.py

import uuid
import re
from urllib.parse import urlparse
import time
from datetime import datetime

def generate_unique_id():
    """Generate a unique identifier"""
    return str(uuid.uuid4())

def validate_url(url):
    """Validate if a string is a proper URL"""
    try:
        result = urlparse(url)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except:
        return False

def extract_domain(url):
    """Extract the domain from a URL"""
    parsed_url = urlparse(url)
    return parsed_url.netloc

def format_timestamp(timestamp=None):
    """Format a timestamp for display"""
    if timestamp is None:
        timestamp = datetime.now()
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

def extract_with_regex(text, pattern):
    """Extract data using regex pattern"""
    matches = re.findall(pattern, text)
    return matches if matches else []

# Common regex patterns for data extraction
REGEX_PATTERNS = {
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'phone': r'(\+\d{1,3}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}',
    'url': r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\.-]*\??[-\w%&=]*',
    'date': r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}',
    'price': r'\$\s*\d+(?:\.\d{2})?',
    'address': r'\d+\s+[A-Za-z\s,]+\s+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Dr|Rd|Blvd|Ln|St)\.?(?:\s+[A-Za-z]+)?(?:,\s+[A-Za-z]+)?(?:,\s+[A-Z]{2})?(?:\s+\d{5})?',
    'name': r'[A-Z][a-z]+ [A-Z][a-z]+',
}

def create_extraction_plan(fields):
    """Create an extraction plan based on requested fields"""
    plan = {}
    
    for field in fields:
        field_lower = field.lower()
        
        # Match field to regex pattern
        if field_lower in REGEX_PATTERNS:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS[field_lower]
            }
        elif 'email' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['email']
            }
        elif 'phone' in field_lower or 'tel' in field_lower or 'mobile' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['phone']
            }
        elif 'date' in field_lower or 'dob' in field_lower or 'birthday' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['date']
            }
        elif 'price' in field_lower or 'cost' in field_lower or 'amount' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['price']
            }
        elif 'address' in field_lower or 'location' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['address']
            }
        elif 'name' in field_lower:
            plan[field] = {
                'type': 'regex',
                'pattern': REGEX_PATTERNS['name']
            }
        else:
            # For unknown fields, create a generic extraction plan
            plan[field] = {
                'type': 'unknown',
                'field': field
            }
    
    return plan

def truncate_string(text, max_length=100):
    """Truncate string to specified length and add ellipsis if needed"""
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."

class RateLimiter:
    """Simple rate limiter for API calls or web requests"""
    def __init__(self, calls_per_minute=60):
        self.calls_per_minute = calls_per_minute
        self.interval = 60 / calls_per_minute
        self.last_call_time = 0
    
    def wait(self):
        """Wait if necessary to respect rate limits"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.interval:
            sleep_time = self.interval - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()

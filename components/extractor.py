# components/extractor.py

import re
import time
import random
from utils import REGEX_PATTERNS, create_extraction_plan

class Extractor:
    """Component for extracting data from HTML content"""
    
    def __init__(self, logger):
        """Initialize the extractor with a logger"""
        self.logger = logger
    
    def extract(self, html_content, fields, method="AI"):
        """
        Extract requested fields from HTML content
        
        Args:
            html_content: HTML content as string
            fields: List of fields to extract
            method: Extraction method ("AI", "Pattern matching", "CSS Selectors")
            
        Returns:
            Dictionary with extracted data
        """
        self.logger.process(f"Extracting data using {method} method")
        
        # Create an extraction plan based on the fields
        extraction_plan = create_extraction_plan(fields)
        
        # Log the extraction plan
        self.logger.info(f"Created extraction plan for {len(fields)} fields")
        
        # Simulate processing time
        time.sleep(random.uniform(0.5, 1.0))
        
        results = {}
        
        if method == "AI":
            # Simulate AI-based extraction
            results = self._extract_with_ai(html_content, fields)
        elif method == "Pattern matching":
            # Use regex patterns for extraction
            results = self._extract_with_patterns(html_content, extraction_plan)
        elif method == "CSS Selectors":
            # Simulate CSS selector-based extraction
            results = self._extract_with_css(html_content, fields)
        
        # Count successful extractions
        successful = sum(1 for f in fields if f in results and results[f])
        self.logger.success(f"Successfully extracted {successful}/{len(fields)} fields")
        
        return results
    
    def _extract_with_patterns(self, html_content, extraction_plan):
        """Extract data using regex patterns"""
        results = {}
        
        for field, plan in extraction_plan.items():
            self.logger.info(f"Extracting field: {field}")
            
            if plan['type'] == 'regex' and 'pattern' in plan:
                pattern = plan['pattern']
                matches = re.findall(pattern, html_content)
                
                if matches:
                    self.logger.success(f"Found {len(matches)} matches for {field}")
                    results[field] = matches
                else:
                    self.logger.warning(f"No matches found for {field}")
                    results[field] = []
            else:
                # For unknown fields, attempt a basic extraction
                results[field] = self._extract_unknown_field(html_content, field)
        
        return results
    
    def _extract_with_ai(self, html_content, fields):
        """Simulate AI-based extraction"""
        results = {}
        
        # Simulate AI processing time
        time.sleep(random.uniform(1.0, 2.0))
        
        for field in fields:
            field_lower = field.lower()
            
            # Simulate different extraction results based on field name
            if 'email' in field_lower:
                # For email fields, simulate finding email addresses
                domain = re.search(r'<title>([^<]+)', html_content)
                domain = domain.group(1).split(' ')[0] if domain else "example.com"
                results[field] = [f"info@{domain}", f"contact@{domain}"]
                
            elif 'phone' in field_lower or 'mobile' in field_lower:
                # For phone fields, simulate finding phone numbers
                results[field] = ["+1 (555) 123-4567", "+1 (555) 987-6543"]
                
            elif 'name' in field_lower:
                # For name fields, simulate finding names
                results[field] = ["John Smith", "Jane Doe"]
                
            elif 'address' in field_lower:
                # For address fields, simulate finding addresses
                results[field] = ["123 Main Street, Anytown, CA 94043"]
                
            elif 'date' in field_lower:
                # For date fields, simulate finding dates
                results[field] = ["10/15/2024", "11/20/2024"]
                
            else:
                # For unknown fields, simulate generic extraction
                self.logger.warning(f"Field '{field}' not recognized for AI extraction, attempting generic extraction")
                results[field] = [f"Sample {field} data"]
                
            # Log the extraction
            self.logger.info(f"AI extracted {len(results[field])} values for {field}")
            
        return results
    
    def _extract_with_css(self, html_content, fields):
        """Simulate CSS selector-based extraction"""
        results = {}
        
        # Simulate CSS selector processing
        time.sleep(random.uniform(0.8, 1.5))
        
        # For demonstration, simulate some extraction results
        for field in fields:
            field_lower = field.lower()
            
            if 'email' in field_lower:
                results[field] = self._simulate_css_extraction('email', html_content)
            elif 'phone' in field_lower or 'mobile' in field_lower:
                results[field] = self._simulate_css_extraction('phone', html_content)
            elif 'name' in field_lower:
                results[field] = self._simulate_css_extraction('name', html_content)
            elif 'address' in field_lower:
                results[field] = self._simulate_css_extraction('address', html_content)
            else:
                results[field] = self._simulate_css_extraction('generic', html_content)
            
            self.logger.info(f"CSS extracted {len(results[field])} values for {field}")
            
        return results
    
    def _simulate_css_extraction(self, field_type, html_content):
        """Simulate extraction results for CSS selectors"""
        # In a real implementation, this would use BeautifulSoup or similar
        # to properly parse HTML and extract data using CSS selectors
        
        # For demonstration, return different simulated results based on field type
        if field_type == 'email':
            return ["info@example.com", "contact@example.com"]
        elif field_type == 'phone':
            return ["+1 (555) 123-4567"]
        elif field_type == 'name':
            return ["John Smith", "Jane Doe"]
        elif field_type == 'address':
            return ["123 Main Street, Anytown, CA 94043"]
        else:
            return [f"Sample {field_type} data"]
    
    def _extract_unknown_field(self, html_content, field):
        """Attempt to extract an unknown field using heuristics"""
        # Try to find content near headings that might contain the field
        pattern = f'<h[1-6][^>]*>.*?{field}.*?</h[1-6]>.*?<p>(.*?)</p>'
        matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if matches:
            return matches
        
        # Try to find content in list items that might contain the field
        pattern = f'<li>.*?{field}:?\s*(.*?)</li>'
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        
        if matches:
            return matches
        
        # Return empty list if nothing found
        return []

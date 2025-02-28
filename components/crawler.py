# components/crawler.py

import time
import random
from urllib.parse import urlparse
from config import SCRAPING_SETTINGS

class Crawler:
    """Web crawler component for fetching web pages"""
    
    def __init__(self, logger):
        """Initialize the crawler with a logger"""
        self.logger = logger
        self.user_agent = SCRAPING_SETTINGS["USER_AGENT"]
        self.timeout = SCRAPING_SETTINGS["DEFAULT_TIMEOUT"]
        self.max_retries = SCRAPING_SETTINGS["MAX_RETRIES"]
        self.retry_delay = SCRAPING_SETTINGS["RETRY_DELAY"]
    
    def crawl(self, url):
        """
        Crawl a URL and retrieve its HTML content
        
        In a real implementation, this would use requests, beautifulsoup, 
        or similar libraries to fetch actual web content.
        
        For this demo, we simulate fetching with random delays and 
        generate fake HTML content.
        """
        domain = urlparse(url).netloc
        self.logger.process(f"Crawling URL: {url}")
        self.logger.info(f"Connecting to {domain}")
        
        # Simulate network latency
        time.sleep(random.uniform(0.5, 1.5))
        
        # Simulate success/failure
        success = random.random() > 0.1  # 90% success rate
        
        if not success:
            self.logger.warning(f"Connection failed, retrying...")
            time.sleep(self.retry_delay)
            success = True  # Assume retry succeeds
        
        if success:
            # Generate simulated HTML content
            html_content = self._generate_html_content(url)
            content_size = len(html_content)
            
            self.logger.success(f"Successfully crawled {url} - {content_size} bytes received")
            return html_content
        else:
            self.logger.error(f"Failed to crawl {url} after multiple attempts")
            return ""
    
    def _generate_html_content(self, url):
        """Generate simulated HTML content for the given URL"""
        domain = urlparse(url).netloc
        
        # Create a simulated HTML structure with potential data to extract
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{domain} - Sample Page</title>
            <meta name="description" content="This is a sample page for {domain}">
        </head>
        <body>
            <header>
                <h1>Welcome to {domain}</h1>
                <nav>
                    <ul>
                        <li><a href="/">Home</a></li>
                        <li><a href="/about">About</a></li>
                        <li><a href="/contact">Contact</a></li>
                    </ul>
                </nav>
            </header>
            
            <main>
                <section class="hero">
                    <h2>Main Content for {domain}</h2>
                    <p>This is a simulated webpage for demonstration purposes.</p>
                </section>
                
                <section class="content">
                    <article>
                        <h3>Article Title</h3>
                        <p>Article content goes here. Published on 10/15/2024.</p>
                        <p>Author: John Smith</p>
                    </article>
                </section>
                
                <section class="contact-info">
                    <h3>Contact Information</h3>
                    <ul>
                        <li>Email: info@{domain}</li>
                        <li>Phone: +1 (555) 123-4567</li>
                        <li>Address: 123 Main Street, Anytown, CA 94043</li>
                    </ul>
                </section>
                
                <section class="team">
                    <h3>Our Team</h3>
                    <div class="team-member">
                        <h4>Jane Doe</h4>
                        <p>CEO</p>
                        <p>Email: jane.doe@{domain}</p>
                        <p>Phone: +1 (555) 987-6543</p>
                    </div>
                    <div class="team-member">
                        <h4>John Smith</h4>
                        <p>CTO</p>
                        <p>Email: john.smith@{domain}</p>
                        <p>Phone: +1 (555) 456-7890</p>
                    </div>
                </section>
            </main>
            
            <footer>
                <p>&copy; 2025 {domain} All rights reserved.</p>
                <p>Contact us: <a href="mailto:info@{domain}">info@{domain}</a></p>
            </footer>
        </body>
        </html>
        """
        
        return html
    
    def extract_links(self, html_content, base_url):
        """
        Extract links from HTML content (simulated)
        
        In a real implementation, this would use BeautifulSoup or similar
        to properly parse HTML and extract links.
        """
        # Simulate extracting 3-7 links
        num_links = random.randint(3, 7)
        domain = urlparse(base_url).netloc
        
        links = []
        for i in range(num_links):
            path = random.choice([
                "/about", 
                "/products", 
                "/services", 
                "/blog", 
                "/contact",
                "/team",
                "/portfolio",
                "/pricing"
            ])
            links.append(f"https://{domain}{path}")
        
        self.logger.info(f"Extracted {len(links)} links from {base_url}")
        return links

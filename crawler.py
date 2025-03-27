import asyncio
import time
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from log import log_info, log_success, log_error, log_warning  # Import logging functions
from database import db
from assets import selected_user_agent,http_headers,random_zigzag_move
import random
from fake_useragent import UserAgent
ua_platform= ['desktop','mobile','tablet']
ua_os= ["Windows", "Linux", "Ubuntu", "Chrome OS", "Mac OS X", "Android", "iOS"]

from fakeagents import get_random_user_agent
import spacy
rawid = ""
async def random_sleep(min_delay=1, max_delay=3):
    """Sleep for a random amount of time between min_delay and max_delay seconds."""
    delay = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay)

async def random_zigzag_move(page, start_x, start_y, end_x, end_y):
    """Simulate a human-like zigzag mouse movement."""
    steps = random.randint(5, 10)
    x_step = (end_x - start_x) / steps
    y_step = (end_y - start_y) / steps

    for i in range(steps):
        x = start_x + x_step * i
        y = start_y + y_step * i
        await page.mouse.move(x + random.randint(-50, 50), y + random.randint(-50, 50))
        await random_sleep(0.1, 0.3)

import asyncio
import random
import time
from crawl4ai import AsyncWebCrawler,BrowserConfig  # Import AsyncWebCrawler
from playwright.async_api import async_playwright
from fake_useragent import UserAgent

# Assuming ua_os, ua_platform, log_info, log_success, log_warning, log_error, random_sleep, random_zigzag_move, db are defined elsewhere

import asyncio
import random
import time
from crawl4ai import AsyncWebCrawler  # Import AsyncWebCrawler
from playwright.async_api import async_playwright
from fake_useragent import UserAgent

# Assuming ua_os, ua_platform, log_info, log_success, log_warning, log_error, random_sleep, random_zigzag_move, db are defined elsewhere

import asyncio
import random
import time
from crawl4ai import AsyncWebCrawler  # Import AsyncWebCrawler
from playwright.async_api import async_playwright
from fake_useragent import UserAgent

# Assuming ua_os, ua_platform, log_info, log_success, log_warning, log_error, random_sleep, random_zigzag_move, db are defined elsewhere

async def get_html(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """
    Fetch HTML content by navigating to a URL and extracting a strictly meaningful container using AsyncWebCrawler.
    """
    ua = UserAgent(os=random.choice(ua_os), platforms=random.choice(ua_platform))
    # print("ua", ua.random)
    options = options or {}
    max_pages = options.get('max_pages', 1)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    numbered = options.get('pagination_method', False) == "Numbered"
    handle_pagination = options.get('handle_pagination', False)
    js_timeout = options.get('js_timeout', 10000)           # Reduced JS timeout (ms)
    navigation_timeout = options.get('navigation_timeout', 30000)  # Reduced navigation timeout (ms)
    retry_attempts = options.get('retry_attempts', 2)
    
    start_time = time.time()
    log_info(f"Starting HTML fetch from: {url} [Options: lazy_loading={handle_lazy_loading}, pagination={handle_pagination}]")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
        )
        log_info("Launched headless Chromium browser with args: --disable-web-security")
        
        for attempt in range(retry_attempts + 1):
            if attempt > 0:
                log_info(f"Retry attempt {attempt + 1}/{retry_attempts + 1}")
            
            print(selected_user_agent)
            # Create a fresh context and page for each attempt.
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=get_random_user_agent(),
                ignore_https_errors=True,
                java_script_enabled=True
            )
            page = await context.new_page()
            print(http_headers)
            # await page.set_extra_http_headers(http_headers)
            page.set_default_timeout(navigation_timeout)
            await random_sleep()
            # page.on("dialog", lambda dialog: asyncio.create_task(dialog.dismiss()))
            start_x, start_y = 100, 100
            await page.mouse.move(start_x, start_y)
            # End position (e.g., somewhere in the middle of the page)
            end_x, end_y = 600, 600

            # Perform the human-like zigzag mouse movement
            await random_zigzag_move(page, start_x, start_y, end_x, end_y)
            try:
                log_info(f"Navigating to {url} with timeout {navigation_timeout}ms")
                nav_start = time.time()
                response = await page.goto(url, timeout=navigation_timeout, wait_until="domcontentloaded")
                nav_time = time.time() - nav_start
                
                if response and response.ok:
                    log_success(f"Navigation succeeded in {nav_time:.2f}s (Status: {response.status})")
                else:
                    log_warning(f"Navigation issue after {nav_time:.2f}s: Status={response.status if response else 'No response'}")
                    await context.close()
                    continue

                # Wait for the initial network activities to settle.
                await page.wait_for_timeout(3000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                    log_success("Page reached network idle state")
                except Exception as e:
                    # log_warning(f"Network idle timeout: {str(e)}")
                    print(str(e))
                
                if handle_pagination and button:
                    await handle_pagination_with_backoff(page, button, loader, max_pages)
                if handle_pagination and numbered:
                    await handle_numbered_pagination_with_backoff(page, url)
                if handle_lazy_loading:
                    await handle_lazy_loading_with_limits(page)
                
                await page.wait_for_timeout(2000)
                html_content = await page.content()
                raw_size = len(html_content)
                
                if raw_size > 500:
                    log_success(f"Extracted raw HTML: {raw_size} bytes")
                    # Use AsyncWebCrawler to extract meaningful content
                    proxy_config = {
                        "server": "http://proxy.example.com:8080",
                        "username": "user",
                        "password": "pass"
                    }

                    # browser_config = BrowserConfig(proxy_config=proxy_config)
                    async with AsyncWebCrawler() as crawler:#config=browser_config
                        result = await crawler.arun(url=url)  # Use arun to process the URL
                        filtered_html = result.html  # Extract the markdown content
                        filtered_size = len(filtered_html)
                        reduction = ((raw_size - filtered_size) / raw_size * 100)
                        log_info(f"Filtered content size: {filtered_size} bytes (reduction: {reduction:.1f}%)")
                        global rawid
                        if options.get('saveToDb', False):
                            print("Saving Raw")
                            rawid = db.save_raw_html(url, filtered_html)
                        await context.close()
                        log_info(f"Fetch completed in {time.time() - start_time:.2f}s")
                        await browser.close()
                        return filtered_html
                else:
                    log_warning(f"Raw HTML too small ({raw_size} bytes), retrying")
                    await context.close()
                    continue
                    
            except Exception as e:
                log_error(f"Error during fetch: {str(e)}")
                await context.close()
            # End of attempt loop
        
        await browser.close()
        log_info("Browser closed")
        log_error(f"All {retry_attempts + 1} attempts failed for {url}")
        return ""
        
             
import asyncio
import json
import csv
import time
import os
import psutil
from datetime import datetime
from playwright.async_api import async_playwright
import signal

# Global variables for tracking and resuming
CHECKPOINT_FILE = "scraper_checkpoint.json"
BATCH_SIZE = 200  # Save data after processing this many agents
MAX_CONCURRENT_PAGES = 5  # Limit concurrent pages to avoid memory issues

class ScraperState:
    def __init__(self):
        self.current_city_page = 1
        self.current_city_index = 0
        self.current_agent_page = 1
        self.processed_agents = 0
        self.total_agents = 0
        self.start_time = time.time()
        self.is_running = True
        self.current_city_name = ""
        
        # Load checkpoint if exists
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    checkpoint = json.load(f)
                    self.current_city_page = checkpoint.get('city_page', 1)
                    self.current_city_index = checkpoint.get('city_index', 0)
                    self.current_agent_page = checkpoint.get('agent_page', 1)
                    self.processed_agents = checkpoint.get('processed_agents', 0)
                    self.current_city_name = checkpoint.get('city_name', "")
                    log_info(f"Resuming from checkpoint: City page {self.current_city_page}, City index {self.current_city_index}, Agent page {self.current_agent_page}")
            except Exception as e:
                log_error(f"Error loading checkpoint: {str(e)}")
    
    def save_checkpoint(self):
        checkpoint = {
            'city_page': self.current_city_page,
            'city_index': self.current_city_index,
            'agent_page': self.current_agent_page,
            'processed_agents': self.processed_agents,
            'city_name': self.current_city_name,
            'timestamp': datetime.now().isoformat()
        }
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        log_info(f"Checkpoint saved: {checkpoint}")

# Initialize logging with timestamps
def log_info(message):
    print(f"[INFO] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def log_success(message):
    print(f"[SUCCESS] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def log_warning(message):
    print(f"[WARNING] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def log_error(message):
    print(f"[ERROR] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / 1024 / 1024  # Convert to MB

# File management functions
def get_output_filenames(base_name="agent_data"):
    """Generate timestamped filenames for output"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_file = f"{base_name}_{timestamp}.json"
    csv_file = f"{base_name}_{timestamp}.csv"
    return json_file, csv_file

def append_to_json_file(data, filename):
    """Append data to JSON file with proper formatting"""
    if not os.path.exists(filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump([], f)
    
    with open(filename, 'r+', encoding='utf-8') as f:
        try:
            file_data = json.load(f)
            file_data.extend(data)
            f.seek(0)
            json.dump(file_data, f, indent=2)
        except json.JSONDecodeError:
            # If file is empty or corrupted
            f.seek(0)
            json.dump(data, f, indent=2)

def append_to_csv_file(data, filename):
    """Append data to CSV file"""
    file_exists = os.path.exists(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        fieldnames = data[0].keys() if data else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerows(data)

async def scrape_agent_data(url: str, options: dict = None, json_file=None, csv_file=None) -> None:
    """
    Optimized function to scrape agent data handling millions of records.
    Uses batch processing and checkpoints for reliability.
    """
    options = options or {}
    js_timeout = options.get('js_timeout', 10000)
    navigation_timeout = options.get('navigation_timeout', 30000)
    retry_attempts = options.get('retry_attempts', 2)
    print(options)
    # Setup output files if not provided
    if not json_file or not csv_file:
        json_file, csv_file = get_output_filenames()
    
    # Initialize scraper state for resuming capability
    state = ScraperState()
    await scrape_agent_data(state)
    # Setup signal handlers for graceful termination
    def signal_handler(sig, frame):
        log_info("Received termination signal, finishing current batch and exiting...")
        state.is_running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Buffer for batch processing
    agent_buffer = []
    
    log_info(f"Starting optimized agent data scraping from: {url}")
    log_info(f"Output files: JSON={json_file}, CSV={csv_file}")
    log_info(f"Initial memory usage: {get_memory_usage():.2f} MB")
    print("here")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-dev-shm-usage',  # Reduces memory usage
                '--disable-gpu',            # Reduces memory usage
                '--no-sandbox'              # Improves performance in some environments
            ]
        )
        log_info("Launched headless Chromium browser with optimized memory settings")
        # print("user agent ua",ua.random)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=get_random_user_agent(),
            ignore_https_errors=True,
            java_script_enabled=True
        )
        
        # Disable unnecessary features to save memory
        await context.route('**/*.{png,jpg,jpeg,gif,svg,pdf,mp4,webp}', lambda route: route.abort())
        await context.route('**/*google*', lambda route: route.abort())
        await context.route('**/*analytics*', lambda route: route.abort())
        await context.route('**/*tracking*', lambda route: route.abort())
        await context.route('**/*advertisement*', lambda route: route.abort())
        
        # Navigate to main page with cities
        main_page = await context.new_page()
        main_page.set_default_timeout(navigation_timeout)
        
        # Semaphore to limit concurrent pages
        page_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        
        try:
            # Skip to the checkpoint city page if resuming
            if state.current_city_page > 1:
                log_info(f"Navigating to city page {state.current_city_page} (resumed from checkpoint)")
                await main_page.goto(url, timeout=navigation_timeout, wait_until="domcontentloaded")
                
                # Navigate to the correct city page
                for _ in range(1, state.current_city_page):
                    pagination = await main_page.query_selector(".pagination ul")
                    if pagination:
                        next_button = await pagination.query_selector("li:last-child:not(.disabled) a.next-icon")
                        if next_button:
                            await next_button.click()
                            await main_page.wait_for_timeout(3000)
                            try:
                                await main_page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                            except Exception as e:
                                log_warning(f"Network idle timeout: {str(e)}")
            else:
                log_info(f"Navigating to main page: {url}")
                await main_page.goto(url, timeout=navigation_timeout, wait_until="domcontentloaded")
            
            await main_page.wait_for_timeout(3000)
            
            # Process all city pages (with pagination)
            has_more_cities = True
            city_page = state.current_city_page
            
            while has_more_cities and state.is_running:
                log_info(f"Processing city page {city_page}")
                
                # Find all city links in the table with class .notranslate
                city_links = await main_page.query_selector_all("tbody.notranslate tr td:first-child a")
                city_count = len(city_links)
                log_info(f"Found {city_count} cities on page {city_page}")
                
                # Process each city, starting from checkpoint if resuming
                for city_idx in range(state.current_city_index, city_count):
                    if not state.is_running:
                        break
                        
                    city_link = city_links[city_idx]
                    city_name = await city_link.text_content()
                    state.current_city_name = city_name
                    state.current_city_index = city_idx
                    
                    log_info(f"Processing city {city_idx+1}/{city_count}: {city_name}")
                    
                    # Open city page in a new tab
                    city_page = await context.new_page()
                    city_href = await city_link.get_attribute("href")
                    city_url = url + city_href if not city_href.startswith("http") else city_href
                    
                    await city_page.goto(city_url, timeout=navigation_timeout, wait_until="domcontentloaded")
                    await city_page.wait_for_timeout(3000)
                    
                    # Process all agent pages for this city (with pagination)
                    has_more_agents = True
                    agent_page_num = state.current_agent_page if city_idx == state.current_city_index else 1
                    
                    # If we're resuming on this city, navigate to the correct agent page
                    if agent_page_num > 1:
                        for _ in range(1, agent_page_num):
                            pagination = await city_page.query_selector(".pagination ul")
                            if pagination:
                                next_button = await pagination.query_selector("li:last-child:not(.disabled) a.next-icon")
                                if next_button:
                                    await next_button.click()
                                    await city_page.wait_for_timeout(3000)
                    
                    while has_more_agents and state.is_running:
                        log_info(f"Processing agent page {agent_page_num} for city: {city_name}")
                        
                        # Find all agent blocks
                        agent_blocks = await city_page.query_selector_all(".agent-team-results .agent-block")
                        agent_count = len(agent_blocks)
                        log_info(f"Found {agent_count} agents on page {agent_page_num}")
                        
                        # Create tasks to process agents concurrently but with limits
                        agent_tasks = []
                        
                        for agent_idx, agent_block in enumerate(agent_blocks):
                            agent_tasks.append(
                                process_agent(
                                    context, url, agent_block, city_name, page_semaphore
                                )
                            )
                            
                        # Wait for all agent processing tasks to complete
                        agent_results = await asyncio.gather(*agent_tasks, return_exceptions=True)
                        
                        # Add successful results to buffer
                        for result in agent_results:
                            if isinstance(result, dict):  # Successful result
                                agent_buffer.append(result)
                                state.processed_agents += 1
                                state.total_agents += 1
                            elif isinstance(result, Exception):
                                log_error(f"Agent processing error: {str(result)}")
                        
                        # Save batches to disk when buffer reaches threshold
                        if len(agent_buffer) >= BATCH_SIZE:
                            log_info(f"Saving batch of {len(agent_buffer)} agents")
                            append_to_json_file(agent_buffer, json_file)
                            append_to_csv_file(agent_buffer, csv_file)
                            
                            # Update progress stats
                            elapsed = time.time() - state.start_time
                            rate = state.total_agents / elapsed if elapsed > 0 else 0
                            memory = get_memory_usage()
                            log_info(f"Progress: {state.total_agents} agents processed ({rate:.2f}/sec), Memory: {memory:.2f} MB")
                            
                            # Save checkpoint
                            state.save_checkpoint()
                            
                            # Clear buffer after saving
                            agent_buffer = []
                        
                        # Check for pagination
                        has_more_agents = False
                        pagination = await city_page.query_selector(".pagination ul")
                        
                        if pagination:
                            next_button = await pagination.query_selector("li:last-child:not(.disabled) a.next-icon")
                            
                            if next_button:
                                log_info("Found next page button, clicking...")
                                await next_button.click()
                                await city_page.wait_for_timeout(3000)
                                try:
                                    await city_page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                                except Exception as e:
                                    log_warning(f"Network idle timeout: {str(e)}")
                                
                                has_more_agents = True
                                agent_page_num += 1
                                state.current_agent_page = agent_page_num
                            else:
                                log_info("No more agent pages for this city")
                    
                    # Reset agent page counter for next city
                    state.current_agent_page = 1
                    
                    # Close city page
                    await city_page.close()
                
                # Reset city index for next page
                state.current_city_index = 0
                
                # Check for city pagination
                has_more_cities = False
                pagination = await main_page.query_selector(".pagination ul")
                
                if pagination and state.is_running:
                    next_button = await pagination.query_selector("li:last-child:not(.disabled) a.next-icon")
                    
                    if next_button:
                        log_info("Found next city page button, clicking...")
                        await next_button.click()
                        await main_page.wait_for_timeout(3000)
                        try:
                            await main_page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                        except Exception as e:
                            log_warning(f"Network idle timeout: {str(e)}")
                        
                        has_more_cities = True
                        city_page += 1
                        state.current_city_page = city_page
                    else:
                        log_info("No more city pages")
            
            # Save any remaining agents in buffer
            if agent_buffer:
                log_info(f"Saving final batch of {len(agent_buffer)} agents")
                append_to_json_file(agent_buffer, json_file)
                append_to_csv_file(agent_buffer, csv_file)
            
            # Final stats
            elapsed = time.time() - state.start_time
            log_success(f"Scraping completed. Extracted data for {state.total_agents} agents in {elapsed:.2f} seconds.")
            log_info(f"Final memory usage: {get_memory_usage():.2f} MB")
            
            # Remove checkpoint file if completed successfully
            if os.path.exists(CHECKPOINT_FILE) and state.is_running:
                os.remove(CHECKPOINT_FILE)
                log_info("Removed checkpoint file (scraping completed successfully)")
            
        except Exception as e:
            log_error(f"Error during scraping: {str(e)}")
            state.save_checkpoint()
        
        finally:
            await browser.close()
            log_info(f"Browser closed")

async def process_agent(context, base_url, agent_block, city_name, semaphore):
    """Process a single agent with semaphore to limit concurrency"""
    async with semaphore:
        try:
            # Extract basic agent data
            name_elem = await agent_block.query_selector(".agent-content-name")
            office_elem = await agent_block.query_selector(".office")
            phone_elem = await agent_block.query_selector(".phone-link")
            
            name = await name_elem.text_content() if name_elem else "N/A"
            office = await office_elem.text_content() if office_elem else "N/A"
            phone = await phone_elem.text_content() if phone_elem else "N/A"
            
            # Get the link to the agent's page
            agent_link = await agent_block.query_selector("a")
            agent_href = await agent_link.get_attribute("href")
            agent_url = base_url + agent_href if not agent_href.startswith("http") else agent_href
            
            # Navigate to agent page to get email
            email_page = await context.new_page()
            try:
                await email_page.goto(agent_url, timeout=30000, wait_until="domcontentloaded")
                await email_page.wait_for_timeout(1500)  # Reduced wait time
                
                # Try to get email
                email = "N/A"
                email_link = await email_page.query_selector(".email-link")
                if email_link:
                    email = await email_link.text_content()
                
                # Create agent data object
                agent_data = {
                    "name": name.strip(),
                    "office": office.strip(),
                    "phone": phone.strip(),
                    "email": email.strip(),
                    "city": city_name.strip(),
                    "timestamp": datetime.now().isoformat()
                }
                
                return agent_data
                
            finally:
                # Always close the page to free resources
                await email_page.close()
                
        except Exception as e:
            raise Exception(f"Error processing agent {name if 'name' in locals() else 'unknown'}: {str(e)}")



async def handle_pagination_with_backoff(page, buttons, loader, max_pages):
    """Handle pagination with detailed logging and strict button checks."""
    log_info(f"Pagination enabled: Searching for button '{buttons}' ")
    page_count = 0
    
    while True:
        for button in buttons.split(","):
            try:
                if not button.startswith(("//", ".", "#")):
                    button_xpath = f"//button[normalize-space()='{button}'] | //*[text()='{button}']"
                    button_selector = f"xpath={button_xpath}"
                else:
                    button_selector = button

                button_element = None
                for timeout_ms in [3000, 5000, 8000]:
                    try:
                        button_element = await page.wait_for_selector(
                            button_selector, timeout=timeout_ms, state="visible"
                        )
                        if button_element:
                            log_success(f"Found button '{button}' after {timeout_ms}ms")
                            break
                    except Exception:
                        if timeout_ms == 8000:
                            log_warning(f"Button '{button}' not found after {timeout_ms}ms")
                            return
                        continue
                
                if not button_element:
                    log_warning(f"Button '{button}' not found, stopping pagination")
                    return

                await button_element.scroll_into_view_if_needed()
                await page.wait_for_timeout(1000)
                
                click_success = False
                for attempt in range(3):
                    try:
                        await button_element.click()
                        click_success = True
                        break
                    except Exception as e:
                        log_warning(f"Click attempt {attempt + 1}/3 failed: {str(e)}")
                        await page.wait_for_timeout(1000)
                
                if not click_success:
                    log_error("Failed to click button after 3 attempts")
                    return
                    
                page_count += 1
                log_success(f"Clicked '{button}' ({page_count}/{max_pages})")
                await page.wait_for_timeout(2000)
                continue
                
                if loader:
                    try:
                        await page.wait_for_selector(loader, state="hidden", timeout=5000)
                        log_success("Loader disappeared")
                    except Exception:
                        log_warning("Loader still present, proceeding anyway")
                        
            except Exception as e:
                log_error(f"Pagination error: {str(e)}")
                return

import asyncio

async def handle_numbered_pagination_with_backoff(page, base_url: str="", max_pages: int=1, loader: str = None):
    """
    Handle numbered pagination with retries and backoff strategy.
    Extracts HTML content from each page while navigating through the pages.
    """
    page_number = 1
    all_html = []  # List to store HTML content of each page
    
    while True:
        try:
            # Call the function that handles pagination and HTML extraction
            log_info(f"Fetching page {page_number}/{max_pages}...")
            
            # Use the helper function to fetch HTML from the current page
            html_content = await handle_numbered_pagination(page, base_url, page_number, loader)
            all_html.extend(html_content)  # Add the current page's HTML to the list
            
            # Log success for the current page
            log_success(f"Successfully extracted HTML from page {page_number}/{max_pages}")
            
            # Increment the page number for the next page
            page_number += 1
            
            # Small delay to avoid overloading the server
            await page.wait_for_timeout(2000)  # 2 seconds delay between page fetches
            
        except Exception as e:
            log_error(f"Error on page {page_number}: {str(e)}")
            break  # Exit the loop if there’s an error (e.g., page failed to load)
    
    return all_html


async def handle_numbered_pagination(page, base_url: str, max_pages: int, loader: str = None) -> list:
    """
    Handle numbered pagination and extract HTML content for each page.
    """
    page_number = 1
    all_html = []  # List to store the HTML content from each page
    
    while page_number <= max_pages:
        try:
            # Construct the URL for the current page (assuming ?page=<page_number> format)
            paginated_url = f"{base_url}?page={page_number}"
            log_info(f"Navigating to {paginated_url}")
            
            # Navigate to the current page
            await page.goto(paginated_url, wait_until="domcontentloaded")
            
            # Wait for the page to load (handle loader if present)
            if loader:
                try:
                    await page.wait_for_selector(loader, state="hidden", timeout=5000)
                    log_success(f"Loader disappeared on page {page_number}")
                except Exception:
                    log_warning(f"Loader still present on page {page_number}, proceeding anyway")

            # Wait for the page to fully load (network idle state)
            await page.wait_for_load_state("networkidle", timeout=10000)
            
            # Extract HTML content of the page
            html_content = await page.content()
            all_html.append(html_content)  # Add the current page's HTML to the list

            log_success(f"Successfully extracted HTML from page {page_number}")

            # Increment page number for next iteration
            page_number += 1
            
            # Add a small delay to avoid overwhelming the server
            await page.wait_for_timeout(2000)  # 2 seconds delay between page fetches

        except Exception as e:
            log_error(f"Error on page {page_number}: {str(e)}")
            break  # Exit if there's an error (e.g., page failed to load)
    
    return all_html


async def wait_for_page_content(page, loader):
    """Wait for content to load after clicking a page number (handle page reloads or JS updates)."""
    if loader:
        try:
            # Wait for loader to disappear, indicating the content is fully loaded
            await page.wait_for_selector(loader, state="hidden", timeout=10000)
            log_success("Page content loaded successfully.")
        except Exception:
            log_warning("Loader still present or timeout occurred while waiting for content.")
            # Proceed anyway if content doesn't load within the timeout
    else:
        # If no loader is specified, wait for some visible content change (e.g., an element that should appear on page load)
        try:
            # For example, we could wait for a specific element that should be visible on the page
            await page.wait_for_selector('div.content', state="visible", timeout=10000)
            log_success("Page content loaded successfully.")
        except Exception:
            log_warning("Page content failed to load within the timeout.")
            # Proceed anyway if content doesn't load within the timeout

async def handle_lazy_loading_with_limits(page):
    """Handle lazy loading with detailed logging."""
    log_info("Handling lazy loading with progressive scrolling")
    max_scroll_attempts = 10
    scroll_step = 0.2
    
    try:
        viewport_height = await page.evaluate("window.innerHeight")
        total_height = await page.evaluate("document.body.scrollHeight")
        log_info(f"Initial page height: {total_height}px, viewport: {viewport_height}px")
        previous_height = total_height
        
        for attempt in range(max_scroll_attempts):
            current_position = await page.evaluate("window.pageYOffset")
            if current_position + viewport_height >= total_height - 100:
                log_info("Reached page bottom, stopping scroll")
                break
                
            next_position = current_position + (viewport_height * scroll_step)
            await page.evaluate(f"window.scrollTo({{top: {next_position}, behavior: 'smooth'}})")
            await page.wait_for_timeout(800)
            
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height > total_height:
                log_success(f"Loaded new content (height: {total_height}px → {new_height}px)")
                total_height = new_height
            else:
                log_info("No increase in page height, stopping lazy loading")
                break
            
            if attempt % 3 == 2:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
                
    except Exception as e:
        log_error(f"Lazy loading error: {str(e)}")

def extract_relevant_container(html_content: str) -> str:
    """Extract a strictly meaningful container with enhanced scoring and logging."""
    soup = BeautifulSoup(html_content, 'lxml')
    log_info("Analyzing HTML for relevant container")
    
    def score_container(element):
        score = 0
        children = element.find_all(recursive=False)
        text_length = len(element.get_text(strip=True))
        class_name = " ".join(element.get("class", [])).lower() if element.get("class") else ""
        
        # Strict exclusions
        if element.name in ['script', 'style', 'nav', 'header', 'footer']:
            
            return -1
        if any(x in class_name for x in ['navbar', 'footer', 'header', 'sidebar', 'ad', 'banner']):
            
            return -1
        
        # Minimum requirements
        if len(children) < 3:
            
            return -1
        if text_length < 100:
            
            return -1
        
        # Scoring
        if element.name in ['div', 'section', 'article', 'main']:
            score += 20
        score += len(children) * 10  # Higher weight for structured content
        score += min(text_length // 200, 50)  # Text contribution capped
        depth = len(list(element.parents))
        if depth < 3:
            score -= 30  # Penalize shallow containers
        if len(set(child.name for child in children)) > 2:  # Reward variety in child tags
            score += 15
            
        # log_info(f"Scored <{element.name} class='{class_name}'>: {score} (children={len(children)}, text={text_length}, depth={depth})")
        return score

    containers = soup.find_all(['div', 'section', 'article', 'main'], recursive=True)
    if not containers:
        log_warning("No candidate containers found")
        return ""

    best_container = max(containers, key=score_container, default=None)
    best_score = score_container(best_container)
    
    if best_container and best_score >= 50:  # Stricter threshold
        log_success(f"Selected container: <{best_container.name}> with score {best_score}")
        return str(best_container)
    else:
        log_warning(f"No container met criteria (best score={best_score})")
        return ""

def extract_data_by_css(html_content, selector):
    """Extract elements using a CSS selector with logging."""
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.select(selector)
    log_info(f"Extracted {len(elements)} elements with selector '{selector}'")
    return [el.get_text(strip=True) for el in elements]

def get_html_sync(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """Synchronous wrapper for async function."""
    print("Here")
    return get_html(url, button, options, loader)

# Example Usage
# if __name__ == "__main__":
#     url = "https://www.c21atwood.com/realestate/agents/group/agents/"
#     options = {
#         "max_pages": 2,
#         "handle_lazy_loading": True,
#         "handle_pagination": True,
#         "js_timeout": 30000,
#         "navigation_timeout": 120000,
#         "retry_attempts": 2
#     }
#     html = get_html_sync(url, button="Load More", options=options, loader=".loading-spinner")
#     print(f"Extracted content (first 500 chars): {html[:500]}...")

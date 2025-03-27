import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import os
from datetime import datetime
import threading
import time
import gc
import logging
import psutil
from mainthread import startThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email
from fakeagents import get_random_user_agent
# Configuration
BATCH_SIZE = 50
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.csv"
RETRY_LIMIT = 5
REQUEST_DELAY = 2
NAVIGATION_TIMEOUT = 90000
CONCURRENT_REQUESTS = 8
MAX_PAGES_PER_CITY = 50
MEMORY_THRESHOLD_MB = 2000

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Global variables
all_agents_global = []
last_save_time = 0
seen_agents = set()
file_lock = threading.Lock()
active_browser = None  # Track the active browser instance

# Setup logging
logging.basicConfig(
    filename=f"{OUTPUT_FOLDER}/compass_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def reset_progress():
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "total_estimated_agents", "estimated_time_remaining", "elapsed_time", "last_city"])
            writer.writerow([0, 50000, "Calculating", 0, ""])
    print("Progress file reset")
    logging.info("Progress file reset")

def clear_existing_files():
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"
    with file_lock:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print(f"Cleared existing file: {csv_file}")
            logging.info(f"Cleared existing file: {csv_file}")

async def update_progress(processed, total, start_time, last_city=""):
    if scraper_stop_event.is_set():
        print("Stopping progress update due to external signal.")
        logging.info("Stopping progress update due to external signal.")
        return
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    est_remaining = (
        f"{(elapsed_seconds / max(1, processed) * (total - processed)):.0f} seconds"
        if processed > 0 else "Calculating"
    )
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "total_estimated_agents", "estimated_time_remaining", "elapsed_time", "last_city"])
            writer.writerow([processed, total, est_remaining, elapsed_seconds, last_city])
    print(f"Progress updated: {processed}/{total} agents, last city: {last_city}")
    logging.info(f"Progress updated: {processed}/{total} agents, last city: {last_city}")

async def fetch_agent_details(page, agent_card, fields_to_extract, retry_count=0):
    if scraper_stop_event.is_set():
        print("Stopping agent details fetch due to external signal.")
        logging.info("Stopping agent details fetch due to external signal.")
        return None

    try:
        agent_data = {}
        
        if "name" in fields_to_extract:
            name_element = await agent_card.query_selector('.agentCard-name')
            name = (await name_element.inner_text()).strip() if name_element else "N/A"
            print(f"Extracted name: {name}")
            agent_data["name"] = name
        
        if "email" in fields_to_extract:
            email_element = await agent_card.query_selector('.agentCard-email')
            email = (await email_element.inner_text()).strip() if email_element else "Not available"
            print(f"Found email: {email}")
            agent_data["email"] = email
            if "pending" in email.lower():
                scraper_stop_event.set()
                print(f"Found pending email for {agent_data.get('name', 'Unknown')}. Stopping.")
                logging.info(f"Found pending email for {agent_data.get('name', 'Unknown')}. Stopping.")
                return None
        
        if "phone" in fields_to_extract or "mobile" in fields_to_extract:
            phone_element = await agent_card.query_selector('.agentCard-phone')
            phone = (await phone_element.inner_text()).strip() if phone_element else "N/A"
            if phone.startswith("M: "):
                phone = phone.replace("M: ", "")
            print(f"Extracted phone: {phone}")
            if "phone" in fields_to_extract:
                agent_data["phone"] = phone
            if "mobile" in fields_to_extract:
                agent_data["mobile"] = phone

        agent_key = tuple(agent_data.get(field, "N/A") for field in fields_to_extract)
        if all(v in ("N/A", "Not available") for v in agent_key):
            print(f"Skipping agent with no valid data: {agent_data}")
            return None
        if agent_key in seen_agents:
            print(f"Duplicate agent found: {agent_data.get('name', 'Unknown')}. Skipping.")
            return None
        seen_agents.add(agent_key)

        return agent_data
    except Exception as e:
        if retry_count < RETRY_LIMIT and not scraper_stop_event.is_set():
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for agent: {str(e)}")
            logging.warning(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for agent: {str(e)}")
            await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))
            return await fetch_agent_details(page, agent_card, fields_to_extract, retry_count + 1)
        print(f"Error fetching agent details after {RETRY_LIMIT} retries: {str(e)}")
        logging.error(f"Error fetching agent details after {RETRY_LIMIT} retries: {str(e)}")
        return None

async def scrape_city_agents(page, city_url, fields_to_extract, start_time):
    global all_agents_global, last_save_time
    print(f"Scraping agents from {city_url}")
    logging.info(f"Scraping agents from {city_url}")
    agents = []

    if scraper_stop_event.is_set():
        print("Stopping due to external signal.")
        return agents

    for attempt in range(RETRY_LIMIT):
        try:
            print(f"Navigating to {city_url} (Attempt {attempt + 1}/{RETRY_LIMIT})")
            await page.goto(city_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
            await asyncio.sleep(REQUEST_DELAY)
            break
        except Exception as e:
            print(f"Navigation attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt == RETRY_LIMIT - 1:
                print(f"Failed to load {city_url} after {RETRY_LIMIT} attempts.")
                logging.error(f"Failed to load {city_url} after {RETRY_LIMIT} attempts.")
                return agents

    page_count = 0
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    while page_count < MAX_PAGES_PER_CITY:
        if scraper_stop_event.is_set():
            print("Stopping during pagination due to external signal.")
            if agents:
                save_data(agents, fields_to_extract)
            break

        try:
            await page.wait_for_selector('.agentCard', state="visible", timeout=30000)
            last_height = await page.evaluate("document.body.scrollHeight")
            while True:
                if scraper_stop_event.is_set():
                    print("Stopping scroll loop due to external signal.")
                    if agents:
                        save_data(agents, fields_to_extract)
                    return agents
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            agent_cards = await page.query_selector_all('.agentCard')
            print(f"Found {len(agent_cards)} agent cards on page {page_count + 1}")
            logging.info(f"Found {len(agent_cards)} agent cards on page {page_count + 1}")

            tasks = []
            async with semaphore:
                for i, card in enumerate(agent_cards):
                    if scraper_stop_event.is_set():
                        break
                    tasks.append(fetch_agent_details(page, card, fields_to_extract))
                    if len(tasks) >= CONCURRENT_REQUESTS:
                        results = await asyncio.gather(*tasks)
                        for agent_data in results:
                            if agent_data:
                                agents.append(agent_data)
                                all_agents_global.append(agent_data)
                                print(f"Processed agent: {agent_data.get('name', 'Unknown')}")
                        tasks = []

            if tasks:
                results = await asyncio.gather(*tasks)
                for agent_data in results:
                    if agent_data:
                        agents.append(agent_data)
                        all_agents_global.append(agent_data)
                        print(f"Processed agent: {agent_data.get('name', 'Unknown')}")

            if len(agents) >= BATCH_SIZE or check_memory():
                save_data(agents, fields_to_extract)
                agents = []
                city_name = city_url.split('/')[-2] if '/' in city_url else ""
                await update_progress(len(all_agents_global), 50000, start_time, city_name)
                print(f"Saved data after page {page_count + 1}")

            next_button = await page.query_selector('button[aria-label="Next Page"]')
            if not next_button:
                print("No next button found. Ending pagination.")
                break

            is_disabled = await next_button.evaluate('btn => btn.hasAttribute("disabled") || btn.disabled')
            if is_disabled:
                print("Next button disabled. Ending pagination.")
                break

            for attempt in range(RETRY_LIMIT):
                try:
                    current_url = page.url
                    await next_button.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=15000)
                    await asyncio.sleep(REQUEST_DELAY)
                    if page.url == current_url:
                        print("Page did not change after click. Ending pagination.")
                        break
                    page_count += 1
                    print(f"Moved to page {page_count + 1}")
                    break
                except PlaywrightTimeoutError as e:
                    print(f"Click attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
                    if attempt == RETRY_LIMIT - 1:
                        return agents
                    await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

        except Exception as e:
            print(f"Error during pagination at {city_url}: {e}")
            logging.error(f"Error during pagination at {city_url}: {e}")
            if agents:
                save_data(agents, fields_to_extract)
            break

    if agents and not scraper_stop_event.is_set():
        save_data(agents, fields_to_extract)
    print(f"Completed scraping {city_url} with {len(all_agents_global)} total agents so far")
    return agents

async def scrape_all_cities(page, fields_to_extract, start_time):
    global all_agents_global, last_save_time
    last_save_time = time.time()

    # Load progress for resuming
    last_city = ""
    processed_so_far = 0
    total_estimate = 50000
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                row = next(reader, None)
                if row and len(row) >= 5:
                    processed_so_far = int(row[0])
                    total_estimate = int(row[1])
                    last_city = row[4]
                    print(f"Resuming from {processed_so_far} agents, last city: {last_city}")
                    logging.info(f"Resuming from {processed_so_far} agents, last city: {last_city}")
                else:
                    print("Progress file malformed. Starting fresh.")
                    reset_progress()
        except Exception as e:
            print(f"Error reading progress file: {e}. Starting fresh.")
            logging.error(f"Error reading progress file: {e}")
            reset_progress()
    else:
        reset_progress()

    all_agents_global = all_agents_global[:processed_so_far]

    for attempt in range(RETRY_LIMIT):
        try:
            print(f"Navigating to main page (Attempt {attempt + 1}/{RETRY_LIMIT})")
            await page.goto("https://www.compass.com/agents/", wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt == RETRY_LIMIT - 1:
                print("Failed to load main page after retries.")
                logging.error("Failed to load main page after retries.")
                return all_agents_global

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(REQUEST_DELAY)
    
    city_links = await page.query_selector_all('.geo-link')
    city_urls = []
    skip_to_last = last_city != ""
    for city_link in city_links:
        city_path = await city_link.get_attribute("href")
        if city_path:
            full_url = f"https://www.compass.com{city_path}"
            city_name = full_url.split('/')[-2]
            if skip_to_last and city_name != last_city:
                print(f"Skipping city {city_name} (before last processed: {last_city})")
                continue
            skip_to_last = False
            city_urls.append(full_url)
    print(f"Found {len(city_urls)} cities to scrape starting from {last_city or 'start'}")

    for i, city_url in enumerate(city_urls):
        if scraper_stop_event.is_set():
            print("Stopping city scraping loop due to external signal.")
            break
        print(f"Processing city {i + 1}/{len(city_urls)}: {city_url}")
        await scrape_city_agents(page, city_url, fields_to_extract, start_time)
        await asyncio.sleep(REQUEST_DELAY)
        gc.collect()
    
    return all_agents_global

def save_data(agents, fields_to_extract):
    global last_save_time
    if not agents or scraper_stop_event.is_set():
        return
        
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"
    with file_lock:
        file_exists = os.path.exists(csv_file)
        with open(csv_file, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields_to_extract)
            if not file_exists:
                writer.writeheader()
            writer.writerows(agents)
        print(f"Saved {len(agents)} agents to {csv_file}")
        logging.info(f"Saved {len(agents)} agents to {csv_file}")
    
    last_save_time = time.time()

def check_memory():
    mem = psutil.Process().memory_info().rss / 1024 / 1024
    if mem > MEMORY_THRESHOLD_MB:
        print(f"Memory usage high ({mem} MB), forcing save")
        logging.warning(f"Memory usage high ({mem} MB), forcing save")
        return True
    return False

async def _run_scraper(fields_to_extract):
    global all_agents_global, last_save_time, seen_agents, active_browser
    all_agents_global = []
    seen_agents = set()
    last_save_time = time.time()
    start_time = datetime.now()
    clear_existing_files()
    success = False
    error_message = None

    # Close any existing browser instance
    if active_browser:
        try:
            await active_browser.close()
            print("Closed existing Playwright browser instance.")
            logging.info("Closed existing Playwright browser instance.")
        except Exception as e:
            print(f"Error closing existing browser: {e}")
            logging.error(f"Error closing existing browser: {e}")
        active_browser = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'])
        active_browser = browser  # Track the new browser instance
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent=get_random_user_agent()
        )
        page = await context.new_page()

        try:
            if scraper_stop_event.is_set():
                print("Stopping before scraping starts due to external signal.")
            else:
                await scrape_all_cities(page, fields_to_extract, start_time)
                success = not scraper_stop_event.is_set()
                if success:
                    message = (
                        f"<p>Compass agent scraping completed successfully on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Total agents scraped: {len(all_agents_global)}</p>"
                        f"<p>Data saved to: {OUTPUT_FOLDER}/compass_agents.csv</p>"
                    )
                    send_email(message)
                    print("Success email sent")
                    logging.info("Success email sent")
                else:
                    error_message = "Scraping stopped early due to pending email or external signal"

        except Exception as e:
            error_message = f"Scraping failed with error: {str(e)}"
            print(error_message)
            logging.error(error_message)

        finally:
            await context.close()
            await browser.close()
            active_browser = None  # Clear the reference
            gc.collect()
            
            if not success and all_agents_global:
                save_data(all_agents_global, fields_to_extract)
            message = (
                f"<p>Compass agent scraping {'completed' if success else 'failed or was interrupted'} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                f"<p>Total agents scraped: {len(all_agents_global)}</p>"
                f"<p>Data saved to: {OUTPUT_FOLDER}/compass_agents.csv</p>"
            )
            if not success:
                message += f"<p>Reason: {error_message or 'Unknown error'}</p>"
            send_email(message)
            print("Completion email sent")
            logging.info("Completion email sent")
            
            print("Browser closed. Scraping completed")

def get_compass_agents(fields_to_extract=['email', 'name', 'phone']):
    global current_scraper_thread, scraper_stop_event

    if not fields_to_extract:
        return "No fields specified to extract. Please provide at least one field."

    # Instead of stopping the thread, we let it run and just close any open browser
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Previous scraper thread is still running. Closing any open browsers and starting new instance.")
        logging.info("Previous scraper thread is still running. Closing any open browsers and starting new instance.")
        # Browser closure is handled within _run_scraper
    else:
        scraper_stop_event.clear()  # Reset stop event if no thread is running

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(fields_to_extract))
        finally:
            loop.close()

    startThread(background_scraper)
    print("Compass agent scraper started in the background.")
    logging.info("Compass agent scraper started in the background.")
    return "Compass agent scraper started in the background. Check email for completion status."

if __name__ == "__main__":
    try:
        result = get_compass_agents(fields_to_extract=['mobile'])
        print(result)
    except Exception as e:
        print(f"Error starting scraper: {str(e)}")
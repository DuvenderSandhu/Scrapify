import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import os
from datetime import datetime
import json
import logging
import psutil
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email
from fakeagents import get_random_user_agent
import time
# Configuration
BATCH_SIZE = 10
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
LOCK_FILE = f"{OUTPUT_FOLDER}/scraper.lock"  # New lock file to detect running instances
CONCURRENT_REQUESTS = 2
RETRY_LIMIT = 3
REQUEST_DELAY = 5
PAGE_LOAD_TIMEOUT = 30000
RATE_LIMIT_DELAY = 2
MEMORY_THRESHOLD_MB = 2000

# Setup logging
logging.basicConfig(
    filename=f"{OUTPUT_FOLDER}/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Ensure output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 50000,
        "estimated_time_remaining": "Calculating",
        "elapsed_time": 0,
        "status": "running",
        "last_city": "",
        "last_inner_city": "",
        "last_page": 1
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)
    print("Progress file reset")

def clear_existing_files():
    """Remove existing CSV file."""
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Cleared existing file: {csv_file}")
        logging.info(f"Cleared existing file: {csv_file}")

def create_lock_file():
    """Create a lock file to indicate the script is running."""
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    print(f"Created lock file with PID: {os.getpid()}")
    logging.info(f"Created lock file with PID: {os.getpid()}")

def remove_lock_file():
    """Remove the lock file when the script stops."""
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
        print("Removed lock file")
        logging.info("Removed lock file")

def check_for_new_instance():
    """Check if a new instance has started by comparing PIDs in the lock file."""
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, "r") as f:
            lock_pid = f.read().strip()
        current_pid = str(os.getpid())
        if lock_pid != current_pid:
            print(f"New instance detected (PID {lock_pid}). Stopping current instance (PID {current_pid}).")
            logging.info(f"New instance detected (PID {lock_pid}). Stopping current instance (PID {current_pid}).")
            return True
    return False

async def fetch_agent_details(context, agent_url, agent_data, retry_count=0):
    """Fetch agent details with retries and rate limiting."""
    full_agent_url = f"https://www.coldwellbankerhomes.com{agent_url}" if agent_url.startswith("/") else agent_url
    print(f"Fetching agent details from {full_agent_url} (Retry {retry_count}/{RETRY_LIMIT})")
    try:
        agent_page = await context.new_page()
        agent_page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        await agent_page.goto(full_agent_url, wait_until="domcontentloaded")
        email_element = await agent_page.query_selector(".email-link")
        if email_element:
            agent_data["email"] = (await email_element.inner_text()).strip()
            print(f"Found email: {agent_data['email']}")
    except PlaywrightTimeoutError as e:
        print(f"Timeout fetching {full_agent_url}: {e}")
        logging.warning(f"Timeout fetching {full_agent_url}: {e}")
        if retry_count < RETRY_LIMIT:
            await asyncio.sleep(REQUEST_DELAY * (2 ** retry_count))
            return await fetch_agent_details(context, agent_url, agent_data, retry_count + 1)
    except Exception as e:
        print(f"Failed to fetch {full_agent_url}: {e}")
        logging.error(f"Failed to fetch {full_agent_url}: {e}")
    finally:
        await agent_page.close()
        await asyncio.sleep(REQUEST_DELAY)
    return agent_data

async def process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents):
    """Process a single page of agents with concurrency control."""
    async with semaphore:
        print(f"Processing page {page_num} for {inner_city_name} in {city_name}")
        logging.info(f"Processing page {page_num} for {inner_city_name}")
        try:
            await page.wait_for_selector(".agent-block", timeout=PAGE_LOAD_TIMEOUT)
            agent_blocks = await page.query_selector_all(".agent-block")
            print(f"Found {len(agent_blocks)} agents on page {page_num}")
            logging.info(f"Found {len(agent_blocks)} agents on page {page_num}")
            
            tasks = []
            for agent_index, agent_block in enumerate(agent_blocks):
                try:
                    agent_name_element = await agent_block.query_selector(".agent-content-name > a")
                    agent_name = (await agent_name_element.inner_text()).strip() if agent_name_element else "N/A"
                    mobile_element = await agent_block.query_selector(".phone-link")
                    mobile = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"
                    agent_url = await agent_name_element.get_attribute("href") if agent_name_element else None
                    
                    current_agent = {
                        "name": agent_name,
                        "email": "N/A",
                        "phone": mobile,
                        "mobile": mobile,
                        "city": city_name,
                        "inner_city": inner_city_name
                    }
                    all_agents.append(current_agent)
                    print(f"Agent {agent_index+1}: {current_agent['name']}")
                    logging.info(f"Agent {agent_index+1}: {current_agent['name']}")
                    
                    if agent_url:
                        tasks.append(fetch_agent_details(context, agent_url, current_agent))
                except Exception as e:
                    print(f"Error processing agent {agent_index+1}: {e}")
                    logging.error(f"Error processing agent: {e}")
            
            if tasks:
                for i in range(0, len(tasks), CONCURRENT_REQUESTS):
                    batch = tasks[i:i + CONCURRENT_REQUESTS]
                    print(f"Processing batch of {len(batch)} agent details concurrently")
                    await asyncio.gather(*batch)
                    await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"Error processing page {page_num}: {e}")
            logging.error(f"Error processing page {page_num}: {e}")
            raise

async def _run_scraper(url, fields_to_extract=None):
    """Main scraping function with checkpointing and memory management."""
    all_agents = []
    start_time = datetime.now()
    processed_agents = 0
    total_estimated_agents = 50000
    success = False
    error_message = None

    # Load progress for resuming
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            progress = json.load(f)
        last_city = progress.get("last_city", "")
        last_inner_city = progress.get("last_inner_city", "")
        last_page = progress.get("last_page", 1)
        processed_agents = progress.get("processed_agents", 0)
        print(f"Resuming from: {last_city}, {last_inner_city}, page {last_page}, {processed_agents} agents processed")
    else:
        clear_existing_files()
        reset_progress()
        last_city = last_inner_city = ""
        last_page = 1
        print("Starting fresh scrape")

    # Create lock file to indicate this instance is running
    create_lock_file()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, slow_mo=150)
            print("Browser launched")
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=get_random_user_agent(),
            )
            page = await context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT)

            print(f"Navigating to main URL: {url}")
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(REQUEST_DELAY)

            main_city_rows = await page.query_selector_all("tbody.notranslate > tr")
            print(f"Found {len(main_city_rows)} main cities")
            city_urls = []
            for row in main_city_rows:
                city_links = await row.query_selector_all("td > a")
                for city_link in city_links:
                    city_name = (await city_link.inner_text()).strip()
                    city_url = await city_link.get_attribute("href")
                    if city_url:
                        city_url = f"https://www.coldwellbankerhomes.com{city_url}" if city_url.startswith("/") else city_url
                        city_urls.append((city_name, city_url))
                        print(f"Added city: {city_name} ({city_url})")

            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
            skip_to_last = last_city != ""

            for city_index, (city_name, city_url) in enumerate(city_urls):
                if scraper_stop_event.is_set() or check_for_new_instance():
                    error_message = "Scraping stopped by user request or new instance detected"
                    print(error_message)
                    break
                if skip_to_last and city_name != last_city:
                    print(f"Skipping city {city_name} (before last processed: {last_city})")
                    continue
                skip_to_last = False

                print(f"\nProcessing city {city_index+1}/{len(city_urls)}: {city_name}")
                logging.info(f"Processing city {city_index+1}/{len(city_urls)}: {city_name}")
                await page.goto(city_url, wait_until="domcontentloaded")
                await asyncio.sleep(REQUEST_DELAY)

                inner_city_rows = await page.query_selector_all("tbody.notranslate > tr")
                inner_city_urls = []
                for inner_row in inner_city_rows:
                    inner_city_links = await inner_row.query_selector_all("td > a")
                    for inner_link in inner_city_links:
                        inner_city_name = (await inner_link.inner_text()).strip()
                        inner_city_url = await inner_link.get_attribute("href")
                        if inner_city_url:
                            inner_city_url = f"https://www.coldwellbankerhomes.com{inner_city_url}" if inner_city_url.startswith("/") else inner_city_url
                            inner_city_urls.append((inner_city_name, inner_city_url))
                            print(f"  - Found inner city: {inner_city_name}")

                inner_skip = last_inner_city != ""
                for inner_index, (inner_city_name, inner_city_url) in enumerate(inner_city_urls):
                    if scraper_stop_event.is_set() or check_for_new_instance():
                        error_message = "Scraping stopped by user request or new instance detected"
                        print(error_message)
                        break
                    if inner_skip and inner_city_name != last_inner_city:
                        print(f"  Skipping inner city {inner_city_name} (before last processed: {last_inner_city})")
                        continue
                    inner_skip = False

                    print(f"\n  Processing inner city {inner_index+1}/{len(inner_city_urls)}: {inner_city_name}")
                    logging.info(f"Processing inner city {inner_index+1}/{len(inner_city_urls)}: {inner_city_name}")
                    await page.goto(inner_city_url, wait_until="domcontentloaded")
                    await asyncio.sleep(REQUEST_DELAY)

                    page_num = last_page if inner_city_name == last_inner_city else 1
                    has_more_pages = True

                    while has_more_pages:
                        if scraper_stop_event.is_set() or check_for_new_instance():
                            error_message = "Scraping stopped by user request or new instance detected"
                            print(error_message)
                            break

                        await process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents)
                        processed_agents = len(all_agents) + progress.get("processed_agents", 0)
                        print(f"Processed {processed_agents} agents so far")

                        if len(all_agents) >= BATCH_SIZE or check_memory():
                            save_data(all_agents, fields_to_extract)
                            print(f"Saved batch of {len(all_agents)} agents")
                            all_agents.clear()

                        update_progress(start_time, processed_agents, total_estimated_agents, city_name, inner_city_name, page_num)

                        try:
                            next_page_button = await page.query_selector(".pagination ul > li:last-child > a")
                            if next_page_button and await next_page_button.get_attribute("href"):
                                next_page_url = f"https://www.coldwellbankerhomes.com{await next_page_button.get_attribute('href')}"
                                print(f"Moving to next page: {next_page_url}")
                                await page.goto(next_page_url, wait_until="domcontentloaded")
                                page_num += 1
                                await asyncio.sleep(REQUEST_DELAY)
                            else:
                                has_more_pages = False
                                print("No more pages for this inner city")
                        except Exception as e:
                            print(f"Error checking next page: {e}")
                            logging.error(f"Error checking next page: {e}")
                            has_more_pages = False

            success = not scraper_stop_event.is_set() and not error_message
    except Exception as e:
        error_message = f"Fatal error: {e}"
        print(error_message)
        logging.error(error_message)
    finally:
        if all_agents:
            save_data(all_agents, fields_to_extract)
            print(f"Final save: {len(all_agents)} agents")
        await page.close()
        await context.close()
        await browser.close()
        print("Browser closed")
        status = "completed" if success else "failed"
        update_progress(start_time, processed_agents, total_estimated_agents, "", "", 1, status)
        send_completion_email(success, processed_agents, error_message)
        remove_lock_file()  # Clean up lock file when done

def check_memory():
    """Check if memory usage exceeds threshold."""
    mem = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    if mem > MEMORY_THRESHOLD_MB:
        print(f"Memory usage high ({mem} MB), forcing save")
        logging.warning(f"Memory usage high ({mem} MB), forcing save")
        return True
    return False

def update_progress(start_time, processed_agents, total_estimated_agents, last_city, last_inner_city, last_page, status="running"):
    """Update progress file with current state."""
    elapsed_time = (datetime.now() - start_time).total_seconds()
    time_per_agent = elapsed_time / max(1, processed_agents)
    remaining_agents = max(0, total_estimated_agents - processed_agents)
    estimated_time_remaining = remaining_agents * time_per_agent
    
    progress_data = {
        "processed_agents": processed_agents,
        "total_estimated_agents": total_estimated_agents,
        "estimated_time_remaining": estimated_time_remaining,
        "elapsed_time": elapsed_time,
        "status": status,
        "last_city": last_city,
        "last_inner_city": last_inner_city,
        "last_page": last_page
    }
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f)
        print(f"Progress updated: {processed_agents} agents, status: {status}")
    except Exception as e:
        print(f"Error updating progress: {e}")
        logging.error(f"Error updating progress: {e}")

def save_data(agents, fields_to_extract=None):
    """Save agent data to CSV."""
    if not agents:
        return
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"
    filtered_agents = [{key: agent.get(key, "N/A") for key in (fields_to_extract or agents[0].keys())} for agent in agents]
    
    try:
        fieldnames = fields_to_extract or filtered_agents[0].keys()
        file_exists = os.path.exists(csv_file)
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(filtered_agents)
        print(f"Saved {len(filtered_agents)} agents to {csv_file}")
        logging.info(f"Saved {len(filtered_agents)} agents to CSV")
    except Exception as e:
        print(f"Error saving CSV: {e}")
        logging.error(f"Error saving CSV: {e}")

def send_completion_email(success, processed_agents, error_message):
    """Send email with completion status."""
    message = (
        f"<p>Coldwell Banker scraping {'completed' if success else 'failed'} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
        f"<p>Total agents scraped: {processed_agents}</p>"
        f"<p>Data saved to: {OUTPUT_FOLDER}/coldwell_agents.csv</p>"
    )
    if not success:
        message += f"<p>Reason: {error_message or 'Unknown'}</p>"
    send_email(message)
    print("Completion email sent")
    logging.info("Completion email sent")

def get_all_data(url="https://www.coldwellbankerhomes.com/sitemap/agents/", fields_to_extract=None):
    """Start scraper in background with thread management."""
    global current_scraper_thread, scraper_stop_event
    
    # Check if another instance is running
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, "r") as f:
            existing_pid = f.read().strip()
        print(f"Another instance (PID {existing_pid}) is running. Stopping it and starting new instance.")
        logging.info(f"Another instance (PID {existing_pid}) is running. Stopping it and starting new instance.")
        if current_scraper_thread and current_scraper_thread.is_alive():
            stopThread()
            current_scraper_thread.join()
        scraper_stop_event.set()  # Signal the existing instance to stop
        time.sleep(2)  # Give time for the previous instance to stop
        remove_lock_file()  # Clean up the old lock file

    scraper_stop_event.clear()  # Reset stop event for the new instance

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(url, fields_to_extract))
        finally:
            loop.close()

    startThread(background_scraper)
    print("Scraper started in background")
    logging.info("Scraper started in background")
    return {
        "status": "started",
        "message": "Scraper running in background. Check data folder and logs.",
        "progress_file": PROGRESS_FILE
    }

if __name__ == "__main__":
    result = get_all_data(fields_to_extract=["name", "email", "city", "inner_city"])
    print(result)
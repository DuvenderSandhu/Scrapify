import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import os
from datetime import datetime
import threading
import time
import gc
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email

# Configuration
BATCH_SIZE = 100
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.csv"
RETRY_LIMIT = 5
REQUEST_DELAY = 5
NAVIGATION_TIMEOUT = 90000
CONCURRENT_REQUESTS = 1
MAX_PAGES_PER_CITY = 50

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Global variables
all_agents_global = []
last_save_time = 0
seen_agents = set()
file_lock = threading.Lock()

def reset_progress():
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "total_estimated_agents", "estimated_time_remaining", "elapsed_time"])
            writer.writerow([0, 0, "Calculating", 0])

def clear_existing_files():
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"
    with file_lock:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print(f"Cleared existing file: {csv_file}")

async def update_progress(processed, total, start_time):
    if scraper_stop_event.is_set():
        print("Stopping progress update due to external signal.")
        return
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    est_remaining = (
        f"{(elapsed_seconds / processed * (total - processed)):.0f} seconds"
        if processed > 0 else "Calculating"
    )
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "total_estimated_agents", "estimated_time_remaining", "elapsed_time"])
            writer.writerow([processed, total, est_remaining, elapsed_seconds])

async def fetch_agent_details(page, agent_card, fields_to_extract, retry_count=0):
    """Extract agent details from an agent card with retries."""
    if scraper_stop_event.is_set():
        print("Stopping agent details fetch due to external signal.")
        return None

    try:
        agent_data = {}
        
        if "name" in fields_to_extract:
            name_element = await agent_card.query_selector('.agentCard-name')  # Update if needed
            name = (await name_element.inner_text()).strip() if name_element else "N/A"
            print(f"Extracted name: {name}")
            agent_data["name"] = name
        
        if "email" in fields_to_extract:
            email_element = await agent_card.query_selector('.agentCard-email')  # Update if needed
            email = (await email_element.inner_text()).strip() if email_element else "Not available"
            print(f"Extracted email: {email}")
            agent_data["email"] = email
            if "pending" in email.lower():
                scraper_stop_event.set()
                print(f"Found pending email for {agent_data.get('name', 'Unknown')}. Stopping.")
                return None
        
        if "phone" in fields_to_extract or "mobile" in fields_to_extract:
            phone_element = await agent_card.query_selector('.agentCard-phone')  # Update if needed
            phone = (await phone_element.inner_text()).strip() if phone_element else "N/A"
            if phone.startswith("M: "):
                phone = phone.replace("M: ", "")
            print(f"Extracted phone: {phone}")
            if "phone" in fields_to_extract:
                agent_data["phone"] = phone
            if "mobile" in fields_to_extract:
                agent_data["mobile"] = phone

        agent_key = tuple(agent_data.get(field, "N/A") for field in fields_to_extract)
        print(f"Agent key: {agent_key}")
        if all(v in ("N/A", "Not available") for v in agent_key):
            print(f"Skipping agent with no valid data: {agent_data}")
            return None
        if agent_key in seen_agents:
            print(f"Duplicate agent found: {agent_data.get('name', 'Unknown')}. Skipping without retry.")
            return None  # Skip immediately, no retries for duplicates
        seen_agents.add(agent_key)

        return agent_data
    except Exception as e:
        if retry_count < RETRY_LIMIT and not scraper_stop_event.is_set():
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for agent: {str(e)}")
            await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))
            return await fetch_agent_details(page, agent_card, fields_to_extract, retry_count + 1)
        print(f"Error fetching agent details after {RETRY_LIMIT} retries: {str(e)}")
        return None

async def scrape_city_agents(page, city_url, fields_to_extract, start_time):
    """Scrape all agents from a city URL with 100% data capture per page."""
    global all_agents_global, last_save_time
    print(f"Scraping agents from {city_url}")
    agents = []

    if scraper_stop_event.is_set():
        print("Stopping due to external signal.")
        return agents

    # Navigate to the city page with retries
    for attempt in range(RETRY_LIMIT):
        if scraper_stop_event.is_set():
            print("Stopping navigation due to external signal.")
            return agents
        try:
            print(f"Navigating to {city_url} (Attempt {attempt + 1}/{RETRY_LIMIT})")
            await page.goto(city_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
            await asyncio.sleep(REQUEST_DELAY)
            break
        except Exception as e:
            print(f"Navigation attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt == RETRY_LIMIT - 1:
                print(f"Failed to load {city_url} after {RETRY_LIMIT} attempts.")
                return agents

    page_count = 0

    while page_count < MAX_PAGES_PER_CITY:
        if scraper_stop_event.is_set():
            print("Stopping during pagination due to external signal.")
            if agents:
                save_data(agents, fields_to_extract)
            break

        try:
            # Wait for agent cards to be fully loaded
            await page.wait_for_selector('.agentCard', state="visible", timeout=30000)
            
            # Scroll incrementally and wait for all content to load
            last_height = await page.evaluate("document.body.scrollHeight")
            while True:
                if scraper_stop_event.is_set():
                    print("Stopping scroll loop due to external signal.")
                    if agents:
                        save_data(agents, fields_to_extract)
                    return agents
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)  # Wait for dynamic content
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break  # No more content loaded
                last_height = new_height

            # Get all agent cards and verify count
            agent_cards = await page.query_selector_all('.agentCard')
            print(f"Found {len(agent_cards)} agent cards on page {page_count + 1}")
            if not agent_cards:
                print("No agent cards found. Ending pagination.")
                break

            # Process each agent card
            for i, card in enumerate(agent_cards):
                if scraper_stop_event.is_set():
                    print("Stopping agent processing due to external signal.")
                    if agents:
                        save_data(agents, fields_to_extract)
                    break
                agent_data = await fetch_agent_details(page, card, fields_to_extract)
                if agent_data:
                    agents.append(agent_data)
                    all_agents_global.append(agent_data)
                    print(f"Successfully processed agent {i + 1}/{len(agent_cards)}")
                # No retry loop here; duplicates are skipped immediately

            # Save data after every page
            if agents and not scraper_stop_event.is_set():
                save_data(agents, fields_to_extract)
                agents = []  # Clear agents list after saving to avoid duplicate saves
                await update_progress(len(all_agents_global), len(all_agents_global) + 100, start_time)
                print(f"Saved data after page {page_count + 1}")

            # Check for next page button
            next_button = await page.query_selector('button[aria-label="Next Page"]')
            if not next_button:
                print("No next button found. Ending pagination.")
                break

            # Verify button state
            is_disabled = await next_button.evaluate('btn => btn.hasAttribute("disabled") || btn.disabled')
            if is_disabled:
                print("Next button disabled. Ending pagination.")
                break

            # Click next page with retries and verify navigation
            for attempt in range(RETRY_LIMIT):
                if scraper_stop_event.is_set():
                    print("Stopping next page navigation due to external signal.")
                    if agents:
                        save_data(agents, fields_to_extract)
                    break
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
                        print("Failed to navigate to next page after retries.")
                        return agents
                    await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

        except Exception as e:
            print(f"Error during pagination at {city_url}: {e}")
            if agents:
                save_data(agents, fields_to_extract)
            break

    # Save any remaining agents for this city (if pagination ends early)
    if agents and not scraper_stop_event.is_set():
        save_data(agents, fields_to_extract)
    print(f"Completed scraping {city_url} with {len(all_agents_global)} total agents so far")
    return agents

async def scrape_all_cities(page, fields_to_extract, start_time):
    """Scrape agents from all cities listed on the main page."""
    global all_agents_global, last_save_time
    last_save_time = time.time()
    
    if scraper_stop_event.is_set():
        print("Stopping before starting city scraping due to external signal.")
        return all_agents_global

    for attempt in range(RETRY_LIMIT):
        if scraper_stop_event.is_set():
            print("Stopping main page navigation due to external signal.")
            return all_agents_global
        try:
            print(f"Navigating to main page (Attempt {attempt + 1}/{RETRY_LIMIT})")
            await page.goto("https://www.compass.com/agents/", wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt == RETRY_LIMIT - 1:
                print("Failed to load main page after retries.")
                return all_agents_global
            await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

    if scraper_stop_event.is_set():
        print("Stopping after main page load due to external signal.")
        return all_agents_global

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(REQUEST_DELAY)
    
    city_links = await page.query_selector_all('.geo-link')
    city_urls = []
    for city_link in city_links:
        if scraper_stop_event.is_set():
            print("Stopping city link collection due to external signal.")
            break
        city_path = await city_link.get_attribute("href")
        if city_path:
            city_urls.append(f"https://www.compass.com{city_path}")
    print(f"Found {len(city_urls)} cities to scrape.")

    for i, city_url in enumerate(city_urls):
        if scraper_stop_event.is_set():
            print("Stopping city scraping loop due to external signal.")
            break
        print(f"Processing city {i + 1}/{len(city_urls)}: {city_url}")
        await scrape_city_agents(page, city_url, fields_to_extract, start_time)
        await asyncio.sleep(REQUEST_DELAY)
    
    return all_agents_global

def save_data(agents, fields_to_extract):
    """Save agent data to CSV."""
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
    
    last_save_time = time.time()

async def _run_scraper(fields_to_extract):
    """Main scraper runner."""
    global all_agents_global, last_save_time, seen_agents
    all_agents_global = []
    seen_agents = set()
    last_save_time = time.time()
    start_time = datetime.now()
    clear_existing_files()
    reset_progress()
    success = False
    error_message = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'])
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
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
                        f"<p>Compass agent scraping completed successfully for all locations on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Total agents scraped: {len(all_agents_global)}</p>"
                        f"<p>Data saved to: {OUTPUT_FOLDER}/compass_agents.csv</p>"
                    )
                    send_email(message)
                    print("Success email sent for all locations")
                else:
                    error_message = "Scraping stopped early due to pending email or external signal"

        except Exception as e:
            error_message = f"Scraping failed with error: {str(e)}"

        finally:
            await context.close()
            await browser.close()
            gc.collect()
            
            if not success:
                message = (
                    f"<p>Compass agent scraping failed or was interrupted on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                    f"<p>Reason: {error_message or 'Unknown error'}</p>"
                    f"<p>Agents scraped before failure: {len(all_agents_global)}</p>"
                    f"<p>Partial data saved to: {OUTPUT_FOLDER}/compass_agents.csv</p>"
                )
                send_email(message)
                print("Failure email sent")
            
            print("Browser closed. Scraping completed")

def get_compass_agents(fields_to_extract=['email', 'name', 'phone']):
    """Start the scraper in a background thread."""
    global current_scraper_thread, scraper_stop_event

    if not fields_to_extract:
        return "No fields specified to extract. Please provide at least one field."

    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()
        current_scraper_thread.join()
        scraper_stop_event.clear()

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(fields_to_extract))
        finally:
            loop.close()

    startThread(background_scraper)
    print("Compass agent scraper started in the background.")
    return "Compass agent scraper started in the background. Check email for completion status."

if __name__ == "__main__":
    try:
        result = get_compass_agents(fields_to_extract=['mobile'])
        print(result)
    except Exception as e:
        print(f"Error starting scraper: {str(e)}")
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
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
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
RETRY_LIMIT = 5  # Increased retries
REQUEST_DELAY = 5  # Increased delay to avoid server overload
NAVIGATION_TIMEOUT = 90000  # Increased timeout for slow server responses
CONCURRENT_REQUESTS = 1  # Process cities sequentially to reduce load
MAX_PAGES_PER_CITY = 50

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Global variables
all_agents_global = []
last_save_time = 0
seen_agents = set()

def reset_progress():
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 0,
        "estimated_time_remaining": "Calculating",
        "elapsed_time": 0
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def clear_existing_files():
    json_file = f"{OUTPUT_FOLDER}/compass_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"
    for file in [json_file, csv_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared existing file: {file}")

async def update_progress(processed, total, start_time):
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    est_remaining = (
        f"{(elapsed_seconds / processed * (total - processed)):.0f} seconds"
        if processed > 0 else "Calculating"
    )
    progress_data = {
        "processed_agents": processed,
        "total_estimated_agents": total,
        "estimated_time_remaining": est_remaining,
        "elapsed_time": elapsed_seconds
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

async def fetch_agent_details(page, agent_card, retry_count=0):
    if scraper_stop_event.is_set():
        return None

    try:
        name_element = await agent_card.query_selector('.agentCard-name')
        email_element = await agent_card.query_selector('.agentCard-email')
        phone_element = await agent_card.query_selector('.agentCard-phone')

        name = await name_element.inner_text() if name_element else "N/A"
        email = await email_element.inner_text() if email_element else "Not available"
        phone = await phone_element.inner_text() if phone_element else "N/A"

        if phone.startswith("M: "):
            phone = phone.replace("M: ", "")

        agent_data = {
            "name": name.strip(),
            "email": email.strip(),
            "phone": phone.strip()
        }

        if "pending" in agent_data["email"].lower():
            message = (
                f"<p>Alert: Agent {agent_data['name']} has a pending email status.</p>"
                f"<p>Email does not exist for this agent. Please do not request email for this agent.</p>"
            )
            send_email(message)
            scraper_stop_event.set()
            print(f"Found pending email for {agent_data['name']}. Stopping the process.")
            return None

        agent_key = (agent_data["name"], agent_data["phone"])
        if agent_key in seen_agents:
            print(f"Duplicate agent found: {agent_data['name']}. Skipping.")
            return None
        seen_agents.add(agent_key)

        return agent_data
    except Exception as e:
        if retry_count < RETRY_LIMIT and not scraper_stop_event.is_set():
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for agent: {e}")
            await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))
            return await fetch_agent_details(page, agent_card, retry_count + 1)
        print(f"Error fetching agent details after {RETRY_LIMIT} retries: {e}")
        return None

async def scrape_city_agents(page, city_url, fields_to_extract, start_time):
    """Scrape agents for a specific city sequentially."""
    global all_agents_global, last_save_time
    print(f"Scraping agents from {city_url}")
    agents = []

    if scraper_stop_event.is_set():
        print("Stopping scraper due to external signal.")
        return agents

    for attempt in range(RETRY_LIMIT):
        if scraper_stop_event.is_set():
            print("Stopping during navigation attempt.")
            return agents
        try:
            print(f"Navigating to {city_url} (Attempt {attempt + 1}/{RETRY_LIMIT})")
            await page.goto(city_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
            await asyncio.sleep(REQUEST_DELAY)  # Wait for server to stabilize
            break
        except Exception as e:
            print(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
            if attempt == RETRY_LIMIT - 1:
                print(f"Failed to load {city_url} after {RETRY_LIMIT} attempts.")
                return agents
            await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

    page_count = 0
    previous_agent_count = -1

    while page_count < MAX_PAGES_PER_CITY:
        if scraper_stop_event.is_set():
            print("Stopping during pagination.")
            break

        try:
            await page.wait_for_selector('.agentCard', state="visible", timeout=15000)
            await asyncio.sleep(REQUEST_DELAY)  # Extra delay for content to load
            for _ in range(3):
                if scraper_stop_event.is_set():
                    break
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)

            agent_cards = await page.query_selector_all('.agentCard')
            for card in agent_cards:
                if scraper_stop_event.is_set():
                    break
                agent_data = await fetch_agent_details(page, card)
                if agent_data:
                    filtered_agent_data = {key: agent_data[key] for key in fields_to_extract if key in agent_data}
                    agents.append(filtered_agent_data)
                    all_agents_global.append(filtered_agent_data)
                    
                    if len(all_agents_global) % BATCH_SIZE == 0:
                        save_data(all_agents_global, fields_to_extract)
                        await update_progress(len(all_agents_global), len(all_agents_global) + 100, start_time)
                        print(f"Saved batch of {len(all_agents_global)} agents")
                        gc.collect()

            if len(agents) == previous_agent_count:
                print("No new agents found. Ending pagination.")
                break
            previous_agent_count = len(agents)

            next_button = await page.query_selector('button[aria-label="Next Page"]')
            if not next_button:
                print("No next button found. Ending pagination.")
                break

            is_disabled = await next_button.evaluate('btn => btn.hasAttribute("disabled") || btn.disabled')
            if is_disabled:
                print("Next button disabled. Ending pagination.")
                break

            for attempt in range(RETRY_LIMIT):
                if scraper_stop_event.is_set():
                    print("Stopping during next page click.")
                    break
                try:
                    await next_button.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=15000)
                    await asyncio.sleep(REQUEST_DELAY)
                    page_count += 1
                    print(f"Moved to page {page_count}")
                    break
                except PlaywrightTimeoutError as e:
                    print(f"Click attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
                    if attempt == RETRY_LIMIT - 1:
                        return agents
                    await asyncio.sleep(REQUEST_DELAY * (attempt + 1))

        except Exception as e:
            print(f"Error during pagination at {city_url}: {e}")
            break

    print(f"Found {len(agents)} agents in {city_url}")
    return agents

async def scrape_all_cities(page, fields_to_extract, start_time):
    """Scrape agents from all cities sequentially."""
    global all_agents_global, last_save_time
    last_save_time = time.time()
    
    if scraper_stop_event.is_set():
        print("Stopping before starting city scraping.")
        return all_agents_global

    for attempt in range(RETRY_LIMIT):
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

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(REQUEST_DELAY)
    
    city_links = await page.query_selector_all('.geo-link')
    city_urls = []
    for city_link in city_links:
        if scraper_stop_event.is_set():
            break
        city_path = await city_link.get_attribute("href")
        if city_path:
            city_urls.append(f"https://www.compass.com{city_path}")
    print(f"Found {len(city_urls)} cities to scrape.")

    # Process cities sequentially to avoid server overload
    for city_url in city_urls:
        if scraper_stop_event.is_set():
            break
        await scrape_city_agents(page, city_url, fields_to_extract, start_time)
        await asyncio.sleep(REQUEST_DELAY)  # Delay between cities
    
    return all_agents_global

def save_data(agents, fields_to_extract):
    global last_save_time
    if not agents:
        return
        
    json_file = f"{OUTPUT_FOLDER}/compass_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"

    filtered_agents = [
        {key: agent[key] for key in fields_to_extract if key in agent}
        for agent in agents
    ]

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_agents, f, indent=4, ensure_ascii=False)
    print(f"Saved JSON data to {json_file}")

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_extract)
        writer.writeheader()
        writer.writerows(filtered_agents)
    print(f"Saved CSV data to {csv_file}")
    
    last_save_time = time.time()

async def _run_scraper(fields_to_extract):
    global all_agents_global, last_save_time, seen_agents
    all_agents_global = []
    seen_agents = set()
    last_save_time = time.time()
    start_time = datetime.now()
    clear_existing_files()
    reset_progress()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            all_agents_global = await scrape_all_cities(page, fields_to_extract, start_time)
            save_data(all_agents_global, fields_to_extract)
            completed_count = len(all_agents_global)
            await update_progress(completed_count, completed_count, start_time)
            if not scraper_stop_event.is_set():
                send_email()
                print("Standard completion email sent")
            else:
                print("Scraping stopped early due to pending email or external signal")
        except Exception as e:
            print(f"Main error: {e}")
            if all_agents_global:
                save_data(all_agents_global, fields_to_extract)
        finally:
            await browser.close()
            print("Browser closed. Scraping completed")

def get_compass_agents(fields_to_extract=['email', 'name', 'phone']):
    global current_scraper_thread, scraper_stop_event

    # if fields_to_extract and 'email' in fields_to_extract:
    #     message = "<p>Hello,</p><p>No Email was found on the provided website consistently. Please check the details and try without Email.</p>"
    #     send_email(message)
    #     return "Email notification sent. The scraping process was not started because emails may not exist consistently."

    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()
        current_scraper_thread.join()
        scraper_stop_event.clear()

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_scraper(fields_to_extract))

    startThread(background_scraper)
    print("Compass agent scraper started in the background.")
    return "Compass agent scraper started in the background. It will stop and notify you if pending emails are found."

if __name__ == "__main__":
    result = get_compass_agents(fields_to_extract=['name', 'phone'])
    print(result)
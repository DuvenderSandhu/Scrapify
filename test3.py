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
BATCH_SIZE = 100  # Save after every 100 agents
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
RETRY_LIMIT = 3  # Retry failed requests up to 3 times
REQUEST_DELAY = 2  # Delay between requests to avoid overloading the server
CONCURRENT_REQUESTS = 10  # Limit concurrent requests

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Global variables
all_agents_global = []
last_save_time = 0

def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 0,
        "estimated_time_remaining": "Calculating Estimate Time",
        "elapsed_time": 0
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def clear_existing_files():
    """Remove existing JSON and CSV files."""
    json_file = f"{OUTPUT_FOLDER}/compass_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"
    for file in [json_file, csv_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared existing file: {file}")

async def fetch_agent_details(page, agent_card):
    """Extract agent details from an agent card."""
    try:
        name_element = await agent_card.query_selector('.agentCard-name')
        email_element = await agent_card.query_selector('.agentCard-email')
        phone_element = await agent_card.query_selector('.agentCard-phone')

        name = await name_element.inner_text() if name_element else "N/A"
        email = await email_element.inner_text() if email_element else "N/A"
        phone = await phone_element.inner_text() if phone_element else "N/A"

        if phone.startswith("M: "):
            phone = phone.replace("M: ", "")

        return {
            "name": name.strip(),
            "email": email.strip(),
            "phone": phone.strip()
        }
    except Exception as e:
        print(f"Error fetching agent details: {e}")
        return None

async def scrape_city_agents(page, city_url, semaphore, fields_to_extract):
    """Scrape all agents for a specific city with scrolling and waiting."""
    global all_agents_global, last_save_time
    print(f"Scraping agents from {city_url}")
    agents = []

    async with semaphore:
        for attempt in range(RETRY_LIMIT):
            try:
                await page.goto(city_url, wait_until="domcontentloaded", timeout=60000)
                break
            except Exception as e:
                print(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed: {e}")
                if attempt == RETRY_LIMIT - 1:
                    return agents
                await asyncio.sleep(REQUEST_DELAY * (attempt + 1))  # Exponential backoff

        page_count = 0
        previous_agent_count = -1

        while True:
            try:
                # Wait for agent cards and scroll multiple times to load all content
                await page.wait_for_selector('.agentCard', state="visible", timeout=15000)
                for _ in range(3):  # Scroll multiple times
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)

                agent_cards = await page.query_selector_all('.agentCard')
                for card in agent_cards:
                    agent_data = await fetch_agent_details(page, card)
                    if agent_data:
                        # Filter fields based on fields_to_extract
                        filtered_agent_data = {key: agent_data[key] for key in fields_to_extract if key in agent_data}
                        agents.append(filtered_agent_data)
                        all_agents_global.append(filtered_agent_data)
                        
                        if len(all_agents_global) % BATCH_SIZE == 0:
                            save_data(all_agents_global, fields_to_extract)
                            print(f"Saved batch of {len(all_agents_global)} agents")
                            gc.collect()  # Force garbage collection

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
                    try:
                        await next_button.click()
                        await page.wait_for_load_state("domcontentloaded", timeout=15000)
                        await asyncio.sleep(3)  # Increased wait time after click
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")  # Scroll again after click
                        page_count += 1
                        print(f"Moved to page {page_count}")
                        break
                    except PlaywrightTimeoutError:
                        print(f"Click attempt {attempt + 1}/{RETRY_LIMIT} failed")
                        if attempt == RETRY_LIMIT - 1:
                            return agents
                        await asyncio.sleep(REQUEST_DELAY * (attempt + 1))  # Exponential backoff

                if page_count > 100:  # Prevent infinite loop
                    print("Pagination limit reached. Possible infinite loop detected.")
                    break

            except Exception as e:
                print(f"Error during pagination: {e}")
                break

    print(f"Found {len(agents)} agents in {city_url}")
    return agents

async def scrape_all_cities(page, fields_to_extract):
    """Scrape agents from all cities sequentially."""
    global all_agents_global, last_save_time
    last_save_time = time.time()
    
    await page.goto("https://www.compass.com/agents/", wait_until="domcontentloaded", timeout=60000)
    # Scroll to load all city links
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2)
    
    city_links = await page.query_selector_all('.geo-link')
    
    city_urls = []
    for city_link in city_links:
        city_path = await city_link.get_attribute("href")
        if city_path:
            city_urls.append(f"https://www.compass.com{city_path}")
    print(f"Found {len(city_urls)} cities to scrape.")
    
    # Process cities with concurrency control
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    tasks = [scrape_city_agents(page, city_url, semaphore, fields_to_extract) for city_url in city_urls]
    await asyncio.gather(*tasks)
    
    return all_agents_global

def save_data(agents, fields_to_extract):
    """Save the agent data to files, only keeping the specified fields."""
    global last_save_time
    if not agents:
        return
        
    json_file = f"{OUTPUT_FOLDER}/compass_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/compass_agents.csv"

    # Filter agents to only include specified fields
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
    
    current_time = time.time()
    progress_data = {
        "processed_agents": len(agents),
        "total_estimated_agents": max(len(agents), 1000),
        "estimated_time_remaining": "Calculating...",
        "elapsed_time": current_time - last_save_time
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)
    last_save_time = current_time

async def _run_scraper(fields_to_extract):
    """The actual scraping function."""
    global all_agents_global, last_save_time
    all_agents_global = []
    last_save_time = time.time()
    start_time = datetime.now()
    clear_existing_files()
    reset_progress()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            await scrape_all_cities(page, fields_to_extract)
            save_data(all_agents_global, fields_to_extract)
            progress_data = {
                "processed_agents": len(all_agents_global),
                "total_estimated_agents": len(all_agents_global),
                "estimated_time_remaining": 0,
                "elapsed_time": (datetime.now() - start_time).total_seconds()
            }
            with open(PROGRESS_FILE, "w") as f:
                json.dump(progress_data, f)
                send_email()
            print(f"Scraping completed. Found {len(all_agents_global)} agents.")
        except Exception as e:
            print(f"Main error: {e}")
            if all_agents_global:
                save_data(all_agents_global, fields_to_extract)
        finally:
            await browser.close()
            print("Browser closed. Scraping completed")

def get_compass_agents(fields_to_extract=['email', 'name', 'phone']):
    """Start the scraper in the background."""
    global current_scraper_thread, scraper_stop_event

    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        scraper_stop_event.set()
        current_scraper_thread.join()
        scraper_stop_event.clear()

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_scraper(fields_to_extract))

    current_scraper_thread = threading.Thread(target=background_scraper)
    current_scraper_thread.daemon = True
    current_scraper_thread.start()

    print("Compass agent scraper started in the background.")
    return "Compass agent scraper has started running in the background. Data will be saved to the 'data' folder."

if __name__ == "__main__":
    # Extract only 'email', 'name', and 'phone'
    result = get_compass_agents(fields_to_extract=['email', 'name', 'phone'])
    print(result)
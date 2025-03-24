import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os
from datetime import datetime
import threading
import gc
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email

# Configuration
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
TIMEOUT = 60000  # 60 seconds timeout
REQUEST_DELAY = 1  # Delay between actions

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

file_lock = threading.Lock()
unique_agents = set()  # To track unique entries

def reset_progress():
    progress_data = {"processed_agents": 0, "elapsed_time": 0}
    with file_lock:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f)

def clear_existing_files():
    for file in [f"{OUTPUT_FOLDER}/remax_agents.json", f"{OUTPUT_FOLDER}/remax_agents.csv"]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared {file}")

def load_existing_data():
    """Load existing data to initialize unique_agents set."""
    json_file = f"{OUTPUT_FOLDER}/remax_agents.json"
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            for agent in existing_data:
                unique_agents.add((agent["name"], agent["phone"]))
        except json.JSONDecodeError:
            print("No valid existing JSON data found. Starting fresh.")

async def process_agents(page, fields_to_extract):
    print("Processing agents across all pages...")
    all_agents_data = []
    page_number = 1
    
    while True:
        print(f"Processing page {page_number}...")
        
        # Wait for agent cards to load
        await page.wait_for_selector('.d-agent-card', timeout=TIMEOUT)
        agent_cards = await page.query_selector_all('.d-agent-card')
        print(f"Found {len(agent_cards)} agents on page {page_number}")
        
        for index, card in enumerate(agent_cards, 1):
            if scraper_stop_event.is_set():
                print("Stopping scraper as requested")
                return all_agents_data
                
            try:
                # Get name
                name_element = await card.query_selector('.d-agent-card-name')
                name = await name_element.inner_text() if name_element else "N/A"
                
                # Get phone number from button
                phone_button = await card.query_selector('a.d-agent-card-link-button[href^="tel:"]')
                phone = "N/A"
                if phone_button:
                    phone_text = await phone_button.inner_text()
                    phone = phone_text.strip() if phone_text else "N/A"
                
                # Check for duplicate using name and phone as unique key
                agent_key = (name, phone)
                if agent_key in unique_agents:
                    print(f"Skipped duplicate {index}/{len(agent_cards)} on page {page_number}: {name} | Phone: {phone}")
                    continue
                
                agent_data = {"name": name, "email": "N/A", "phone": phone}
                all_agents_data.append(agent_data)
                unique_agents.add(agent_key)
                save_data([agent_data], fields_to_extract)
                
                print(f"Processed {index}/{len(agent_cards)} on page {page_number}: {name} | Phone: {phone}")
                
            except Exception as e:
                print(f"Error processing agent {index} on page {page_number}: {e}")
        
        # Update progress
        elapsed_time = (datetime.now() - start_time).total_seconds()
        with file_lock:
            with open(PROGRESS_FILE, "w") as f:
                json.dump({"processed_agents": len(all_agents_data), "elapsed_time": elapsed_time}, f)
        
        # Check for next page button
        next_button = await page.query_selector('button.d-pagination-page-button[aria-label="Next Page"]:not([disabled])')
        if not next_button:
            print("No more pages to process.")
            break
            
        print("Clicking 'Next' button...")
        await next_button.click()
        await asyncio.sleep(REQUEST_DELAY)  # Wait for page to load
        page_number += 1

    return all_agents_data

def save_data(agents, fields_to_extract):
    if not agents:
        return
    json_file = f"{OUTPUT_FOLDER}/remax_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/remax_agents.csv"
    with file_lock:
        existing_data = []
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = []

        # Filter agents to only include specified fields
        filtered_agents = [
            {key: agent[key] for key in fields_to_extract if key in agent}
            for agent in agents
        ]

        # Only add new unique agents
        new_agents = [agent for agent in filtered_agents if (agent["name"], agent["phone"]) in unique_agents]
        if not new_agents:
            return
        
        existing_data.extend(new_agents)
        
        # Write to JSON
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)
        
        # Write to CSV
        fieldnames = fields_to_extract
        file_exists = os.path.exists(csv_file)
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_agents)
        
        print(f"Saved {len(new_agents)} unique agents. Total unique: {len(existing_data)}")

async def _run_scraper(url, fields_to_extract):
    global start_time
    start_time = datetime.now()

    clear_existing_files()
    reset_progress()
    load_existing_data()  # Load any existing data to avoid duplicates from previous runs

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()
        processed_agents = 0
        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
            
            agents = await process_agents(page, fields_to_extract)
            processed_agents = len(agents)
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            with file_lock:
                with open(PROGRESS_FILE, "w") as f:
                    json.dump({"processed_agents": processed_agents, "elapsed_time": elapsed_time}, f)
                    
        except Exception as e:
            print(f"Fatal error in scraper: {e}")
        finally:
            await browser.close()
            try:
                send_email()
            except Exception as e:
                print(f"Failed to send email: {e}")
            print(f"Scraping completed. Processed {processed_agents} unique agents.")

def get_remax_all_data(fields_to_extract=[ 'name', 'phone']):
    """
    Start the Remax scraper in the background and return immediately.
    This is the function that users will call directly.

    Args:
        fields_to_extract (list): List of fields to extract and save (e.g., ['email', 'name', 'phone']).
                                 If None, all fields will be saved.
    """
    if fields_to_extract and 'email' in fields_to_extract:
        message = "<p>Hello,</p><p>No Email was found on the provided website. Please check the details and try without Email.</p>"
        send_email(message)
        return "Email notification sent. The scraping process was not started because emails do not exist on the website."

    url = "https://www.remax.com/real-estate-agents"
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()

    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_scraper(url, fields_to_extract))

    startThread(background_scraper)
    return "Remax scraper has started running in the background. Data will be saved to the 'data' folder."

if __name__ == "__main__":
    try:
        # Example usage with custom fields
        result = get_remax_all_data(fields_to_extract=['email', 'name', 'mobile'])
        print(result)
    except ImportError:
        print("Playwright is not installed. Run 'pip install playwright' and 'playwright install' first.")
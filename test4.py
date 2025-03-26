import asyncio
from playwright.async_api import async_playwright
import csv
import os
from datetime import datetime
import threading
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email

# Configuration
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.csv"
TIMEOUT = 60000  # 60 seconds timeout
REQUEST_DELAY = 1  # Delay between actions

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

file_lock = threading.Lock()

def reset_progress():
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "elapsed_time"])
            writer.writerow([0, 0])

def clear_existing_files():
    csv_file = f"{OUTPUT_FOLDER}/remax_agents.csv"
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Cleared {csv_file}")

async def process_agents(page, fields_to_extract):
    print("Processing agents across all pages...")
    page_number = 1
    total_processed = 0  # Track total agents processed for progress
    
    while True:
        if scraper_stop_event.is_set():
            print("Stopping scraper due to external signal")
            break
        
        print(f"Processing page {page_number}...")
        
        await page.wait_for_selector('.d-agent-card', timeout=TIMEOUT)
        agent_cards = await page.query_selector_all('.d-agent-card')
        print(f"Found {len(agent_cards)} agents on page {page_number}")
        
        agents_to_save = []
        for index, card in enumerate(agent_cards, 1):
            if scraper_stop_event.is_set():
                print("Stopping scraper as requested during agent processing")
                if agents_to_save:  # Save any collected data before stopping
                    save_data(agents_to_save, fields_to_extract)
                return
            
            try:
                name_element = await card.query_selector('.d-agent-card-name')
                name = await name_element.inner_text() if name_element else "N/A"
                
                phone_button = await card.query_selector('a.d-agent-card-link-button[href^="tel:"]')
                phone = "N/A"
                if phone_button:
                    phone_text = await phone_button.inner_text()
                    phone = phone_text.strip() if phone_text else "N/A"
                
                # No uniqueness check; just collect and save
                agent_data = {"name": name, "phone": phone, "mobile": phone}
                agents_to_save.append(agent_data)
                print(f"Processed {index}/{len(agent_cards)} on page {page_number}: {name} | Phone: {phone}")
                
            except Exception as e:
                print(f"Error processing agent {index} on page {page_number}: {e}")
        
        # Save data after processing each page
        if agents_to_save:
            save_data(agents_to_save, fields_to_extract)
            total_processed += len(agents_to_save)
            agents_to_save = []  # Clear buffer after saving
        
        # Update progress
        elapsed_time = (datetime.now() - start_time).total_seconds()
        with file_lock:
            with open(PROGRESS_FILE, "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["processed_agents", "elapsed_time"])
                writer.writerow([total_processed, elapsed_time])
        
        next_button = await page.query_selector('button.d-pagination-page-button[aria-label="Next Page"]:not([disabled])')
        if not next_button:
            print("No more pages to process.")
            break
            
        print("Clicking 'Next' button...")
        await next_button.click()
        await asyncio.sleep(REQUEST_DELAY)
        page_number += 1

def save_data(agents, fields_to_extract):
    if not agents:
        return
        
    csv_file = f"{OUTPUT_FOLDER}/remax_agents.csv"
    with file_lock:
        file_exists = os.path.exists(csv_file)
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields_to_extract)
            if not file_exists:
                writer.writeheader()
            # Filter and write all agents, no uniqueness check
            filtered_agents = [
                {key: agent[key] for key in fields_to_extract if key in agent}
                for agent in agents
            ]
            writer.writerows(filtered_agents)
        
        print(f"Saved {len(agents)} agents to {csv_file}")

async def _run_scraper(url, fields_to_extract):
    global start_time
    start_time = datetime.now()

    clear_existing_files()
    reset_progress()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()
        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
            await process_agents(page, fields_to_extract)
        except Exception as e:
            print(f"Fatal error in scraper: {e}")
        finally:
            await browser.close()
            try:
                message = (
                    f"<p>Re/Max agent scraping completed on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                    f"<p>Data saved to: {OUTPUT_FOLDER}/remax_agents.csv</p>"
                )
                send_email(message)
                print("Success email sent")
            except Exception as e:
                message = "<p>Scraping failed due to a fatal error. Kindly rerun it.</p>"
                send_email(messageHTML=message)
                print(f"Failed to send email: {e}")
            print(f"Scraping completed. Processed agents saved to {OUTPUT_FOLDER}/remax_agents.csv")

def get_remax_all_data(fields_to_extract=['name', 'phone']):
    """
    Start the Re/Max scraper in the background, stopping any previous instance.
    """
    global current_scraper_thread, scraper_stop_event
    
    # Stop any existing scraper thread with certainty
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Another scraper is running. Stopping it now...")
        stopThread()  # Signal the thread to stop
        scraper_stop_event.set()  # Ensure asyncio loop sees the stop signal
        current_scraper_thread.join()  # Wait for the thread to fully terminate
        scraper_stop_event.clear()  # Reset the event for the new run
        print("Previous scraper fully stopped.")

    url = "https://www.remax.com/real-estate-agents"
    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(url, fields_to_extract))
        except Exception as e:
            print(f"Scraper encountered an error: {e}")
        finally:
            loop.close()

    startThread(background_scraper)
    return "Re/Max scraper has started running in the background. Data will be saved to 'data/remax_agents.csv'."

if __name__ == "__main__":
    try:
        result = get_remax_all_data(fields_to_extract=['name', 'phone'])
        print(result)
    except ImportError:
        print("Playwright is not installed. Run 'pip install playwright' and 'playwright install' first.")
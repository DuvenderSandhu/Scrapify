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
import datetime
from fakeagents import get_random_user_agent
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email

# Configuration
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.csv"
TIMEOUT = 60000  # 60 seconds timeout
REQUEST_DELAY = 2  # Increased delay for stability
MEMORY_THRESHOLD_MB = 2000  # 2 GB threshold
CONCURRENT_REQUESTS = 2  # Controlled concurrency

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

file_lock = threading.Lock()

# Setup logging
logging.basicConfig(
    filename=f"{OUTPUT_FOLDER}/remax_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def reset_progress():
    with file_lock:
        with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["processed_agents", "elapsed_time", "last_page"])
            writer.writerow([0, 0, 1])
    print("Progress file reset")
    logging.info("Progress file reset")

def clear_existing_files():
    csv_file = f"{OUTPUT_FOLDER}/remax_agents.csv"
    with file_lock:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print(f"Cleared {csv_file}")
            logging.info(f"Cleared {csv_file}")

async def process_agents(page, fields_to_extract, start_time):
    print("Processing agents across all pages...")
    logging.info("Processing agents across all pages...")
    
    # Load progress for resuming
    processed_agents = 0
    last_page = 1
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                row = next(reader, None)
                if row and len(row) >= 3:
                    processed_agents = int(row[0])
                    last_page = int(row[2])
                    print(f"Resuming from {processed_agents} agents, page {last_page}")
                    logging.info(f"Resuming from {processed_agents} agents, page {last_page}")
                else:
                    reset_progress()
        except Exception as e:
            print(f"Error reading progress file: {e}. Starting fresh.")
            logging.error(f"Error reading progress file: {e}")
            reset_progress()

    page_number = last_page
    total_processed = processed_agents
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    while True:
        if scraper_stop_event.is_set():
            print("Stopping scraper due to external signal before processing page")
            logging.info("Stopping scraper due to external signal before processing page")
            break
        
        print(f"Processing page {page_number}...")
        logging.info(f"Processing page {page_number}...")
        
        try:
            await page.wait_for_selector('.d-agent-card', timeout=TIMEOUT)
            agent_cards = await page.query_selector_all('.d-agent-card')
            print(f"Found {len(agent_cards)} agents on page {page_number}")
            logging.info(f"Found {len(agent_cards)} agents on page {page_number}")
        except PlaywrightTimeoutError:
            print(f"Timeout waiting for agent cards on page {page_number}. Ending.")
            logging.error(f"Timeout waiting for agent cards on page {page_number}")
            break
        except Exception as e:
            print(f"Error loading agent cards on page {page_number}: {e}")
            logging.error(f"Error loading agent cards on page {page_number}: {e}")
            break

        agents_to_save = []
        tasks = []
        
        async with semaphore:
            for index, card in enumerate(agent_cards, 1):
                if scraper_stop_event.is_set():
                    print(f"Stopping scraper as requested during agent processing on page {page_number}")
                    logging.info(f"Stopping scraper as requested during agent processing on page {page_number}")
                    if agents_to_save:
                        save_data(agents_to_save, fields_to_extract)
                    return
                
                async def fetch_agent_data(card):
                    if scraper_stop_event.is_set():
                        return None
                    agent_data = {}
                    name_element = await card.query_selector('.d-agent-card-name')
                    name = await name_element.inner_text() if name_element else "N/A"
                    
                    phone_button = await card.query_selector('a.d-agent-card-link-button[href^="tel:"]')
                    phone = "N/A"
                    if phone_button:
                        phone_text = await phone_button.inner_text()
                        phone = phone_text.strip() if phone_text else "N/A"
                    
                    agent_data = {"name": name, "phone": phone, "mobile": phone}
                    return agent_data

                # Create task explicitly
                task = asyncio.create_task(fetch_agent_data(card))
                tasks.append(task)
                
                if len(tasks) >= CONCURRENT_REQUESTS:
                    done, pending = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_EXCEPTION)
                    if scraper_stop_event.is_set():
                        print(f"Stop signal detected during task processing on page {page_number}")
                        logging.info(f"Stop signal detected during task processing on page {page_number}")
                        for task in pending:
                            task.cancel()
                        if agents_to_save:
                            save_data(agents_to_save, fields_to_extract)
                        return
                    
                    for task in done:
                        agent_data = task.result()
                        if agent_data:
                            agents_to_save.append(agent_data)
                            print(f"Processed {len(agents_to_save)}/{len(agent_cards)} on page {page_number}: {agent_data['name']} | Phone: {agent_data['phone']}")
                    tasks = list(pending)

            if tasks:
                done, pending = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_EXCEPTION)
                if scraper_stop_event.is_set():
                    print(f"Stop signal detected during final task processing on page {page_number}")
                    logging.info(f"Stop signal detected during final task processing on page {page_number}")
                    for task in pending:
                        task.cancel()
                    if agents_to_save:
                        save_data(agents_to_save, fields_to_extract)
                    return
                
                for task in done:
                    agent_data = task.result()
                    if agent_data:
                        agents_to_save.append(agent_data)
                        print(f"Processed {len(agents_to_save)}/{len(agent_cards)} on page {page_number}: {agent_data['name']} | Phone: {agent_data['phone']}")

        if agents_to_save:
            save_data(agents_to_save, fields_to_extract)
            total_processed += len(agents_to_save)
            agents_to_save = []
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            with file_lock:
                with open(PROGRESS_FILE, "w", newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["processed_agents", "elapsed_time", "last_page"])
                    writer.writerow([total_processed, elapsed_time, page_number])
            print(f"Progress updated: {total_processed} agents processed")
            logging.info(f"Progress updated: {total_processed} agents processed")

        if check_memory():
            print("Memory threshold reached, forcing save")
            logging.warning("Memory threshold reached, forcing save")
            gc.collect()

        try:
            if scraper_stop_event.is_set():
                print(f"Stopping scraper before next page check on page {page_number}")
                logging.info(f"Stopping scraper before next page check on page {page_number}")
                break

            next_button = await page.query_selector('button.d-pagination-page-button[aria-label="Next Page"]:not([disabled])')
            if not next_button:
                print("No more pages to process.")
                logging.info("No more pages to process.")
                break
            
            print("Clicking 'Next' button...")
            logging.info("Clicking 'Next' button...")
            await next_button.click()
            await asyncio.sleep(REQUEST_DELAY)
            page_number += 1
        except PlaywrightTimeoutError:
            print("Timeout clicking next button. Ending.")
            logging.error("Timeout clicking next button")
            break
        except Exception as e:
            print(f"Error navigating to next page: {e}")
            logging.error(f"Error navigating to next page: {e}")
            break

def save_data(agents, fields_to_extract):
    if not agents or scraper_stop_event.is_set():
        return
        
    csv_file = f"{OUTPUT_FOLDER}/remax_agents.csv"
    with file_lock:
        file_exists = os.path.exists(csv_file)
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields_to_extract)
            if not file_exists:
                writer.writeheader()
            filtered_agents = [
                {key: agent[key] for key in fields_to_extract if key in agent}
                for agent in agents
            ]
            writer.writerows(filtered_agents)
        
        print(f"Saved {len(agents)} agents to {csv_file}")
        logging.info(f"Saved {len(agents)} agents to {csv_file}")

def check_memory():
    mem = psutil.Process().memory_info().rss / 1024 / 1024
    if mem > MEMORY_THRESHOLD_MB:
        print(f"Memory usage high ({mem} MB), forcing save")
        logging.warning(f"Memory usage high ({mem} MB), forcing save")
        return True
    return False

async def _run_scraper(url, fields_to_extract):
    start_time = datetime.datetime.now()  # Use datetime for consistency
    success = False
    error_message = None

    clear_existing_files()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'])  # Open browser visibly
        context = await browser.new_context(viewport={'width': 1280, 'height': 800},user_agent=get_random_user_agent())
        page = await context.new_page()
        
        try:
            if scraper_stop_event.is_set():
                print("Stopping before navigation due to external signal")
                logging.info("Stopping before navigation due to external signal")
                return

            print(f"Navigating to {url}")
            logging.info(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT)
            await process_agents(page, fields_to_extract, start_time)
            success = not scraper_stop_event.is_set()
        except PlaywrightTimeoutError as e:
            error_message = f"Timeout error: {e}"
            print(error_message)
            logging.error(error_message)
        except Exception as e:
            error_message = f"Fatal error in scraper: {e}"
            print(error_message)
            logging.error(error_message)
        finally:
            await browser.close()
            gc.collect()
            
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            with open(PROGRESS_FILE, "r", encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                total_processed = int(next(reader, [0])[0])
            
            try:
                if success:
                    message = (
                        f"<p>Re/Max agent scraping completed on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Total agents processed: {total_processed}</p>"
                        f"<p>Data saved to: {OUTPUT_FOLDER}/remax_agents.csv</p>"
                    )
                    send_email(message)
                    print("Success email sent")
                    logging.info("Success email sent")
                else:
                    message = (
                        f"<p>Re/Max agent scraping failed or was interrupted on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Total agents processed before failure: {total_processed}</p>"
                        f"<p>Data saved to: {OUTPUT_FOLDER}/remax_agents.csv</p>"
                        f"<p>Reason: {error_message or 'Stopped by user'}</p>"
                    )
                    send_email(messageHTML=message)
                    print("Failure email sent")
                    logging.info("Failure email sent")
            except Exception as e:
                print(f"Failed to send email: {e}")
                logging.error(f"Failed to send email: {e}")
            
            print(f"Scraping completed. Processed {total_processed} agents saved to {OUTPUT_FOLDER}/remax_agents.csv")
            logging.info(f"Scraping completed. Processed {total_processed} agents")

def get_remax_all_data(fields_to_extract=['name', 'phone']):
    """
    Start the Re/Max scraper in the background, stopping any previous instance with 100% certainty.
    """
    global current_scraper_thread, scraper_stop_event
    
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Another scraper is running. Stopping it now...")
        logging.info("Another scraper is running. Stopping it now...")
        scraper_stop_event.set()  # Signal stop immediately
        stopThread()  # Request thread termination
        current_scraper_thread.join(timeout=10)  # Wait up to 10 seconds
        if current_scraper_thread.is_alive():
            print("Warning: Previous thread did not stop cleanly. Forcing termination.")
            logging.warning("Previous thread did not stop cleanly. Forcing termination.")
        scraper_stop_event.clear()  # Reset for new run
        time.sleep(2)  # Ensure full cleanup
        print("Previous scraper fully stopped.")
        logging.info("Previous scraper fully stopped.")

    url = "https://www.remax.com/real-estate-agents"
    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(url, fields_to_extract))
        except Exception as e:
            print(f"Scraper encountered an error: {e}")
            logging.error(f"Scraper encountered an error: {e}")
        finally:
            loop.close()

    startThread(background_scraper)
    print("Re/Max scraper has started running in the background.")
    logging.info("Re/Max scraper has started running in the background.")
    return "Re/Max scraper has started running in the background. Data will be saved to 'data/remax_agents.csv'."

if __name__ == "__main__":
    try:
        result = get_remax_all_data(fields_to_extract=['name', 'phone'])
        print(result)
    except ImportError:
        print("Playwright is not installed. Run 'pip install playwright' and 'playwright install' first.")
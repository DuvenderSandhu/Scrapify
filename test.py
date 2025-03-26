import asyncio
from playwright.async_api import async_playwright
import csv
import os
from datetime import datetime
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email
import json

# Configuration
BATCH_SIZE = 50
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 3
RETRY_LIMIT = 3
REQUEST_DELAY = 5
PAGE_LOAD_TIMEOUT = 30000
RATE_LIMIT_DELAY = 2

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 50000,
        "estimated_time_remaining": "Calculating Estimate Time",
        "elapsed_time": 0,
        "status": "running"
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def clear_existing_files():
    """Remove existing CSV file."""
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Cleared existing file: {csv_file}")

async def fetch_agent_details(context, agent_url, agent_data, retry_count=0):
    """Fetch agent details (e.g., email) with retries and rate limiting."""
    try:
        full_agent_url = f'https://www.coldwellbankerhomes.com{agent_url}' if agent_url.startswith('/') else agent_url
        agent_page = await context.new_page()
        agent_page.set_default_timeout(PAGE_LOAD_TIMEOUT)
        
        try:
            await agent_page.goto(full_agent_url, wait_until="domcontentloaded")
            email_element = await agent_page.query_selector('.email-link')
            if email_element:
                agent_data["email"] = (await email_element.inner_text()).strip()
        except Exception as e:
            print(f"Error loading agent page {full_agent_url}: {e}")
            if retry_count < RETRY_LIMIT:
                await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))
                return await fetch_agent_details(context, agent_url, agent_data, retry_count + 1)
        finally:
            await agent_page.close()
            await asyncio.sleep(REQUEST_DELAY)
    except Exception as e:
        print(f"Failed to fetch agent details for {agent_url}: {e}")
    finally:
        return agent_data

async def process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents):
    """Process a single page of agents with concurrency control."""
    async with semaphore:
        print(f"    Processing page {page_num} for {inner_city_name}")
        try:
            await page.wait_for_selector('.agent-block', timeout=PAGE_LOAD_TIMEOUT)
            agent_blocks = await page.query_selector_all('.agent-block')
            print(f"    Found {len(agent_blocks)} agents on page {page_num}")
            
            current_page_agents = []
            tasks = []
            
            for agent_index, agent_block in enumerate(agent_blocks):
                try:
                    agent_name_element = await agent_block.query_selector('.agent-content-name > a')
                    agent_name = (await agent_name_element.inner_text()).strip() if agent_name_element else "N/A"
                    mobile_element = await agent_block.query_selector('.phone-link')
                    mobile = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"
                    agent_url = await agent_name_element.get_attribute('href') if agent_name_element else None
                    
                    current_agent = {
                        "name": agent_name,
                        "email": "N/A",
                        "phone": mobile,
                        "mobile": mobile,
                        "city": city_name,
                        "inner_city": inner_city_name
                    }
                    current_page_agents.append(current_agent)
                    
                    if agent_url:
                        await asyncio.sleep(0.5)
                        tasks.append(fetch_agent_details(context, agent_url, current_agent))
                    print(f"      Agent {agent_index+1}: {current_agent['name']}")
                except Exception as e:
                    print(f"      Error processing agent: {e}")
            
            if tasks:
                for i in range(0, len(tasks), CONCURRENT_REQUESTS):
                    batch = tasks[i:i + CONCURRENT_REQUESTS]
                    await asyncio.gather(*batch)
                    await asyncio.sleep(RATE_LIMIT_DELAY)
            
            all_agents.extend(current_page_agents)
        except Exception as e:
            print(f"Error processing page {page_num}: {e}")
            raise

async def _run_scraper(url, fields_to_extract=None):
    """The actual scraping function that runs in the background."""
    all_agents = []
    start_time = datetime.now()
    processed_agents = 0
    total_estimated_agents = 50000
    success = False
    error_message = None

    clear_existing_files()
    reset_progress()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, slow_mo=150)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            )
            page = await context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT)

            try:
                print(f"Navigating to {url}")
                await page.goto(url, wait_until="domcontentloaded")
                await asyncio.sleep(REQUEST_DELAY)

                main_city_rows = await page.query_selector_all('tbody.notranslate > tr')
                print(f"Found {len(main_city_rows)} main cities")
                
                city_urls = []
                for row in main_city_rows:
                    city_links = await row.query_selector_all('td > a')
                    for city_link in city_links:
                        city_name = (await city_link.inner_text()).strip()
                        city_url = await city_link.get_attribute('href')
                        if city_url:
                            city_url = f'https://www.coldwellbankerhomes.com{city_url}' if city_url.startswith('/') else city_url
                            city_urls.append((city_name, city_url))
                            print(f"Added city: {city_name} ({city_url})")
                
                semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

                for city_index, (city_name, city_url) in enumerate(city_urls):
                    if scraper_stop_event.is_set():
                        error_message = "Scraping stopped by user request"
                        break

                    print(f"\nProcessing city {city_index+1}/{len(city_urls)}: {city_name}")
                    try:
                        await page.goto(city_url, wait_until="domcontentloaded")
                        await asyncio.sleep(REQUEST_DELAY)
                        
                        inner_city_rows = await page.query_selector_all('tbody.notranslate > tr')
                        inner_city_urls = []

                        for inner_row in inner_city_rows:
                            inner_city_links = await inner_row.query_selector_all('td > a')
                            for inner_link in inner_city_links:
                                inner_city_name = (await inner_link.inner_text()).strip()
                                inner_city_url = await inner_link.get_attribute('href')
                                if inner_city_url:
                                    inner_city_url = f'https://www.coldwellbankerhomes.com{inner_city_url}' if inner_city_url.startswith('/') else inner_city_url
                                    inner_city_urls.append((inner_city_name, inner_city_url))
                                    print(f"  - Found inner city: {inner_city_name}")

                        for inner_index, (inner_city_name, inner_city_url) in enumerate(inner_city_urls):
                            if scraper_stop_event.is_set():
                                error_message = "Scraping stopped by user request"
                                break

                            print(f"\n  Processing inner city {inner_index+1}/{len(inner_city_urls)}: {inner_city_name}")
                            try:
                                await page.goto(inner_city_url, wait_until="domcontentloaded")
                                await asyncio.sleep(REQUEST_DELAY)

                                page_num = 1
                                has_more_pages = True

                                while has_more_pages:
                                    if scraper_stop_event.is_set():
                                        error_message = "Scraping stopped by user request"
                                        break

                                    await process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents)
                                    processed_agents = len(all_agents)
                                    print(f"Processed {processed_agents} agents so far.")

                                    if len(all_agents) >= BATCH_SIZE:
                                        save_data(all_agents, fields_to_extract)
                                        print(f"Saved {len(all_agents)} agents.")
                                        all_agents.clear()

                                    update_progress(start_time, processed_agents, total_estimated_agents)

                                    try:
                                        next_page_button = await page.query_selector('.pagination ul > li:last-child > a')
                                        if next_page_button:
                                            next_page_url = await next_page_button.get_attribute('href')
                                            if next_page_url:
                                                await page.goto(f'https://www.coldwellbankerhomes.com{next_page_url}', wait_until="domcontentloaded")
                                                page_num += 1
                                                await asyncio.sleep(REQUEST_DELAY)
                                            else:
                                                has_more_pages = False
                                        else:
                                            has_more_pages = False
                                    except Exception as e:
                                        print(f"Error checking for next page: {e}")
                                        has_more_pages = False

                            except Exception as e:
                                print(f"  Error processing inner city {inner_city_name}: {e}")
                                continue
                    except Exception as e:
                        print(f"Error processing city {city_name}: {e}")
                        continue

                success = not scraper_stop_event.is_set() and not error_message
            except Exception as e:
                error_message = f"Main navigation error: {e}"
                update_progress(start_time, processed_agents, total_estimated_agents, "error")
            finally:
                if all_agents:
                    save_data(all_agents, fields_to_extract)
                    print(f"Final save: {len(all_agents)} agents saved.")
                
                await page.close()
                await context.close()
                await browser.close()
                print("Browser closed. Scraping completed")
                
                update_progress(start_time, processed_agents, total_estimated_agents, "completed" if success else "failed")
                
                if success:
                    message = (
                        f"<p>Coldwell Banker agent scraping completed successfully on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Total agents scraped: {processed_agents}</p>"
                        f"<p>Data saved to: {OUTPUT_FOLDER}/coldwell_agents.csv</p>"
                    )
                    send_email(message)
                    print("Success email sent")
                else:
                    message = (
                        f"<p>Coldwell Banker agent scraping failed or was interrupted on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
                        f"<p>Reason: {error_message or 'Unknown error'}</p>"
                        f"<p>Agents scraped before failure: {processed_agents}</p>"
                        f"<p>Partial data saved to: {OUTPUT_FOLDER}/coldwell_agents.csv</p>"
                    )
                    send_email(message)
                    print("Failure email sent")

    except Exception as e:
        error_message = f"Fatal error in scraper: {e}"
        update_progress(start_time, processed_agents, total_estimated_agents, "error")
        if all_agents:
            save_data(all_agents, fields_to_extract)
        message = (
            f"<p>Coldwell Banker agent scraping failed on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>"
            f"<p>Reason: {error_message}</p>"
            f"<p>Agents scraped before failure: {processed_agents}</p>"
            f"<p>Partial data saved to: {OUTPUT_FOLDER}/coldwell_agents.csv</p>"
        )
        send_email(message)
        print("Failure email sent")

def update_progress(start_time, processed_agents, total_estimated_agents, status="running"):
    """Update the progress file with current statistics."""
    elapsed_time = (datetime.now() - start_time).total_seconds()
    time_per_agent = elapsed_time / max(1, processed_agents)
    remaining_agents = max(0, total_estimated_agents - processed_agents)
    estimated_time_remaining = remaining_agents * time_per_agent
    
    progress_data = {
        "processed_agents": processed_agents,
        "total_estimated_agents": total_estimated_agents,
        "estimated_time_remaining": estimated_time_remaining,
        "elapsed_time": elapsed_time,
        "status": status
    }
    
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f)
    except Exception as e:
        print(f"Error updating progress file: {e}")

def save_data(agents, fields_to_extract=None):
    """Save the agent data to CSV file only, keeping specified fields."""
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"
    if not agents:
        return
    print(fields_to_extract)
    if fields_to_extract:
        filtered_agents = [
            {key: agent.get(key, "N/A") for key in fields_to_extract}
            for agent in agents
        ]
        print(filtered_agents)
    else:
        filtered_agents = agents

    try:
        fieldnames = fields_to_extract if fields_to_extract else filtered_agents[0].keys()
        file_exists = os.path.exists(csv_file)
        
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(filtered_agents)
        print(f"Appended {len(filtered_agents)} agents to CSV")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

def get_all_data(url="https://www.coldwellbankerhomes.com/sitemap/agents/", fields_to_extract=None):
    """Start the scraper in the background and stop any previous scraping task."""
    global current_scraper_thread, scraper_stop_event  # Ensure we're working with global references
    
    # Stop any existing scraper thread before starting a new one
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()  # Stop the running thread
        current_scraper_thread.join()  # Wait for it to fully stop
        scraper_stop_event.clear()  # Reset the stop event for the new run
        print("Previous scraper thread stopped.")

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
    print("Scraper started in the background.")
    return {
        "status": "started",
        "message": "Scraper has started running in the background. Data will be saved to the 'data' folder as CSV.",
        "progress_file": PROGRESS_FILE
    }

if __name__ == "__main__":
    result = get_all_data(fields_to_extract=['name', 'email', 'city', 'inner_city'])
    print(result)
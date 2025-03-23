import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os
from datetime import datetime
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event

# Configuration
BATCH_SIZE = 100  # Save data in batches of 100 agents
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 10  # Limit concurrency to avoid overloading the server
RETRY_LIMIT = 3  # Retry failed requests up to 3 times
REQUEST_DELAY = 2  # Delay between requests in seconds

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 50000,
        "estimated_time_remaining": "Calculating Estimate Time",
        "elapsed_time": 0
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def clear_existing_files():
    """Remove existing JSON and CSV files."""
    json_file = f"{OUTPUT_FOLDER}/coldwell_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"
    for file in [json_file, csv_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared existing file: {file}")

async def fetch_agent_details(context, agent_url, agent_data, retry_count=0):
    """Fetch agent details (e.g., email) with retries and rate limiting."""
    try:
        full_agent_url = f'https://www.coldwellbankerhomes.com{agent_url}' if agent_url.startswith('/') else agent_url
        agent_page = await context.new_page()
        await agent_page.goto(full_agent_url, wait_until="domcontentloaded")
        email_element = await agent_page.query_selector('.email-link')
        if email_element:
            agent_data["email"] = (await email_element.inner_text()).strip()
        await agent_page.close()
    except Exception as e:
        if retry_count < RETRY_LIMIT:
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for {agent_url}: {e}")
            await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))  # Exponential backoff
            await fetch_agent_details(context, agent_url, agent_data, retry_count + 1)
        else:
            print(f"Failed to fetch agent details for {agent_url}: {e}")
    finally:
        await asyncio.sleep(REQUEST_DELAY)  # Rate limiting

async def process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents):
    """Process a single page of agents with concurrency control."""
    async with semaphore:
        print(f"    Processing page {page_num} for {inner_city_name}")
        agent_blocks = await page.query_selector_all('.agent-block')
        print(f"    Found {len(agent_blocks)} agents on page {page_num}")
        
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
                    "email": "N/A",  # Default value, updated later if found
                    "mobile": mobile
                }
                all_agents.append(current_agent)
                
                if agent_url:
                    tasks.append(fetch_agent_details(context, agent_url, current_agent))
                
                print(f"      Agent {agent_index+1}: {current_agent['name']}")
            except Exception as e:
                print(f"      Error processing agent: {e}")
        
        if tasks:
            await asyncio.gather(*tasks)

async def _run_scraper(url, fields_to_extract=None):
    """The actual scraping function that runs in the background."""
    all_agents = []
    start_time = datetime.now()
    processed_agents = 0
    total_estimated_agents = 50000  # Approximate total agents (can be adjusted)

    # Clear existing files and reset progress
    clear_existing_files()
    reset_progress()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="networkidle")

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
                print(f"\nProcessing city {city_index+1}/{len(city_urls)}: {city_name}")
                try:
                    await page.goto(city_url, wait_until="networkidle")

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
                        print(f"\n  Processing inner city {inner_index+1}/{len(inner_city_urls)}: {inner_city_name}")

                        try:
                            await page.goto(inner_city_url, wait_until="networkidle")

                            page_num = 1
                            has_more_pages = True

                            while has_more_pages:
                                if scraper_stop_event.is_set():
                                    print("Scraping stopped by user request.")
                                    return

                                await process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents)
                                processed_agents = len(all_agents)
                                print(f"Processed {processed_agents} agents so far.")

                                # Save data after every batch of 100 agents
                                if processed_agents >= BATCH_SIZE:
                                    save_data(all_agents, fields_to_extract)
                                    print(f"Saved {processed_agents} agents so far.")
                                    all_agents.clear()  # Clear the list to free memory

                                # Update progress
                                elapsed_time = (datetime.now() - start_time).total_seconds()
                                time_per_agent = elapsed_time / max(1, processed_agents)
                                remaining_agents = total_estimated_agents - processed_agents
                                estimated_time_remaining = remaining_agents * time_per_agent

                                progress_data = {
                                    "processed_agents": processed_agents,
                                    "total_estimated_agents": total_estimated_agents,
                                    "estimated_time_remaining": estimated_time_remaining,
                                    "elapsed_time": elapsed_time
                                }
                                with open(PROGRESS_FILE, "w") as f:
                                    json.dump(progress_data, f)

                                # Check if there's a next page
                                next_page_button = await page.query_selector('.pagination ul > li:last-child > a')
                                if next_page_button:
                                    next_page_url = await next_page_button.get_attribute('href')
                                    if next_page_url:
                                        await page.goto(f'https://www.coldwellbankerhomes.com{next_page_url}', wait_until="networkidle")
                                        page_num += 1
                                    else:
                                        has_more_pages = False
                                else:
                                    has_more_pages = False

                        except Exception as e:
                            print(f"  Error processing inner city {inner_city_name}: {e}")

                except Exception as e:
                    print(f"Error processing city {city_name}: {e}")

        except Exception as e:
            print(f"Main error: {e}")

        finally:
            # Save remaining agents after all processing
            if all_agents:
                save_data(all_agents, fields_to_extract)
                print(f"Final save: {len(all_agents)} agents saved.")
            await browser.close()
            print("Browser closed. Scraping completed")

def save_data(agents, fields_to_extract=None):
    """Save the agent data to files, only keeping the specified fields."""
    json_file = f"{OUTPUT_FOLDER}/coldwell_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"

    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created directory: {OUTPUT_FOLDER}")

    # Filter agents to only include specified fields
    if fields_to_extract:
        filtered_agents = [
            {key: agent[key] for key in fields_to_extract if key in agent}
            for agent in agents
        ]
    else:
        filtered_agents = agents  # Save all fields if no specific fields are provided

    # Append to JSON file
    if os.path.exists(json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_data.extend(filtered_agents)
    else:
        existing_data = filtered_agents

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)
    print(f"Updated JSON data in {json_file}")

    # Append to CSV file
    fieldnames = fields_to_extract if fields_to_extract else filtered_agents[0].keys()
    file_exists = os.path.exists(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(filtered_agents)
    print(f"Updated CSV data in {csv_file}")

def get_all_data(url="https://www.coldwellbankerhomes.com/sitemap/agents/", fields_to_extract=None):
    """
    Start the scraper in the background and return immediately.
    This is the function that users will call directly.

    Args:
        url (str): The URL to scrape.
        fields_to_extract (list): List of fields to extract and save (e.g., ['email', 'mobile', 'name']).
                                 If None, all fields will be saved.
    """
    # Stop the previous scraper thread if it's running
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()
        print("Stopped previous scraper thread...")

    # Define a function to run in the background thread
    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_scraper(url, fields_to_extract))

    # Start the background thread
    startThread(background_scraper)

    print("Scraper started in the background.")
    return "Scraper has started running in the background. Data will be saved to the 'data' folder as it's collected."

# Example usage
if __name__ == "__main__":
    # Extract only 'email' and 'name'
    result = get_all_data(fields_to_extract=['email', 'name'])
    print(result)
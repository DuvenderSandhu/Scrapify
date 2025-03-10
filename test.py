import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os
import logging
from datetime import datetime
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log_info = logging.info
log_error = logging.error

# Constants
BATCH_SIZE = 100  # Save data in batches of 100 agents
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 10  # Adjust based on server capacity

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

async def fetch_agent_details(context, agent_url, agent_data):
    """Fetch agent details (e.g., email) concurrently."""
    try:
        full_agent_url = f'https://www.coldwellbankerhomes.com{agent_url}' if agent_url.startswith('/') else agent_url
        agent_page = await context.new_page()
        await agent_page.goto(full_agent_url, wait_until="domcontentloaded")
        email_element = await agent_page.query_selector('.email-link')
        if email_element:
            agent_data["email"] = (await email_element.inner_text()).strip()
        await agent_page.close()
    except Exception as e:
        log_error(f"Error fetching agent details for {agent_url}: {e}")

async def process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents):
    """Process a single page of agents with concurrency control."""
    async with semaphore:
        log_info(f"    Processing page {page_num} for {inner_city_name}")
        agent_blocks = await page.query_selector_all('.agent-block')
        log_info(f"    Found {len(agent_blocks)} agents on page {page_num}")
        
        tasks = []
        
        for agent_index, agent_block in enumerate(agent_blocks):
            try:
                agent_name_element = await agent_block.query_selector('.agent-content-name > a')
                agent_name = (await agent_name_element.inner_text()).strip() if agent_name_element else "N/A"
                
                office_element = await agent_block.query_selector('.office > a')
                office = (await office_element.inner_text()).strip() if office_element else "N/A"
                
                mobile_element = await agent_block.query_selector('.phone-link')
                mobile = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"
                
                agent_url = await agent_name_element.get_attribute('href') if agent_name_element else None
                
                current_agent = {
                    "name": agent_name,
                    "office": office,
                    "mobile": mobile,
                    "email": "N/A",
                    "main_city": city_name,
                    "inner_city": inner_city_name,
                    "url": agent_url,
                    "page_num": page_num
                }
                
                all_agents.append(current_agent)
                
                if agent_url:
                    tasks.append(fetch_agent_details(context, agent_url, current_agent))
                
                log_info(f"      Agent {agent_index+1}: {current_agent['name']}")
            except Exception as e:
                log_error(f"      Error processing agent: {e}")
        
        if tasks:
            await asyncio.gather(*tasks)

import os
import json
import csv
from datetime import datetime
import asyncio
from playwright.async_api import async_playwright

# Global variable to track if the scraping is running
running = False
OUTPUT_FOLDER = "data"  # Define your output folder path
PROGRESS_FILE = "./progress.json"  # Define your progress file path
BATCH_SIZE = 100  # Set batch size to 100
CONCURRENT_REQUESTS = 5  # Adjust as necessary

def log_info(message):
    print(f"[INFO] {message}")

def log_error(message):
    print(f"[ERROR] {message}")

async def get_all_data(url):

    all_agents = []
    start_time = datetime.now()
    processed_agents = 0
    total_estimated_agents = 50000  # Approximate total agents (can be adjusted)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            log_info(f"Navigating to {url}")
            await page.goto(url, wait_until="networkidle")

            main_city_rows = await page.query_selector_all('tbody.notranslate > tr')
            log_info(f"Found {len(main_city_rows)} main cities")

            city_urls = []
            for row in main_city_rows:
                city_links = await row.query_selector_all('td > a')
                for city_link in city_links:
                    city_name = (await city_link.inner_text()).strip()
                    city_url = await city_link.get_attribute('href')
                    if city_url:
                        city_url = f'https://www.coldwellbankerhomes.com{city_url}' if city_url.startswith('/') else city_url
                        city_urls.append((city_name, city_url))
                        log_info(f"Added city: {city_name} ({city_url})")

            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

            for city_index, (city_name, city_url) in enumerate(city_urls):
                log_info(f"\nProcessing city {city_index+1}/{len(city_urls)}: {city_name}")

                try:
                    await page.goto(city_url, wait_until="networkidle")
                    await asyncio.sleep(2)

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
                                log_info(f"  - Found inner city: {inner_city_name}")

                    for inner_index, (inner_city_name, inner_city_url) in enumerate(inner_city_urls):
                        log_info(f"\n  Processing inner city {inner_index+1}/{len(inner_city_urls)}: {inner_city_name}")

                        try:
                            await page.goto(inner_city_url, wait_until="networkidle")
                            await asyncio.sleep(2)

                            page_num = 1
                            has_more_pages = True

                            while has_more_pages:
                                await process_page(context, page, inner_city_name, city_name, page_num, semaphore, all_agents)
                                processed_agents = len(all_agents)

                                # Save data after every batch of 100 agents
                                if processed_agents % BATCH_SIZE == 0:
                                   
                                    save_data(all_agents)  # Pass append=True to append data
                                    log_info(f"Saved {processed_agents} agents so far.")

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
                                if st.session_state.is_scraping==False:
                                    return "<h1>Exited Successfully</h1>"

                                # Check if there's a next page
                                next_page_button = await page.query_selector('.pagination ul > li:last-child > a')
                                if next_page_button:
                                    next_page_url = await next_page_button.get_attribute('href')
                                    if next_page_url:
                                        await page.goto(f'https://www.coldwellbankerhomes.com{next_page_url}', wait_until="networkidle")
                                        page_num += 1
                                        await asyncio.sleep(1)
                                    else:
                                        has_more_pages = False
                                else:
                                    has_more_pages = False

                        except Exception as e:
                            log_error(f"  Error processing inner city {inner_city_name}: {e}")

                except Exception as e:
                    log_error(f"Error processing city {city_name}: {e}")

        except Exception as e:
            log_error(f"Main error: {e}")

        finally:
            # Save remaining agents after all processing
            if all_agents:
               
                save_data(all_agents)  # Overwrite when finished
                log_info(f"Final save: {len(all_agents)} agents saved.")
            await browser.close()
            log_info("Browser closed. Scraping completed")

def save_data(agents):
    """Save the agent data to files. If it's a new run, clear the files first, else append unique data."""
    json_file = f"{OUTPUT_FOLDER}/coldwell_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/coldwell_agents.csv"

    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        log_info(f"Created directory: {OUTPUT_FOLDER}")

    global running

    # If this is the first run, remove existing files to start fresh
    if not running:
        for file in [json_file, csv_file]:
            if os.path.exists(file):
                os.remove(file)
        running = True  # Mark as running

    # Load existing JSON data to avoid duplicates
    existing_agents = set()
    if os.path.exists(json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            try:
                existing_agents = {tuple(agent.items()) for agent in json.load(f)}
            except json.JSONDecodeError:
                existing_agents = set()  # Handle corrupt/incomplete file

    # Filter out duplicate agents
    new_agents = [agent for agent in agents if tuple(agent.items()) not in existing_agents]
    if not new_agents:
        log_info("No new agents to save (avoiding duplicates).")
        return

    # Append new data to the JSON file
    existing_agents.update(tuple(agent.items()) for agent in new_agents)
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump([dict(agent) for agent in existing_agents], f, indent=4, ensure_ascii=False)
    log_info(f"Updated JSON data in {json_file}")

    # Save CSV without duplicating rows
    fieldnames = agents[0].keys()
    write_header = not os.path.exists(csv_file)  # Write header if new file

    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_agents)
    log_info(f"Updated CSV data in {csv_file}")

    # Set running = False when done (this should be done at the end of scraping)


# async def main():
#     target_url = "https://www.coldwellbankerhomes.com/sitemap/agents/"
#     await get_all_data(target_url)

# if __name__ == "__main__":
#     asyncio.run(main())
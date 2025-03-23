import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os

# Configuration
BATCH_SIZE = 100
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/script1_progress.json"  # Unique per script
JSON_FILE = f"{OUTPUT_FOLDER}/script1_agents.json"       # Unique per script
CSV_FILE = f"{OUTPUT_FOLDER}/script1_agents.csv"         # Unique per script
CONCURRENT_REQUESTS = 10
RETRY_LIMIT = 3
REQUEST_DELAY = 2

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Keep your existing fetch_agent_details, process_page, save_data functions
# (Omitted here for brevity; assume they remain unchanged unless noted)

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
            await asyncio.sleep(REQUEST_DELAY * (retry_count + 1))
            await fetch_agent_details(context, agent_url, agent_data, retry_count + 1)
        else:
            print(f"Failed to fetch agent details for {agent_url}: {e}")
    finally:
        await asyncio.sleep(REQUEST_DELAY)

async def process_page(context, page, fields, semaphore, all_agents, max_agents):
    """Process a single page of agents with concurrency control."""
    async with semaphore:
        agent_blocks = await page.query_selector_all('.agent-block')
        tasks = []
        for agent_block in agent_blocks:
            if len(all_agents) >= max_agents:
                break
            agent_data = {}
            if 'name' in fields:
                agent_name_element = await agent_block.query_selector('.agent-content-name > a')
                agent_data['name'] = (await agent_name_element.inner_text()).strip() if agent_name_element else "N/A"
            else:
                agent_data['name'] = "N/A"
            
            if 'mobile' in fields or 'phone' in fields:
                mobile_element = await agent_block.query_selector('.phone-link')
                agent_data['mobile'] = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"
            else:
                agent_data['mobile'] = "N/A"
            
            if 'email' in fields and agent_name_element:
                agent_url = await agent_name_element.get_attribute('href')
                if agent_url:
                    tasks.append(fetch_agent_details(context, agent_url, agent_data))
            else:
                agent_data['email'] = "N/A"
            
            all_agents.append(agent_data)
        
        if tasks:
            await asyncio.gather(*tasks)

def save_data(agents):
    """Save the agent data to script-specific files."""
    json_file = JSON_FILE
    csv_file = CSV_FILE

    filtered_agents = [{key: agent[key] for key in ['name', 'email', 'mobile'] if key in agent} for agent in agents]

    if os.path.exists(json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_data.extend(filtered_agents)
    else:
        existing_data = filtered_agents

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

    fieldnames = ['name', 'email', 'mobile']
    file_exists = os.path.exists(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(filtered_agents)

async def _run_scraper(url, max_agents=500, fields=['name', 'email', 'mobile'], start_page=1, start_agent=None):
    """Modified scraper function to support limits and progress tracking."""
    all_agents = []
    processed_agents = 0
    last_page = start_page
    last_agent_name = start_agent

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="networkidle")

            # Fast-forward to start_page
            if start_page > 1:
                for _ in range(1, start_page):
                    next_page_button = await page.query_selector('.pagination ul > li:last-child > a')
                    if next_page_button:
                        next_url = await next_page_button.get_attribute('href')
                        await page.goto(f'https://www.coldwellbankerhomes.com{next_url}', wait_until="networkidle")
                    else:
                        break

            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
            has_more_pages = True
            page_num = start_page

            while has_more_pages and processed_agents < max_agents:
                await process_page(context, page, fields, semaphore, all_agents, max_agents)
                processed_agents = len(all_agents)
                
                if processed_agents >= BATCH_SIZE:
                    save_data(all_agents)
                    all_agents.clear()

                # Update last_agent_name
                if all_agents:
                    last_agent_name = all_agents[-1].get('name', 'N/A')

                # Pagination
                next_page_button = await page.query_selector('.pagination ul > li:last-child > a')
                if next_page_button and processed_agents < max_agents:
                    next_url = await next_page_button.get_attribute('href')
                    if next_url:
                        await page.goto(f'https://www.coldwellbankerhomes.com{next_url}', wait_until="networkidle")
                        page_num += 1
                        last_page = page_num
                    else:
                        has_more_pages = False
                else:
                    has_more_pages = False

        except Exception as e:
            print(f"Error in {__name__}: {e}")
        finally:
            if all_agents:
                save_data(all_agents)
            await browser.close()

    return all_agents, last_page, last_agent_name
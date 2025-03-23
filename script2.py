import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os

# Constants
BATCH_SIZE = 100  # Save data in batches of 100 agents
OUTPUT_FOLDER = "data"
JSON_FILE = f"{OUTPUT_FOLDER}/c21_agents.json"
CSV_FILE = f"{OUTPUT_FOLDER}/c21_agents.csv"
CONCURRENT_REQUESTS = 20  # Increase concurrency for faster scraping
RETRY_LIMIT = 3  # Retry failed requests up to 3 times

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

async def fetch_agent_details(context, agent_data, retry_count=0):
    """Fetch agent details (e.g., email) concurrently with retries."""
    try:
        email_element = await context.query_selector('a[href^="mailto:"]')
        if email_element:
            email = await email_element.get_attribute('href')
            agent_data["email"] = email.replace('mailto:', '')
    except Exception as e:
        if retry_count < RETRY_LIMIT:
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for email fetch: {e}")
            await asyncio.sleep(2 ** retry_count)  # Exponential backoff
            await fetch_agent_details(context, agent_data, retry_count + 1)
        else:
            print(f"Failed to fetch email for agent: {e}")

async def load_all_agents(page, max_agents, start_agent=None):
    """Load agents up to max_agents by clicking 'Load More'."""
    print("Starting to load agents...")
    click_count = 0
    max_attempts = 30  # Maximum number of attempts to click "Load More"
    all_agents = []
    last_agent_name = start_agent

    await page.wait_for_selector('.agent-info', state="visible", timeout=60000)
    agent_items = await page.query_selector_all('.agent-info')
    initial_count = len(agent_items)

    while len(all_agents) < max_agents and click_count < max_attempts:
        load_more_button = await page.query_selector('#show-more-agents')
        if not load_more_button or not await load_more_button.is_visible():
            print("No 'Load More' button found or not visible.")
            break

        print(f"Clicking 'Load More' button (Attempt {click_count + 1})...")
        await load_more_button.click()

        try:
            await page.wait_for_selector('#progress', state="visible", timeout=5000)
            await page.wait_for_selector('#progress', state="hidden", timeout=15000)
        except Exception:
            await page.wait_for_timeout(5000)

        agent_items = await page.query_selector_all('.agent-info')
        current_count = len(agent_items)
        if current_count == initial_count:
            print("No new agents loaded. Stopping.")
            break

        initial_count = current_count
        click_count += 1

    # Collect agent data up to max_agents
    for i, agent_item in enumerate(agent_items):
        if len(all_agents) >= max_agents:
            break
        if start_agent and last_agent_name == start_agent and i == 0:
            continue  # Skip until we reach the start_agent
        agent_data = await collect_agent_data(agent_item)
        all_agents.append(agent_data)
        last_agent_name = agent_data.get('name', 'N/A')

    print(f"Loaded {len(all_agents)} agents.")
    return all_agents, last_agent_name

async def collect_agent_data(agent_item):
    """Collect data for a single agent."""
    agent_data = {}
    name_element = await agent_item.query_selector('[itemprop="name"]')
    agent_data['name'] = (await name_element.inner_text()).strip() if name_element else "N/A"

    mobile_element = await agent_item.query_selector('.agent-list-cell-phone a')
    agent_data['mobile'] = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"

    agent_data['email'] = "N/A"  # Email fetched later
    return agent_data

async def fetch_email_for_agents(context, agents, fields):
    """Fetch email addresses for agents if 'email' is in fields."""
    if 'email' not in fields:
        return
    print(f"Fetching email addresses for {len(agents)} agents...")
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def fetch_with_semaphore(agent):
        async with semaphore:
            await fetch_agent_details(context, agent)
            print(f"Fetched details for: {agent['name']}")

    tasks = [fetch_with_semaphore(agent) for agent in agents]
    await asyncio.gather(*tasks)
    print("Email fetching completed")

def save_data(agents):
    """Save the agent data to script-specific files."""
    json_file = JSON_FILE
    csv_file = CSV_FILE

    filtered_agents = [{key: agent[key] for key in ['name', 'mobile', 'email'] if key in agent} for agent in agents]

    if os.path.exists(json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_data.extend(filtered_agents)
    else:
        existing_data = filtered_agents

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)
    print(f"Saved JSON data to {json_file}")

    fieldnames = ['name', 'mobile', 'email']
    file_exists = os.path.exists(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(filtered_agents)
    print(f"Saved CSV data to {csv_file}")

async def _run_scraper(url, max_agents=500, fields=['name', 'email', 'mobile'], start_page=1, start_agent=None):
    """Modified scraper function to support limits and progress tracking."""
    all_agents = []
    last_agent_name = start_agent

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Load agents up to max_agents
            agents, last_agent_name = await load_all_agents(page, max_agents, start_agent)
            all_agents.extend(agents)

            # Fetch emails if requested
            await fetch_email_for_agents(context, all_agents, fields)

            # Save data
            if all_agents:
                save_data(all_agents)

            # For script2, we don't have traditional pages, so we return 1 as a placeholder
            last_page = 1

        except Exception as e:
            print(f"Error in {__name__}: {e}")
        finally:
            await browser.close()
            print("Browser closed.")

    return all_agents, last_page, last_agent_name

# No CLI or threading here; switchScript.py will call _run_scraper directly
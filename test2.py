import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os
from datetime import datetime
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email  # Ensure this function is defined elsewhere

# Constants
BATCH_SIZE = 100  # Save data in batches of 100 agents
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 20  # Increase concurrency for faster scraping
RETRY_LIMIT = 3  # Retry failed requests up to 3 times

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)


def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 200,  # Adjust based on website estimate
        "estimated_time_remaining": "Calculating Estimate Time",
        "elapsed_time": 0
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)


def clear_existing_files():
    """Remove existing JSON and CSV files."""
    json_file = f"{OUTPUT_FOLDER}/c21_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/c21_agents.csv"
    for file in [json_file, csv_file]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Cleared existing file: {file}")


async def fetch_agent_details(context, agent_data, retry_count=0):
    """Fetch agent details (e.g., email) concurrently with retries."""
    try:
        # Assuming the email can be fetched directly from the agent's page
        # Modify this logic based on how emails are fetched
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


async def load_all_agents(page):
    """Keep clicking 'Load More' until all agents are loaded or no new agents are added."""
    print("Starting to load all agents...")
    click_count = 0
    max_attempts = 30  # Maximum number of attempts to click "Load More"

    # Wait for the page to fully load
    await page.wait_for_selector('.agent-info', state="visible", timeout=60000)

    # Get initial count of agents
    initial_agents = await page.query_selector_all('.agent-info')
    print(f"Initial agent count: {len(initial_agents)}")

    while click_count < max_attempts:
        # Check if the "Load More" button exists and is visible
        load_more_button = await page.query_selector('#show-more-agents')  # Update selector if needed
        if not load_more_button or not await load_more_button.is_visible():
            print("No 'Load More' button found or not visible. All agents loaded.")
            break

        # Click the "Load More" button
        print(f"Clicking 'Load More' button (Attempt {click_count + 1})...")
        await load_more_button.click()

        # Wait for new content to load
        try:
            # Wait for the loading spinner to appear and then disappear (if applicable)
            await page.wait_for_selector('#progress', state="visible", timeout=5000)
            await page.wait_for_selector('#progress', state="hidden", timeout=15000)
        except Exception as e:
            print(f"No loading spinner found or timeout waiting for spinner: {e}")

        # Wait additional time for content to load and stabilize
        await page.wait_for_timeout(5000)  # Adjust this delay as needed

        # Get current count of agents
        current_agents = await page.query_selector_all('.agent-info')
        print(f"Current agent count after click {click_count + 1}: {len(current_agents)}")

        # If the agent count hasn't increased, stop clicking
        if len(current_agents) == len(initial_agents):
            print("No new agents loaded. Stopping.")
            break

        initial_agents = current_agents
        click_count += 1

    # Collect and return all agent data
    all_agents = await collect_all_agent_data(page)
    print(f"All agents loaded. Total agent count: {len(all_agents)}")
    return all_agents


async def collect_all_agent_data(page):
    """Collect data for all the loaded agents."""
    print("Collecting data for all loaded agents...")
    all_agents = []

    # Wait for agent list to be fully loaded
    await page.wait_for_selector('.agent-info', state="visible")

    # Get all agent items
    agent_items = await page.query_selector_all('.agent-info')
    print(f"Found {len(agent_items)} total agents to process")

    for agent_index, agent_item in enumerate(agent_items):
        try:
            # Extract agent name
            name_element = await agent_item.query_selector('[itemprop="name"]')
            agent_name = (await name_element.inner_text()).strip() if name_element else "N/A"

            # Extract agent mobile
            mobile_element = await agent_item.query_selector('.agent-list-cell-phone a')
            mobile = (await mobile_element.inner_text()).strip() if mobile_element else "N/A"

            current_agent = {
                "name": agent_name,
                "mobile": mobile,
                "email": "N/A"  # Email will be fetched later
            }

            print(f"  Agent {agent_index + 1}/{len(agent_items)}: {current_agent['name']}")
            all_agents.append(current_agent)

        except Exception as e:
            print(f"  Error processing agent {agent_index + 1}: {e}")

    return all_agents


async def fetch_email_for_agents(context, agents):
    """Fetch email addresses for all agents concurrently."""
    print(f"Fetching email addresses for {len(agents)} agents...")
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def fetch_with_semaphore(agent):
        async with semaphore:
            await fetch_agent_details(context, agent)
            print(f"Fetched details for: {agent['name']}")

    tasks = [fetch_with_semaphore(agent) for agent in agents]
    await asyncio.gather(*tasks)
    print("Email fetching completed")


def save_data(agents, fields_to_extract=None):
    """Save the agent data to files, only keeping the specified fields."""
    json_file = f"{OUTPUT_FOLDER}/c21_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/c21_agents.csv"

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

    # Save to JSON file
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_agents, f, indent=4, ensure_ascii=False)
    print(f"Saved JSON data to {json_file}")

    # Save to CSV file
    if filtered_agents:  # Make sure we have agents to save
        fieldnames = fields_to_extract if fields_to_extract else filtered_agents[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for agent in filtered_agents:
                writer.writerow({field: agent[field] for field in fieldnames})
        print(f"Saved CSV data to {csv_file}")


async def _run_scraper(url, fields_to_extract=None):
    """The actual scraping function that runs in the background."""
    start_time = datetime.now()

    # Clear existing files and reset progress
    clear_existing_files()
    reset_progress()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Use headless=False for debugging
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)  # Increased timeout

            # First load all agents by repeatedly clicking "Load More"
            total_agents = await load_all_agents(page)

            # Update progress with accurate total count
            progress_data = {
                "processed_agents": 0,
                "total_estimated_agents": total_agents,
                "estimated_time_remaining": "Calculating",
                "elapsed_time": (datetime.now() - start_time).total_seconds()
            }
            with open(PROGRESS_FILE, "w") as f:
                json.dump(progress_data, f)

            # Now collect data for all agents
            all_agents = await collect_all_agent_data(page)

            # Fetch email addresses for all agents
            await fetch_email_for_agents(context, all_agents)

            # Save all data
            save_data(all_agents, fields_to_extract)

            # Final progress update
            progress_data = {
                "processed_agents": len(all_agents),
                "total_estimated_agents": total_agents,
                "estimated_time_remaining": 0,
                "elapsed_time": (datetime.now() - start_time).total_seconds()
            }
            with open(PROGRESS_FILE, "w") as f:
                json.dump(progress_data, f)

            # Send completion email
            send_email()
            print(f"Scraping completed. Found {len(all_agents)} agents.")

        except Exception as e:
            print(f"Main error: {e}")

        finally:
            await browser.close()
            print("Browser closed. Scraping completed")


def get_c21_agents( fields_to_extract=None):
    """
    Start the C21 agent scraper in the background and return immediately.
    This is the function that users will call directly.

    Args:
        url (str): The URL to scrape.
        fields_to_extract (list): List of fields to extract and save (e.g., ['email', 'mobile', 'name']).
                                 If None, all fields will be saved.
    """
    # Stop the previous scraper thread if it's running
    url="https://www.c21atwood.com/realestate/agents/group/agents/"
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

    print("C21 agent scraper started in the background.")
    return "C21 agent scraper has started running in the background. Data will be saved to the 'data' folder once all agents are loaded."


# Example usage
if __name__ == "__main__":
    target_url = "https://www.c21atwood.com/realestate/agents/group/agents/"
    result = get_c21_agents(target_url, fields_to_extract=['email', 'name'])
    print(result)  # This will immediately print the message while scraping continues in background
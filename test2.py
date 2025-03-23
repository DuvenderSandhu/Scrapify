import asyncio
from playwright.async_api import async_playwright
import json
import csv
import os
from datetime import datetime
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email

# Constants - Optimized for better speed/reliability balance
BATCH_SIZE = 100
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 10  # Reduced from 20 to avoid overloading
RETRY_LIMIT = 3
LOAD_MORE_TIMEOUT = 10000  # 10 seconds timeout for load more operations
PAGE_STABILIZE_DELAY = 2000  # 2 seconds delay between loads
MAX_LOAD_ATTEMPTS = 50  # Increased to ensure we get all agents

# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def reset_progress():
    """Reset progress.json to initial state."""
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 200,
        "estimated_time_remaining": "Calculating",
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

async def update_progress(processed, total, start_time):
    """Update the progress file with current status."""
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
    
    # Calculate estimated time remaining
    if processed > 0:
        avg_time_per_agent = elapsed_seconds / processed
        remaining_agents = total - processed
        est_remaining_seconds = avg_time_per_agent * remaining_agents
        est_remaining = f"{est_remaining_seconds:.0f} seconds"
    else:
        est_remaining = "Calculating"
    
    progress_data = {
        "processed_agents": processed,
        "total_estimated_agents": total,
        "estimated_time_remaining": est_remaining,
        "elapsed_time": elapsed_seconds
    }
    
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

async def fetch_agent_details(page, agent_data, agent_url, retry_count=0):
    """Fetch agent details from their individual page."""
    if scraper_stop_event.is_set():
        return
        
    try:
        # Navigate to agent page
        await page.goto(agent_url, timeout=30000)
        
        # Try to find email - adapt selectors based on actual website structure
        email_element = await page.query_selector('a[href^="mailto:"]')
        if email_element:
            email = await email_element.get_attribute('href')
            agent_data["email"] = email.replace('mailto:', '')
        else:
            agent_data["email"] = "Not available"
            
    except Exception as e:
        if retry_count < RETRY_LIMIT and not scraper_stop_event.is_set():
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for {agent_data['name']}: {e}")
            await asyncio.sleep(2 ** retry_count)  # Exponential backoff
            await fetch_agent_details(page, agent_data, agent_url, retry_count + 1)
        else:
            print(f"Failed to fetch details for {agent_data['name']}: {e}")
            agent_data["email"] = "Failed to retrieve"

async def load_all_agents(page):
    """Load all agents with improved reliability and speed."""
    print("Starting to load all agents...")
    
    # Initial wait for page to load
    await page.wait_for_selector('.agent-info', state="visible", timeout=60000)
    
    click_count = 0
    prev_count = 0
    same_count_streak = 0
    
    while click_count < MAX_LOAD_ATTEMPTS:
        if scraper_stop_event.is_set():
            break
            
        # Get current count of agents
        agent_elements = await page.query_selector_all('.agent-info')
        current_count = len(agent_elements)
        print(f"Current agent count: {current_count}")
        
        # Check for load more button
        load_more_button = await page.query_selector('#show-more-agents')
        if not load_more_button or not await load_more_button.is_visible():
            print("No 'Load More' button found or not visible. All agents loaded.")
            break
            
        # Check if count has stabilized
        if current_count == prev_count:
            same_count_streak += 1
            if same_count_streak >= 3:  # If count hasn't changed for 3 attempts, we're done
                print("Agent count stable for 3 attempts. Assuming all loaded.")
                break
        else:
            same_count_streak = 0
            
        # Click the button
        try:
            await load_more_button.click(timeout=LOAD_MORE_TIMEOUT)
            print(f"Clicked 'Load More' button (Attempt {click_count + 1})...")
            
            # Wait for new content to load with reduced timeouts
            try:
                await page.wait_for_selector('#progress', state="visible", timeout=3000)
                await page.wait_for_selector('#progress', state="hidden", timeout=10000)
            except:
                # It's ok if we don't see the spinner - continue anyway
                pass
                
            # Small delay to let page stabilize
            await page.wait_for_timeout(PAGE_STABILIZE_DELAY)
            
        except Exception as e:
            print(f"Error clicking load more: {e}")
            await page.wait_for_timeout(3000)  # Wait a bit and try again
            
        prev_count = current_count
        click_count += 1
        
        # Re-evaluate agent elements after loading
        agent_elements = await page.query_selector_all('.agent-info')
        print(f"Loaded {len(agent_elements)} agents so far")
        
    # Return the final count
    final_elements = await page.query_selector_all('.agent-info')
    print(f"All agents loaded. Total: {len(final_elements)}")
    return len(final_elements)

async def collect_all_agent_data(page, start_time, total_estimated):
    """Collect basic data for all loaded agents."""
    print("Collecting basic data for all agents...")
    all_agents = []
    
    # Get all agent elements
    agent_elements = await page.query_selector_all('.agent-info')
    total_count = len(agent_elements)
    
    for idx, agent_item in enumerate(agent_elements):
        if scraper_stop_event.is_set():
            break
            
        try:
            # Extract name
            name_element = await agent_item.query_selector('[itemprop="name"]')
            agent_name = await name_element.inner_text() if name_element else "Unknown"
            
            # Extract mobile
            mobile_element = await agent_item.query_selector('.agent-list-cell-phone a')
            mobile = await mobile_element.inner_text() if mobile_element else "N/A"
            
            # Try to get agent profile URL
            profile_element = await agent_item.query_selector('a.agent-name-link')
            profile_url = await profile_element.get_attribute('href') if profile_element else None
            
            agent_data = {
                "name": agent_name.strip(),
                "mobile": mobile.strip(),
                "email": "Pending",
                "profile_url": profile_url
            }
            
            all_agents.append(agent_data)
            
            # Update progress every 5 agents
            if idx % 5 == 0:
                await update_progress(idx, total_count, start_time)
                print(f"Processed {idx}/{total_count} agents")
                
        except Exception as e:
            print(f"Error processing agent {idx+1}: {e}")
            
    return all_agents

async def fetch_emails_concurrently(browser, agents, start_time):
    """Fetch email addresses concurrently with better resource management."""
    print(f"Fetching email addresses for {len(agents)} agents...")
    
    # Create a pool of contexts/pages for concurrent use
    contexts = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    # Create the contexts upfront
    for _ in range(CONCURRENT_REQUESTS):
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()
        contexts.append((context, page))
    
    async def fetch_with_semaphore(agent, idx):
        if scraper_stop_event.is_set():
            return
            
        async with semaphore:
            # Get an available context/page
            context, page = contexts[idx % CONCURRENT_REQUESTS]
            
            if agent.get("profile_url"):
                await fetch_agent_details(page, agent, agent["profile_url"])
                print(f"Fetched details for: {agent['name']}")
            else:
                agent["email"] = "No profile URL found"
                
            # Update progress
            processed = sum(1 for a in agents if a["email"] != "Pending")
            if processed % 5 == 0:
                await update_progress(processed, len(agents), start_time)
    
    # Create and run tasks
    tasks = []
    for idx, agent in enumerate(agents):
        if agent.get("profile_url"):
            task = asyncio.create_task(fetch_with_semaphore(agent, idx))
            tasks.append(task)
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    # Close all contexts
    for context, _ in contexts:
        await context.close()
        
    print("Email fetching completed")
    return agents

def save_data(agents, fields_to_extract=None):
    """Save the agent data to files."""
    if not agents:
        print("No agents to save")
        return
        
    json_file = f"{OUTPUT_FOLDER}/c21_agents.json"
    csv_file = f"{OUTPUT_FOLDER}/c21_agents.csv"
    
    # Determine fields to save
    if fields_to_extract:
        filtered_agents = []
        for agent in agents:
            filtered_agent = {}
            for field in fields_to_extract:
                filtered_agent[field] = agent.get(field, "N/A")
            filtered_agents.append(filtered_agent)
    else:
        filtered_agents = agents
    
    # Save to JSON
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_agents, f, indent=2, ensure_ascii=False)
    
    # Save to CSV
    if filtered_agents:
        fieldnames = fields_to_extract if fields_to_extract else filtered_agents[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for agent in filtered_agents:
                writer.writerow({field: agent.get(field, "N/A") for field in fieldnames})
    
    print(f"Saved data to {json_file} and {csv_file}")

async def _run_scraper(url, fields_to_extract=None):
    """Main scraper function with improved error handling and performance."""
    start_time = datetime.now()
    
    # Initialize files
    clear_existing_files()
    reset_progress()
    
    async with async_playwright() as p:
        # Launch browser with more optimized settings
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage']
        )
        
        # Create main context
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        )
        
        # Add error handling for the entire process
        try:
            page = await context.new_page()
            
            # Set longer navigation timeout for initial load
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Load all agents first
            total_agents = await load_all_agents(page)
            await update_progress(0, total_agents, start_time)
            
            # Collect basic data
            agents_with_basic_data = await collect_all_agent_data(page, start_time, total_agents)
            
            # Now fetch the emails concurrently
            complete_agents = await fetch_emails_concurrently(browser, agents_with_basic_data, start_time)
            
            # Save the data
            save_data(complete_agents, fields_to_extract)
            
            # Final progress update
            await update_progress(len(complete_agents), total_agents, start_time)
            
            # Send email notification about completion
            try:
                send_email()
                print("Completion email sent")
            except Exception as email_err:
                print(f"Failed to send email: {email_err}")
                
            print(f"Scraping completed in {(datetime.now() - start_time).total_seconds()} seconds")
            
        except Exception as e:
            print(f"Critical error in scraper: {e}")
            
        finally:
            # Clean up
            await context.close()
            await browser.close()
            print("Resources released. Scraper finished.")

def get_c21_agents(fields_to_extract=None):
    """
    Start the C21 agent scraper in the background.
    
    Args:
        fields_to_extract (list): List of fields to extract (e.g., ['email', 'name']).
                                 If None, all fields will be saved.
    """
    url = "https://www.c21atwood.com/realestate/agents/group/agents/"
    
    # Stop any existing scraper
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        stopThread()
        print("Previous scraper thread stopped")
    
    # Define background thread function
    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_scraper(url, fields_to_extract))
    
    # Start the thread
    startThread(background_scraper)
    
    return "C21 agent scraper started in the background. Data will be saved to the 'data' folder. Progress can be monitored in data/progress.json"

# Example usage
if __name__ == "__main__":
    result = get_c21_agents(fields_to_extract=['name', 'email', 'mobile'])
    print(result)
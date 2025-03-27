import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import csv
import os
from datetime import datetime
import threading
import json
from mainthread import startThread, stopThread, current_scraper_thread, scraper_stop_event
from emailSender import send_email
from fakeagents import get_random_user_agent

# Constants
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/progress.json"
CONCURRENT_REQUESTS = 10
RETRY_LIMIT = 3
LOAD_MORE_TIMEOUT = 10000
PAGE_STABILIZE_DELAY = 2000
MAX_LOAD_ATTEMPTS = 50

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Global list to track all browser instances
active_browsers = []

def reset_progress():
    progress_data = {
        "processed_agents": 0,
        "total_estimated_agents": 200,
        "estimated_time_remaining": "Calculating",
        "elapsed_time": 0
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress_data, f)

def clear_existing_files():
    csv_file = f"{OUTPUT_FOLDER}/c21_agents.csv"
    if os.path.exists(csv_file):
        os.remove(csv_file)
        print(f"Cleared existing file: {csv_file}")

def send_email_notification(message_html):
    try:
        send_email(message_html)
        print("Email notification sent successfully")
    except Exception as e:
        print(f"Failed to send email notification: {e}")

async def update_progress(processed, total, start_time):
    elapsed_seconds = (datetime.now() - start_time).total_seconds()
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
    if scraper_stop_event.is_set():
        return
        
    try:
        await page.goto(agent_url, timeout=30000)
        email_element = await page.query_selector('a[href^="mailto:"]')
        if email_element:
            email = await email_element.get_attribute('href')
            agent_data["email"] = email.replace('mailto:', '')
            if "pending" in agent_data["email"].lower():
                message = f"<p>Alert: Agent {agent_data['name']} has a pending email status.</p><p>Email does not exist for this agent. Please do not request email for this agent.</p>"
                send_email_notification(message)
                scraper_stop_event.set()
                print(f"Found pending email for {agent_data['name']}. Stopping the process.")
                return
        else:
            agent_data["email"] = "Not available"
            
    except Exception as e:
        if retry_count < RETRY_LIMIT and not scraper_stop_event.is_set():
            print(f"Retrying ({retry_count + 1}/{RETRY_LIMIT}) for {agent_data['name']}: {e}")
            await asyncio.sleep(2 ** retry_count)
            await fetch_agent_details(page, agent_data, agent_url, retry_count + 1)
        else:
            print(f"Failed to fetch details for {agent_data['name']}: {e}")
            agent_data["email"] = "Failed to retrieve"

async def load_all_agents(page):
    print("Starting to load all agents...")
    await page.wait_for_selector('.agent-info', state="visible", timeout=60000)
    
    click_count = 0
    prev_count = 0
    same_count_streak = 0
    
    while click_count < MAX_LOAD_ATTEMPTS:
        if scraper_stop_event.is_set():
            break
            
        agent_elements = await page.query_selector_all('.agent-info')
        current_count = len(agent_elements)
        print(f"Current agent count: {current_count}")
        
        load_more_button = await page.query_selector('#show-more-agents')
        if not load_more_button or not await load_more_button.is_visible():
            print("No 'Load More' button found or not visible. All agents loaded.")
            break
            
        if current_count == prev_count:
            same_count_streak += 1
            if same_count_streak >= 3:
                print("Agent count stable for 3 attempts. Assuming all loaded.")
                break
        else:
            same_count_streak = 0
            
        try:
            await load_more_button.click(timeout=LOAD_MORE_TIMEOUT)
            print(f"Clicked 'Load More' button (Attempt {click_count + 1})...")
            try:
                await page.wait_for_selector('#progress', state="visible", timeout=3000)
                await page.wait_for_selector('#progress', state="hidden", timeout=10000)
            except:
                pass
            await page.wait_for_timeout(PAGE_STABILIZE_DELAY)
            
        except Exception as e:
            print(f"Error clicking load more: {e}")
            await page.wait_for_timeout(3000)
            
        prev_count = current_count
        click_count += 1
        
    final_elements = await page.query_selector_all('.agent-info')
    print(f"All agents loaded. Total: {len(final_elements)}")
    return len(final_elements)

async def collect_all_agent_data(page, start_time, total_estimated):
    print("Collecting basic data for all agents...")
    all_agents = []
    
    agent_elements = await page.query_selector_all('.agent-info')
    total_count = len(agent_elements)
    
    for idx, agent_item in enumerate(agent_elements):
        if scraper_stop_event.is_set():
            break
            
        try:
            name_element = await agent_item.query_selector('[itemprop="name"]')
            agent_name = await name_element.inner_text() if name_element else "Unknown"
            
            mobile_element = await agent_item.query_selector('.agent-list-cell-phone a')
            mobile = await mobile_element.inner_text() if mobile_element else "N/A"
            
            profile_element = await agent_item.query_selector('a.agent-name-link')
            profile_url = await profile_element.get_attribute('href') if profile_element else None
            
            agent_data = {
                "name": agent_name.strip(),
                "mobile": mobile.strip(),
                "phone": mobile.strip(),
                "email": "Pending" if profile_url else "Not available",
                "profile_url": profile_url
            }
            
            all_agents.append(agent_data)
            
            if idx % 5 == 0:
                await update_progress(idx, total_count, start_time)
                print(f"Processed {idx}/{total_count} agents")
                
        except Exception as e:
            print(f"Error processing agent {idx+1}: {e}")
            
    return all_agents

async def fetch_emails_concurrently(browser, agents, start_time):
    print(f"Fetching email addresses for {len(agents)} agents...")
    
    agents_with_url = [agent for agent in agents if agent.get("profile_url")]
    if not agents_with_url:
        print("No agents with profile URLs found. Skipping email fetching.")
        return agents
    
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    pages = []

    # Create pages within the single browser instance
    for _ in range(CONCURRENT_REQUESTS):
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()
        pages.append((context, page))
    
    async def fetch_with_semaphore(agent, idx):
        if scraper_stop_event.is_set():
            return
            
        async with semaphore:
            context, page = pages[idx % CONCURRENT_REQUESTS]
            if agent.get("profile_url"):
                await fetch_agent_details(page, agent, agent["profile_url"])
                print(f"Fetched details for: {agent['name']}")
                
            processed = sum(1 for a in agents if a["email"] != "Pending")
            if processed % 5 == 0:
                await update_progress(processed, len(agents), start_time)
    
    tasks = [asyncio.create_task(fetch_with_semaphore(agent, idx)) for idx, agent in enumerate(agents_with_url)]
    
    if not tasks:
        print("No tasks to run. Skipping email fetching.")
        return agents
    
    try:
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED if not scraper_stop_event.is_set() else asyncio.FIRST_COMPLETED)
    except Exception as e:
        print(f"Error during task execution: {e}")
    
    # Close all pages and contexts
    for context, page in pages:
        await page.close()
        await context.close()
        
    print("Email fetching completed or stopped")
    return agents

def save_data(agents, fields_to_extract=None):
    if not agents:
        print("No agents to save")
        return
    
    csv_file = f"{OUTPUT_FOLDER}/c21_agents.csv"
    
    if fields_to_extract:
        normalized_fields = []
        for field in fields_to_extract:
            if field.lower() == 'phone' and 'mobile' in agents[0]:
                normalized_fields.append('mobile')
            else:
                normalized_fields.append(field)
        fields_to_extract = normalized_fields
        
        filtered_agents = [{field: agent.get(field, "N/A") for field in fields_to_extract} for agent in agents]
    else:
        filtered_agents = agents
    
    if filtered_agents:
        fieldnames = fields_to_extract if fields_to_extract else filtered_agents[0].keys()
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for agent in filtered_agents:
                writer.writerow({field: agent.get(field, "N/A") for field in fieldnames})
    
    print(f"Saved data for {len(filtered_agents)} agents to {csv_file}")

async def close_all_browsers():
    global active_browsers
    for browser in active_browsers[:]:  # Copy to avoid modifying list during iteration
        try:
            await browser.close()
            print(f"Closed browser instance: {browser}")
        except Exception as e:
            print(f"Error closing browser: {e}")
    active_browsers.clear()

async def _run_scraper(url, fields_to_extract=None):
    global active_browsers
    start_time = datetime.now()
    clear_existing_files()
    reset_progress()
    
    # Close any existing browsers
    await close_all_browsers()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-gpu', '--no-sandbox', '--disable-dev-shm-usage']
        )
        active_browsers.append(browser)
        
        try:
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
                user_agent=get_random_user_agent()
            )
            page = await context.new_page()
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            total_agents = await load_all_agents(page)
            await update_progress(0, total_agents, start_time)
            
            agents_with_basic_data = await collect_all_agent_data(page, start_time, total_agents)
            
            complete_agents = await fetch_emails_concurrently(browser, agents_with_basic_data, start_time)
            
            save_data(complete_agents, fields_to_extract)
            
            completed_count = len(complete_agents)
            await update_progress(completed_count, total_agents, start_time)
            
            if scraper_stop_event.is_set():
                print("Scraping was stopped early due to pending email detection")
            else:
                try:
                    send_email()
                    print("Standard completion email sent")
                except Exception as email_err:
                    print(f"Failed to send email: {email_err}")
                
            elapsed_time = (datetime.now() - start_time).total_seconds()
            print(f"Scraping {'completed' if not scraper_stop_event.is_set() else 'stopped'} in {elapsed_time} seconds")
            
        except Exception as e:
            print(f"Critical error in scraper: {e}")
            
        finally:
            await context.close()
            await browser.close()
            active_browsers.remove(browser) if browser in active_browsers else None
            print("Resources released. Scraper finished.")

def get_c21_agents(fields_to_extract=None):
    global current_scraper_thread, scraper_stop_event
    
    # Check if 'email' is requested
    if fields_to_extract and 'email' in [field.lower() for field in fields_to_extract]:
        message = "<p>Hello,</p><p>No Email was found on the provided website. Please check the details and try without Email.</p>"
        send_email_notification(message)
        return "Email notification sent. The scraping process was not started because emails do not exist on the website."

    url = "https://www.c21atwood.com/realestate/agents/group/agents/"
    
    # Stop any existing scraper and close all browsers
    if current_scraper_thread and current_scraper_thread.is_alive():
        print("Stopping previous scraper thread...")
        scraper_stop_event.set()
        stopThread()
        current_scraper_thread.join(timeout=10)
        if current_scraper_thread.is_alive():
            print("Warning: Previous thread did not stop cleanly.")
        scraper_stop_event.clear()
        asyncio.run(close_all_browsers())  # Ensure all browsers are closed
        print("Previous scraper thread and browsers stopped")
    
    def background_scraper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_run_scraper(url, fields_to_extract))
        finally:
            loop.close()
    
    startThread(background_scraper)
    
    return "C21 agent scraper started in the background. It will stop and notify you if pending emails are found."

if __name__ == "__main__":
    # Requesting only name and phone
    result = get_c21_agents(fields_to_extract=['name', 'phone'])
    print(result)
    
    # Requesting email (will stop immediately)
    result = get_c21_agents(fields_to_extract=['name', 'email'])
    print(result)
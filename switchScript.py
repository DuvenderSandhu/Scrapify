import importlib
import os
import json
import time
import asyncio

# Configuration
OUTPUT_FOLDER = "data"
PROGRESS_FILE = f"{OUTPUT_FOLDER}/switch_progress.json"
AGENTS_PER_SCRIPT = 500  # Default limit per script
SWITCH_DELAY = 5  # Delay in seconds between switching scripts

# Ensure output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

def load_progress():
    """Load progress from file or initialize it."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {
        "scripts": {},
        "total_agents": 0,
        "last_script_index": -1
    }

def save_progress(progress):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=4)

async def run_scraper(script_module, url, max_agents, fields, start_page=1, start_agent=None):
    """
    Run a single scraper script with limits and progress tracking.
    Assumes the script has a modified _run_scraper function that saves its own data.
    """
    try:
        # Call the script's _run_scraper function with parameters
        agents, last_page, last_agent_name = await script_module._run_scraper(
            url=url,
            max_agents=max_agents,
            fields=fields,
            start_page=start_page,
            start_agent=start_agent
        )
        return agents, last_page, last_agent_name
    except AttributeError as e:
        print(f"Error: {script_module.__name__} does not have a suitable _run_scraper function: {e}")
        return [], start_page, start_agent
    except Exception as e:
        print(f"Unexpected error running {script_module.__name__}: {e}")
        return [], start_page, start_agent

def switch_scrapers(fields=['name', 'email', 'mobile'], scripts=None, resume=True):
    """
    Switch between scraper scripts, limiting each to 500 agents, and track progress.
    Each script saves its own data to its own files.

    Args:
        fields (list): Fields to scrape (e.g., ['name', 'email', 'mobile']).
        scripts (list): List of script names (e.g., ['script1', 'script2']). Defaults to 4 scripts.
        resume (bool): Whether to resume from last progress.

    Returns:
        int: Total number of agents scraped across all scripts.
    """
    if scripts is None:
        scripts = ['script1', 'script2']

    progress = load_progress() if resume else {"scripts": {}, "total_agents": 0, "last_script_index": -1}
    total_agents = progress["total_agents"]
    start_index = progress["last_script_index"] + 1 if resume and progress["last_script_index"] < len(scripts) - 1 else 0

    for i in range(start_index, len(scripts)):
        script_name = scripts[i]
        print(f"Running {script_name}...")

        # Dynamically import the script
        try:
            script_module = importlib.import_module(script_name)
        except ImportError as e:
            print(f"Failed to import {script_name}: {e}")
            continue

        # Load script-specific progress
        script_progress = progress["scripts"].get(script_name, {"last_page": 1, "last_agent": None})
        start_page = script_progress["last_page"]
        start_agent = script_progress["last_agent"]

        # Define the URL (customize per script if needed)
        url = "https://www.coldwellbankerhomes.com/sitemap/agents/"  # Default; adjust as needed

        # Run the scraper
        loop = asyncio.new_event_loop()  # Use new event loop to avoid conflicts
        asyncio.set_event_loop(loop)
        agents, last_page, last_agent_name = loop.run_until_complete(
            run_scraper(script_module, url, AGENTS_PER_SCRIPT, fields, start_page, start_agent)
        )

        # Update total agents count (data is saved by the script itself)
        total_agents += len(agents)
        print(f"Collected {len(agents)} agents from {script_name}. Total across all scripts: {total_agents}")

        # Update progress
        progress["scripts"][script_name] = {
            "last_page": last_page,
            "last_agent": last_agent_name
        }
        progress["total_agents"] = total_agents
        progress["last_script_index"] = i
        save_progress(progress)

        # Delay before switching, unless it's the last script
        if i < len(scripts) - 1:
            print(f"Waiting {SWITCH_DELAY} seconds before switching to next script...")
            time.sleep(SWITCH_DELAY)

    print("All scripts completed.")
    return total_agents
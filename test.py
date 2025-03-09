from playwright.sync_api import sync_playwright
import time
import traceback
import json
import csv
import os
from datetime import datetime
from playwright.async_api import async_playwright
from log import log_info, log_success, log_error, log_warning 
import asyncio

async def get_all_data(url):
    all_agents = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
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
                                log_info(f"    Processing page {page_num} for {inner_city_name}")
                                agent_blocks = await page.query_selector_all('.agent-block')
                                log_info(f"    Found {len(agent_blocks)} agents on page {page_num}")
                                
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
                                        
                                        if agent_url:
                                            full_agent_url = f'https://www.coldwellbankerhomes.com{agent_url}' if agent_url.startswith('/') else agent_url
                                            agent_page = await context.new_page()
                                            await agent_page.goto(full_agent_url, wait_until="networkidle")
                                            await asyncio.sleep(1)
                                            
                                            email_element = await agent_page.query_selector('.email-link')
                                            if email_element:
                                                current_agent["email"] = (await email_element.inner_text()).strip()
                                            
                                            await agent_page.close()
                                        
                                        all_agents.append(current_agent)
                                        log_info(f"      Agent {agent_index+1}: {current_agent['name']}")
                                        
                                        if len(all_agents) % 10 == 0:
                                            save_data(all_agents, timestamp, save_json=True, save_csv=True)
                                    except Exception as e:
                                        log_error(f"      Error processing agent: {e}")
                                        traceback.print_exc()
                                
                                has_more_pages = False
                                try:
                                    pagination = await page.query_selector('.pagination')
                                    if pagination:
                                        next_button = await pagination.query_selector('ul > li:last-child > a')
                                        is_disabled = await pagination.query_selector('ul > li:last-child.disabled')
                                        
                                        if next_button and not is_disabled:
                                            log_info(f"    Moving to next page for {inner_city_name}")
                                            await next_button.click()
                                            await page.wait_for_load_state('networkidle')
                                            await asyncio.sleep(2)
                                            page_num += 1
                                            has_more_pages = True
                                        else:
                                            log_info(f"    No more pages for {inner_city_name}")
                                except Exception as e:
                                    log_error(f"    Error checking pagination: {e}")
                                    traceback.print_exc()
                        except Exception as e:
                            log_error(f"  Error processing inner city {inner_city_name}: {e}")
                            traceback.print_exc()
                except Exception as e:
                    log_error(f"Error processing city {city_name}: {e}")
                    traceback.print_exc()
            
            if all_agents:
                save_data(all_agents, timestamp, save_json=True, save_csv=True)
                log_success(f"Scraped data for {len(all_agents)} agents successfully!")
        except Exception as e:
            log_error(f"Main error: {e}")
            traceback.print_exc()
            if all_agents:
                save_data(all_agents, timestamp, save_json=True, save_csv=True)
                log_info(f"Saved partial data for {len(all_agents)} agents before error.")
        finally:
            await browser.close()
            log_info("Browser closed. Scraping completed")
  
            
def save_data(agents, timestamp, save_json=True, save_csv=True):
    """Save the agent data to files"""
    # Create data directory if it doesn't exist
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # Save as JSON
    if save_json:
        json_file = f"data/coldwell_agents_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(agents, f, indent=4, ensure_ascii=False)
        print(f"Saved JSON data to {json_file}")
    
    # Save as CSV
    if save_csv:
        csv_file = f"data/coldwell_agents_{timestamp}.csv"
        if agents:
            fieldnames = agents[0].keys()
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(agents)
            print(f"Saved CSV data to {csv_file}")

if __name__ == "__main__":
    # Target URL
    target_url = "https://www.coldwellbankerhomes.com/sitemap/agents/"
    main(target_url)
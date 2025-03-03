import asyncio
import time
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from log import log_info, log_success, log_error, log_warning  # Import logging functions
from database import db

rawid = ""

async def get_html(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """
    Fetch HTML content by navigating to a URL and extracting a strictly meaningful container.
    """
    options = options or {}
    max_pages = options.get('max_pages', 1)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    handle_pagination = options.get('handle_pagination', False)
    js_timeout = options.get('js_timeout', 10000)           # Reduced JS timeout (ms)
    navigation_timeout = options.get('navigation_timeout', 30000)  # Reduced navigation timeout (ms)
    retry_attempts = options.get('retry_attempts', 2)
    
    start_time = time.time()
    log_info(f"Starting HTML fetch from: {url} [Options: lazy_loading={handle_lazy_loading}, pagination={handle_pagination}]")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
        )
        log_info("Launched headless Chromium browser with args: --disable-web-security")
        
        for attempt in range(retry_attempts + 1):
            if attempt > 0:
                log_info(f"Retry attempt {attempt + 1}/{retry_attempts + 1}")
                
            # Create a fresh context and page for each attempt.
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                ignore_https_errors=True,
                java_script_enabled=True
            )
            page = await context.new_page()
            page.set_default_timeout(navigation_timeout)
            page.on("dialog", lambda dialog: asyncio.create_task(dialog.dismiss()))
            
            try:
                log_info(f"Navigating to {url} with timeout {navigation_timeout}ms")
                nav_start = time.time()
                response = await page.goto(url, timeout=navigation_timeout, wait_until="domcontentloaded")
                nav_time = time.time() - nav_start
                
                if response and response.ok:
                    log_success(f"Navigation succeeded in {nav_time:.2f}s (Status: {response.status})")
                else:
                    log_warning(f"Navigation issue after {nav_time:.2f}s: Status={response.status if response else 'No response'}")
                    await context.close()
                    continue

                # Wait for the initial network activities to settle.
                await page.wait_for_timeout(3000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                    log_success("Page reached network idle state")
                except Exception as e:
                    # log_warning(f"Network idle timeout: {str(e)}")
                    print(str(e))
                
                if handle_pagination and button:
                    await handle_pagination_with_backoff(page, button, loader, max_pages)
                
                if handle_lazy_loading:
                    await handle_lazy_loading_with_limits(page)
                
                await page.wait_for_timeout(2000)
                html_content = await page.content()
                raw_size = len(html_content)
                
                if raw_size > 500:
                    log_success(f"Extracted raw HTML: {raw_size} bytes")
                    filtered_html = extract_relevant_container(html_content)
                    filtered_size = len(filtered_html)
                    reduction = ((raw_size - filtered_size) / raw_size * 100)
                    log_info(f"Filtered content size: {filtered_size} bytes (reduction: {reduction:.1f}%)")
                    global rawid
                    rawid = db.save_raw_html(url, filtered_html)
                    await context.close()
                    log_info(f"Fetch completed in {time.time() - start_time:.2f}s")
                    await browser.close()
                    return filtered_html
                else:
                    log_warning(f"Raw HTML too small ({raw_size} bytes), retrying")
                    await context.close()
                    continue
                    
            except Exception as e:
                log_error(f"Error during fetch: {str(e)}")
                await context.close()
            # End of attempt loop
        
        await browser.close()
        log_info("Browser closed")
        log_error(f"All {retry_attempts + 1} attempts failed for {url}")
        return ""

async def handle_pagination_with_backoff(page, button, loader, max_pages):
    """Handle pagination with detailed logging and strict button checks."""
    log_info(f"Pagination enabled: Searching for button '{button}' (max_pages={max_pages})")
    page_count = 0
    
    while page_count < max_pages:
        try:
            if not button.startswith(("//", ".", "#")):
                button_xpath = f"//button[normalize-space()='{button}'] | //*[text()='{button}']"
                button_selector = f"xpath={button_xpath}"
            else:
                button_selector = button

            button_element = None
            for timeout_ms in [3000, 5000, 8000]:
                try:
                    button_element = await page.wait_for_selector(
                        button_selector, timeout=timeout_ms, state="visible"
                    )
                    if button_element:
                        log_success(f"Found button '{button}' after {timeout_ms}ms")
                        break
                except Exception:
                    if timeout_ms == 8000:
                        log_warning(f"Button '{button}' not found after {timeout_ms}ms")
                        return
                    continue
            
            if not button_element:
                log_warning(f"Button '{button}' not found, stopping pagination")
                return

            await button_element.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)
            
            click_success = False
            for attempt in range(3):
                try:
                    await button_element.click()
                    click_success = True
                    break
                except Exception as e:
                    log_warning(f"Click attempt {attempt + 1}/3 failed: {str(e)}")
                    await page.wait_for_timeout(1000)
            
            if not click_success:
                log_error("Failed to click button after 3 attempts")
                return
                
            page_count += 1
            log_success(f"Clicked '{button}' ({page_count}/{max_pages})")
            await page.wait_for_timeout(2000)
            
            if loader:
                try:
                    await page.wait_for_selector(loader, state="hidden", timeout=5000)
                    log_success("Loader disappeared")
                except Exception:
                    log_warning("Loader still present, proceeding anyway")
                    
        except Exception as e:
            log_error(f"Pagination error: {str(e)}")
            return

async def handle_lazy_loading_with_limits(page):
    """Handle lazy loading with detailed logging."""
    log_info("Handling lazy loading with progressive scrolling")
    max_scroll_attempts = 10
    scroll_step = 0.2
    
    try:
        viewport_height = await page.evaluate("window.innerHeight")
        total_height = await page.evaluate("document.body.scrollHeight")
        log_info(f"Initial page height: {total_height}px, viewport: {viewport_height}px")
        previous_height = total_height
        
        for attempt in range(max_scroll_attempts):
            current_position = await page.evaluate("window.pageYOffset")
            if current_position + viewport_height >= total_height - 100:
                log_info("Reached page bottom, stopping scroll")
                break
                
            next_position = current_position + (viewport_height * scroll_step)
            await page.evaluate(f"window.scrollTo({{top: {next_position}, behavior: 'smooth'}})")
            await page.wait_for_timeout(800)
            
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height > total_height:
                log_success(f"Loaded new content (height: {total_height}px → {new_height}px)")
                total_height = new_height
            else:
                log_info("No increase in page height, stopping lazy loading")
                break
            
            if attempt % 3 == 2:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
                
    except Exception as e:
        log_error(f"Lazy loading error: {str(e)}")

def extract_relevant_container(html_content: str) -> str:
    """Extract a strictly meaningful container with enhanced scoring and logging."""
    soup = BeautifulSoup(html_content, 'lxml')
    log_info("Analyzing HTML for relevant container")
    
    def score_container(element):
        score = 0
        children = element.find_all(recursive=False)
        text_length = len(element.get_text(strip=True))
        class_name = " ".join(element.get("class", [])).lower() if element.get("class") else ""
        
        # Strict exclusions
        if element.name in ['script', 'style', 'nav', 'header', 'footer']:
            
            return -1
        if any(x in class_name for x in ['navbar', 'footer', 'header', 'sidebar', 'ad', 'banner']):
            
            return -1
        
        # Minimum requirements
        if len(children) < 3:
            
            return -1
        if text_length < 100:
            
            return -1
        
        # Scoring
        if element.name in ['div', 'section', 'article', 'main']:
            score += 20
        score += len(children) * 10  # Higher weight for structured content
        score += min(text_length // 200, 50)  # Text contribution capped
        depth = len(list(element.parents))
        if depth < 3:
            score -= 30  # Penalize shallow containers
        if len(set(child.name for child in children)) > 2:  # Reward variety in child tags
            score += 15
            
        # log_info(f"Scored <{element.name} class='{class_name}'>: {score} (children={len(children)}, text={text_length}, depth={depth})")
        return score

    containers = soup.find_all(['div', 'section', 'article', 'main'], recursive=True)
    if not containers:
        log_warning("No candidate containers found")
        return ""

    best_container = max(containers, key=score_container, default=None)
    best_score = score_container(best_container)
    
    if best_container and best_score >= 50:  # Stricter threshold
        log_success(f"Selected container: <{best_container.name}> with score {best_score}")
        return str(best_container)
    else:
        log_warning(f"No container met criteria (best score={best_score})")
        return ""

def extract_data_by_css(html_content, selector):
    """Extract elements using a CSS selector with logging."""
    soup = BeautifulSoup(html_content, 'lxml')
    elements = soup.select(selector)
    log_info(f"Extracted {len(elements)} elements with selector '{selector}'")
    return [el.get_text(strip=True) for el in elements]

def get_html_sync(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """Synchronous wrapper for async function."""
    return asyncio.run(get_html(url, button, options, loader))

# Example Usage
# if __name__ == "__main__":
#     url = "https://www.c21atwood.com/realestate/agents/group/agents/"
#     options = {
#         "max_pages": 2,
#         "handle_lazy_loading": True,
#         "handle_pagination": True,
#         "js_timeout": 30000,
#         "navigation_timeout": 120000,
#         "retry_attempts": 2
#     }
#     html = get_html_sync(url, button="Load More", options=options, loader=".loading-spinner")
#     print(f"Extracted content (first 500 chars): {html[:500]}...")

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from log import log_info, log_success, log_error,log_warning  # Import logging functions from app.py
from database import db

# from playwright.async_api import async_playwright

# from playwright.async_api import async_playwright

rawid= ""

async def get_html(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """
    Fetch HTML content by navigating to a URL, waiting for JavaScript-loaded content,
    with optional button clicking and other behaviors.
    """
    options = options or {}
    max_pages = options.get('max_pages', 1)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    handle_pagination = options.get('handle_pagination', False)
    js_timeout = options.get('js_timeout', 20000)
    navigation_timeout = options.get('navigation_timeout', 90000)  # Increased to 90 seconds
    retry_attempts = options.get('retry_attempts', 2)  # Add retry functionality
    
    log_info(f"Starting to fetch HTML from: {url}")

    # Retry loop
    for attempt in range(retry_attempts + 1):
        if attempt > 0:
            log_info(f"Retry attempt {attempt}/{retry_attempts}")
            
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
            )
            log_info("Launched headless Chromium browser with additional args")

            try:
                # Use a more permissive context
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                    ignore_https_errors=True,
                    java_script_enabled=True
                )
                
                # Create page with adjusted settings
                page = await context.new_page()
                page.set_default_timeout(navigation_timeout)
                
                # Set up handler for dialog boxes that might interrupt navigation
                page.on("dialog", lambda dialog: asyncio.create_task(dialog.dismiss()))
                
                # Try to navigate with progressive fallbacks
                try:
                    log_info(f"Navigating to {url} with timeout {navigation_timeout}ms")
                    
                    # Use a more basic navigation approach first
                    response = await page.goto(
                        url, 
                        timeout=navigation_timeout,
                        wait_until="domcontentloaded"  # Less strict loading requirement
                    )
                    
                    if not response or not response.ok:
                        log_warning(f"Initial navigation had issues: {response and response.status}")
                        # Continue anyway - we might still get usable content
                    else:
                        log_success(f"Successfully navigated to {url} (Status: {response.status})")
                    
                    # Give a bit of time for initial rendering
                    await page.wait_for_timeout(3000)
                    
                    # Try to wait for network idle but don't block on it
                    try:
                        await page.wait_for_load_state("networkidle", timeout=js_timeout // 2)
                        log_success("Page reached network idle state")
                    except Exception as e:
                        log_warning(f"Network idle waiting timed out: {e}")
                    
                    # Handle pagination - with more cautious approach
                    if handle_pagination and button and page_count < max_pages:
                        await handle_pagination_with_backoff(page, button, loader, max_pages)
                        
                    # Handle lazy loading - with more cautious approach
                    if handle_lazy_loading:
                        await handle_lazy_loading_with_limits(page)
                    
                    # Give a final chance for any pending content
                    await page.wait_for_timeout(2000)
                    
                    # Capture the content
                    html_content = await page.content()
                    
                    # If we got substantial content, consider it successful
                    if len(html_content) > 500:  # Arbitrary threshold for "meaningful" content
                        log_success(f"Successfully extracted HTML content ({len(html_content)} bytes)")
                        # Save the HTML content
                        rawid = db.save_raw_html(url, html_content)
                        await browser.close()
                        return html_content
                    else:
                        log_warning(f"Retrieved content seems too small ({len(html_content)} bytes)")
                        
                except Exception as e:
                    log_error(f"Navigation error: {e}")
                    html_content = ""
                    
            except Exception as e:
                log_error(f"Browser error: {e}")
                html_content = ""
                
            finally:
                # Always close the browser
                try:
                    await browser.close()
                    log_info("Browser closed")
                except:
                    log_error("Error closing browser")

    # If we're here, all attempts failed
    log_error(f"All {retry_attempts + 1} attempts to fetch content failed")
    return ""


# Helper functions to keep the main function cleaner
async def handle_pagination_with_backoff(page, button, loader, max_pages):
    """Handle pagination with exponential backoff for retries"""
    log_info("Pagination enabled. Attempting to click 'Load More' button")
    page_count = 0
    
    while page_count < max_pages:
        try:
            # Prepare button selector
            if not button.startswith(("//", ".", "#")):
                button_xpath = f"//button[normalize-space()='{button}'] | //*[text()='{button}']"
                button_selector = f"xpath={button_xpath}"
            else:
                button_selector = button

            # Try to find the button with increasing timeouts
            for timeout_ms in [3000, 5000, 8000]:
                try:
                    button_element = await page.wait_for_selector(
                        button_selector, 
                        timeout=timeout_ms,
                        state="visible"
                    )
                    if button_element:
                        break
                except:
                    if timeout_ms == 8000:  # Last attempt
                        log_warning("Button not found after multiple attempts")
                        return
                    continue
                    
            # Try to ensure the button is clickable
            await button_element.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)  # Give it a moment to become fully interactive
            
            # Try to click with retry
            click_success = False
            for attempt in range(3):
                try:
                    await button_element.click()
                    click_success = True
                    break
                except Exception as e:
                    await page.wait_for_timeout(1000)  # Wait and retry
                    
            if not click_success:
                log_warning("Failed to click button after multiple attempts")
                return
                
            page_count += 1
            log_success(f"Clicked 'Load More' button ({page_count}/{max_pages})")
            
            # Wait for content to update
            await page.wait_for_timeout(2000)
            
            # Check for loader
            if loader:
                try:
                    await page.wait_for_selector(loader, state="hidden", timeout=5000)
                except:
                    log_warning("Loader didn't disappear, continuing anyway")
                    
        except Exception as e:
            log_error(f"Pagination error: {e}")
            return


async def handle_lazy_loading_with_limits(page):
    """Handle lazy loading with strict limits and progressive scrolling"""
    log_info("Handling lazy loading with progressive scrolling")
    
    try:
        max_scroll_attempts = 10  # Strict limit
        scroll_step = 0.2  # Scroll 20% of viewport at a time
        
        # Get initial page height
        viewport_height = await page.evaluate("window.innerHeight")
        total_height = await page.evaluate("document.body.scrollHeight")
        
        for attempt in range(max_scroll_attempts):
            current_position = await page.evaluate("window.pageYOffset")
            
            # If we've reached bottom, no need to continue
            if current_position + viewport_height >= total_height - 100:
                log_info("Reached bottom of page, stopping scroll")
                break
                
            # Calculate next scroll position (progressive)
            next_position = current_position + (viewport_height * scroll_step)
            
            # Smooth scroll to next position
            await page.evaluate(f"window.scrollTo({{top: {next_position}, behavior: 'smooth'}})")
            await page.wait_for_timeout(800)  # Wait for scroll and content load
            
            # Check if page height changed
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height > total_height:
                log_success(f"New content loaded after scroll (height: {total_height} → {new_height})")
                total_height = new_height
            
            # Periodically do a full scroll to bottom to trigger more aggressive loading
            if attempt % 3 == 2:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
                
    except Exception as e:
        log_error(f"Error during lazy loading: {e}")
def extract_data_by_css(html_content, selector):
    """Extract elements using a given CSS selector and return text content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    elements = soup.select(selector)
    return [el.get_text(strip=True) for el in elements]

def get_html_sync(url: str, button: str = None, options:str={},loader: str = None, use_text: bool = False):
    """Synchronous wrapper for the async function."""
    return asyncio.run(get_html(url, button, loader, use_text))

# Example Usage (Synchronous)
# url = "https://example.com"
# button_selector = ".load-more-button"  # Example CSS selector
# button_text = "Load More"              # Example button text
# loader_selector = ".loading-spinner"   # Example loader selector

# # Click by CSS selector
# html_content = get_html_sync(url, button=button_selector, loader=loader_selector, use_text=False)

# Click by Button Text
# html_content = get_html_sync(url, button=button_text, loader=loader_selector, use_text=True)

# Extract data
# data = extract_data_by_css(html_content, ".product-title")
# print(data)  # Extracts product titles

async def fetch_html(url: str) -> str:
    """Fetches the HTML content of a given URL using AsyncWebCrawler."""
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        return result.html  # Return the HTML content


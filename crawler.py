import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from log import log_info, log_success, log_error  # Import logging functions from app.py

# from playwright.async_api import async_playwright

# from playwright.async_api import async_playwright

async def get_html(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """
    Fetch HTML content by navigating to a URL, waiting for JavaScript-loaded content,
    with optional button clicking and other behaviors.
    
    Args:
        url (str): The URL to navigate to
        button (str): Optional button text or selector to click for loading more content
        options (dict): Optional configuration settings:
            - max_pages (int): Maximum number of times to click the button
            - handle_lazy_loading (bool): Enable scrolling to trigger lazy loading
            - handle_pagination (bool): Enable pagination handling
            - pagination_method (str): Method to use for pagination
            - js_timeout (int): Milliseconds to wait for JavaScript execution
        loader (str): Optional selector for a loading indicator
    
    Returns:
        str: The full HTML content of the page after JavaScript execution
    """
    options = options or {}
    max_pages = options.get('max_pages', 1)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    handle_pagination = options.get('handle_pagination', False)
    js_timeout = options.get('js_timeout', 10000)  # Default 10 seconds for JS loading
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        # Navigate to URL and wait for JavaScript to load content
        await page.goto(url)
        
        # Wait for network activity to quiet down first
        try:
            await page.wait_for_load_state("networkidle", timeout=js_timeout)
        except Exception:
            pass  # Continue even if timeout
            
        # Extra wait for any JavaScript processing
        await page.wait_for_timeout(5000)
        
        # Handle pagination if enabled
        if handle_pagination and button:
            page_count = 0
            
            # Loop to click the "load more" button
            while page_count < max_pages:
                try:
                    # Determine the button selector: if button is text, convert to XPath
                    if not button.startswith(("//", ".", "#")):
                        button_xpath = f"//button[normalize-space()='{button}'] | //*[text()='{button}']"
                        button_selector = f"xpath={button_xpath}"
                    else:
                        button_selector = button
                    
                    # Try to find the button (scrolling into view if needed)
                    button_element = await page.wait_for_selector(button_selector, timeout=5000)
                    await button_element.scroll_into_view_if_needed()
                    
                    # Click the button
                    await button_element.click()
                    page_count += 1
                    
                    # Wait for new content and network activity to quiet down
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        await page.wait_for_timeout(3000)  # Fallback timeout
                    
                    # Wait for loader to disappear if provided
                    if loader:
                        try:
                            await page.wait_for_selector(loader, state="hidden", timeout=5000)
                        except Exception:
                            pass  # Continue even if loader doesn't disappear
                
                except Exception:
                    break  # Stop if button not found or can't be clicked
        
        # Handle lazy loading if enabled
        if handle_lazy_loading:
            try:
                # More aggressive scrolling for lazy-loaded content
                prev_height = await page.evaluate("document.body.scrollHeight")
                scroll_attempts = 0
                max_scroll_attempts = 5
                
                while scroll_attempts < max_scroll_attempts:
                    # Scroll in increments
                    for i in range(1, 11):
                        scroll_position = i * prev_height / 10
                        await page.evaluate(f"window.scrollTo(0, {scroll_position})")
                        await page.wait_for_timeout(300)
                    
                    # Final scroll to bottom
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)
                    
                    # Check if content height changed
                    new_height = await page.evaluate("document.body.scrollHeight")
                    if new_height == prev_height:
                        scroll_attempts += 1
                    else:
                        scroll_attempts = 0  # Reset counter if new content loaded
                        
                    prev_height = new_height
                    
                    # Wait for any network activity after scrolling
                    try:
                        await page.wait_for_load_state("networkidle", timeout=3000)
                    except Exception:
                        pass
            except Exception:
                pass  # Continue even if scrolling fails
        
        # Final wait to ensure all JavaScript has completed
        await page.wait_for_timeout(2000)
        
        # Extract HTML content
        html_content = await page.content()
        
        await browser.close()
        return html_content
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


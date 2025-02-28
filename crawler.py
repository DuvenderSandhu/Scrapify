import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from log import log_info, log_success, log_error  # Import logging functions from app.py
from database import db
# from playwright.async_api import async_playwright

# from playwright.async_api import async_playwright



async def get_html(url: str, button: str = None, options: dict = None, loader: str = None) -> str:
    """
    Fetch HTML content by navigating to a URL, waiting for JavaScript-loaded content,
    with optional button clicking and other behaviors.
    """
    options = options or {}
    max_pages = options.get('max_pages', 1)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    handle_pagination = options.get('handle_pagination', False)
    js_timeout = options.get('js_timeout', 10000)  # Default 10 seconds for JS loading

    log_info(f"Starting to fetch HTML from: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        log_info("Launched headless Chromium browser")

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            await page.goto(url)
            log_success(f"Successfully navigated to {url}")

            # Wait for network idle
            try:
                await page.wait_for_load_state("networkidle", timeout=js_timeout)
                log_success("Page loaded successfully")
            except Exception:
                log_error("Timed out waiting for network idle state")

            await page.wait_for_timeout(5000)

            # Handle pagination if enabled
            if handle_pagination and button:
                log_info("Pagination enabled. Attempting to click 'Load More' button")
                page_count = 0
                while page_count < max_pages:
                    try:
                        if not button.startswith(("//", ".", "#")):
                            button_xpath = f"//button[normalize-space()='{button}'] | //*[text()='{button}']"
                            button_selector = f"xpath={button_xpath}"
                        else:
                            button_selector = button

                        button_element = await page.wait_for_selector(button_selector, timeout=5000)
                        await button_element.scroll_into_view_if_needed()
                        await button_element.click()
                        page_count += 1
                        log_success(f"Clicked 'Load More' button ({page_count}/{max_pages})")

                        await page.wait_for_load_state("networkidle", timeout=5000)

                        if loader:
                            try:
                                await page.wait_for_selector(loader, state="hidden", timeout=5000)
                                log_success("Loader disappeared successfully")
                            except Exception:
                                log_error("Loader did not disappear in time")
                    except Exception:
                        log_error("Could not find or click 'Load More' button")
                        break

            # Handle lazy loading if enabled
            if handle_lazy_loading:
                log_info("Handling lazy loading by scrolling")
                try:
                    prev_height = await page.evaluate("document.body.scrollHeight")
                    scroll_attempts = 0
                    max_scroll_attempts = 5

                    while scroll_attempts < max_scroll_attempts:
                        for i in range(1, 11):
                            scroll_position = i * prev_height / 10
                            await page.evaluate(f"window.scrollTo(0, {scroll_position})")
                            await page.wait_for_timeout(300)

                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(2000)

                        new_height = await page.evaluate("document.body.scrollHeight")
                        if new_height == prev_height:
                            scroll_attempts += 1
                            log_info(f"Scrolling attempt {scroll_attempts}/{max_scroll_attempts} - No new content loaded")
                        else:
                            scroll_attempts = 0
                            log_success("New content loaded after scrolling")

                        prev_height = new_height

                        try:
                            await page.wait_for_load_state("networkidle", timeout=3000)
                        except Exception:
                            log_error("Timeout while waiting for network idle after scrolling")
                except Exception as e:
                    log_error(f"Error during lazy loading: {e}")

            await page.wait_for_timeout(2000)

            html_content = await page.content()
            log_success("Successfully extracted HTML content")

        except Exception as e:
            log_error(f"Error occurred: {e}")
            html_content = ""

        await browser.close()
        log_info("Browser closed")
        db.save_raw_html(url, html_content)
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


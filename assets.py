import asyncio
import random
import time
from playwright.async_api import async_playwright

# Function to simulate human-like zigzag mouse movement
async def random_zigzag_move(page, start_x, start_y, end_x, end_y, duration=1.0, zigzag_count=5):
    steps = 50  # Number of total steps in the movement
    # Calculate the basic step increments along x and y
    dx = (end_x - start_x) / steps
    dy = (end_y - start_y) / steps

    for step in range(steps):
        # Calculate the position along the line
        x = start_x + dx * step
        y = start_y + dy * step

        # Add randomness (zigzag effect) to the y-coordinate
        y += random.uniform(-10, 10)  # Random vertical deviation (you can adjust the range)

        # Move the mouse to the new position
        await page.mouse.move(x, y)
        
        # Wait to make the movement smooth
        await asyncio.sleep(duration / steps)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 4 XL Build/QD1A.190805.007) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0 Edg/91.0.864.59",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0 Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0 Opera/68.0.3618.125",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (iPad; CPU OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Linux; ARM; Android 9; Pixel 3 Build/QQ3A.200805.001) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:62.0) Gecko/20100101 Firefox/62.0 Opera/62.0.3331.43",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0 Chrome/93.0.4577.82 Safari/537.36"
]

# Randomly pick a user-agent for each session
selected_user_agent = random.choice(user_agents)


http_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "User-Agent":selected_user_agent,
    "DNT": "1",
    "TE": "Trailers",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://google.com",
    "Origin": "https://gpogle.com",
    "Cache-Control": "max-age=0",
    "If-Modified-Since": "Tue, 27 Jul 2021 12:28:53 GMT",
    "X-Requested-With": "XMLHttpRequest",
    "X-Frame-Options": "SAMEORIGIN",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Forwarded-For": "203.0.113.195",
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
}

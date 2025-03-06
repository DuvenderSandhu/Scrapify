import requests
from bs4 import BeautifulSoup
import time
import json
rawid=""
from collections import defaultdict


# from collections import defaultdict
# from bs4 import BeautifulSoup

def find_elements_by_selector(html_content, selector):
    """Extract elements using a given CSS selector and group by their parent elements."""
    soup = BeautifulSoup(html_content, 'html.parser')

    grouped_elements = defaultdict(list)

    for element in soup.select(selector):
        parent = element.find_parent()  # Get parent container
        grouped_elements[parent].append(element.get_text(strip=True))  # Store values under the same parent

    # Remove duplicates within each group while preserving order
    grouped_texts = [list(dict.fromkeys(texts)) for texts in grouped_elements.values()]

    # **Fix the "list index out of range" error**
    if not grouped_texts:
        return []  # Return empty list instead of causing an error
    
    # Flatten the list if there's only one group
    flattened_texts = [item for sublist in grouped_texts for item in sublist]

    # If only one group exists, return the flattened list directly
    return flattened_texts if len(grouped_texts) > 1 else flattened_texts

from bs4 import BeautifulSoup

def clean_html(html_content):
    """Convert HTML to clean text while removing redundant elements."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove unnecessary elements
    for tag in soup(["script", "style", "meta", "link", "noscript", "think"]):
        tag.decompose()  # Remove the tag completely

    # Try to find the main content or fall back to body
    # soup = soup.find('main') 
    # print(soup.prettify())

    # Get the minimized HTML string
    # minimized_html = soup.prettify(formatter="minimal")
    # if not soup:
    #     print("No main or body element found.")
    #     return ""

    # Get plain text
    text = soup.get_text(separator=" ", strip=True)

    # Reduce excessive whitespace
    cleaned_text = text

    return cleaned_text  # Limit to 5000 chars for AI efficiency

def call_groq_ai(prompt: str,GROQ_API_KEY):
    print("call groq")
    """Send a request to Groq AI for extraction."""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "deepseek-r1-distill-qwen-32b",
        "messages": [{"role": "system", "content": "Extract structured data from text."},
                     {"role": "user", "content": prompt}],
        "temperature": 0.7
    }
    
    response = requests.post(url, headers=headers, json=data)
    print("response",response)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    return None

def call_openai(prompt, api_key):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}]}

    retries = 5
    for i in range(retries):
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        elif response.status_code == 429:  # Rate limit hit
            wait_time = (2 ** i)  # Exponential backoff
            print(f"Rate limit hit. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None
    return None
def call_gemini(prompt: str,GEMINI_API_KEY=""):
    """Send a request to Google Gemini for extraction."""
    print(GEMINI_API_KEY)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    
    response = requests.post(url, headers=headers, json=data)
    print(response)
    if response.status_code == 200:
        return response.json()["candidates"][0]["content"]["parts"][0]["text"]
    return "Error fetching data from Gemini"

def call_deepseek(prompt: str,DEEPSEEK_API_KEY=""):
    """Send a request to DeepSeek for extraction."""
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
    data = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0}
    
    response = requests.post(url, headers=headers, json=data)
    print(response)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    return "Error fetching data from DeepSeek"


def extract_data_with_ai(html_content, field,ai="groq",api=""):
    """Extract structured data from HTML using AI."""
    print(html_content)
    cleaned_text = clean_html(html_content)
    print("clean_html",cleaned_text)
    ai_result= ""
    ai_prompt = f"""You are an intelligent text extraction and conversion assistant. Your task is to extract structured information from the given text and convert it .
                        with no additional commentary, explanations, or extraneous information.don't generate anything random from your side it should 100% authentic from the html
                        You could encounter cases where you can't find the data of the fields you have to extract or the data will be in a foreign language it here is not any data you can return empty don't generate anything or anything Dummy Text.it should 100% follow the sturcture of fields
                        Please process the following text and provide the output Extract {",".join(field)} Return the output in the following python dict with any extra text structure:"field_name_1": ["text1", "text2", "text3"],"field_name_2": ["text4", "text5"]
 from the following text: data should have duplicate data data should be in this format seperated by ',' response should be very professional like if email all should be in lower if contact number is should be put country code +1 and give them without seperator like - or space it should be +18923827373 \n\n{cleaned_text} be 100% you don't generate any thing from your site like if email it should be 100% exist there think of its surity first"""
    # print("aiPrompt",ai_prompt)
    print("ai Lower",ai,ai.lower())
    if ai.lower()=='groq':
        print("groq here")
        ai_result = call_groq_ai(ai_prompt,api)
    elif ai.lower() == 'openai':
        print("i am here")
        ai_result = call_openai(ai_prompt,api)
    elif ai.lower() == 'gemini':
        ai_result = call_gemini(ai_prompt,api)
    elif ai.lower()=='deepseek':
        ai_result = call_deepseek(ai_prompt,api)
    print(ai_result)
    text=clean_html(ai_result)
    cleaned_ai_response = json.loads(text.replace("```python", "").replace("```", "").strip())
    print(cleaned_ai_response)
    if cleaned_ai_response:
        print(cleaned_ai_response)
        return cleaned_ai_response  # Convert AI response to list
    
    return {data:f"No {field} found"}




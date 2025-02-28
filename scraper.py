import requests
from bs4 import BeautifulSoup

def find_elements_by_selector(html_content, selector):
    """Extract elements using a given CSS selector and return text content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Select elements using the given CSS selector
    elements = soup.select(selector)
    
    # Return extracted text content
    return [el.get_text(strip=True) for el in elements]


def clean_html(html_content):
    """Convert HTML to clean text while removing redundant elements."""
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove unnecessary elements
    for tag in soup(["script", "style", "meta", "link", "noscript","think"]):
        tag.decompose()  # Remove tag completely

    # Get plain text
    text = soup.get_text(separator=" ", strip=True)
    
    # Reduce excessive whitespace
    cleaned_text = ' '.join(text.split())

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

def call_openai(prompt: str,OPENAI_API_KEY=""):
    """Send a request to OpenAI for extraction."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    data = {"model": "gpt-4-turbo", "messages": [{"role": "user", "content": prompt}], "temperature": 0.7}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    return "Error fetching data from OpenAI"

def call_gemini(prompt: str,GEMINI_API_KEY=""):
    """Send a request to Google Gemini for extraction."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateText?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["candidates"][0]["content"]["parts"][0]["text"]
    return "Error fetching data from Gemini"

def call_deepseek(prompt: str,DEEPSEEK_API_KEY=""):
    """Send a request to DeepSeek for extraction."""
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
    data = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.7}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    return "Error fetching data from DeepSeek"


def extract_data_with_ai(html_content, field,ai="groq",api=""):
    """Extract structured data from HTML using AI."""
    cleaned_text = clean_html(html_content)
    # print("clean_html",cleaned_text)
    ai_result= ""
    ai_prompt = f"""You are an intelligent text extraction and conversion assistant. Your task is to extract structured information from the given text and convert it .
                        with no additional commentary, explanations, or extraneous information. 
                        You could encounter cases where you can't find the data of the fields you have to extract or the data will be in a foreign language.
                        Please process the following text and provide the output Extract {",".join(field)} Return the output in the following JSON structure:"field_name_1": ["text1", "text2", "text3"],"field_name_2": ["text4", "text5"]
 from the following text: data should have duplicate data data should be in this format seperated by ',' response should be very professional like if email all should be in lower if contact number is should be put country code +1 and give them without seperator like - or space it should be +18923827373 \n\n{cleaned_text} """
    # print("aiPrompt",ai_prompt)
    print("ai Lower",ai,ai.lower())
    if ai.lower()=='groq':
        print("groq here")
        ai_result = call_groq_ai(ai_prompt,api)
    elif ai.lower() == 'openai':
        ai_result = call_openai(ai_prompt,api)
    elif ai.lower() == 'gemini':
        ai_result = call_gemini(ai_prompt,api)
    elif ai.lower()=='deepseek':
        ai_result = call_deepseek(ai_prompt,api)
    
    cleaned_ai_response = clean_html(ai_result)
    if cleaned_ai_response:
        return cleaned_ai_response.split(",")  # Convert AI response to list
    
    return [f"No {field} found"]




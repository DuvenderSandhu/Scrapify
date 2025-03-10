import threading
import time
import asyncio
import os
import requests
from flask import Flask, request, jsonify, send_file
from app import crawl_url, extract_data  # Assuming these functions are defined elsewhere in your app

app = Flask(__name__)

# In-memory storage for work ID status
work_status = {}

# Define the API route for crawling URLs
@app.route('/api/crawl', methods=['POST'])
def api():
    # Get the JSON data from the request
    data = request.get_json()

    # Extract the URL from the data (it is required)
    url = data.get('url')

    if not url:
        return jsonify({"error": "URL is required"}), 400  # Return an error if URL is missing

    # Get options from the data (using default values if missing)
    options = data.get('options', {})

    # Set default values for the options if they are not provided
    follow_links = options.get('follow_links', None)
    max_depth = options.get('max_depth', None)
    max_pages = options.get('max_pages', len([url]))  # Use length of URL list as default
    stay_on_domain = options.get('stay_on_domain', None)
    handle_pagination = options.get('handle_pagination', None)
    handle_lazy_loading = options.get('handle_lazy_loading', False)
    pagination_method = options.get('pagination_method', None)
    hyphen_separator = options.get('hyphen_separator', False)
    country_code = options.get('country_code', False)

    # Prepare options
    crawl_options = {
        'follow_links': follow_links,
        'max_depth': max_depth,
        'max_pages': max_pages,
        'stay_on_domain': stay_on_domain,
        'handle_pagination': handle_pagination,
        'handle_lazy_loading': handle_lazy_loading,
        'pagination_method': pagination_method,
        'hyphen_separator': hyphen_separator,
        'country_code': country_code
    }

    # Generate a unique work ID (using timestamp as a simple work ID)
    work_id = str(int(time.time() * 1000))

    # Create a new thread to process the crawling task asynchronously
    def run_crawl():
        # Run the crawling function asynchronously using asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Crawl the URL
        crawl_result = loop.run_until_complete(crawl_url(url, crawl_options))

        html_content = crawl_result.get('html', '')
        print("htmlContent:", html_content)

        # Extract the data from the HTML content (or any other data)
        extracted_data = extract_data(html_content, data.get('fields', ["phone", "email"]))

        if not extracted_data:  # Check if extracted data is empty
            extracted_data = {"message": "No data found"}  # Return a message if no data found

        print("Extracted_data:", extracted_data)

        # Generate the file name using work_id
        result_file_path = os.path.join("data_files", f"{work_id}.txt")

        # Save the extracted data to a file named with the work_id
        os.makedirs("data_files", exist_ok=True)  # Ensure the directory exists
        with open(result_file_path, 'w') as temp_file:
            temp_file.write(str(extracted_data))  # Save the extracted data (converted to string)

        # Once the task is done, update the work status and store the file path
        work_status[work_id] = {
            'status': 'completed',
            'result_file': result_file_path  # Save the file path for later access
        }

    # Store initial status as 'in progress'
    work_status[work_id] = {'status': 'in progress'}

    # Start the crawling task in a separate thread to run asynchronously
    threading.Thread(target=run_crawl).start()

    # Return the work ID to the user
    response_data = {
        "work_id": work_id,
        "message": "Crawl initiated, use the work_id to check the status."
    }

    return jsonify(response_data)


# API endpoint to check the status of a task
@app.route('/api/status/<work_id>', methods=['GET'])
def check_status(work_id):
    # Check if the work_id exists in the work_status dictionary
    if work_id in work_status:
        status = work_status[work_id]['status']
        if status == 'completed':
            result_file = work_status[work_id]['result_file']
            return jsonify({"status": "completed", "result_file": result_file})
        return jsonify({"status": "in progress"})
    else:
        return jsonify({"error": "Work ID not found or task not yet completed"}), 404


# API endpoint to download the result file
@app.route('/api/download/<work_id>', methods=['GET'])
def download_result(work_id):
    if work_id in work_status:
        # Check if the task is completed
        status = work_status[work_id]['status']
        if status == 'completed':
            result_file = work_status[work_id]['result_file']
            # Return the file to the user
            return send_file(result_file, as_attachment=True)
        return jsonify({"status": "in progress"}), 202
    return jsonify({"error": "Work ID not found"}), 404


# Function to cleanup temporary files after the user downloads it
@app.after_request
def cleanup(response):
    for work_id, status in work_status.items():
        if status['status'] == 'completed':
            result_file = status['result_file']
            # Remove the temporary file after the result has been fetched
            if os.path.exists(result_file):
                os.remove(result_file)
                work_status[work_id]['status'] = 'removed'  # Mark as removed
    return response


# Automatically hit the API after the Flask app starts
def hit_api_after_start():
    url = "https://www.google.com"
    options = {
        "follow_links": True,
        "max_depth": 3,
        "max_pages": 10,
        "stay_on_domain": True,
        "handle_pagination": False,
        "handle_lazy_loading": False,
        "pagination_method": "next_page",
        "hyphen_separator": True,
        "country_code": "US",
        "extraction_method": "regex",
        "fields": ["title"]
    }

    data = {
        "url": url,
        "options": options
    }

    # POST request to the /api/crawl endpoint
    response = requests.post("http://127.0.0.1:5000/api/crawl", json=data)

    # Print the response from the API
    print(response.json())


# Main block
if __name__ == '__main__':
    # Run the API call after starting the Flask app
    # hit_api_after_start()

    # Start Flask app
    app.run(debug=True, use_reloader=False)

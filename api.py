import threading
import time
import asyncio
import os
import requests
from flask import Flask, request, jsonify, send_file
import resend 
app = Flask(__name__)

# In-memory storage for work ID status
work_status = {}

# Mock functions for crawling and data extraction
async def crawl_url(url, options):
    # Simulate a crawling process
    await asyncio.sleep(5)  # Simulate network delay
    return {"html": f"<html>Mock HTML content from {url}</html>"}

def extract_data(html_content, fields):
    # Simulate data extraction
    return {"data": f"Extracted data from {html_content} using fields {fields}"}

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

    # Prepare options
    crawl_options = {
        'follow_links': options.get('follow_links', False),
        'max_depth': options.get('max_depth', 1),
        'max_pages': options.get('max_pages', 1),
        'stay_on_domain': options.get('stay_on_domain', True),
        'handle_pagination': options.get('handle_pagination', False),
        'handle_lazy_loading': options.get('handle_lazy_loading', False),
        'pagination_method': options.get('pagination_method', None),
        'hyphen_separator': options.get('hyphen_separator', False),
        'country_code': options.get('country_code', None)
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


# Main block
if __name__ == '__main__':
    # Start Flask app
    app.run(debug=True, use_reloader=False)
import threading
import time
import asyncio
import os
import requests
from flask import Flask, request, jsonify, send_file
from app import crawl_url, extract_data
app = Flask(__name__)

# In-memory storage for work ID status
work_status = {}

# Mock functions for crawling and data extraction


# Define the API route for crawling URLs
@app.route('/api/crawl', methods=['POST'])
def api():
    # Get the JSON data from the request
    data = request.get_json()

    # Extract the URL from the data (it is required)
    url = data.get('url')

    if not url:
        return jsonify({"error": "URL is required"}), 400

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

    # Create a new event loop for the thread
    def run_crawl():
        # Get or create an event loop for this thread
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Crawl the URL
        crawl_result = loop.run_until_complete(crawl_url(url, crawl_options))

        html_content = crawl_result.get('html', '')
        print("htmlContent:", html_content)

        # Extract the data from the HTML content
        extracted_data = extract_data(html_content, data.get('fields', ["phone", "email"]))

        if not extracted_data:  # Check if extracted data is empty
            extracted_data = {"message": "No data found"}

        print("Extracted_data:", extracted_data)

        # Generate the file name using work_id
        result_file_path = os.path.join("data_files", f"{work_id}.txt")

        # Save the extracted data to a file named with the work_id
        os.makedirs("data_files", exist_ok=True)
        with open(result_file_path, 'w', encoding='utf-8') as temp_file:
            temp_file.write(str(extracted_data))

        # Update the work status and store the file path
        work_status[work_id] = {
            'status': 'completed',
            'result_file': result_file_path
        }

    # Store initial status as 'in progress'
    work_status[work_id] = {'status': 'in progress'}

    # Start the crawling task in a separate thread
    threading.Thread(target=run_crawl, daemon=True).start()

    # Return the work ID to the user
    response_data = {
        "work_id": work_id,
        "message": "Crawl initiated, use the work_id to check the status."
    }

    return jsonify(response_data), 202

# API endpoint to check the status of a task
@app.route('/api/status/<work_id>', methods=['GET'])
def check_status(work_id):
    if work_id in work_status:
        status_info = work_status[work_id]
        status = status_info['status']
        if status == 'completed':
            result_file_path = status_info['result_file']
            file_content= ""
            # Read the file content (assuming JSON file)
            with open(result_file_path, 'r', encoding='utf-8') as file:
                file_content = json.load(file)  # Use .read() for plain text files
            
            return jsonify({"status": "completed","data":file_content, "result_file": status_info['result_file']})
        elif status == 'in progress':
            return jsonify({"status": "in progress"})
        else:
            return jsonify({"status": "removed"})
    return jsonify({"error": "Work ID not found"}), 404

# API endpoint to download the result file
@app.route('/api/download/<work_id>', methods=['GET'])
def download_result(work_id):
    if work_id in work_status:
        status_info = work_status[work_id]
        status = status_info['status']
        if status == 'completed':
            result_file = status_info['result_file']
            if os.path.exists(result_file):
                return send_file(result_file, as_attachment=True, download_name=f"{work_id}_result.txt")
            else:
                return jsonify({"error": "Result file not found"}), 404
        elif status == 'in progress':
            return jsonify({"status": "in progress"}), 202
        else:
            return jsonify({"error": "Result already downloaded and removed"}), 410
    return jsonify({"error": "Work ID not found"}), 404

# Cleanup temporary files after download
@app.after_request
def cleanup(response):
    # Only clean up if the response is successful and from the download endpoint
    if response.status_code == 200 and request.path.startswith('/api/download/'):
        work_id = request.path.split('/')[-1]
        if work_id in work_status and work_status[work_id]['status'] == 'completed':
            result_file = work_status[work_id]['result_file']
            if os.path.exists(result_file):
                try:
                    os.remove(result_file)
                    work_status[work_id]['status'] = 'removed'
                except OSError as e:
                    print(f"Error removing file {result_file}: {e}")
    return response

# Main block
if __name__ == '__main__':
    # Ensure data_files directory exists
    os.makedirs("data_files", exist_ok=True)
    # Start Flask app
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
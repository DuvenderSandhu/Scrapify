import threading

# Global variables to track the current scraper thread and stop event
current_scraper_thread = None
scraper_stop_event = threading.Event()

def startThread(background_scraper):
    global current_scraper_thread
    # Stop the previous thread if it's running
    if current_scraper_thread and current_scraper_thread.is_alive():
        stopThread()
    
    # Create and start a new thread
    current_scraper_thread = threading.Thread(target=background_scraper)
    current_scraper_thread.daemon = True  # Makes thread exit when main program exits
    current_scraper_thread.start()

def stopThread():
    global current_scraper_thread, scraper_stop_event
    if current_scraper_thread and current_scraper_thread.is_alive():
        scraper_stop_event.set()  # Signal the scraper to stop
        current_scraper_thread.join()  # Wait for the thread to finish
        scraper_stop_event.clear()  # Reset the stop event
        current_scraper_thread = None
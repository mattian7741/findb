import os
import subprocess
import sys
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Paths and settings
DROPZONE_PATH = os.path.abspath('./dropzone')
COMPLETED_PATH = os.path.abspath('./completed')
INGESTION_SCRIPT = './ingest.py'
MERCHANT_SCRIPT = './merchant.py'
MERCHANT_META_SCRIPT = './merchant_meta.py'
CONFIG_FILE = './config.yaml'

class DropzoneHandler(FileSystemEventHandler):
    """Handler for monitoring new files in the dropzone folder structure."""

    def __init__(self):
        super().__init__()
        self.pending_files = 0

    def on_created(self, event):
        """Triggered when a new file is created in the dropzone."""
        if event.is_directory or not event.src_path.endswith('.csv'):
            return  # Skip directories and non-CSV files

        account_folder = os.path.relpath(event.src_path, DROPZONE_PATH)
        account_name = os.path.dirname(account_folder)
        file_path = event.src_path
        
        print(f"New .csv file detected for account '{account_name}': {file_path}")
        
        self.pending_files += 1
        success = self.process_file(account_name, file_path)
        
        if success:
            self.move_to_completed(account_name, file_path)
            self.pending_files -= 1

            # Check if all files have been processed and run merchant scripts if so
            if self.pending_files == 0 and self.is_dropzone_empty():
                self.run_merchant_scripts()

    def process_file(self, account_name, file_path):
        """Invoke the ingestion script with account name and file path."""
        try:
            command = [
                'python', INGESTION_SCRIPT,
                account_name, file_path,
                '--config', CONFIG_FILE
            ]
            print(f"Processing file '{file_path}' for account '{account_name}'...")
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error processing file '{file_path}': {e.stderr}")
            return False

    def move_to_completed(self, account_name, file_path):
        """Move a processed file to the completed folder with a timestamped filename."""
        completed_folder = os.path.join(COMPLETED_PATH, account_name)
        os.makedirs(completed_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = os.path.basename(file_path)
        new_filename = f"{timestamp}_{filename}"
        completed_path = os.path.join(completed_folder, new_filename)
        
        os.rename(file_path, completed_path)
        print(f"File '{file_path}' moved to '{completed_path}'")

    def ingest_existing_files(self):
        """Process all existing .csv files in the dropzone on service startup."""
        print("Starting ingestion of existing .csv files in the dropzone...")
        for account_name in os.listdir(DROPZONE_PATH):
            account_folder = os.path.join(DROPZONE_PATH, account_name)
            if os.path.isdir(account_folder):
                for file_name in os.listdir(account_folder):
                    file_path = os.path.join(account_folder, file_name)
                    if file_name.endswith('.csv') and os.path.isfile(file_path):
                        print(f"Found existing .csv file '{file_path}' for account '{account_name}'")
                        self.pending_files += 1
                        success = self.process_file(account_name, file_path)
                        if success:
                            self.move_to_completed(account_name, file_path)
                            self.pending_files -= 1

    def is_dropzone_empty(self):
        """Check if there are any remaining .csv files in the dropzone directory."""
        for root, _, files in os.walk(DROPZONE_PATH):
            if any(file.endswith('.csv') for file in files):
                return False
        return True


    def run_merchant_scripts(self):
        """Run the merchant.py script, followed by the merchant_meta.py script to add metadata."""
        print("Running merchant update script...")
        merchant_success = False
        
        try:
            # Run merchant.py to update the merchant table
            result = subprocess.run(['python', MERCHANT_SCRIPT], check=True, text=True, stdout=sys.stdout, stderr=sys.stderr)
            print("Merchant update completed.")
            merchant_success = True
        except subprocess.CalledProcessError as e:
            print(f"Error running merchant update script: {e.stderr}")

        # Only run merchant_meta.py if merchant.py ran successfully
        if False: # merchant_success:
            print("Running merchant metadata update script...")
            try:
                # Stream output directly to avoid buffering issues
                result = subprocess.run(['python', MERCHANT_META_SCRIPT], check=True, text=True, stdout=sys.stdout, stderr=sys.stderr)
                print("Merchant metadata update completed.")
            except subprocess.CalledProcessError as e:
                print(f"Error running merchant metadata update script: {e.stderr}")


def run_service():
    """Set up and run the dropzone folder monitoring service."""
    event_handler = DropzoneHandler()
    
    # Process existing .csv files in the dropzone on startup
    print("Processing existing .csv files in the dropzone on startup...")
    event_handler.ingest_existing_files()

    # Run merchant scripts after initial ingestion of existing files
    if event_handler.pending_files == 0 and event_handler.is_dropzone_empty():
        event_handler.run_merchant_scripts()

    observer = Observer()
    observer.schedule(event_handler, path=DROPZONE_PATH, recursive=True)
    
    print(f"Starting dropzone monitoring service for folder: {DROPZONE_PATH}")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping service...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    run_service()

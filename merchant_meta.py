import sqlite3
import json
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI

print("hello")
# Load OpenAI API key from file
with open("openai_key.txt", "r") as key_file:
    openai_api_key = key_file.read().strip()

# Initialize the OpenAI LLM with the loaded key
llm = OpenAI(openai_api_key=openai_api_key)

# Define the system prompt to request JSON format
template = """I will provide a list of merchant names. You will respond with a JSON array of objects, each object containing the following keys:
- merchant (this value must exactly match the input value for merchant)
- city
- region (state if in the USA)
- country
- phone_number
- URL
- gpt_category (an inferred budget category based on the merchant's information)

Use an empty string ("") for any values you cannot reasonably infer. This response is for data integration, so the JSON structure must be clean, consistent, and without any introductory or concluding text.

MERCHANTS:
{data}"""

# Create a prompt template
prompt = PromptTemplate(input_variables=["data"], template=template)

# Database file path
DB_FILE = 'financials.db'

def get_merchants_without_metadata(batch_size=3):
    """Retrieve merchant IDs from the merchant table where metadata fields are empty."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT merchant_id FROM merchant
        WHERE city IS NULL AND region IS NULL AND country IS NULL
        LIMIT ?
    """, (batch_size,))
    
    merchants = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return merchants

def fetch_merchant_metadata(merchants, retry_limit=3):
    """Fetch metadata for a list of merchants using the ChatGPT API, adjusting for response truncation and errors."""
    all_metadata = []
    retries = retry_limit

    while retries > 0:
        try:
            merchant_data = "\n".join(merchants)
            formatted_prompt = prompt.format(data=merchant_data)

            print(f"DEBUG: Processing batch with {len(merchants)} merchants:\n{merchant_data}")
            response = llm.invoke(formatted_prompt)
            print(f"DEBUG: Received response length: {len(response)}")

            if response.startswith("[") and response.endswith("]"):
                json_data = json.loads(response)
                
                for item in json_data:
                    all_metadata.append({
                        "merchant_id": item.get("merchant", "").strip(),
                        "city": item.get("city", "").strip(),
                        "region": item.get("region", "").strip(),
                        "country": item.get("country", "").strip(),
                        "phone_number": item.get("phone_number", "").strip(),
                        "url": item.get("URL", "").strip(),
                        "gpt_category": item.get("gpt_category", "").strip()
                    })
                break

            else:
                print("DEBUG: Response truncated or malformed. Retrying with fewer merchants.")
                merchants = merchants[:len(merchants) - 1]  # Reduce the list size for retrying

        except json.JSONDecodeError:
            print("ERROR: JSON decoding failed. Retrying...")
            retries -= 1
        
        except Exception as e:
            print(f"ERROR: Exception occurred: {e}")
            retries -= 1

    return all_metadata

def update_merchant_metadata(metadata_list):
    """Update merchant table with metadata for each merchant."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    update_query = """
    UPDATE merchant
    SET city = ?, region = ?, country = ?, phone_number = ?, url = ?, gpt_category = ?
    WHERE merchant_id = ?
    """
    
    for metadata in metadata_list:
        print(f"DEBUG: Updating metadata for merchant ID: {metadata['merchant_id']}")
        cursor.execute(update_query, (
            metadata['city'],
            metadata['region'],
            metadata['country'],
            metadata['phone_number'],
            metadata['url'],
            metadata['gpt_category'],
            metadata['merchant_id']
        ))
    
    conn.commit()
    print(f"DEBUG: Finished updating merchant metadata. Total rows updated: {len(metadata_list)}")
    conn.close()

print("Script started")  # Ensure this is the very first line in the file

def main():
    print("Starting main function in merchant_meta.py")
    merchants = get_merchants_without_metadata()
    print(f"Retrieved merchants without metadata: {merchants}")
    if not merchants:
        print("DEBUG: No merchants found without metadata.")
        return
    
    # Fetch metadata using ChatGPT for merchants
    merchant_data = fetch_merchant_metadata(merchants)
    print(f"Fetched merchant metadata: {merchant_data}")
    if merchant_data:
        update_merchant_metadata(merchant_data)
    else:
        print("DEBUG: No metadata returned for merchants.")

if __name__ == "__main__":
    print("Executing merchant_meta.py as the main module")
    main()

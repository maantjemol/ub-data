import asyncio
import aiohttp
import pandas as pd
from lxml import etree
from tqdm import tqdm
import time
import sys

# Base URL for the OAI-PMH endpoint
BASE_URL = "https://scholarlypublications.universiteitleiden.nl/oai2"
initial_params = {
    "verb": "ListRecords",
    "metadataPrefix": "oai_dc",
    # Uncomment if server supports a larger batch size
    # "maxRecords": "500"  
}

# Define namespaces used in OAI-PMH XML
namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/'
}

def extract_records(xml_content):
    """Extract record data using lxml for faster parsing."""
    try:
        root = etree.fromstring(xml_content.encode('utf-8') if isinstance(xml_content, str) else xml_content)
        records = []
        
        for record in root.xpath('//oai:record', namespaces=namespaces):
            record_data = {}
            
            identifier = record.xpath('./oai:header/oai:identifier/text()', namespaces=namespaces)
            if identifier:
                record_data['identifier'] = identifier[0]
            
            datestamp = record.xpath('./oai:header/oai:datestamp/text()', namespaces=namespaces)
            if datestamp:
                record_data['datestamp'] = datestamp[0]
            
            # Extract Dublin Core elements
            for element in record.xpath('.//oai_dc:dc/*', namespaces=namespaces):
                tag = etree.QName(element).localname
                if tag in record_data:
                    if isinstance(record_data[tag], list):
                        record_data[tag].append(element.text)
                    else:
                        record_data[tag] = [record_data[tag], element.text]
                else:
                    record_data[tag] = element.text
                        
            records.append(record_data)
        
        # Check for resumption token
        resumption_tokens = root.xpath('//oai:resumptionToken/text()', namespaces=namespaces)
        token = resumption_tokens[0] if resumption_tokens and resumption_tokens[0].strip() else None
        
        return records, token
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return [], None

async def fetch_records(session, params, retry_count=3):
    """Fetch records with retry mechanism"""
    for i in range(retry_count):
        try:
            async with session.get(BASE_URL, params=params, timeout=30) as response:
                if response.status == 503:  # Service Unavailable
                    wait_time = min(2 ** i, 30)  # Exponential backoff
                    print(f"Server busy (503). Waiting {wait_time}s before retry {i+1}/{retry_count}")
                    await asyncio.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return await response.text()
        except asyncio.TimeoutError:
            wait_time = min(2 ** i, 30)
            print(f"Request timed out. Waiting {wait_time}s before retry {i+1}/{retry_count}")
            await asyncio.sleep(wait_time)
        except Exception as e:
            if i < retry_count - 1:
                wait_time = min(2 ** i, 30)
                print(f"Error: {e}. Retrying in {wait_time}s ({i+1}/{retry_count})")
                await asyncio.sleep(wait_time)
            else:
                print(f"Failed after {retry_count} attempts: {e}")
                return None
    
    return None

async def harvest_records_async(max_concurrent=5):
    """Harvest all records using async requests for better performance"""
    all_records = []
    params = initial_params.copy()
    resumption_token = None
    
    async with aiohttp.ClientSession() as session:
        print("Starting data harvest...")
        
        # Get first batch
        response_text = await fetch_records(session, params)
        if not response_text:
            print("Failed to retrieve initial batch. Exiting.")
            return []
            
        records, resumption_token = extract_records(response_text)
        all_records.extend(records)
        
        print(f"Retrieved {len(records)} records. Starting concurrent harvesting...")
        
        # Create progress bar
        pbar = tqdm(initial=len(records), unit="records")
        
        # For tracking active tasks
        tasks = []
        resumption_tokens = []
        if resumption_token:
            resumption_tokens.append(resumption_token)
        
        # While we have tokens to process
        while resumption_tokens:
            # Fill the task queue up to max_concurrent
            while len(tasks) < max_concurrent and resumption_tokens:
                token = resumption_tokens.pop(0)
                token_params = {
                    "verb": "ListRecords",
                    "resumptionToken": token
                }
                tasks.append(asyncio.create_task(fetch_records(session, token_params)))
            
            # Wait for the first task to complete
            if tasks:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                
                for task in done:
                    response_text = task.result()
                    if response_text:
                        batch_records, next_token = extract_records(response_text)
                        all_records.extend(batch_records)
                        pbar.update(len(batch_records))
                        
                        if next_token:
                            resumption_tokens.append(next_token)
                    
                    # Remove the completed task
                    tasks = list(pending)
            
            # Short pause to prevent overwhelming the server
            await asyncio.sleep(0.05)
        
        # Process any remaining tasks
        if tasks:
            for response_text in await asyncio.gather(*tasks):
                if response_text:
                    batch_records, _ = extract_records(response_text)
                    all_records.extend(batch_records)
                    pbar.update(len(batch_records))
        
        pbar.close()
    
    print(f"Harvesting complete. Total records: {len(all_records)}")
    return all_records

def save_to_csv(records, filename='leiden_publications.csv'):
    """Save records to CSV file"""
    df = pd.DataFrame(records)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")
    return df

async def main():
    # Set the concurrency level (adjust based on server capabilities)
    concurrency = 8
    if len(sys.argv) > 1:
        try:
            concurrency = int(sys.argv[1])
            print(f"Using concurrency level: {concurrency}")
        except ValueError:
            print(f"Invalid concurrency value, using default: {concurrency}")
    
    start_time = time.time()
    records = await harvest_records_async(max_concurrent=concurrency)
    
    if records:
        df = save_to_csv(records)
        print(f"\nDataFrame Info:")
        print(f"Total records: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nHarvesting completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
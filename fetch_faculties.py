import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
import math
import time  # Optional: Add a small delay to be polite
import random  # Optional: For random delays
from tqdm import tqdm  # For progress bar
import datetime  # For calculating estimated time to finish

# --- Configuration ---
BASE_URL = "https://scholarlypublications.universiteitleiden.nl/search?type=edismax&islandora_solr_search_navigation=1&f[0]=ancestors_ms%3A%22collection%3A20801%22"
MAX_WORKERS = 32
REQUEST_TIMEOUT = 30  # seconds

cookies = {
    "cookie_hide_notification": "true",
    "cookie_accepted": "true",
    # NOTE: studentcontext, TS, and SESS cookies might be session-specific or load-balancer related.
    # Using potentially stale values might cause issues.
    # If scraping fails, try removing these or fetching them dynamically.
    # For this example, we'll keep them as provided but be aware.
    "studentcontext": "4306660BB3B0C866AFAD37AB6EC00E1AC4DA55C28F16B04B519BDE9C9C4087A4DCF8B8B8A4460C171FCE714C2B7F857B536F85296266CD47E246492B0ADD2F8BE632B48127270E486B0A9FDDA6871E9BFE39F1AD02596506093D890D0B89C4DF",
    "TS5f4ffdf3027": "08fd114d42ab200077b5b6ed852e9245a20ad822adc037ef23a210625ca256ce1da999050f6d51190847af3152113000bda6c68314f4a38151ebbb43c5a8d02db1a1ccc24d9e325ceccf2985e837d12387dd5ea81443f514c24a127912389b6e",
    "SESS4388544b24c700e52909809ea47e4e67": "GYXOfpY1SCgbiv5TFvvwd6YKhJNtW3_QSRV4lxCB0tA",
    "TS01b74e5f": "01152819cdf70de044b719dd1ca56a8f8403450f632bd2c1a5101db2e314aba89e0129c9e71032e518c069463d1bed8c8135313a52",
    "TS01b0d53e": "01152819cdc236c68b588418cd6ffe68b7eab51a7e4061d98cfe01a5134eba3cf0f3ac0141567db549f50b4916ccdb80450a9f764d3699ca06a23f6c70b8fc7aa7561e5396",
}

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL,  # Refer to the base URL or the previous page if tracking
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0, i",
}


def extract_publication_data(html_content):
    """
    Extracts publication data from the provided HTML content using BeautifulSoup.
    (This function is largely the same as your original, but cleaned up slightly)

    Args:
        html_content: A string containing the HTML source code of the search results page.

    Returns:
        A list of dictionaries, where each dictionary represents a publication
        entry and contains the extracted information. Returns an empty list
        if no results are found or parsing fails.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    publications = []

    # Find all the main search result containers
    results = soup.select("div.ubl-resultrow.islandora-solr-search-result")

    if not results:
        # No results found on this page
        return []

    for result in results:
        item_data = {}

        # Extract ID from the link's href attribute
        id_link = result.select_one('a[href^="/handle/1887/"]')
        if id_link:
            href = id_link.get("href")
            match = re.search(r"/1887/(\d+)", href)
            item_data["id"] = match.group(1) if match else None
        else:
            item_data["id"] = None

        # Extract Title
        title_tag = result.select_one("dd.mods-titleinfo-title-custom-ms a")
        item_data["title"] = title_tag.text.strip() if title_tag else None

        # Extract Author(s) and Year
        author_dd = result.select_one("dd.mods-name-authorrole-namepart-custom-ms")
        if author_dd:
            year_span = author_dd.select_one(
                "span.mods-origininfo-encoding-w3cdtf-keyDate-yes-dateIssued-year-custom-ms"
            )
            year_text = year_span.text.strip() if year_span else ""

            full_author_text = author_dd.text.strip()

            # Remove the year text from the full text to get authors
            if year_text:
                # Use regex to remove the year and potentially trailing semicolon
                author_text = re.sub(
                    r"\s*" + re.escape(year_text) + r"\s*$", "", full_author_text
                )
                author_text = re.sub(
                    r";\s*$", "", author_text
                ).strip()  # Remove potential trailing semicolon
            else:
                author_text = full_author_text

            item_data["author"] = author_text if author_text else None
            item_data["year"] = year_text if year_text else None
        else:
            item_data["author"] = None
            item_data["year"] = None

        # Extract Resource Type
        resource_type_dd = result.select_one("dd.mods-genre-authority-local-ms")
        item_data["resource_type"] = (
            resource_type_dd.text.strip() if resource_type_dd else None
        )

        # Extract Availability
        availability_dd = result.select_one(
            "dd.mods-genre-authority-local-ms.ubl-embargo"
        )
        item_data["availability"] = (
            availability_dd.text.strip() if availability_dd else None
        )

        # Extract Abstract
        # The abstract content is usually inside a span with class 'toggle-wrapper'
        # and the full text is in the last span child within that wrapper.
        abstract_span = result.select_one(
            "dd.mods-abstract-ms span.toggle-wrapper span"
        )  # Check for the wrapper content
        if abstract_span:
            # Find the last span containing the full abstract text
            full_abstract_span = (
                abstract_span.find_parent("span").select_one("span:last-of-type")
                if abstract_span.find_parent("span")
                else None
            )

            if full_abstract_span:
                # Extract text content, preserving newlines from <br> tags
                abstract_content_parts = []
                for content in full_abstract_span.contents:
                    if isinstance(content, str):
                        abstract_content_parts.append(content.strip())
                    elif content.name == "br":
                        abstract_content_parts.append("\n")
                item_data["abstract"] = "".join(abstract_content_parts).strip()
            else:
                # Fallback: if toggle-wrapper structure is different, get text from the dd directly
                abstract_dd_direct = result.select_one("dd.mods-abstract-ms")
                if abstract_dd_direct and not abstract_dd_direct.select_one(
                    ".ubl-embargo"
                ):
                    item_data["abstract"] = abstract_dd_direct.text.strip()
                else:
                    item_data["abstract"] = None
        else:
            # Fallback: if no toggle-wrapper found, get text from the dd directly
            abstract_dd_direct = result.select_one("dd.mods-abstract-ms")
            # Ensure it's not the availability field again
            if abstract_dd_direct and not abstract_dd_direct.select_one(".ubl-embargo"):
                item_data["abstract"] = abstract_dd_direct.text.strip()
            else:
                item_data["abstract"] = None

        publications.append(item_data)

    return publications


def get_total_pages(base_url, cookies, headers, timeout):
    """
    Fetches the first page to determine the total number of pages.
    """
    print("Fetching page 1 to determine total pages...")
    url = f"{base_url}&page=1"
    try:
        response = requests.get(url, cookies=cookies, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for bad status codes
        soup = BeautifulSoup(response.text, "html.parser")

        # Look for the pager element
        pager_ul = soup.select_one("ul.pager")

        if not pager_ul:
            # If no pager is found, assume there's only 1 page
            print("No pager found. Assuming 1 page.")
            return 1

        # Find all links within the pager and extract page numbers
        page_numbers = set()
        for link in pager_ul.select("li a"):
            href = link.get("href")
            if href:
                # Use regex to find the page parameter
                match = re.search(r"[?&]page=(\d+)", href)
                if match:
                    try:
                        page_num = int(match.group(1))
                        page_numbers.add(page_num)
                    except ValueError:
                        continue  # Ignore links with non-numeric page params

        if not page_numbers:
            # Should not happen if pager_ul is found, but safety check
            print("Pager found, but no page number links found. Assuming 1 page.")
            return 1

        total_pages = max(page_numbers)
        print(f"Found {total_pages} total pages.")
        return total_pages

    except requests.exceptions.RequestException as e:
        # print(f"Error fetching page 1: {e}")
        return 0  # Indicate failure to get total pages
    except Exception as e:
        print(f"Error parsing page 1 for total pages: {e}")
        return 0


def fetch_and_parse_page(page_num, base_url, cookies, headers, timeout):
    """
    Fetches a single page and extracts data. Designed for concurrent execution.
    """
    url = f"{base_url}&page={page_num}"
    # print(f"Fetching page {page_num}...")
    try:
        # Optional: add a small random delay
        # time.sleep(random.uniform(0.1, 0.5))

        response = requests.get(url, cookies=cookies, headers=headers, timeout=timeout)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        page_data = extract_publication_data(response.text)

        # print(f"Finished page {page_num}. Found {len(page_data)} items.")
        return page_data

    except requests.exceptions.Timeout:
        print(f"Request timed out for page {page_num}.")
        return []  # Return empty list on failure
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_num}: {e}")
        return []  # Return empty list on failure
    except Exception as e:
        print(f"Error processing page {page_num}: {e}")
        return []  # Return empty list on failure


# --- Main Script ---
if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    # 1. Get total number of pages
    # Temporarily remove the page=N from the base URL for the initial check
    base_url_no_page = BASE_URL.split("&page=")[0] if "&page=" in BASE_URL else BASE_URL
    total_pages = get_total_pages(base_url_no_page, cookies, headers, REQUEST_TIMEOUT)

    if total_pages <= 0:
        print("Could not determine total pages or no results found. Exiting.")
    else:
        all_publications_data = []
        pages_to_scrape = range(1, total_pages + 1)  # Pages are 1-indexed

        # Create a progress bar
        pbar = tqdm(total=total_pages, desc="Pages scraped", unit="page")
        completed_pages = 0

        # Store processing times for ETA calculation
        processing_times = []

        # 2. Use ThreadPoolExecutor to fetch pages concurrently
        print(f"Starting scraping with {MAX_WORKERS} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all page fetching tasks
            # We use a dict to map futures back to their page number for logging
            future_to_page = {
                executor.submit(
                    fetch_and_parse_page,
                    page_num,
                    base_url_no_page,
                    cookies,
                    headers,
                    REQUEST_TIMEOUT,
                ): page_num
                for page_num in pages_to_scrape
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                page_start_time = time.time()
                try:
                    page_data = (
                        future.result()
                    )  # This gets the return value from fetch_and_parse_page
                    all_publications_data.extend(
                        page_data
                    )  # Add data from this page to the main list

                    # Update progress
                    completed_pages += 1
                    pbar.update(1)

                    # Calculate page processing time
                    page_time = time.time() - page_start_time
                    processing_times.append(page_time)

                    # Calculate and display ETA
                    if processing_times:
                        avg_time_per_page = sum(processing_times) / len(
                            processing_times
                        )
                        pages_remaining = total_pages - completed_pages
                        eta_seconds = (
                            avg_time_per_page
                            * pages_remaining
                            / min(
                                MAX_WORKERS,
                                pages_remaining if pages_remaining > 0 else 1,
                            )
                        )
                        eta_formatted = str(
                            datetime.timedelta(seconds=int(eta_seconds))
                        )
                        pbar.set_postfix_str(f"ETA: {eta_formatted}")

                except Exception as exc:
                    # This catches exceptions that happened *inside* fetch_and_parse_page
                    # (though that function already has internal error handling and returns [],
                    # this is a safety net for unexpected issues with the Future itself)
                    print(f"Page {page_num} generated an unhandled exception: {exc}")
                    pbar.update(1)  # Still update progress even if there was an error

            pbar.close()

        # Calculate total time elapsed
        total_time = time.time() - start_time
        total_time_formatted = str(datetime.timedelta(seconds=int(total_time)))
        print(f"\nScraping completed in {total_time_formatted}")

        # 3. Create a pandas DataFrame
        print("\nCreating DataFrame...")
        df = pd.DataFrame(all_publications_data)

        # 4. Display or save the DataFrame
        print(f"\nTotal items scraped: {len(df)}")
        print("\nFirst 5 rows of the DataFrame:")
        print(df.head())

        print("\nDataFrame Info:")
        df.info()

        # Example: Save to CSV
        df.to_csv("./data/Faculty_of_LUMC.csv", index=False)
        print("\nData saved to Faculty_of_LUMC.csv")

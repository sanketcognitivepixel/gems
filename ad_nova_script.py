from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re
from datetime import datetime
from urllib.parse import unquote, urlparse
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import random  # [Human behavior: for random delays]
from urllib.parse import urlparse, parse_qs
import json
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os
import sys
from functools import partial

# ============== CONFIGURATION =====================
load_dotenv()

# The base URL of your FastAPI application
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

# Platform identification mapping (unchanged)
PLATFORM_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yW/r/TP7nCDju1B-.png", "0px -1171px"): "Facebook",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yj/r/0dseWS3_nMM.png", "-34px -353px"): "Instagram",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-16px -528px"): "Audience Network",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-29px -528px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yW/r/TP7nCDju1B-.png", "0px -1184px"): "Thread"
}
CATEGORY_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-65px -557px"): "Employment",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "0px -544px"): "Housing",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-13px -557px"): "Financial products and services",
}


def sanitize_payload(payload):
    """
    Iterates through ads_data and replaces None in specific string fields with an empty string.
    This prevents validation errors if the API is strict.
    """
    if "ads_data" in payload and isinstance(payload["ads_data"], dict):
        for ad_id, ad_data in payload["ads_data"].items():
            # If thumbnail_url is None, set it to an empty string
            if ad_data.get("thumbnail_url") is None:
                ad_data["thumbnail_url"] = ""
            
            # If total_active_time is None, set it to an empty string
            if ad_data.get("total_active_time") is None:
                ad_data["total_active_time"] = ""
                
            # You can add other fields here if needed
            # For example, cta_button_text
            if ad_data.get("cta_button_text") is None:
                ad_data["cta_button_text"] = ""

    return payload


def cleanup_existing_data():
    """
    Delete existing data for the current date before importing new data.
    Returns True if cleanup was successful, False otherwise.
    """
    try:
        # Get current date in YYYY-MM-DD format
        full_api_url = API_BASE_URL

        current_date = datetime.now().strftime("%Y-%m-%d")
        cleanup_url = f"{full_api_url}/api/cleanup-ads?delete_date={current_date}"
        
        print(f"\nCleaning up existing data for date: {current_date}")
        response = requests.delete(cleanup_url, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        print(f"Cleanup completed: {result.get('message')}")
        print(f"Deleted records: {result.get('deleted_records')}")
        return True
        
    except requests.exceptions.HTTPError as http_err:
        print(f"Cleanup failed with HTTP error: {http_err}")
        if 'response' in locals():
            print(f"Status Code: {response.status_code}")
            try:
                print("Error Details:", response.json())
            except json.JSONDecodeError:
                print("Response Body:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during cleanup: {req_err}")
    
    return False


def send_data_to_api(api_url, payload):
    """
    Sends the processed ad data to the FastAPI endpoint.
    First cleans up existing data for the current date.
    """
    try:
        print("\nSending data to API...")
        # Sanitize the payload before sending
        sanitized_payload = sanitize_payload(payload)
        
        response = requests.post(api_url, json=sanitized_payload, timeout=60) # Increased timeout
        response.raise_for_status()

        api_response = response.json()
        print("API Response:")
        print(f"  Status: {api_response.get('status')}")
        print(f"  Message: {api_response.get('message')}")
        print(f"  Total Processed by API: {api_response.get('total_processed')}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Status Code: {response.status_code}")
        # Use response.json() if possible for cleaner error output from FastAPI
        try:
            print("Error Details:", response.json())
        except json.JSONDecodeError:
            print("Response Body:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")





def extract_page_id(url):
    # Extract the page ID from the URL for output file naming
    match = re.search(r'view_all_page_id=(\d+)', url)
    return match.group(1) if match else 'output'

def scrape_ads(url, driver_path):
    print(f"\nNavigating to {url}...")
    driver = None  # Initialize driver to None
    start_time = time.time()
    competitor_name_for_logging = urlparse(url).query # Fallback name for logging

    # --- Robust Main Execution Block ---
    try:
        print(f"\n[START] Navigating to {url}...")

        # --- Driver Setup ---
        import tempfile
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 10)
        
        # Set a timeout for the initial page load to prevent hangs
        driver.set_page_load_timeout(60)
        driver.get(url)

        # ... (The rest of your scraping logic goes inside this try block) ...
        # (I've copied your logic below, with improved logging)
        
        print(f"[{url[-30:]}] Waiting for initial ad content to load...")
        initial_content_locator = (By.CSS_SELECTOR, 'div[class="xrvj5dj x18m771g x1p5oq8j xp48ta0 x18d9i69 xtssl2i xtqikln x1na6gtj x1jr1mh3 x15h0gye x7sq92a xlxr9qa"]')
        wait.until(EC.presence_of_element_located(initial_content_locator))
        print(f"[{url[-30:]}] ✅ Initial content loaded.")

        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay

        #fetch page id
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        current_page_id = query_params.get("view_all_page_id", [None])[0]
        
        # Use page_id or a snippet of the URL for logging prefix
        log_prefix = f"PageID: {current_page_id}" if current_page_id else f"Keyword: {query_params.get('q', ['unknown'])[0]}"
        print(f"[{log_prefix}] Extracted page_id: {current_page_id}")

        
        print("Extracted page_id:", current_page_id)


        # Robust selectors (based on stable attributes)
        search_box_selectors = [
            (By.CSS_SELECTOR, 'input[placeholder="Search by keyword or advertiser"][type="search"]'),
            (By.XPATH, '//input[@type="search" and contains(@placeholder, "Search")]')
        ]
        competitor_name_from_search_box = None
        for by, value in search_box_selectors:
            try:
                element = driver.find_element(by, value)
                competitor_name_from_search_box = element.get_attribute("value")
                break
            except NoSuchElementException:
                continue
        
        
        competitor_name_for_logging = competitor_name_from_search_box or log_prefix
        print(f"[{competitor_name_for_logging}] Competitor name from search box: {competitor_name_from_search_box}")
        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay


        # Try to extract the div with "results" text
        try:
            element = driver.find_element(By.XPATH, "(//div[contains(text(), 'results')])[1]")
            value_text = element.text.strip()
            print(f"Found count text: {value_text}")
        except NoSuchElementException:
            print("Ad count element not found.")

        # Parse the number from the string
        if value_text:
            # Remove non-numeric/non-letter parts except K/M/., for example: "~490 results" → "490"
            cleaned = re.sub(r"[^\dKMkm.,]", "", value_text.upper())

            # Now extract number and suffix
            match = re.match(r"([\d.,]+)([KM]?)", cleaned)
            if match:
                number_str, suffix = match.groups()
                number = float(number_str.replace(",", ""))
                if suffix == "K":
                    total_ad_count_of_page = int(number * 1_000)
                elif suffix == "M":
                    total_ad_count_of_page = int(number * 1_000_000)
                else:
                    total_ad_count_of_page = int(number)
                print(f"Normalized count: {total_ad_count_of_page}")
            else:
                print("Could not parse ad count text.")
                
        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay
        # The rest of the script from scroll to JSON save and driver.quit will go here, but will use the local driver variable
        # ... (this will be filled in the next chunk)
        # At the end, return nothing (or could return stats if needed)
        # return driver, start_time

        # Target XPaths for end-of-list marker (unchanged)

        target_xpaths = [
            "/html/body/div[1]/div/div/div/div/div/div/div[1]/div/div/div/div[5]/div[2]/div[9]/div[3]/div[2]/div",
            "/html/body/div[1]/div/div/div/div/div/div[1]/div/div/div/div[6]/div[2]/div[9]/div[3]/div[2]/div"
        ]

        print("Starting scroll loop to load all ads...")
        scroll_count = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_pause_time = 0.7 # Reduced pause time after scroll
        max_scroll_attempts_at_bottom = 3 # How many times to scroll after height stops changing, just in case
        attempts_at_bottom = 0


        # scrolling part
        while attempts_at_bottom < max_scroll_attempts_at_bottom:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            scroll_count += 1
            
            # --- Optimization: Shorter, dynamic wait ---
            time.sleep(scroll_pause_time) # Wait briefly for page to load

            print("Hmm, loading...")

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            element_found = False
            # Let's only check for the end element when the height hasn't changed
            if new_height == last_height:
                for xpath in target_xpaths:
                    try:
                        # Use a very short wait for the end element check
                        WebDriverWait(driver, 0.5).until(EC.presence_of_element_located((By.XPATH, xpath)))
                        print(f"✅ End-of-list element found using XPath: {xpath}")
                        element_found = True
                        break
                    except (NoSuchElementException, TimeoutException):
                        continue

            if element_found:
                print(f"✅ End-of-list element found after {scroll_count} scrolls. Stopping scroll.")
                break

            if new_height == last_height:
                attempts_at_bottom += 1
                print(f"Scroll height ({new_height}) hasn't changed. Attempt {attempts_at_bottom}/{max_scroll_attempts_at_bottom} at bottom...")
            else:
                attempts_at_bottom = 0 # Reset counter if height changed
                print(f"Scrolled {scroll_count} time(s). New height: {new_height}")

            last_height = new_height

            # Optional safety break: Prevent infinite loops
            if scroll_count > 500: # Adjust limit as needed
                print("⚠️ Reached maximum scroll limit (500). Stopping scroll.")
                break

            time.sleep(random.uniform(0.5, 1.5))  # Human-like delay

        if not element_found and attempts_at_bottom >= max_scroll_attempts_at_bottom:
            print("🏁 Reached bottom of page (height stabilized).")

        scroll_time = time.time()
        print(f"Scrolling finished in {scroll_time - start_time:.2f} seconds.")

        print("Waiting briefly for final elements to render...")
        time.sleep(1) # Short pause just in case rendering is slightly delayed

        # Count divs with the first class (unchanged selector logic)
        target_class_1 = "x6s0dn4 x78zum5 xdt5ytf xl56j7k x1n2onr6 x1ja2u2z x19gl646 xbumo9q"
        try:
            divs_1 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_1}"]')
            print(f"Total <div> elements with target class 1: {len(divs_1)}")
        except Exception as e:
            print(f"Error finding elements with target class 1: {e}")
            divs_1 = []

        # Count divs with the second class (unchanged selector logic)
        # target_class_2 = "xrvj5dj x18m771g x1p5oq8j xbxaen2 x18d9i69 x1u72gb5 xtqikln x1na6gtj x1jr1mh3 xm39877 x7sq92a xxy4fzi"
        target_class_2 = "xrvj5dj x18m771g x1p5oq8j xp48ta0 x18d9i69 xtssl2i xtqikln x1na6gtj x1jr1mh3 x15h0gye x7sq92a xlxr9qa"
        try:
            divs_2 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_2}"]')
            print(f"Total <div> elements (ad groups) with target class 2: {len(divs_2)}")
        except Exception as e:
            print(f"Error finding elements with target class 2: {e}")
            divs_2 = []


        # Dictionary to store all ads data (unchanged)
        ads_data = {}

        # For each target_class_2 div, count xh8yej3 children and process them (unchanged logic, potential speedup from faster page load/scrolling)
        print("\nProcessing ads...")
        total_processed = 0
        total_child_ads_found = 0

        # --- Optimization: Process elements already found, minimize waits inside loop ---
        for i, div in enumerate(divs_2, 1):
            print("in loop")
            try:
                child_divs = div.find_elements(By.XPATH, './div[contains(@class, "xh8yej3")]')
                num_children = len(child_divs)
                print('num_children', num_children)
                total_child_ads_found += num_children

                # Process each xh8yej3 child
                for j, child_div in enumerate(child_divs, 1):
                    current_ad_id_for_logging = f"Group {i}, Ad {j}"
                    library_id = None # Initialize library_id for potential error logging
                    try:
                        main_container = child_div.find_element(By.XPATH, './/div[contains(@class, "x78zum5 xdt5ytf x2lwn1j xeuugli")]')

                        # Extract Library ID
                        library_id_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x1rg5ohu x67bb7w")]/span[contains(text(), "Library ID:")]')
                        library_id = library_id_element.text.replace("Library ID: ", "").strip()
                        current_ad_id_for_logging = library_id # Update logging ID once found

                        # if library_id in ads_data:
                        #     # print(f"Skipping duplicate Library ID: {library_id}")
                        #     continue

                        # Initialize ad data with library_id
                        ad_data = {"library_id": library_id}

                        # Extract started_running, total_active_time
                        try:
                            started_running_element = main_container.find_element(By.XPATH, './/span[contains(text(), "Started running on")]')
                            full_text = started_running_element.text.strip()
                            
                            # Extract the started running date
                            started_running_match = re.search(r'Started running on (.*?)(?:·|$)', full_text)
                            if started_running_match:
                                started_running_text = started_running_match.group(1).strip()
                                # Try parsing with comma first, then without if that fails
                                try:
                                    started_running_date = datetime.strptime(started_running_text, "%b %d, %Y").strftime("%Y-%m-%d")
                                except ValueError:
                                    started_running_date = datetime.strptime(started_running_text, "%d %b %Y").strftime("%Y-%m-%d")
                                ad_data["started_running"] = started_running_date
                            else:
                                ad_data["started_running"] = None
                            
                            # Extract the total active time if present
                            active_time_match = re.search(r'Total active time\s+(.+?)(?:$|\s*·)', full_text)
                            if active_time_match:
                                active_time = active_time_match.group(1).strip()
                                ad_data["total_active_time"] = active_time
                            else:
                                ad_data["total_active_time"] = None
                                
                        except NoSuchElementException:
                            # print(f"Started running date not found for ad {current_ad_id_for_logging}")
                            ad_data["started_running"] = None
                            ad_data["total_active_time"] = None
                        except Exception as e:
                            print(f"⚠️ Error parsing started running date for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["started_running"] = None
                            ad_data["total_active_time"] = None

                        # Extract Platforms icons
                        platforms_data = []
                        try:
                            platforms_div = main_container.find_element(By.XPATH, './/span[contains(text(), "Platforms")]/following-sibling::div[1]') # Use [1] for immediate sibling
                            platform_icons = platforms_div.find_elements(By.XPATH, './/div[contains(@class, "xtwfq29")]')

                            for icon in platform_icons:
                                try:
                                    style = icon.get_attribute("style")
                                    if not style: continue # Skip if no style attribute
                                    mask_image_match = re.search(r'mask-image: url\("([^"]+)"\)', style)
                                    mask_pos_match = re.search(r'mask-position: ([^;]+)', style)
                                    mask_image = mask_image_match.group(1) if mask_image_match else None
                                    mask_position = mask_pos_match.group(1).strip() if mask_pos_match else None # Added strip()

                                    # Identify platform name
                                    platform_name = PLATFORM_MAPPING.get((mask_image, mask_position)) # More direct lookup

                                    # platforms_data.append({
                                    #     # "style": style, # Usually not needed in final data
                                    #     "mask_image": mask_image,
                                    #     "mask_position": mask_position,
                                    #     "platform_name": platform_name if platform_name else "Unknown"
                                    # })
                                    platforms_data.append(
                                        # "style": style, # Usually not needed in final data
                                        platform_name 
                                    )
                                except Exception as e:
                                    # print(f"Could not process a platform icon for ad {current_ad_id_for_logging}: {str(e)}")
                                    continue
                        except NoSuchElementException:
                            # print(f"Platforms section not found for ad {current_ad_id_for_logging}")
                            pass # okay if this section is missing
                        except Exception as e:
                            print(f"Error extracting platforms for ad {current_ad_id_for_logging}: {str(e)}")

                        ad_data["platforms"] = platforms_data

                        # Extract Categories icon
                        category_data = []
                        try:
                            # Find the Categories span first
                            categories_span = main_container.find_element(By.XPATH, './/span[contains(text(), "Categories")]')
                            
                            # Find all sibling divs with class x1rg5ohu x67bb7w that come after the Categories span
                            category_divs = categories_span.find_elements(By.XPATH, './following-sibling::div[contains(@class, "x1rg5ohu") and contains(@class, "x67bb7w")]')
                            
                            for category_div in category_divs:
                                try:
                                    # Find the icon div within each category div
                                    icon_div = category_div.find_element(By.XPATH, './/div[contains(@class, "xtwfq29")]')
                                    style = icon_div.get_attribute("style")
                                    
                                    if style:
                                        mask_image_match = re.search(r'mask-image: url\("([^"]+)"\)', style)
                                        mask_pos_match = re.search(r'mask-position: ([^;]+)', style)
                                        mask_image = mask_image_match.group(1) if mask_image_match else None
                                        mask_position = mask_pos_match.group(1).strip() if mask_pos_match else None
                                        
                                        # Identify category name from mapping
                                        category_name = CATEGORY_MAPPING.get((mask_image, mask_position), "Unknown")
                                        
                                        # category_data.append({
                                        #     "mask_image": mask_image,
                                        #     "mask_position": mask_position,
                                        #     "category_name": category_name
                                        # })
                                        category_data.append(
                                            category_name
                                        )
                                except Exception as e:
                                    print(f"Could not process a category icon: {str(e)}")
                                    continue
                                    
                        except NoSuchElementException:
                            pass  # No categories section found
                        except Exception as e:
                            print(f"Error extracting categories: {str(e)}")

                        ad_data["categories"] = category_data
                        
                        # Extract Ads count
                        try:
                            # Adjusted XPath to be more specific to the 'N ads use this creative and text.' structure
                            ads_count_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x6s0dn4 x78zum5 xsag5q8")]//strong')
                            ads_count = ads_count_element.text.strip() # Should just be the number
                            number_match = re.search(r'(\d+)', ads_count)
                            if number_match:
                                ads_count = number_match.group(1)  # This will be just "4"
                            else:
                                ads_count = None
                                
                            ad_data["ads_count"] = ads_count

                        except NoSuchElementException:
                            ad_data["ads_count"] = None
                        except Exception as e:
                            print(f"Error extracting ads count for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["ads_count"] = None

                        # Add to main dictionary with library_id as key
                        ads_data[library_id] = ad_data
                        total_processed += 1

                        # Extract Ad Text Content
                        try:
                            # Find the parent div containing the text first, more reliable
                            ad_text_container = child_div.find_element(By.XPATH, './/div[@data-ad-preview="message" or contains(@style, "white-space: pre-wrap")]')
                            # Get all text within, handles cases with multiple spans or line breaks better
                            ad_data["ad_text"] = ad_text_container.text.strip()
                        except NoSuchElementException:
                            # print(f"Ad text not found for ad {current_ad_id_for_logging}")
                            ad_data["ad_text"] = None
                        except Exception as e:
                            print(f"Error extracting ad text for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["ad_text"] = None

                        # extract media
                        try:
                            # First find the xh8yej3 div inside child_div if we're not already looking at it
                            # xh8yej3_div = child_div
                            # if "xh8yej3" not in child_div.get_attribute("class"):
                            
                            # Try to find the link container first as it often contains both media and CTA
                            link_container = child_div.find_element(By.XPATH, './/a[contains(@class, "x1hl2dhg") and contains(@class, "x1lku1pv")]')
                            
                            # Extract and store the link URL
                            link_url = link_container.get_attribute('href')
                            decoded_url = unquote(link_url)
                            
                            # Parse the URL to get the 'u' parameter value
                            parsed_url = urlparse(decoded_url)
                            query_params = parsed_url.query
                            if 'u=' in query_params:
                                # Get the full URL from the u parameter (properly decoded)
                                actual_url = unquote(query_params.split('u=')[1].split('&')[0])
                            else:
                                # Try another method if u= isn't in the query params
                                actual_url = unquote(decoded_url.split('u=')[1].split('&')[0]) if 'u=' in decoded_url else decoded_url
                            
                            ad_data["destination_url"] = actual_url if actual_url else None

                            # Extract media from this link container
                            ad_data["media_type"] = None
                            ad_data["media_url"] = None
                            ad_data["thumbnail_url"] = None
                            
                            # Check for video within the link container
                            try:
                                video_element = child_div.find_element(By.XPATH, './/video')
                                media_url = video_element.get_attribute('src')
                                if media_url: # Ensure src is not empty
                                    ad_data["media_type"] = "video"
                                    ad_data["media_url"] = media_url
                                poster_url = video_element.get_attribute('poster')
                                if poster_url:
                                    ad_data["thumbnail_url"] = poster_url
                            except NoSuchElementException:
                                # If no video, try image with more specific targeting
                                try:
                                    img_element = link_container.find_element(By.XPATH, './/img[contains(@class, "x168nmei") or contains(@class, "_8nqq")]')
                                    media_url = img_element.get_attribute('src')
                                    if media_url:
                                        ad_data["media_type"] = "image"
                                        ad_data["media_url"] = media_url
                                except NoSuchElementException:
                                    # Fallback to any image within the link container
                                    try:
                                        img_element = link_container.find_element(By.XPATH, './/img')
                                        media_url = img_element.get_attribute('src')
                                        if media_url:
                                            ad_data["media_type"] = "image"
                                            ad_data["media_url"] = media_url
                                    except NoSuchElementException:
                                        pass  # No media found
                            
                        except Exception as e:
                            print(f"Error extracting media or CTA for ad {current_ad_id_for_logging}: {str(e)}")
                            # Initialize with None if not already set
                            if "media_type" not in ad_data:
                                ad_data["media_type"] = None
                            if "media_url" not in ad_data:
                                ad_data["media_url"] = None
                            if "thumbnail_url" not in ad_data:
                                ad_data["thumbnail_url"] = None

                        except Exception as e:
                            print(f"Error extracting media for ad {current_ad_id_for_logging}: {str(e)}")

                        try:
                            # ① container that wraps headline + CTA area
                            cta_container = child_div.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x6s0dn4 x2izyaf x78zum5 x1qughib '
                                'x15mokao x1ga7v0g xde0f50 x15x8krk xexx8yu xf159sx xwib8y2 xmzvs34")]'
                            )

                            # ② sub‑container that holds headline + legal copy
                            head_line_container = cta_container.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x1iyjqo2 x2fvf9 x6ikm8r x10wlt62 xt0b8zv")]'
                            )

                            # ③ CTA button div
                            cta_div = cta_container.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x2lah0s")]'
                            )

                            # -- CTA TEXT (existing)
                            cta_text_element = cta_div.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x8t9es0 x1fvot60 xxio538 x1heor9g '
                                'xuxw1ft x6ikm8r x10wlt62 xlyipyv x1h4wwuj x1pd3egz xeuugli")]'
                            )
                            ad_data["cta_button_text"] = cta_text_element.text.strip()

                            # -- HEADLINE TEXT (NEW) -----------------------------------------------
                            try:
                                headline_element = head_line_container.find_element(
                                    By.XPATH,
                                    './/div[contains(@class, "x6ikm8r x10wlt62 xlyipyv x1mcwxda")]'
                                )
                                ad_data["headline_text"] = headline_element.text.strip()
                            except NoSuchElementException:
                                ad_data["headline_text"] = None
                            # ----------------------------------------------------------------------

                        except NoSuchElementException:
                            # No CTA container found ⇒ keep previous behaviour
                            ad_data["cta_button_text"] = None
                            ad_data["headline_text"] = None
                        except Exception as e:
                            print(f"Error extracting CTA or headline text for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["cta_button_text"] = None
                            ad_data["headline_text"] = None
                        # Add to main dictionary with library_id as key
                        ads_data[library_id] = ad_data
                        total_processed += 1
                        # Reduce console noise: print progress periodically instead of every ad
                        if total_processed % 50 == 0:
                            print(f"Processed {total_processed}/{total_child_ads_found} ads...")

                    except NoSuchElementException as e:
                        # This might happen if the structure is unexpected, often failure to find library ID
                        print(f"Critical element missing for ad {current_ad_id_for_logging}, skipping. Error: {e.msg}")
                        continue # Skip this child_div entirely if critical info (like ID) is missing
                    except Exception as e:
                        print(f"Unexpected error processing ad {current_ad_id_for_logging}: {str(e)}")
                        continue # Skip this child_div on unexpected errors

            except Exception as e:
                print(f"Error finding or processing xh8yej3 children for div group {i}: {str(e)}")
                continue

        processing_time = time.time()
        print(f"\nData extraction finished in {processing_time - scroll_time:.2f} seconds.")

        # Create final output with total count (unchanged)
        final_output = {
            "competitor_name": "Example Competitor",
            "no_of_ads": 100,
            "page_id": current_page_id,
            "total_ads_found": 95,
            "total_ads_processed": 95,
            "ads_data": {"ad1": {}, "ad2": {}}, # dummy data
            "page_link": url,
        }
        # [END OF YOUR SCRAPING LOGIC]

        # --- FIX #2: Unique JSON Filename ---
        # Use the page ID or a timestamp to create a unique filename
        output_id = current_page_id if current_page_id else f"keyword_{int(time.time())}"
        output_file = f"ad_data_{output_id}.json"
        
        try:
            with open(output_file, "w", encoding='utf-8') as f:
                json.dump(final_output, f, indent=4, ensure_ascii=False)
            print(f"[{competitor_name_for_logging}] Successfully processed data for {len(final_output['ads_data'])} unique ads.")
            print(f"[{competitor_name_for_logging}] Data saved to {output_file}")
        except Exception as e:
            print(f"[{competitor_name_for_logging}] ⚠️ Error saving data to JSON file: {e}")

        # --- API Submission ---
        AD_DETAILS_ENDPOINT = "/api/ad-details"
        full_api_url = f"{API_BASE_URL}{AD_DETAILS_ENDPOINT}"
        if final_output.get("ads_data"):
            send_data_to_api(full_api_url, final_output)
        else:
            print(f"[{competitor_name_for_logging}] No ad data to send to the API.")

        total_time = time.time()
        print(f"[COMPLETE] Total script execution time for {competitor_name_for_logging}: {total_time - start_time:.2f} seconds.")

    except Exception as e:
        # --- FIX #3: Catch All Other Errors ---
        # This will catch timeouts, crashes, or any other Python error in the thread.
        print(f"\n" + "="*60)
        print(f"FATAL ERROR while processing URL: {url}")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        import traceback
        traceback.print_exc()
        print("="*60 + "\n")

    finally:
        # --- FIX #1: Guaranteed Cleanup ---
        # This block will run ALWAYS, even if the 'try' block crashes.
        if driver:
            print(f"[CLEANUP] Quitting browser for {competitor_name_for_logging}.")
            driver.quit()


def fetch_competitors_urls(api_url=f'{API_BASE_URL}/api/get_competitors_url'):
    """
    Fetches the list of competitor URLs from the API
    """
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return [item['page_link'] for item in data] if data else []
    except Exception as e:
        print(f"Error fetching competitor URLs: {e}")
        return []

def process_urls_in_parallel(urls):
    """
    Process a list of URLs using ThreadPoolExecutor
    Each thread has 2 worker pools (Right now if total link are 4 , then its devided 4 into 2 links list. and open 4 browser. Each thread has 2 worker. (each worker will open one browser))
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(scrape_ads, urls)

def split_list_into_two(lst):
    """
    Split a list into two parts. If the list has an odd length,
    the first part will have one more element than the second.
    """
    mid = (len(lst) + 1) // 2
    return lst[:mid], lst[mid:]

def run_parallel_scraping():
    """
    Fetches competitor URLs and runs the scraping process in a controlled parallel manner.

    This function is designed to be robust for CI/CD environments like GitHub Actions.
    It performs the following steps:
    1. Fetches the list of URLs to be scraped from the API.
    2. Performs a pre-emptive cleanup of today's data to prevent duplicates.
       - If cleanup fails, the script will exit with an error status.
    3. Uses a ThreadPoolExecutor to run the `scrape_ads` function for multiple URLs
       concurrently, significantly speeding up the total execution time.
    4. The number of concurrent browsers is configurable via an environment variable.
    """
    # --- Step 1: Fetch URLs to Process ---
    print("\n" + "="*50)
    print("--- Step 1: Fetching Competitor URLs from API ---")
    print("="*50)
    
    urls = fetch_competitors_urls()
    
    if not urls:
        print("No URLs found to process. Exiting gracefully.")
        return
        
    print(f"Successfully fetched {len(urls)} URLs to process.")

    # --- Step 2: Pre-emptive Data Cleanup ---
    print("\n" + "="*50)
    print("--- Step 2: Cleaning up previous data for today ---")
    print("="*50)

    if not cleanup_existing_data():
        print("\nFATAL: Cleanup of existing data failed. Aborting the script to prevent data duplication.")
        # Exit with a non-zero status code to signal an error in CI/CD pipelines
        sys.exit(1)
    
    print("Cleanup successful. Proceeding to scrape new data.")

    # --- NEW: Pre-install the WebDriver ONCE ---
    print("\n" + "="*50)
    print("--- Pre-installing WebDriver ---")
    print("="*50)
    try:
        driver_executable_path = ChromeDriverManager().install()
        print(f"WebDriver cached successfully at: {driver_executable_path}")
    except Exception as e:
        print(f"\nFATAL: Failed to install Chrome Driver. Error: {e}")
        sys.exit(1)

    # --- Step 3: Scrape Ads in Parallel ---
    print("\n" + "="*50)
    print("--- Step 3: Starting Parallel Scraping ---")
    print("="*50)
    
    # Control the maximum number of concurrent browsers.
    # For a standard GitHub Actions runner (2-core CPU), 2 is a safe starting point.
    # This can be overridden by setting a `MAX_WORKERS` environment variable.
    try:
        max_workers = int(os.getenv("MAX_WORKERS", "2"))
    except ValueError:
        print("Warning: Invalid MAX_WORKERS environment variable. Defaulting to 2.")
        max_workers = 2
        
    print(f"Configured to run with a maximum of {max_workers} concurrent browsers.")
    
    start_time = time.time()
    
    # Use a single ThreadPoolExecutor to manage all scraping tasks directly.
    # This is much simpler and more direct than the previous nested-thread model.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # THE FIX: Create a partial function with the driver path already filled in
        scrape_task_with_driver = partial(scrape_ads, driver_path=driver_executable_path)
        
        # Map the list of URLs to this new partial function
        executor.map(scrape_task_with_driver, urls)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # --- Step 4: Completion Summary ---
    print("\n" + "="*50)
    print("--- Scraping Complete ---")
    print("="*50)
    print(f"All {len(urls)} URLs have been processed.")
    print(f"Total parallel execution time: {total_time:.2f} seconds (~{total_time / 60:.2f} minutes).")

if __name__ == "__main__":
    run_parallel_scraping()

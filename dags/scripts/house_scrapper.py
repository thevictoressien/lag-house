import csv
import io
import logging
import time
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dags.scripts.gcp_manager import GCSManager
from dags.scripts import config

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_session() -> requests.Session:
    """Create and configure a requests session with retries."""
    session = requests.Session()
    retry = Retry(total=15, connect=15, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_page(
    session: requests.Session, url: str, headers: Dict[str, str]
) -> Optional[requests.Response]:
    """Fetch a web page and return the raw response."""
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while making the request: {e}")
        return None


def extract_listing_data(listings: List[BeautifulSoup]) -> List[Dict[str, str]]:
    """Extract data from multiple property listings."""
    all_properties = list()

    for listing in listings:
        data = dict()
        some_soup = BeautifulSoup(listing.content, "lxml")
       
        data["location"] = some_soup.find("address").text.strip() if some_soup.find("address") else "N/A"
        
        price_element = some_soup.find('span', class_="price", itemprop='price')
        data['price'] = price_element['content'] if price_element else "N/A"
        
        currency_element = some_soup.find('span', class_="price", itemprop="priceCurrency")
        data['currency'] = currency_element['content'] if currency_element else "N/A"


        status = (None,)
        bedrooms = (None,)
        bathrooms = (None,)
        toilets = (None,)
        property_type = (None,)
        is_furnished = ("no",)
        is_serviced = ("no",)
        is_shared = "no"
        total_area = None
        covered_area = None

        other_headers = some_soup.find_all("td")
        for header in other_headers:
            text = header.get_text()
            if "Market Status:" in text:
                status = text.split(":")[1].strip()
                data["status"] = status
            elif "Bedrooms:" in text:
                bedrooms = text.split(":")[1].strip()
                data["bedrooms"] = bedrooms
            elif "Bathrooms:" in text:
                bathrooms = text.split(":")[1].strip()
                data["bathrooms"] = bathrooms
            elif "Toilets:" in text:
                toilets = text.split(":")[1].strip()
                data["toilets"] = toilets
            elif "Type:" in text:
                property_type = text.split(":")[1].strip()
                data["property_type"] = property_type
            elif "Servicing" in text:
                is_serviced = "yes"
                data["is_serviced"] = is_serviced
            elif "Furnishing:" in text:
                is_furnished = "yes"
                data["is_furnished"] = is_furnished
            elif "Sharing:" in text:
                is_shared = "yes"
                data["is_shared"] = is_shared
            elif 'Total Area:' in text:
                total_area = text.split(':')[1].strip()
                data['total_area'] = total_area
            elif 'Covered Area:' in text:
                covered_area = text.split(':')[1].strip()
                data['covered_area'] = covered_area

        all_properties.append(data)

    return all_properties


def upload_to_gcs(bucket_name: str, file_name: str, csv_content: str) -> None:
    """Upload CSV content to Google Cloud Storage."""
    gcs_client = GCSManager(project_id=config.PROJECT_ID)

    gcs_client.upload_file_from_string(
        bucket_name=bucket_name,
        buffer=csv_content,
        destination_blob_name=file_name,
        content_type="text/csv",
    )

    logging.info(f"Data uploaded to GCS bucket '{bucket_name}' as '{file_name}'.")


def process_chunk(session: requests.Session, headers: Dict[str, str], base_url: str, category: str, city: str, start_page: int, end_page: int) -> str:
    output = io.StringIO()
    csv_writer = csv.writer(output)
    header_row = [
        "location", "status", "bedrooms", "bathrooms", "toilets", "property_type",
        "is_furnished", "is_serviced", "is_shared", "total_area", "covered_area",
        "price", "currency",
    ]
    csv_writer.writerow(header_row)

    for page in range(start_page, end_page + 1):
        page_url = f"{base_url}/{category}/{city}?page={page}"
        logging.info(f"Fetching data from: {page_url}")
        response = fetch_page(session, page_url, headers)
        if not response:
            logging.warning(f"Failed to fetch page: {page_url}")
            continue

        soup = BeautifulSoup(response.content, "lxml")
        raw_links = soup.find_all("div", class_="row property-list")
        click_links = [
            link.find("div", class_="description hidden-xs")
            .find_all("a")[-1]
            .get("href")
            for link in raw_links
        ]

        listings = [requests.get(f"{base_url}{link}", timeout=30) for link in click_links]

        properties = extract_listing_data(listings)

        for property_data in properties:
            csv_writer.writerow([property_data.get(key, "") for key in header_row])
        
        logging.info(f"Processed {len(properties)} properties on page {page}")
        time.sleep(1)  # Add a small delay between pages

    output.seek(0)
    return output.getvalue()


def house_scrapper(
    bucket_name: str,
    file_name: str,
    base_url: str,
    category: str,
    city: str,
    start_page: int,
    end_page: int,
    chunk_size: int = 20
) -> None:
    """Scrape house listings and upload data to GCS as a CSV file."""
    session = create_session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
    }

    for chunk_start in range(start_page, end_page + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_page)
        logging.info(f"Processing chunk: pages {chunk_start} to {chunk_end}")
        
        csv_content = process_chunk(session, headers, base_url, category, city, chunk_start, chunk_end)
        
        chunk_file_name = f"{file_name.split('.')[0]}_{chunk_start}_{chunk_end}.csv"
        upload_to_gcs(bucket_name, chunk_file_name, csv_content)
        
        logging.info(f"Chunk {chunk_start} to {chunk_end} uploaded to GCS bucket '{bucket_name}' as '{chunk_file_name}'.")


def scrape_and_upload(**kwargs):
    bucket_name = kwargs.get('bucket_name')
    file_name = kwargs.get('file_name')
    base_url = kwargs.get('base_url')
    category = kwargs.get('category')
    city = kwargs.get('city')
    start_page = kwargs.get('start_page')
    end_page = kwargs.get('end_page')

    house_scrapper(bucket_name, file_name, base_url, category, city, start_page, end_page)



# def main():
#     parser = argparse.ArgumentParser(
#         description="Scrape house listings from real estate website and upload to Google Cloud Storage(GCS)."
#     )
#     parser.add_argument(
#         "--bucket_name", type=str, required=True, help="GCS bucket name."
#     )
#     parser.add_argument(
#         "--file_name",
#         type=str,
#         default="house_listings.csv",
#         help="Destination file name in GCS.",
#     )
#     parser.add_argument(
#         "--base_url", type=str, default=None, help="Base URL for scraping."
#     )
#     parser.add_argument(
#         "--category", type=str, default=None, help="Category for scraping."
#     )
#     parser.add_argument("--city", type=str, default=None, help="City for scraping.")
#     parser.add_argument(
#         "--start_page", type=int, default=1, help="Starting page number."
#     )
#     parser.add_argument("--end_page", type=int, default=10, help="Ending page number.")

#     args = parser.parse_args()

#     house_scrapper(
#         args.bucket_name,
#         args.file_name,
#         args.base_url,
#         args.category,
#         args.city,
#         args.start_page,
#         args.end_page,
#     )
#     print("Done.")


# if __name__ == "__main__":
#     main()

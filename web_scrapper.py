import asyncio
import csv
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import logging
import re
import time
from urllib.parse import urljoin, urlparse
from nameparser import HumanName
logging.basicConfig(
    filename='scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)
websites = [
    'https://www.blutechconsulting.com/',
    'https://averox.com/',
    'https://www.teradata.com/',
    'https://www.systemsltd.com/',
    'https://www.abl.com/',
    'https://www.clustox.com/',
    'https://www.piecyfer.com/',
    'https://www.hashe.com/',
    'https://algorepublic.com/',
    'https://www.goodcore.co.uk/',
    'https://abacus-global.com/',
    'https://creuto.com/',
    'https://www.pixelcrayons.com/',
    'https://sdlccorp.com/',
    'https://algorepublic.com/',
    'https://synergytop.com/',
    'https://www.bestremoteteam.com/',
    'https://www.nickelfox.com/',
    'https://www.appsierra.com/',
    'https://www.openxcell.com/',
    'https://futureinltd.com/'
    
]
ABOUT_PATTERNS = [
    '/about', '/about-us', '/company/about', '/our-story', '/about/team', 
    '/aboutus', '/management', '/pages/management', '/who-we-are', 
    '/about/company', '/company-profile', '/about/mission', '/team', 
    '/company/team', '/corporate/about', '/about/history', '/about/overview', 
    '/about/leadership', '/leadership', '/our-team', '/about/values', 
    '/pages/about', '/about/executives', '/about/company-profile'
]

CONTACT_PATTERNS = [
    '/contact', '/contact-us', '/get-in-touch', '/contact/info', '/support', 
    '/contact/details', '/customer-service', '/contactus', '/help/contact', 
    '/connect', '/reach-us', '/contact-us-page', '/contact/form', '/contact/office', 
    '/contact-us/info', '/contact/company', '/enquiry', '/contact/support'
]
designations = [
    "CEO", "Chief Executive Officer", "Founder", "Director", "Partner",
    "Manager", "Consultant", "Head", "President", "Vice President",
    "Solution Architect", "Senior", "Client Engagement Partner", "Chief Executive Officer",
    "Founder/CEO"
]
async def scrape_page(playwright, url):
    """Scrape the content of a page using Playwright."""
    logging.info(f"Requesting URL: {url}")
    try:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, timeout=30000)
        content = await page.content()
        await browser.close()
        return content
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return None
def extract_text(html):
    """Extract text content from the HTML using BeautifulSoup."""
    soup = BeautifulSoup(html, 'html.parser')
    for script_or_style in soup(['script', 'style', 'noscript']):
        script_or_style.decompose()
    return soup.get_text(separator=' ', strip=True)
async def find_page(playwright, base_url, patterns):
    """Find the page URL based on patterns."""
    for pattern in patterns:
        potential_url = urljoin(base_url, pattern)
        logging.info(f"Checking pattern: {potential_url}")
        html_content = await scrape_page(playwright, potential_url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.title.string if soup.title else ''
            if title:
                logging.info(f"Found page: {potential_url}")
                return potential_url
    return None
def extract_names_and_designations(text):
    """Extract names and designations from text using regex."""
    name_pattern = r'([A-Z][a-z]+(?: [A-Z][a-z]+)*(?: [A-Za-z]+)*)\s*[-:]*\s*(CEO|Chief Executive Officer|Founder|Client Engagement Partner|Chief Executive Officer|Partner|Director|Manager|Consultant|Head|President|Vice President|Solution Architect|Senior(?: [A-Za-z]+)?)'
    
    matches = re.findall(name_pattern, text)
    results = []
    for match in matches:
        name = HumanName(match[0]).full_name
        designation = match[1]
        logging.info(f"Found match: Name = {name}, Designation = {designation}")
        if len(name.split()) > 1 and "Our" not in name and "New" not in name:
            if designation in designations:
                results.append(f"{name} ({designation})") 
    return results
async def process_data():
    with open('website_info.csv', mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        rows = list(reader)  
    with open('website_info.csv', mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for row in rows[1:]: 
            website = row[0]
            about_content = row[2]  
            if about_content:
                extracted_data = extract_names_and_designations(about_content)
                if extracted_data:
                    row.append("; ".join(extracted_data))  
                else:
                    row.append("No employees found")
            else:
                row.append("No About Us Content")
            writer.writerow(row)
    logging.info(f"Employee names and designations added to website_info.csv.")
async def main():
    async with async_playwright() as playwright:
        with open('website_info.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Website', 'About Us URL', 'About Us Content', 'Contact URL', 'Contact Content', 'Employees (Names and Designations)'])
            for site in websites:
                logging.info(f"Processing website: {site}")
                print(f"Processing website: {site}")
                parsed_url = urlparse(site)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                about_url = await find_page(playwright, base_url, ABOUT_PATTERNS)
                about_content = ''
                about_names_designations = ''
                if about_url:
                    html_content = await scrape_page(playwright, about_url)
                    if html_content:
                        about_content = extract_text(html_content)
                        about_names_designations = extract_names_and_designations(about_content)
                time.sleep(2)
                contact_url = await find_page(playwright, base_url, CONTACT_PATTERNS)
                contact_content = ''
                if contact_url:
                    html_content = await scrape_page(playwright, contact_url)
                    if html_content:
                        contact_content = extract_text(html_content)
                if about_content == about_names_designations:
                    about_names_designations = ''
                writer.writerow([site, about_url or 'Not Found', about_content or 'Not Found', contact_url or 'Not Found', contact_content or 'Not Found', "; ".join(about_names_designations)])
                logging.info(f"Finished processing: {site}")
                print(f"Finished processing: {site}")
        await process_data()
if __name__ == "__main__":
    asyncio.run(main())

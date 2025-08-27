"""
CivicAtlas.in Web Scraper
Comprehensive scraper to extract urban local bodies and ward data
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
import re
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional, Tuple
import os
from utils import retry_on_failure, normalize_text, progress_bar

class CivicAtlasScraper:
    def __init__(self):
        self.base_url = "https://civicatlas.in"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        self.logger = logging.getLogger(__name__)
        self.output_file = "civicatlas_urban_bodies_wards.csv"
        
        # Statistics tracking
        self.stats = {
            'states_processed': 0,
            'urban_bodies_processed': 0,
            'wards_extracted': 0,
            'errors': 0,
            'skipped': 0
        }
        
        # CSV fieldnames
        self.csv_fieldnames = [
            'Ward Number',
            'Ward Name', 
            'Urban Local Body Name',
            'Urban Local Body Type',
            'District',
            'State',
            'LGD Code'
        ]

    def scrape_all_data(self) -> bool:
        """Main scraping function that orchestrates the entire process"""
        try:
            # Step 1: Get all state URLs
            self.logger.info("Starting to scrape CivicAtlas.in")
            print("🔍 Step 1: Discovering all states and union territories...")
            
            state_urls = self.get_state_urban_body_urls()
            if not state_urls:
                self.logger.error("No state URLs found")
                print("❌ Failed to discover any states")
                return False
                
            print(f"✅ Found {len(state_urls)} states/UTs")
            
            # Initialize CSV file
            self._initialize_csv_file()
            
            # Step 2: Process each state
            print("\n🏛️  Step 2: Processing urban local bodies for each state...")
            
            for i, (state_name, state_url) in enumerate(state_urls.items(), 1):
                try:
                    print(f"\n📍 Processing {state_name} ({i}/{len(state_urls)})")
                    self.process_state(state_name, state_url)
                    self.stats['states_processed'] += 1
                    
                    # Respectful delay between states
                    if i < len(state_urls):
                        time.sleep(2)
                        
                except Exception as e:
                    self.logger.error(f"Error processing state {state_name}: {str(e)}")
                    self.stats['errors'] += 1
                    print(f"   ⚠️  Error processing {state_name}: {str(e)}")
                    continue
            
            print(f"\n🎉 Scraping completed! Processed {self.stats['states_processed']} states")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error in scrape_all_data: {str(e)}")
            print(f"❌ Fatal error: {str(e)}")
            return False

    @retry_on_failure(max_retries=3, delay=2)
    def get_state_urban_body_urls(self) -> Dict[str, str]:
        """Extract all state URLs that link to urban local bodies listings"""
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            state_urls = {}
            
            # Find all links that contain "urban-local-bodies-list-in-"
            urban_body_links = soup.find_all('a', href=re.compile(r'/urban-local-bodies-list-in-.*-state-\d+'))
            
            for link in urban_body_links:
                href = link.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    
                    # Extract state name from the link text or URL
                    state_name = self._extract_state_name_from_link(link, href)
                    
                    if state_name:
                        state_urls[state_name] = full_url
                        self.logger.debug(f"Found state: {state_name} -> {full_url}")
            
            self.logger.info(f"Found {len(state_urls)} states with urban local bodies")
            return state_urls
            
        except requests.RequestException as e:
            self.logger.error(f"Network error getting state URLs: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error parsing state URLs: {str(e)}")
            raise

    def _extract_state_name_from_link(self, link, href: str) -> Optional[str]:
        """Extract state name from link element or URL"""
        try:
            # First try to get from link text
            link_text = link.get_text(strip=True)
            if link_text and link_text != "Urban Local Bodies":
                # Remove any numbers at the end (ULB count)
                state_name = re.sub(r'\s+\d+$', '', link_text).strip()
                if state_name:
                    return state_name
            
            # If link text doesn't work, extract from URL
            # URL pattern: /urban-local-bodies-list-in-STATE-NAME-state-ID
            match = re.search(r'/urban-local-bodies-list-in-(.+?)-state-\d+', href)
            if match:
                state_slug = match.group(1)
                # Convert slug to readable name (basic conversion)
                state_name = state_slug.replace('-', ' ').title()
                return state_name
                
            return None
            
        except Exception as e:
            self.logger.warning(f"Error extracting state name from link: {str(e)}")
            return None

    def process_state(self, state_name: str, state_url: str):
        """Process all urban local bodies for a given state"""
        try:
            urban_bodies = self.get_urban_bodies_from_state(state_name, state_url)
            
            if not urban_bodies:
                print(f"   📭 No urban bodies found for {state_name}")
                return
                
            print(f"   🏘️  Found {len(urban_bodies)} urban local bodies")
            
            # Process each urban body
            for i, urban_body in enumerate(urban_bodies, 1):
                try:
                    district_info = f"[{urban_body.get('district', 'Unknown')}]" if urban_body.get('district') else ""
                    print(f"   🔄 Processing {urban_body['name']} {district_info} ({i}/{len(urban_bodies)})", end=' ')
                    
                    wards = self.get_wards_from_urban_body(urban_body['url'])
                    
                    if wards:
                        self._save_wards_to_csv(wards, urban_body, state_name)
                        self.stats['wards_extracted'] += len(wards)
                        print(f"✅ {len(wards)} wards")
                    else:
                        print("⚠️ No wards found")
                        self.stats['skipped'] += 1
                    
                    self.stats['urban_bodies_processed'] += 1
                    
                    # Respectful delay between requests
                    time.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Error processing urban body {urban_body.get('name', 'Unknown')}: {str(e)}")
                    print(f"❌ Error")
                    self.stats['errors'] += 1
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing state {state_name}: {str(e)}")
            raise

    @retry_on_failure(max_retries=3, delay=1)
    def get_urban_bodies_from_state(self, state_name: str, state_url: str) -> List[Dict[str, str]]:
        """Extract all urban local bodies from a state page"""
        try:
            response = self.session.get(state_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            urban_bodies = []
            
            # Look for district tables first
            district_links = soup.find_all('a', href=re.compile(r'/urban-local-bodies-list-in-.*-district-\d+'))
            
            if district_links:
                # State has district-wise listing, process each district
                for district_link in district_links:
                    district_url = urljoin(self.base_url, district_link.get('href'))
                    district_name = district_link.get_text(strip=True)
                    
                    try:
                        district_bodies = self.get_urban_bodies_from_district(district_url, district_name)
                        urban_bodies.extend(district_bodies)
                        time.sleep(0.5)  # Small delay between district requests
                    except Exception as e:
                        self.logger.warning(f"Error processing district {district_name}: {str(e)}")
                        continue
            else:
                # Direct state listing, look for urban body links in tables
                tables = soup.find_all('table')
                
                for table in tables:
                    tbody = table.find('tbody')
                    if not tbody:
                        continue
                        
                    rows = tbody.find_all('tr')
                    
                    for row in rows:
                        # Look for links in the row that point to urban body pages
                        links = row.find_all('a', href=re.compile(r'/(municipal-corporations|municipality|town-panchayat|notified-area-council|cantonment-board|nct-municipal-council|city-municipal-council|town-municipal-council)-'))
                        
                        for link in links:
                            name = normalize_text(link.get_text())
                            url = urljoin(self.base_url, link.get('href'))
                            
                            # Extract urban body type from URL
                            ulb_type = self._extract_ulb_type_from_url(url)
                            
                            # Try to extract district from the row context
                            district = self._extract_district_from_row(row) or "Unknown"
                            
                            urban_bodies.append({
                                'name': name,
                                'url': url,
                                'type': ulb_type,
                                'district': district
                            })
            
            # Remove duplicates based on URL
            unique_bodies = []
            seen_urls = set()
            for body in urban_bodies:
                if body['url'] not in seen_urls:
                    unique_bodies.append(body)
                    seen_urls.add(body['url'])
            
            self.logger.info(f"Found {len(unique_bodies)} urban bodies in {state_name}")
            return unique_bodies
            
        except requests.RequestException as e:
            self.logger.error(f"Network error getting urban bodies for {state_name}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error parsing urban bodies for {state_name}: {str(e)}")
            raise

    @retry_on_failure(max_retries=3, delay=1)
    def get_urban_bodies_from_district(self, district_url: str, district_name: str) -> List[Dict[str, str]]:
        """Extract urban bodies from a district page"""
        try:
            response = self.session.get(district_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            urban_bodies = []
            
            # Look for tables with urban body listings
            tables = soup.find_all('table')
            
            for table in tables:
                tbody = table.find('tbody')
                if not tbody:
                    continue
                    
                rows = tbody.find_all('tr')
                
                for row in rows:
                    # Look for links to urban body pages
                    links = row.find_all('a', href=re.compile(r'/(municipal-corporations|municipality|town-panchayat|notified-area-council|cantonment-board|nct-municipal-council|city-municipal-council|town-municipal-council)-'))
                    
                    for link in links:
                        name = normalize_text(link.get_text())
                        url = urljoin(self.base_url, link.get('href'))
                        ulb_type = self._extract_ulb_type_from_url(url)
                        
                        urban_bodies.append({
                            'name': name,
                            'url': url,
                            'type': ulb_type,
                            'district': district_name
                        })
            
            return urban_bodies
            
        except Exception as e:
            self.logger.error(f"Error getting urban bodies from district {district_name}: {str(e)}")
            raise

    def _extract_ulb_type_from_url(self, url: str) -> str:
        """Extract urban local body type from URL"""
        type_mapping = {
            'municipal-corporations': 'Municipal Corporation',
            'municipality': 'Municipality',
            'town-panchayat': 'Town Panchayat',
            'notified-area-council': 'Notified Area Council',
            'cantonment-board': 'Cantonment Board',
            'nct-municipal-council': 'NCT Municipal Council',
            'city-municipal-council': 'City Municipal Council',
            'town-municipal-council': 'Town Municipal Council'
        }
        
        for url_pattern, type_name in type_mapping.items():
            if url_pattern in url:
                return type_name
                
        return 'Unknown'

    def _extract_district_from_row(self, row) -> Optional[str]:
        """Try to extract district name from table row context"""
        try:
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                text = normalize_text(cell.get_text())
                # Look for district indicators
                if 'district' in text.lower() and len(text) < 50:
                    return text.replace('district', '').replace('District', '').strip()
            return None
        except:
            return None

    @retry_on_failure(max_retries=3, delay=1)
    def get_wards_from_urban_body(self, urban_body_url: str) -> List[Dict[str, str]]:
        """Extract ward information from an urban body page"""
        try:
            response = self.session.get(urban_body_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            wards = []
            
            # Look for tables with ward information
            tables = soup.find_all('table')
            
            for table in tables:
                # Check if this table contains ward information
                headers = table.find_all(['th', 'td'])
                header_text = ' '.join([h.get_text().lower() for h in headers[:5]])  # Check first few cells
                
                if any(keyword in header_text for keyword in ['ward', 'name', 'no']):
                    tbody = table.find('tbody')
                    if not tbody:
                        # If no tbody, look for rows directly in table
                        rows = table.find_all('tr')[1:]  # Skip header row
                    else:
                        rows = tbody.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:  # Ensure we have enough columns
                            try:
                                # Try to extract ward information
                                ward_info = self._extract_ward_info_from_cells(cells)
                                if ward_info:
                                    wards.append(ward_info)
                            except Exception as e:
                                self.logger.debug(f"Error extracting ward from row: {str(e)}")
                                continue
            
            self.logger.debug(f"Extracted {len(wards)} wards from {urban_body_url}")
            return wards
            
        except requests.RequestException as e:
            self.logger.error(f"Network error getting wards from {urban_body_url}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error parsing wards from {urban_body_url}: {str(e)}")
            raise

    def _extract_ward_info_from_cells(self, cells) -> Optional[Dict[str, str]]:
        """Extract ward information from table cells"""
        try:
            # Convert all cell contents to text
            cell_texts = [normalize_text(cell.get_text()) for cell in cells]
            
            # Skip empty rows
            if not any(cell_texts):
                return None
            
            ward_info = {'ward_number': '', 'ward_name': '', 'lgd_code': ''}
            
            # Expected columns: #, Ward Name, Ward No, LGD Code
            # Try to map based on position and content
            for i, text in enumerate(cell_texts):
                if not text:
                    continue
                    
                # Column 0: Serial number (skip)
                if i == 0 and text.isdigit():
                    continue
                # Column 1: Ward Name
                elif i == 1 and 'ward' in text.lower():
                    ward_info['ward_name'] = text
                # Column 2: Ward Number
                elif i == 2 and text.isdigit():
                    ward_info['ward_number'] = text
                # Column 3: LGD Code
                elif i == 3 and text.isdigit():
                    ward_info['lgd_code'] = text
                # Fallback: try to identify based on content
                elif text.isdigit() and len(text) >= 3 and not ward_info['lgd_code']:
                    ward_info['lgd_code'] = text
                elif text.isdigit() and len(text) <= 2 and not ward_info['ward_number']:
                    ward_info['ward_number'] = text
                elif 'ward' in text.lower() and len(text) > 5 and not ward_info['ward_name']:
                    ward_info['ward_name'] = text
                elif re.search(r'ward\s*no\.?\s*(\d+)', text, re.IGNORECASE) and not ward_info['ward_name']:
                    match = re.search(r'ward\s*no\.?\s*(\d+)', text, re.IGNORECASE)
                    ward_info['ward_number'] = match.group(1)
                    ward_info['ward_name'] = text
            
            # Validate that we have at least some ward information
            if ward_info['ward_number'] or ward_info['ward_name'] or ward_info['lgd_code']:
                return ward_info
                
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting ward info from cells: {str(e)}")
            return None

    def _initialize_csv_file(self):
        """Initialize the CSV file with headers"""
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                writer.writeheader()
            self.logger.info(f"Initialized CSV file: {self.output_file}")
        except Exception as e:
            self.logger.error(f"Error initializing CSV file: {str(e)}")
            raise

    def _save_wards_to_csv(self, wards: List[Dict[str, str]], urban_body: Dict[str, str], state_name: str):
        """Save ward data to CSV file"""
        try:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_fieldnames)
                
                for ward in wards:
                    writer.writerow({
                        'Ward Number': ward.get('ward_number', ''),
                        'Ward Name': ward.get('ward_name', ''),
                        'Urban Local Body Name': urban_body['name'],
                        'Urban Local Body Type': urban_body['type'],
                        'District': urban_body.get('district', ''),
                        'State': state_name,
                        'LGD Code': ward.get('lgd_code', '')
                    })
                    
        except Exception as e:
            self.logger.error(f"Error saving wards to CSV: {str(e)}")
            raise

    def display_summary(self):
        """Display summary statistics of the scraping process"""
        print("📊 Scraping Summary:")
        print("-" * 50)
        print(f"States/UTs processed: {self.stats['states_processed']}")
        print(f"Urban bodies processed: {self.stats['urban_bodies_processed']}")
        print(f"Total wards extracted: {self.stats['wards_extracted']}")
        print(f"Errors encountered: {self.stats['errors']}")
        print(f"Bodies skipped (no wards): {self.stats['skipped']}")
        print(f"Output file: {self.output_file}")
        
        if os.path.exists(self.output_file):
            file_size = os.path.getsize(self.output_file)
            print(f"File size: {file_size:,} bytes")

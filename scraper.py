import requests
from bs4 import BeautifulSoup
import time
import random
import re
from urllib.parse import urljoin, quote, urlparse
import logging
from typing import List, Dict, Any, Optional
import trafilatura
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import cloudscraper
from fake_useragent import UserAgent

class AdvancedTurmericBuyerScraper:
    """Advanced web scraper for collecting real turmeric buyer company data"""
    
    def __init__(self, delay_seconds: int = 3):
        self.delay_seconds = delay_seconds
        
        # Initialize CloudScraper for advanced bot protection bypass
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        
        # Initialize User Agent rotation
        self.ua = UserAgent()
        
        # Setup retry strategy (using new urllib3 syntax)
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        # Regular session for fallback
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Advanced headers
        self._update_headers()
        
        # Real sources for turmeric buyers
        self.real_sources = {
            'government_directories': [
                'https://www.dgft.gov.in',
                'https://www.apeda.gov.in',
                'https://www.agricoop.gov.in'
            ],
            'trade_platforms': [
                'https://www.tradeindia.com',
                'https://www.indiamart.com',
                'https://www.exportersindia.com',
                'https://www.alibaba.com',
                'https://dir.indiamart.com'
            ],
            'company_registries': [
                'https://www.mca.gov.in',
                'https://www.zauba.com',
                'https://www.tofler.in'
            ]
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _update_headers(self):
        """Update session headers with advanced fingerprinting"""
        headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8,id;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Google Chrome";v="120", "Chromium";v="120", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Pragma': 'no-cache'
        }
        
        self.session.headers.update(headers)
        self.scraper.headers.update(headers)
    
    def scrape_source(self, source: str, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Advanced scraping from multiple real sources"""
        try:
            if source == 'tradeindia':
                return self._advanced_scrape_tradeindia(search_term, limit)
            elif source == 'indiamart':
                return self._advanced_scrape_indiamart(search_term, limit)
            elif source == 'exportersindia':
                return self._advanced_scrape_exportersindia(search_term, limit)
            elif source == 'zauba':
                return self._scrape_zauba_companies(search_term, limit)
            elif source == 'tofler':
                return self._scrape_tofler_companies(search_term, limit)
            elif source == 'government_data':
                return self._scrape_government_sources(search_term, limit)
            elif source == 'alibaba':
                return self._scrape_alibaba_buyers(search_term, limit)
            else:
                self.logger.warning(f"Unknown source: {source}")
                return []
        except Exception as e:
            self.logger.error(f"Error scraping {source}: {str(e)}")
            return []
    
    def _scrape_tradeindia(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape TradeIndia for turmeric buyers"""
        companies = []
        base_url = "https://www.tradeindia.com"
        
        try:
            # Search URL
            search_url = f"{base_url}/search.html?ss={quote(search_term)}&t=buyer"
            
            page = 1
            while len(companies) < limit and page <= 5:  # Limit to 5 pages
                url = f"{search_url}&page={page}"
                
                response = self._make_request(url)
                if not response:
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find company listings
                company_cards = soup.find_all('div', class_=['item', 'result-item', 'listing-item'])
                
                if not company_cards:
                    # Try alternative selectors
                    company_cards = soup.find_all('div', attrs={'data-company': True})
                    if not company_cards:
                        company_cards = soup.find_all('a', href=re.compile(r'/company/'))
                
                for card in company_cards:
                    if len(companies) >= limit:
                        break
                    
                    try:
                        company_data = self._extract_tradeindia_company(card, base_url)
                        if company_data and company_data not in companies:
                            companies.append(company_data)
                    except Exception as e:
                        self.logger.error(f"Error extracting TradeIndia company: {str(e)}")
                        continue
                
                page += 1
                time.sleep(self.delay_seconds)
                
        except Exception as e:
            self.logger.error(f"Error scraping TradeIndia: {str(e)}")
        
        return companies[:limit]
    
    def _extract_tradeindia_company(self, card, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract company information from TradeIndia card"""
        try:
            company_data = {
                'source': 'TradeIndia',
                'company_name': '',
                'contact_person': '',
                'city': '',
                'state': '',
                'country': 'India',
                'phone': '',
                'email': '',
                'website': '',
                'description': '',
                'products': '',
                'company_url': ''
            }
            
            # Company name
            name_elem = card.find(['h3', 'h4', 'h5'], class_=re.compile(r'company|name|title')) or \
                       card.find('a', href=re.compile(r'/company/'))
            if name_elem:
                company_data['company_name'] = name_elem.get_text(strip=True)
                if name_elem.get('href'):
                    company_data['company_url'] = urljoin(base_url, name_elem['href'])
            
            # Contact person
            contact_elem = card.find(text=re.compile(r'Contact Person|Mr\.|Ms\.|Mrs\.'))
            if contact_elem:
                contact_parent = contact_elem.parent if contact_elem.parent else contact_elem
                company_data['contact_person'] = contact_parent.get_text(strip=True)
            
            # Location
            location_elem = card.find(text=re.compile(r'Location:|City:')) or \
                           card.find('span', class_=re.compile(r'location|city|address'))
            if location_elem:
                location_text = location_elem.get_text(strip=True) if hasattr(location_elem, 'get_text') else str(location_elem)
                location_parts = location_text.split(',')
                if len(location_parts) >= 1:
                    company_data['city'] = location_parts[0].strip()
                if len(location_parts) >= 2:
                    company_data['state'] = location_parts[1].strip()
            
            # Phone
            phone_elem = card.find(text=re.compile(r'\+91|\d{10}|\d{11}')) or \
                        card.find('a', href=re.compile(r'tel:'))
            if phone_elem:
                phone_text = phone_elem.get_text(strip=True) if hasattr(phone_elem, 'get_text') else str(phone_elem)
                phone_match = re.search(r'(\+91[\s-]?)?\d{10,11}', phone_text)
                if phone_match:
                    company_data['phone'] = phone_match.group(0)
            
            # Email
            email_elem = card.find('a', href=re.compile(r'mailto:')) or \
                        card.find(text=re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'))
            if email_elem:
                if hasattr(email_elem, 'get') and email_elem.get('href'):
                    company_data['email'] = email_elem['href'].replace('mailto:', '')
                else:
                    email_text = email_elem.get_text(strip=True) if hasattr(email_elem, 'get_text') else str(email_elem)
                    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email_text)
                    if email_match:
                        company_data['email'] = email_match.group(0)
            
            # Description/Products
            desc_elem = card.find(['p', 'div'], class_=re.compile(r'description|product|detail'))
            if desc_elem:
                company_data['description'] = desc_elem.get_text(strip=True)
            
            # Only return if we have at least company name
            if company_data['company_name']:
                return company_data
            
        except Exception as e:
            self.logger.error(f"Error extracting TradeIndia company data: {str(e)}")
        
        return None
    
    def _scrape_indiamart(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape IndiaMart for turmeric buyers"""
        companies = []
        base_url = "https://www.indiamart.com"
        
        try:
            # Search URL
            search_url = f"{base_url}/search.mp?ss={quote(search_term)}"
            
            page = 1
            while len(companies) < limit and page <= 5:
                url = f"{search_url}&page={page}"
                
                response = self._make_request(url)
                if not response:
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find company listings
                company_cards = soup.find_all('div', class_=['lst', 'company-card', 'seller-card'])
                
                if not company_cards:
                    # Try alternative selectors
                    company_cards = soup.find_all('div', attrs={'data-company-name': True})
                
                for card in company_cards:
                    if len(companies) >= limit:
                        break
                    
                    try:
                        company_data = self._extract_indiamart_company(card, base_url)
                        if company_data and company_data not in companies:
                            companies.append(company_data)
                    except Exception as e:
                        self.logger.error(f"Error extracting IndiaMart company: {str(e)}")
                        continue
                
                page += 1
                time.sleep(self.delay_seconds)
                
        except Exception as e:
            self.logger.error(f"Error scraping IndiaMart: {str(e)}")
        
        return companies[:limit]
    
    def _extract_indiamart_company(self, card, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract company information from IndiaMart card"""
        try:
            company_data = {
                'source': 'IndiaMart',
                'company_name': '',
                'contact_person': '',
                'city': '',
                'state': '',
                'country': 'India',
                'phone': '',
                'email': '',
                'website': '',
                'description': '',
                'products': '',
                'company_url': ''
            }
            
            # Company name
            name_elem = card.find(['h3', 'h4', 'h5'], class_=re.compile(r'company|name|seller')) or \
                       card.find('a', class_=re.compile(r'company|seller'))
            if name_elem:
                company_data['company_name'] = name_elem.get_text(strip=True)
                if name_elem.get('href'):
                    company_data['company_url'] = urljoin(base_url, name_elem['href'])
            
            # Location
            location_elem = card.find(class_=re.compile(r'location|city|address')) or \
                           card.find('span', text=re.compile(r'[A-Z][a-z]+,'))
            if location_elem:
                location_text = location_elem.get_text(strip=True)
                location_parts = location_text.split(',')
                if len(location_parts) >= 1:
                    company_data['city'] = location_parts[0].strip()
                if len(location_parts) >= 2:
                    company_data['state'] = location_parts[1].strip()
            
            # Contact details
            contact_elem = card.find(text=re.compile(r'Contact|Call'))
            if contact_elem:
                contact_parent = contact_elem.parent
                if contact_parent:
                    contact_text = contact_parent.get_text()
                    # Extract phone
                    phone_match = re.search(r'(\+91[\s-]?)?\d{10,11}', contact_text)
                    if phone_match:
                        company_data['phone'] = phone_match.group(0)
            
            # Products/Description
            product_elem = card.find(class_=re.compile(r'product|item|service'))
            if product_elem:
                company_data['products'] = product_elem.get_text(strip=True)
            
            # Only return if we have at least company name
            if company_data['company_name']:
                return company_data
            
        except Exception as e:
            self.logger.error(f"Error extracting IndiaMart company data: {str(e)}")
        
        return None
    
    def _scrape_exportersindia(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape ExportersIndia for turmeric buyers"""
        companies = []
        base_url = "https://www.exportersindia.com"
        
        try:
            # Search URL
            search_url = f"{base_url}/search/{quote(search_term)}-buyers.html"
            
            page = 1
            while len(companies) < limit and page <= 5:
                url = f"{search_url}?page={page}"
                
                response = self._make_request(url)
                if not response:
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find company listings
                company_cards = soup.find_all('div', class_=['company-list', 'buyer-list', 'listing'])
                
                if not company_cards:
                    # Try alternative selectors
                    company_cards = soup.find_all('tr', class_='listing-row')
                
                for card in company_cards:
                    if len(companies) >= limit:
                        break
                    
                    try:
                        company_data = self._extract_exportersindia_company(card, base_url)
                        if company_data and company_data not in companies:
                            companies.append(company_data)
                    except Exception as e:
                        self.logger.error(f"Error extracting ExportersIndia company: {str(e)}")
                        continue
                
                page += 1
                time.sleep(self.delay_seconds)
                
        except Exception as e:
            self.logger.error(f"Error scraping ExportersIndia: {str(e)}")
        
        return companies[:limit]
    
    def _extract_exportersindia_company(self, card, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract company information from ExportersIndia card"""
        try:
            company_data = {
                'source': 'ExportersIndia',
                'company_name': '',
                'contact_person': '',
                'city': '',
                'state': '',
                'country': 'India',
                'phone': '',
                'email': '',
                'website': '',
                'description': '',
                'products': '',
                'company_url': ''
            }
            
            # Company name
            name_elem = card.find(['h3', 'h4', 'strong'], class_=re.compile(r'company|name')) or \
                       card.find('a', href=re.compile(r'/company/'))
            if name_elem:
                company_data['company_name'] = name_elem.get_text(strip=True)
                if name_elem.get('href'):
                    company_data['company_url'] = urljoin(base_url, name_elem['href'])
            
            # Location
            location_elem = card.find(text=re.compile(r'Location|City')) or \
                           card.find('td', class_=re.compile(r'location|city'))
            if location_elem:
                location_text = location_elem.get_text(strip=True) if hasattr(location_elem, 'get_text') else str(location_elem)
                location_parts = location_text.split(',')
                if len(location_parts) >= 1:
                    company_data['city'] = location_parts[0].strip()
                if len(location_parts) >= 2:
                    company_data['state'] = location_parts[1].strip()
            
            # Phone
            phone_elem = card.find(text=re.compile(r'\+91|\d{10}')) or \
                        card.find('td', class_=re.compile(r'phone|mobile'))
            if phone_elem:
                phone_text = phone_elem.get_text(strip=True) if hasattr(phone_elem, 'get_text') else str(phone_elem)
                phone_match = re.search(r'(\+91[\s-]?)?\d{10,11}', phone_text)
                if phone_match:
                    company_data['phone'] = phone_match.group(0)
            
            # Products
            product_elem = card.find('td', class_=re.compile(r'product|requirement'))
            if product_elem:
                company_data['products'] = product_elem.get_text(strip=True)
            
            # Only return if we have at least company name
            if company_data['company_name']:
                return company_data
            
        except Exception as e:
            self.logger.error(f"Error extracting ExportersIndia company data: {str(e)}")
        
        return None
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and better headers"""
        for attempt in range(max_retries):
            try:
                # Update headers for each attempt
                self._update_headers()
                
                # Add referrer for better acceptance
                headers = self.session.headers.copy()
                headers['Referer'] = 'https://www.google.com/'
                
                response = self.session.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = random.uniform(2 ** attempt, 2 ** (attempt + 1))
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All retry attempts failed for URL: {url}")
        return None
    
    def _generate_sample_data(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Generate realistic sample data for demonstration"""
        # Note: This is for demonstration purposes when live scraping is blocked
        sample_companies = [
            {
                'company_name': 'Spice Trading Corporation',
                'contact_person': 'Rajesh Kumar',
                'city': 'Mumbai',
                'state': 'Maharashtra',
                'country': 'India',
                'phone': '+91-9876543210',
                'email': 'contact@spicetradingcorp.com',
                'website': 'www.spicetradingcorp.com',
                'description': 'Leading importer and distributor of turmeric and spices',
                'products': 'Turmeric powder, whole turmeric, organic turmeric',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Golden Turmeric Imports',
                'contact_person': 'Priya Sharma',
                'city': 'Delhi',
                'state': 'Delhi',
                'country': 'India',
                'phone': '+91-9123456789',
                'email': 'orders@goldenturmeric.in',
                'website': 'www.goldenturmeric.in',
                'description': 'Wholesale buyer of premium quality turmeric',
                'products': 'Curcuma longa, turmeric fingers, turmeric powder',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Maharaja Spice Enterprises',
                'contact_person': 'Suresh Patel',
                'city': 'Ahmedabad',
                'state': 'Gujarat',
                'country': 'India',
                'phone': '+91-9987654321',
                'email': 'purchase@maharajaspice.com',
                'website': 'www.maharajaspice.com',
                'description': 'Bulk buyer of turmeric for export markets',
                'products': 'Salem turmeric, Erode turmeric, organic turmeric',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Indo Global Commodities',
                'contact_person': 'Amit Singh',
                'city': 'Kolkata',
                'state': 'West Bengal',
                'country': 'India',
                'phone': '+91-9876543012',
                'email': 'trading@indoglobal.co.in',
                'website': 'www.indoglobal.co.in',
                'description': 'International trader specializing in turmeric procurement',
                'products': 'Turmeric fingers, turmeric powder, value-added turmeric',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Sunrise Agro Trading',
                'contact_person': 'Kavita Reddy',
                'city': 'Hyderabad',
                'state': 'Telangana',
                'country': 'India',
                'phone': '+91-9345678901',
                'email': 'buyer@sunriseagro.net',
                'website': 'www.sunriseagro.net',
                'description': 'Regional distributor seeking quality turmeric suppliers',
                'products': 'Nizamabad turmeric, Duggirala turmeric, processed turmeric',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Turmeric World Pvt. Ltd.',
                'contact_person': 'Ramesh Gupta',
                'city': 'Jaipur',
                'state': 'Rajasthan',
                'country': 'India',
                'phone': '+91-9998887776',
                'email': 'info@turmericworld.in',
                'website': 'www.turmericworld.in',
                'description': 'Premium turmeric buyer for retail chains',
                'products': 'Rajasthan turmeric, powdered turmeric, turmeric capsules',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Global Spice Network',
                'contact_person': 'Meera Joshi',
                'city': 'Pune',
                'state': 'Maharashtra',
                'country': 'India',
                'phone': '+91-9876509876',
                'email': 'procurement@globalspice.net',
                'website': 'www.globalspice.net',
                'description': 'International spice procurement company',
                'products': 'Export quality turmeric, organic certification',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Saffron & Spice Co.',
                'contact_person': 'Vikram Shah',
                'city': 'Indore',
                'state': 'Madhya Pradesh',
                'country': 'India',
                'phone': '+91-9123509876',
                'email': 'orders@saffronspice.com',
                'website': 'www.saffronspice.com',
                'description': 'Wholesale buyer of premium spices including turmeric',
                'products': 'MP turmeric, high curcumin content turmeric',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Eastern Trading House',
                'contact_person': 'Arjun Das',
                'city': 'Bhubaneswar',
                'state': 'Odisha',
                'country': 'India',
                'phone': '+91-9876123456',
                'email': 'buying@easterntrading.co.in',
                'website': 'www.easterntrading.co.in',
                'description': 'Regional distributor for eastern India markets',
                'products': 'Turmeric fingers, ground turmeric, medicinal grade',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Nature Fresh Ingredients',
                'contact_person': 'Deepika Nair',
                'city': 'Kochi',
                'state': 'Kerala',
                'country': 'India',
                'phone': '+91-9998765432',
                'email': 'purchase@naturefresh.in',
                'website': 'www.naturefresh.in',
                'description': 'Organic spice buyer for health food industry',
                'products': 'Organic turmeric, wild turmeric, turmeric extracts',
                'source': 'Sample Data',
                'company_url': ''
            },
            {
                'company_name': 'Spice Valley Enterprises',
                'contact_person': 'Kiran Kumar',
                'city': 'Coimbatore',
                'state': 'Tamil Nadu',
                'country': 'India',
                'phone': '+91-9876543219',
                'email': 'trade@spicevalley.com',
                'website': 'www.spicevalley.com',
                'description': 'South Indian spice trading company',
                'products': 'Erode turmeric, Salem turmeric, turmeric oleoresin',
                'source': 'Sample Data',
                'company_url': ''
            }
        ]
        
        # Add more companies based on search term
        extended_companies = []
        for i in range(10):
            cities = ['Chennai', 'Bangalore', 'Lucknow', 'Kanpur', 'Nagpur', 'Surat', 'Vadodara', 'Ludhiana', 'Amritsar', 'Chandigarh']
            states = ['Tamil Nadu', 'Karnataka', 'Uttar Pradesh', 'Uttar Pradesh', 'Maharashtra', 'Gujarat', 'Gujarat', 'Punjab', 'Punjab', 'Chandigarh']
            
            company = {
                'company_name': f'{search_term.title()} {random.choice(["Trading", "Enterprises", "Corporation", "Industries", "Exports"])} {i+1}',
                'contact_person': random.choice(['Rajesh Kumar', 'Priya Sharma', 'Amit Singh', 'Kavita Reddy', 'Suresh Patel']),
                'city': cities[i % len(cities)],
                'state': states[i % len(states)],
                'country': 'India',
                'phone': f'+91-9{random.randint(100000000, 999999999)}',
                'email': f'contact@{search_term.replace(" ", "").lower()}{i+1}.com',
                'website': f'www.{search_term.replace(" ", "").lower()}{i+1}.com',
                'description': f'Wholesale {search_term} procurement and distribution',
                'products': f'{search_term}, spices, agricultural commodities',
                'source': 'Sample Data',
                'company_url': ''
            }
            extended_companies.append(company)
        
        # Combine and randomize
        all_companies = sample_companies + extended_companies
        random.shuffle(all_companies)
        return all_companies[:limit]
    
    def _advanced_scrape_tradeindia(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Advanced TradeIndia scraping with CloudScraper"""
        companies = []
        base_url = "https://www.tradeindia.com"
        
        try:
            # Use multiple search approaches
            search_urls = [
                f"{base_url}/search.html?ss={quote(search_term)}&t=buyer",
                f"{base_url}/buyers/{quote(search_term.replace(' ', '-'))}.html",
                f"{base_url}/search.html?q={quote(search_term)}&cat=buyer"
            ]
            
            for search_url in search_urls:
                if len(companies) >= limit:
                    break
                    
                try:
                    # Use CloudScraper for better bypass
                    response = self._make_advanced_request(search_url)
                    if not response:
                        continue
                    
                    # Use trafilatura for content extraction
                    content = trafilatura.extract(response.text, include_links=True, include_tables=True)
                    if content:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        company_data = self._extract_companies_from_content(soup, base_url, 'TradeIndia')
                        companies.extend(company_data[:limit - len(companies)])
                    
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    self.logger.error(f"Error in advanced TradeIndia scraping: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error in TradeIndia advanced scraping: {str(e)}")
        
        return companies[:limit]
    
    def _advanced_scrape_indiamart(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Advanced IndiaMart scraping with multiple endpoints"""
        companies = []
        base_url = "https://www.indiamart.com"
        
        try:
            # Multiple search strategies
            search_endpoints = [
                f"{base_url}/impcat/{quote(search_term.replace(' ', '-'))}.html",
                f"{base_url}/city/{random.choice(['mumbai', 'delhi', 'chennai', 'kolkata'])}/{quote(search_term.replace(' ', '-'))}/",
                f"{base_url}/proddetail/{quote(search_term.replace(' ', '-'))}"
            ]
            
            for endpoint in search_endpoints:
                if len(companies) >= limit:
                    break
                
                try:
                    response = self._make_advanced_request(endpoint)
                    if response:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        extracted_companies = self._extract_companies_from_content(soup, base_url, 'IndiaMart')
                        companies.extend(extracted_companies[:limit - len(companies)])
                    
                    time.sleep(random.uniform(3, 5))
                    
                except Exception as e:
                    self.logger.error(f"Error in IndiaMart endpoint scraping: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error in IndiaMart advanced scraping: {str(e)}")
        
        return companies[:limit]
    
    def _scrape_zauba_companies(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape real company data from Zauba"""
        companies = []
        base_url = "https://www.zauba.com"
        
        try:
            search_url = f"{base_url}/search?query={quote(search_term)}"
            response = self._make_advanced_request(search_url)
            
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for company listings
                company_links = soup.find_all('a', href=re.compile(r'/company/'))
                
                for link in company_links[:limit]:
                    try:
                        company_url = urljoin(base_url, link.get('href'))
                        company_response = self._make_advanced_request(company_url)
                        
                        if company_response:
                            company_soup = BeautifulSoup(company_response.text, 'html.parser')
                            company_data = self._extract_zauba_company_details(company_soup)
                            if company_data:
                                companies.append(company_data)
                        
                        time.sleep(random.uniform(2, 4))
                        
                    except Exception as e:
                        self.logger.error(f"Error extracting Zauba company: {str(e)}")
                        continue
            
        except Exception as e:
            self.logger.error(f"Error scraping Zauba: {str(e)}")
        
        return companies[:limit]
    
    def _scrape_government_sources(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape from government trade directories"""
        companies = []
        
        try:
            # APEDA (Agricultural and Processed Food Products Export Development Authority)
            apeda_url = "https://www.apeda.gov.in/apedawebsite/six_head_product/FFV.htm"
            response = self._make_advanced_request(apeda_url)
            
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Extract exporters/buyers information
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:
                            company_data = self._extract_government_company_data(cells, 'APEDA')
                            if company_data:
                                companies.append(company_data)
                                if len(companies) >= limit:
                                    break
            
        except Exception as e:
            self.logger.error(f"Error scraping government sources: {str(e)}")
        
        return companies[:limit]
    
    def _make_advanced_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Advanced request with CloudScraper and fallback"""
        for attempt in range(max_retries):
            try:
                # Update headers for each attempt
                self._update_headers()
                
                # Try CloudScraper first (best for bypassing protection)
                try:
                    response = self.scraper.get(url, timeout=30)
                    if response.status_code == 200:
                        return response
                except Exception as e:
                    self.logger.warning(f"CloudScraper failed: {str(e)}")
                
                # Fallback to regular session with advanced headers
                headers = self.session.headers.copy()
                headers['Referer'] = 'https://www.google.com/search?q=' + quote(url.split('/')[-1])
                
                response = self.session.get(url, timeout=30, headers=headers)
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Advanced request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = random.uniform(2 ** attempt, 2 ** (attempt + 1))
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All advanced retry attempts failed for URL: {url}")
        return None
    
    def _extract_companies_from_content(self, soup: BeautifulSoup, base_url: str, source: str) -> List[Dict[str, Any]]:
        """Advanced company extraction using multiple selectors"""
        companies = []
        
        # Multiple selector strategies for different layouts
        company_selectors = [
            {'container': 'div[class*="company"]', 'name': 'h3, h4, h5, .company-name', 'details': 'p, div, span'},
            {'container': 'tr[class*="listing"]', 'name': 'td:first-child, .name', 'details': 'td'},
            {'container': 'article, .result-item', 'name': '.title, h3, h4', 'details': '.description, p'},
            {'container': '[data-company], [data-seller]', 'name': 'h3, h4, .name', 'details': '.details, p'},
            {'container': '.card, .item', 'name': '.title, h3, h4', 'details': '.content, p'}
        ]
        
        for selector_set in company_selectors:
            try:
                containers = soup.select(selector_set['container'])
                
                for container in containers[:10]:  # Limit per selector
                    try:
                        company_data = {
                            'source': source,
                            'company_name': '',
                            'contact_person': '',
                            'city': '',
                            'state': '',
                            'country': 'India',
                            'phone': '',
                            'email': '',
                            'website': '',
                            'description': '',
                            'products': '',
                            'company_url': ''
                        }
                        
                        # Extract company name
                        name_elem = container.select_one(selector_set['name'])
                        if name_elem:
                            company_data['company_name'] = name_elem.get_text(strip=True)
                            
                            # Get company URL if available
                            link = name_elem.find('a') or container.find('a', href=re.compile(r'/company/|/seller/'))
                            if link and link.get('href'):
                                company_data['company_url'] = urljoin(base_url, link['href'])
                        
                        # Extract all text content for parsing
                        all_text = container.get_text()
                        
                        # Smart extraction using regex patterns
                        company_data.update(self._smart_extract_company_info(all_text))
                        
                        # Only add if we have meaningful data
                        if (company_data['company_name'] and 
                            len(company_data['company_name']) > 3 and
                            not any(invalid in company_data['company_name'].lower() 
                                   for invalid in ['search', 'result', 'page', 'more', 'view'])):
                            companies.append(company_data)
                    
                    except Exception as e:
                        continue
                        
            except Exception as e:
                continue
        
        return companies
    
    def _smart_extract_company_info(self, text: str) -> Dict[str, str]:
        """Smart extraction of company information using advanced regex"""
        info = {}
        
        # Phone number extraction (Indian numbers)
        phone_patterns = [
            r'\+91[\s-]?[789]\d{9}',
            r'91[\s-]?[789]\d{9}',
            r'[789]\d{9}',
            r'\(\+91\)[\s-]?\d{10}',
            r'0\d{2,4}[\s-]?\d{6,8}'
        ]
        
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                info['phone'] = match.group(0).strip()
                break
        
        # Email extraction
        email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        if email_match:
            info['email'] = email_match.group(0).lower()
        
        # Website extraction
        website_patterns = [
            r'www\.[A-Za-z0-9.-]+\.[A-Za-z]{2,4}',
            r'https?://[A-Za-z0-9.-]+\.[A-Za-z]{2,4}',
            r'\b[A-Za-z0-9.-]+\.com\b',
            r'\b[A-Za-z0-9.-]+\.in\b'
        ]
        
        for pattern in website_patterns:
            match = re.search(pattern, text)
            if match:
                info['website'] = match.group(0).strip()
                break
        
        # Location extraction (Indian cities and states)
        location_patterns = [
            r'\b(Mumbai|Delhi|Bangalore|Hyderabad|Ahmedabad|Chennai|Kolkata|Surat|Pune|Jaipur|Lucknow|Kanpur|Nagpur|Indore|Thane|Bhopal|Visakhapatnam|Pimpri|Patna|Vadodara|Ghaziabad|Ludhiana|Agra|Nashik|Faridabad|Meerut|Rajkot|Kalyan|Vasai|Varanasi|Srinagar|Aurangabad|Dhanbad|Amritsar|Navi Mumbai|Allahabad|Ranchi|Howrah|Coimbatore|Jabalpur|Gwalior|Vijayawada|Jodhpur|Madurai|Raipur|Kota|Guwahati|Chandigarh|Solapur|Hubli|Tiruchirappalli|Bareilly|Mysore|Tiruppur|Gurgaon|Aligarh|Jalandhar|Bhubaneswar|Salem|Mira|Warangal|Guntur|Bhiwandi|Saharanpur|Gorakhpur|Bikaner|Amravati|Noida|Jamshedpur|Bhilai|Cuttack|Firozabad|Kochi|Nellore|Bhavnagar|Dehradun|Durgapur|Asansol|Rourkela|Nanded|Kolhapur|Ajmer|Akola|Gulbarga|Jamnagar|Ujjain|Loni|Siliguri|Jhansi|Ulhasnagar|Jammu|Sangli|Mangalore|Erode|Belgaum|Ambattur|Tirunelveli|Malegaon|Gaya|Jalgaon|Udaipur|Maheshtala)\b',
            r'\b(Maharashtra|Gujarat|Rajasthan|Punjab|Haryana|Madhya Pradesh|Uttar Pradesh|West Bengal|Tamil Nadu|Karnataka|Kerala|Andhra Pradesh|Telangana|Bihar|Odisha|Jharkhand|Assam|Chhattisgarh|Himachal Pradesh|Uttarakhand|Goa|Tripura|Meghalaya|Manipur|Nagaland|Arunachal Pradesh|Mizoram|Sikkim|Delhi|Chandigarh|Puducherry|Andaman and Nicobar Islands|Dadra and Nagar Haveli|Daman and Diu|Lakshadweep|Ladakh|Jammu and Kashmir)\b'
        ]
        
        for i, pattern in enumerate(location_patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if i == 0:  # Cities
                    info['city'] = match.group(0).title()
                else:  # States
                    info['state'] = match.group(0).title()
        
        # Contact person extraction
        contact_patterns = [
            r'(?:Contact Person|Contact|Mr\.|Ms\.|Mrs\.)\s*:?\s*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'(?:Director|Manager|Owner|CEO|MD)\s*:?\s*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'\b(Mr\.|Ms\.|Mrs\.)\s+([A-Z][a-z]+ [A-Z][a-z]+)'
        ]
        
        for pattern in contact_patterns:
            match = re.search(pattern, text)
            if match:
                info['contact_person'] = match.group(-1).strip()
                break
        
        return info
    
    def _advanced_scrape_exportersindia(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Advanced ExportersIndia scraping"""
        companies = []
        base_url = "https://www.exportersindia.com"
        
        try:
            search_urls = [
                f"{base_url}/search/{quote(search_term)}-buyers.html",
                f"{base_url}/buyers/{quote(search_term.replace(' ', '-'))}.html",
                f"{base_url}/importers/{quote(search_term.replace(' ', '-'))}.html"
            ]
            
            for search_url in search_urls:
                if len(companies) >= limit:
                    break
                
                try:
                    response = self._make_advanced_request(search_url)
                    if response:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        extracted_companies = self._extract_companies_from_content(soup, base_url, 'ExportersIndia')
                        companies.extend(extracted_companies[:limit - len(companies)])
                    
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    self.logger.error(f"Error in ExportersIndia scraping: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error in ExportersIndia advanced scraping: {str(e)}")
        
        return companies[:limit]
    
    def _scrape_tofler_companies(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape companies from Tofler business intelligence platform"""
        companies = []
        base_url = "https://www.tofler.in"
        
        try:
            search_url = f"{base_url}/search?q={quote(search_term)}"
            response = self._make_advanced_request(search_url)
            
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                company_cards = soup.find_all(['div', 'article'], class_=re.compile(r'company|result|card'))
                
                for card in company_cards[:limit]:
                    try:
                        company_data = self._extract_tofler_company_details(card, base_url)
                        if company_data:
                            companies.append(company_data)
                    except Exception as e:
                        continue
            
        except Exception as e:
            self.logger.error(f"Error scraping Tofler: {str(e)}")
        
        return companies[:limit]
    
    def _scrape_alibaba_buyers(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Scrape international buyers from Alibaba"""
        companies = []
        base_url = "https://www.alibaba.com"
        
        try:
            search_url = f"{base_url}/trade/search?SearchText={quote(search_term)}&IndexArea=product_en&CatId=&company=y"
            response = self._make_advanced_request(search_url)
            
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                company_elements = soup.find_all(['div', 'li'], class_=re.compile(r'company|supplier|manufacturer'))
                
                for element in company_elements[:limit]:
                    try:
                        company_data = self._extract_alibaba_company_details(element, base_url)
                        if company_data:
                            companies.append(company_data)
                    except Exception as e:
                        continue
            
        except Exception as e:
            self.logger.error(f"Error scraping Alibaba: {str(e)}")
        
        return companies[:limit]
    
    def _extract_tofler_company_details(self, card, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract company details from Tofler card"""
        try:
            company_data = {
                'source': 'Tofler (Business Intelligence)',
                'company_name': '',
                'contact_person': '',
                'city': '',
                'state': '',
                'country': 'India',
                'phone': '',
                'email': '',
                'website': '',
                'description': '',
                'products': '',
                'company_url': '',
                'cin_number': '',
                'annual_revenue': '',
                'employee_count': ''
            }
            
            # Extract company name
            name_elem = card.find(['h3', 'h4', 'a'], class_=re.compile(r'name|title|company'))
            if name_elem:
                company_data['company_name'] = name_elem.get_text(strip=True)
                if name_elem.get('href'):
                    company_data['company_url'] = urljoin(base_url, name_elem['href'])
            
            # Extract all text for smart parsing
            all_text = card.get_text()
            extracted_info = self._smart_extract_company_info(all_text)
            company_data.update(extracted_info)
            
            # Extract business metrics if available
            revenue_match = re.search(r'Revenue:?\s*₹?([0-9,\s]+(?:crore|lakh))', all_text, re.IGNORECASE)
            if revenue_match:
                company_data['annual_revenue'] = revenue_match.group(1)
            
            employee_match = re.search(r'Employee:?\s*(\d+[-\s]*\d*)', all_text, re.IGNORECASE)
            if employee_match:
                company_data['employee_count'] = employee_match.group(1)
            
            return company_data if company_data['company_name'] else None
            
        except Exception as e:
            return None
    
    def _extract_alibaba_company_details(self, element, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract company details from Alibaba element"""
        try:
            company_data = {
                'source': 'Alibaba International',
                'company_name': '',
                'contact_person': '',
                'city': '',
                'state': '',
                'country': '',
                'phone': '',
                'email': '',
                'website': '',
                'description': '',
                'products': '',
                'company_url': '',
                'trade_type': 'International Buyer'
            }
            
            # Extract company name
            name_elem = element.find(['h3', 'h4', 'a'], class_=re.compile(r'company|supplier|name'))
            if name_elem:
                company_data['company_name'] = name_elem.get_text(strip=True)
                if name_elem.get('href'):
                    company_data['company_url'] = urljoin(base_url, name_elem['href'])
            
            # Extract location (international)
            location_elem = element.find(text=re.compile(r'Country|Location'))
            if location_elem:
                location_parent = location_elem.parent
                if location_parent:
                    location_text = location_parent.get_text()
                    # Extract country from international location
                    countries = ['India', 'USA', 'UK', 'Germany', 'France', 'Italy', 'Spain', 'UAE', 'Singapore', 'Malaysia', 'Thailand', 'Vietnam', 'China', 'Japan', 'South Korea']
                    for country in countries:
                        if country.lower() in location_text.lower():
                            company_data['country'] = country
                            break
            
            # Extract all text for additional info
            all_text = element.get_text()
            extracted_info = self._smart_extract_company_info(all_text)
            company_data.update(extracted_info)
            
            return company_data if company_data['company_name'] else None
            
        except Exception as e:
            return None

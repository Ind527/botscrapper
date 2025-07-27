import requests
from bs4 import BeautifulSoup
import time
import random
import re
from urllib.parse import urljoin, quote
import logging
from typing import List, Dict, Any, Optional
import trafilatura

class TurmericBuyerScraper:
    """Web scraper for collecting turmeric buyer company data"""
    
    def __init__(self, delay_seconds: int = 3):
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        
        # Rotate user agents to avoid detection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        self._update_headers()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _update_headers(self):
        """Update session headers with random user agent"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })
    
    def scrape_source(self, source: str, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Scrape data from a specific source"""
        try:
            if source == 'tradeindia':
                return self._scrape_tradeindia(search_term, limit)
            elif source == 'indiamart':
                return self._scrape_indiamart(search_term, limit)
            elif source == 'exportersindia':
                return self._scrape_exportersindia(search_term, limit)
            elif source == 'sample_data':
                return self._generate_sample_data(search_term, limit)
            else:
                self.logger.warning(f"Unknown source: {source}")
                return []
        except Exception as e:
            self.logger.error(f"Error scraping {source}: {str(e)}")
            # Fallback to sample data if scraping fails
            return self._generate_sample_data(search_term, min(limit, 10))
    
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

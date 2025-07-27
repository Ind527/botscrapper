import requests
from bs4 import BeautifulSoup
import time
import random
import re
from urllib.parse import urljoin, quote
import logging
from typing import List, Dict, Any, Optional

class TurmericBuyerScraper:
    """Web scraper for collecting turmeric buyer company data"""
    
    def __init__(self, delay_seconds: int = 3):
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def scrape_source(self, source: str, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Scrape data from a specific source"""
        try:
            if source == 'tradeindia':
                return self._scrape_tradeindia(search_term, limit)
            elif source == 'indiamart':
                return self._scrape_indiamart(search_term, limit)
            elif source == 'exportersindia':
                return self._scrape_exportersindia(search_term, limit)
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
        """Make HTTP request with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                else:
                    self.logger.error(f"All retry attempts failed for URL: {url}")
        return None

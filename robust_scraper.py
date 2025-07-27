import requests
import cloudscraper
from bs4 import BeautifulSoup
import re
import random
import time
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse, quote
import json
from fake_useragent import UserAgent
from retrying import retry
import pandas as pd
import sqlite3
from datetime import datetime
import os

class RobustTurmericScraper:
    """Robust Turmeric Buyer Scraper dengan Error Handling Lengkap"""
    
    def __init__(self, delay_seconds: float = 0.1):
        self.delay_seconds = delay_seconds
        
        # Setup logging system
        self.setup_logging()
        
        # Initialize user agent rotation
        self.ua = UserAgent()
        
        # Proxy rotation (free proxies - dalam implementasi nyata gunakan premium)
        self.proxy_list = [
            None,  # No proxy
            # Tambahkan proxy premium di sini jika tersedia
        ]
        self.current_proxy_index = 0
        
        # Session dengan cloudscraper untuk bypass cloudflare
        self.session = cloudscraper.create_scraper()
        
        # Statistik scraping
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'companies_found': 0,
            'retries_used': 0
        }
        
        # Cache untuk menghindari duplicate scraping
        self.scraped_urls = set()
        
        # Data sources dengan fallback mechanism
        self.data_sources = [
            {
                'name': 'tradeindia',
                'base_url': 'https://www.tradeindia.com',
                'search_path': '/Seller/search.html',
                'selectors': {
                    'companies': '.seller_detail, .company-info',
                    'name': '.seller_name a, .company_name',
                    'email': '.email, [href^="mailto:"]',
                    'phone': '.phone, .mobile',
                    'location': '.seller_location, .location'
                },
                'enabled': True,
                'priority': 1
            },
            {
                'name': 'indiamart',
                'base_url': 'https://dir.indiamart.com',
                'search_path': '/search.mp',
                'selectors': {
                    'companies': '.company-name-text, .lst',
                    'name': 'a, .company_name',
                    'email': '.email, [href^="mailto:"]',
                    'phone': '.contact-no, .mobile',
                    'location': '.city-name, .location'
                },
                'enabled': True,
                'priority': 2
            },
            {
                'name': 'exportersindia',
                'base_url': 'https://www.exportersindia.com',
                'search_path': '/search',
                'selectors': {
                    'companies': '.company_profile, .search-result',
                    'name': '.company_name, h3',
                    'email': '.email, [href^="mailto:"]',
                    'phone': '.phone, .mobile',
                    'location': '.location, .city'
                },
                'enabled': True,
                'priority': 3
            }
        ]
        
        # Keywords AI-generated untuk search yang lebih pintar
        self.ai_keywords = self._generate_smart_keywords()
        
    def setup_logging(self):
        """Setup sistem logging untuk debugging"""
        # Create logs directory
        os.makedirs('logs', exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/scraper_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Robust Turmeric Scraper initialized")
    
    def _generate_smart_keywords(self) -> List[str]:
        """Generate smart keywords untuk pencarian yang lebih efektif"""
        base_terms = ['turmeric', 'curcuma longa', 'haldi', 'turmeric powder']
        business_types = ['importer', 'buyer', 'wholesaler', 'distributor', 'trader']
        locations = ['india', 'usa', 'uk', 'germany', 'france', 'italy', 'spain']
        
        keywords = []
        
        # Kombinasi base terms dengan business types
        for base in base_terms:
            for business in business_types:
                keywords.extend([
                    f"{base} {business}",
                    f"bulk {base} {business}",
                    f"{base} {business} company",
                    f"international {base} {business}"
                ])
        
        # Tambah lokasi spesifik
        for location in locations:
            keywords.extend([
                f"turmeric importer {location}",
                f"spice importer {location}"
            ])
        
        return list(set(keywords))[:50]  # Limit 50 keywords unik
    
    def get_next_proxy(self) -> Optional[Dict]:
        """Rotasi proxy untuk menghindari blocking"""
        if not self.proxy_list:
            return None
            
        proxy = self.proxy_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        if proxy:
            return {
                'http': proxy,
                'https': proxy
            }
        return None
    
    def get_random_headers(self) -> Dict[str, str]:
        """Generate random headers untuk menghindari detection"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def make_request(self, url: str, params: Dict = None) -> Optional[str]:
        """Make HTTP request dengan retry mechanism dan error handling"""
        try:
            self.stats['total_requests'] += 1
            
            # Skip jika URL sudah pernah di-scrape
            url_key = f"{url}?{str(params)}" if params else url
            if url_key in self.scraped_urls:
                self.logger.debug(f"Skipping already scraped URL: {url}")
                return None
            
            # Setup headers dan proxy
            headers = self.get_random_headers()
            proxies = self.get_next_proxy()
            
            # Update session headers
            self.session.headers.update(headers)
            
            # Make request
            response = self.session.get(
                url, 
                params=params,
                proxies=proxies,
                timeout=15,
                allow_redirects=True
            )
            
            # Check response
            if response.status_code == 200:
                self.stats['successful_requests'] += 1
                self.scraped_urls.add(url_key)
                self.logger.info(f"✅ Success: {url} - Status: {response.status_code}")
                return response.text
            
            elif response.status_code in [403, 429]:
                self.logger.warning(f"⚠️  Rate limited or blocked: {url} - Status: {response.status_code}")
                time.sleep(random.uniform(2, 5))  # Wait longer for rate limits
                raise Exception(f"Rate limited: {response.status_code}")
            
            elif response.status_code == 404:
                self.logger.warning(f"⚠️  Not found: {url} - Status: {response.status_code}")
                return None
            
            else:
                self.logger.error(f"❌ HTTP Error: {url} - Status: {response.status_code}")
                raise Exception(f"HTTP {response.status_code}")
                
        except Exception as e:
            self.stats['failed_requests'] += 1
            self.stats['retries_used'] += 1
            self.logger.error(f"❌ Request failed: {url} - Error: {str(e)}")
            
            # Random delay sebelum retry
            time.sleep(random.uniform(1, 3))
            raise e
        
        return None
    
    def extract_company_data(self, html_content: str, source_config: Dict, search_term: str) -> List[Dict[str, Any]]:
        """Extract company data dengan error handling dan validation"""
        try:
            if not html_content:
                return []
            
            soup = BeautifulSoup(html_content, 'html.parser')
            companies = []
            
            # Check if content is relevant untuk turmeric
            text_content = soup.get_text().lower()
            turmeric_keywords = ['turmeric', 'haldi', 'curcuma', 'spice', 'herb']
            
            if not any(keyword in text_content for keyword in turmeric_keywords):
                self.logger.debug(f"Content not relevant for turmeric: {source_config['name']}")
                return []
            
            # Extract companies menggunakan selectors
            company_elements = soup.select(source_config['selectors']['companies'])
            
            for element in company_elements[:20]:  # Limit 20 per page
                try:
                    company_data = self._extract_single_company(element, source_config, search_term)
                    if company_data and self._validate_company_data(company_data):
                        companies.append(company_data)
                        self.stats['companies_found'] += 1
                        
                except Exception as e:
                    self.logger.debug(f"Error extracting single company: {str(e)}")
                    continue
            
            self.logger.info(f"✅ Extracted {len(companies)} companies from {source_config['name']}")
            return companies
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting companies from {source_config['name']}: {str(e)}")
            return []
    
    def _extract_single_company(self, element, source_config: Dict, search_term: str) -> Optional[Dict[str, Any]]:
        """Extract data untuk single company dengan error handling"""
        try:
            # Extract company name
            name_element = element.select_one(source_config['selectors']['name'])
            company_name = name_element.get_text().strip() if name_element else ""
            
            if not company_name or len(company_name) < 3:
                return None
            
            # Extract email
            email_element = element.select_one(source_config['selectors']['email'])
            email = ""
            if email_element:
                if email_element.get('href'):
                    email = email_element.get('href').replace('mailto:', '')
                else:
                    email_text = email_element.get_text()
                    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', email_text)
                    if email_match:
                        email = email_match.group()
            
            # Extract phone
            phone_element = element.select_one(source_config['selectors']['phone'])
            phone = ""
            if phone_element:
                phone_text = phone_element.get_text()
                phone_match = re.search(r'[\+]?[1-9]?[\d\s\-\(\)]{10,20}', phone_text)
                if phone_match:
                    phone = phone_match.group().strip()
            
            # Extract location
            location_element = element.select_one(source_config['selectors']['location'])
            location = location_element.get_text().strip() if location_element else ""
            
            # Extract additional contact info from text
            element_text = element.get_text()
            if not email:
                email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', element_text)
                if email_match:
                    email = email_match.group()
            
            if not phone:
                phone_match = re.search(r'[\+]?[1-9]?[\d\s\-\(\)]{10,20}', element_text)
                if phone_match:
                    phone = phone_match.group().strip()
            
            # Calculate relevance score
            relevance_score = self._calculate_relevance(element_text, search_term)
            
            # Create company data
            company_data = {
                'company_name': company_name,
                'email': email,
                'phone': phone,
                'website': self._extract_website(element_text),
                'location': location,
                'source': source_config['name'],
                'search_term': search_term,
                'relevance_score': relevance_score,
                'scraped_at': datetime.now().isoformat(),
                'raw_text': element_text[:500]  # Store sample text
            }
            
            return company_data
            
        except Exception as e:
            self.logger.debug(f"Error in _extract_single_company: {str(e)}")
            return None
    
    def _extract_website(self, text: str) -> str:
        """Extract website dari text"""
        website_patterns = [
            r'https?://[^\s<>"]+',
            r'www\.[^\s<>"]+\.[a-z]{2,}',
            r'[a-zA-Z0-9-]+\.(com|org|net|co\.in|co\.uk|de|fr|it)'
        ]
        
        for pattern in website_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                website = match.group()
                if not website.startswith('http'):
                    website = 'https://' + website
                return website
        
        return ""
    
    def _calculate_relevance(self, text: str, search_term: str) -> float:
        """Calculate relevance score berdasarkan content"""
        text_lower = text.lower()
        search_lower = search_term.lower()
        
        score = 0.0
        
        # Search term presence
        if search_lower in text_lower:
            score += 0.3
        
        # Turmeric related keywords
        turmeric_keywords = ['turmeric', 'haldi', 'curcuma', 'spice', 'herb']
        for keyword in turmeric_keywords:
            if keyword in text_lower:
                score += 0.15
        
        # Business keywords
        business_keywords = ['import', 'export', 'wholesale', 'trade', 'distribute', 'supply']
        for keyword in business_keywords:
            if keyword in text_lower:
                score += 0.1
        
        # Contact info presence
        if '@' in text:
            score += 0.2
        if re.search(r'\+?\d{1,4}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{1,10}', text):
            score += 0.2
        
        return min(score, 1.0)
    
    def _validate_company_data(self, company_data: Dict[str, Any]) -> bool:
        """Validate company data quality"""
        # Must have company name
        if not company_data.get('company_name') or len(company_data['company_name']) < 3:
            return False
        
        # Must have at least email or phone
        if not company_data.get('email') and not company_data.get('phone'):
            return False
        
        # Email validation
        if company_data.get('email'):
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            if not re.match(email_pattern, company_data['email']):
                return False
        
        # Phone validation
        if company_data.get('phone'):
            if len(re.sub(r'[^\d]', '', company_data['phone'])) < 7:
                return False
        
        # Relevance score minimum
        if company_data.get('relevance_score', 0) < 0.3:
            return False
        
        return True
    
    def scrape_source(self, source_config: Dict, search_term: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        """Scrape single source dengan pagination dan error handling"""
        all_companies = []
        
        try:
            for page in range(1, max_pages + 1):
                self.logger.info(f"🔍 Scraping {source_config['name']} - Term: {search_term} - Page: {page}")
                
                # Build search URL
                search_params = {
                    'ss': search_term,
                    'page': page
                }
                
                search_url = f"{source_config['base_url']}{source_config['search_path']}"
                
                # Make request dengan retry
                html_content = self.make_request(search_url, search_params)
                
                if html_content:
                    companies = self.extract_company_data(html_content, source_config, search_term)
                    all_companies.extend(companies)
                    
                    # Break jika tidak ada hasil di page ini
                    if not companies:
                        self.logger.info(f"No results on page {page}, stopping pagination")
                        break
                else:
                    self.logger.warning(f"Failed to get content for page {page}")
                    break
                
                # Delay antar page
                time.sleep(random.uniform(self.delay_seconds, self.delay_seconds * 2))
                
        except Exception as e:
            self.logger.error(f"❌ Error scraping {source_config['name']} for {search_term}: {str(e)}")
        
        return all_companies
    
    def scrape_buyers(self, search_terms: List[str], target_count: int = 50) -> List[Dict[str, Any]]:
        """Main scraping method dengan fallback dan error recovery"""
        all_companies = []
        
        self.logger.info(f"🚀 Starting robust scraping for {len(search_terms)} terms, target: {target_count}")
        
        # Reset stats
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'companies_found': 0,
            'retries_used': 0
        }
        
        # Use AI keywords jika search terms kosong
        if not search_terms or search_terms == ['']:
            search_terms = self.ai_keywords[:10]
            self.logger.info("Using AI-generated keywords for search")
        
        # Scrape each source dengan fallback
        active_sources = [s for s in self.data_sources if s['enabled']]
        active_sources.sort(key=lambda x: x['priority'])
        
        for search_term in search_terms:
            if len(all_companies) >= target_count:
                break
                
            self.logger.info(f"🔍 Processing search term: {search_term}")
            
            for source_config in active_sources:
                if len(all_companies) >= target_count:
                    break
                
                try:
                    companies = self.scrape_source(source_config, search_term)
                    
                    if companies:
                        all_companies.extend(companies)
                        self.logger.info(f"✅ {source_config['name']}: Found {len(companies)} companies")
                    else:
                        self.logger.warning(f"⚠️  {source_config['name']}: No companies found for '{search_term}'")
                        
                        # Auto retry dengan keyword variation jika hasil kosong
                        if search_term == search_terms[0]:  # Hanya retry pada search term pertama
                            retry_terms = [
                                f"bulk {search_term}",
                                f"{search_term} importer",
                                f"{search_term} wholesale"
                            ]
                            
                            for retry_term in retry_terms:
                                if len(all_companies) >= target_count:
                                    break
                                    
                                self.logger.info(f"🔄 Auto retry with: {retry_term}")
                                retry_companies = self.scrape_source(source_config, retry_term)
                                if retry_companies:
                                    all_companies.extend(retry_companies)
                                    self.logger.info(f"✅ Retry success: Found {len(retry_companies)} companies")
                                    break
                
                except Exception as e:
                    self.logger.error(f"❌ Error with {source_config['name']}: {str(e)}")
                    continue  # Fallback ke source berikutnya
                
                # Delay antar source
                time.sleep(random.uniform(0.5, 1.5))
        
        # Remove duplicates
        unique_companies = self._remove_duplicates(all_companies)
        
        # Filter high quality results
        quality_companies = [c for c in unique_companies if c.get('relevance_score', 0) >= 0.4]
        
        # Sort by relevance score
        quality_companies.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        # Add validation status
        for company in quality_companies:
            company['status_verified'] = 'VALID' if self._validate_company_data(company) else 'PENDING'
        
        # Take top results
        final_results = quality_companies[:target_count]
        
        # Log statistics
        self.log_scraping_stats(final_results)
        
        # Save results
        self.save_results(final_results)
        
        return final_results
    
    def _remove_duplicates(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate companies berdasarkan multiple criteria"""
        seen = set()
        unique_companies = []
        
        for company in companies:
            # Create fingerprint berdasarkan multiple fields
            name = company.get('company_name', '').lower().strip()
            email = company.get('email', '').lower().strip()
            phone = re.sub(r'[^\d]', '', company.get('phone', ''))
            
            fingerprints = [
                name,
                email,
                phone,
                f"{name}_{email}",
                f"{name}_{phone}"
            ]
            
            # Check duplicates
            is_duplicate = any(fp in seen for fp in fingerprints if fp)
            
            if not is_duplicate and name:
                for fp in fingerprints:
                    if fp:
                        seen.add(fp)
                unique_companies.append(company)
        
        self.logger.info(f"🔄 Removed {len(companies) - len(unique_companies)} duplicates")
        return unique_companies
    
    def log_scraping_stats(self, final_results: List[Dict[str, Any]]):
        """Log scraping statistics untuk monitoring"""
        stats_msg = f"""
📊 SCRAPING STATISTICS:
- Total Requests: {self.stats['total_requests']}
- Successful: {self.stats['successful_requests']}
- Failed: {self.stats['failed_requests']}
- Retries Used: {self.stats['retries_used']}
- Companies Found: {len(final_results)}
- Success Rate: {(self.stats['successful_requests']/max(self.stats['total_requests'], 1))*100:.1f}%
        """
        self.logger.info(stats_msg)
    
    def save_results(self, companies: List[Dict[str, Any]]):
        """Save results ke file dengan error handling"""
        try:
            if not companies:
                self.logger.warning("No companies to save")
                return
            
            # Save to CSV
            df = pd.DataFrame(companies)
            csv_filename = f'turmeric_buyers_robust_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(csv_filename, index=False)
            self.logger.info(f"💾 Saved {len(companies)} companies to {csv_filename}")
            
            # Save to SQLite database
            db_filename = 'turmeric_buyers_robust.db'
            conn = sqlite3.connect(db_filename)
            df.to_sql('buyers', conn, if_exists='append', index=False)
            
            # Create indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_company_name ON buyers(company_name)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_email ON buyers(email)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_relevance ON buyers(relevance_score)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_scraped_at ON buyers(scraped_at)')
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"💾 Saved to database: {db_filename}")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {str(e)}")

# Compatibility wrapper untuk app.py yang sudah ada
class HyperTurmericBuyerScraper(RobustTurmericScraper):
    """Wrapper untuk compatibility dengan existing app"""
    
    def __init__(self, delay_seconds: float = 0.1):
        super().__init__(delay_seconds)
        self.logger.info("🚀 Robust Turmeric Scraper initialized with full error handling")
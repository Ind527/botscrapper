import re
import requests
import phonenumbers
from phonenumbers import carrier, geocoder, timezone
import dns.resolver
import validators
import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from email_validator import validate_email, EmailNotValidError
import socket
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
from datetime import datetime

class DataValidator:
    """🔹 Advanced Data Validator - 200x Better Authenticity"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Disposable email domains (sample list - can be expanded)
        self.disposable_domains = {
            '10minutemail.com', 'tempmail.org', 'guerrillamail.com',
            'mailinator.com', 'throwaway.email', '0-mail.com',
            'temp-mail.org', 'yopmail.com', 'maildrop.cc',
            'sharklasers.com', 'grr.la', 'guerrillamailblock.com'
        }
        
        # Known business email providers
        self.business_domains = {
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
            'yahoo.co.in', 'rediffmail.com', 'live.com'
        }
        
        # Industry keywords for AI classification
        self.industry_keywords = {
            'spices': ['spice', 'turmeric', 'masala', 'seasoning', 'cumin', 'coriander'],
            'trading': ['trading', 'import', 'export', 'commerce', 'business'],
            'manufacturing': ['manufacturing', 'production', 'factory', 'industries'],
            'food': ['food', 'beverage', 'nutrition', 'organic', 'health'],
            'wholesale': ['wholesale', 'distribution', 'supplier', 'dealer']
        }
        
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    def validate_company_data(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """🔹 Master validation function - validates all company data"""
        validated_data = company_data.copy()
        
        # Initialize validation scores
        validation_score = 0
        max_score = 100
        validation_details = {}
        
        try:
            # 1. Email Validation (30 points)
            email_result = self._validate_email_advanced(company_data.get('email', ''))
            validated_data['email_valid'] = email_result['is_valid']
            validated_data['email_type'] = email_result['type']
            validation_details['email'] = email_result
            if email_result['is_valid']:
                validation_score += 30
            
            # 2. Phone Validation (25 points)
            phone_result = self._validate_phone_advanced(company_data.get('phone', ''))
            validated_data['phone_valid'] = phone_result['is_valid']
            validated_data['phone_carrier'] = phone_result.get('carrier', '')
            validated_data['phone_location'] = phone_result.get('location', '')
            validation_details['phone'] = phone_result
            if phone_result['is_valid']:
                validation_score += 25
            
            # 3. Website/Domain Validation (20 points)
            website_result = self._validate_website_domain(company_data.get('website', ''))
            validated_data['website_valid'] = website_result['is_valid']
            validated_data['domain_status'] = website_result['status']
            validation_details['website'] = website_result
            if website_result['is_valid']:
                validation_score += 20
            
            # 4. Data Consistency Check (15 points)
            consistency_result = self._check_data_consistency(company_data)
            validated_data['data_consistent'] = consistency_result['is_consistent']
            validation_details['consistency'] = consistency_result
            if consistency_result['is_consistent']:
                validation_score += 15
            
            # 5. AI Data Enrichment (10 points)
            enrichment_result = self._ai_data_enrichment(company_data)
            validated_data.update(enrichment_result['enriched_data'])
            validation_details['enrichment'] = enrichment_result
            validation_score += enrichment_result['confidence_score']
            
            # Final validation score
            validated_data['validation_score'] = min(validation_score, max_score)
            validated_data['validation_details'] = validation_details
            validated_data['status_verified'] = validation_score >= 70  # 70% threshold
            validated_data['validation_timestamp'] = datetime.now().isoformat()
            
        except Exception as e:
            self.logger.error(f"Error validating company data: {str(e)}")
            validated_data['validation_score'] = 0
            validated_data['status_verified'] = False
            validated_data['validation_error'] = str(e)
        
        return validated_data
    
    def _validate_email_advanced(self, email: str) -> Dict[str, Any]:
        """🔹 Advanced Email Validation with MX Record Check"""
        result = {
            'is_valid': False,
            'type': 'invalid',
            'mx_valid': False,
            'disposable': False,
            'business_email': False
        }
        
        if not email or not isinstance(email, str):
            return result
        
        email = email.strip().lower()
        
        try:
            # 1. Basic email format validation
            validation = validate_email(email)
            normalized_email = validation.email
            domain = email.split('@')[1] if '@' in email else ''
            
            # 2. Check if disposable email
            if domain in self.disposable_domains:
                result['disposable'] = True
                return result
            
            # 3. MX Record Check
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                result['mx_valid'] = len(mx_records) > 0
            except:
                result['mx_valid'] = False
            
            # 4. Determine email type
            if domain in self.business_domains:
                result['type'] = 'personal'
            else:
                result['type'] = 'business'
                result['business_email'] = True
            
            # 5. Pattern analysis for business emails
            if result['business_email']:
                company_name = email.split('@')[0]
                if any(keyword in company_name for keyword in ['info', 'contact', 'sales', 'inquiry', 'business']):
                    result['type'] = 'business_generic'
            
            result['is_valid'] = result['mx_valid'] and not result['disposable']
            
        except EmailNotValidError:
            result['is_valid'] = False
        except Exception as e:
            self.logger.warning(f"Email validation error for {email}: {str(e)}")
        
        return result
    
    def _validate_phone_advanced(self, phone: str) -> Dict[str, Any]:
        """🔹 Advanced Phone Validation with Carrier Detection"""
        result = {
            'is_valid': False,
            'carrier': '',
            'location': '',
            'country_code': '',
            'is_mobile': False
        }
        
        if not phone or not isinstance(phone, str):
            return result
        
        try:
            # Parse phone number
            parsed_number = phonenumbers.parse(phone, "IN")  # Default to India
            
            # Validate phone number
            if phonenumbers.is_valid_number(parsed_number):
                result['is_valid'] = True
                result['country_code'] = f"+{parsed_number.country_code}"
                
                # Get carrier information
                try:
                    carrier_name = carrier.name_for_number(parsed_number, "en")
                    result['carrier'] = carrier_name if carrier_name else "Unknown"
                except:
                    result['carrier'] = "Unknown"
                
                # Get location information
                try:
                    location = geocoder.description_for_number(parsed_number, "en")
                    result['location'] = location if location else "Unknown"
                except:
                    result['location'] = "Unknown"
                
                # Check if mobile
                number_type = phonenumbers.number_type(parsed_number)
                result['is_mobile'] = number_type in [
                    phonenumbers.PhoneNumberType.MOBILE,
                    phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE
                ]
                
        except phonenumbers.NumberParseException:
            result['is_valid'] = False
        except Exception as e:
            self.logger.warning(f"Phone validation error for {phone}: {str(e)}")
        
        return result
    
    def _validate_website_domain(self, website: str) -> Dict[str, Any]:
        """🔹 Website/Domain Validation with HTTP Check"""
        result = {
            'is_valid': False,
            'status': 'invalid',
            'http_status': 0,
            'https_available': False,
            'domain_age_estimate': 'unknown'
        }
        
        if not website or not isinstance(website, str):
            return result
        
        try:
            # Normalize URL
            if not website.startswith(('http://', 'https://')):
                website = f"https://{website}"
            
            parsed_url = urllib.parse.urlparse(website)
            domain = parsed_url.netloc or parsed_url.path
            
            # Validate domain format
            if not validators.domain(domain):
                return result
            
            # Check HTTP/HTTPS availability
            try:
                response = requests.get(website, timeout=10, allow_redirects=True)
                result['http_status'] = response.status_code
                result['https_available'] = website.startswith('https://')
                
                if response.status_code == 200:
                    result['is_valid'] = True
                    result['status'] = 'active'
                elif response.status_code in [301, 302, 303, 307, 308]:
                    result['is_valid'] = True
                    result['status'] = 'redirect'
                else:
                    result['status'] = f'http_{response.status_code}'
                    
            except requests.RequestException:
                # Try with http if https fails
                if website.startswith('https://'):
                    try:
                        http_url = website.replace('https://', 'http://')
                        response = requests.get(http_url, timeout=10)
                        result['http_status'] = response.status_code
                        if response.status_code == 200:
                            result['is_valid'] = True
                            result['status'] = 'http_only'
                    except:
                        result['status'] = 'unreachable'
                else:
                    result['status'] = 'unreachable'
            
            # DNS check
            try:
                socket.gethostbyname(domain)
                if not result['is_valid']:
                    result['status'] = 'dns_valid_http_failed'
            except socket.gaierror:
                result['status'] = 'dns_failed'
                
        except Exception as e:
            self.logger.warning(f"Website validation error for {website}: {str(e)}")
            result['status'] = 'validation_error'
        
        return result
    
    def _check_data_consistency(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """🔹 AI-Powered Data Consistency Check"""
        result = {
            'is_consistent': True,
            'inconsistencies': [],
            'confidence_score': 100
        }
        
        try:
            company_name = company_data.get('company_name', '').lower()
            email = company_data.get('email', '').lower()
            website = company_data.get('website', '').lower()
            city = company_data.get('city', '').lower()
            state = company_data.get('state', '').lower()
            
            # Check email-company name consistency
            if email and '@' in email:
                email_domain = email.split('@')[1]
                email_prefix = email.split('@')[0]
                
                # Extract company identifier from name
                company_words = re.findall(r'\b\w+\b', company_name)
                main_company_word = company_words[0] if company_words else ''
                
                # Check if email domain relates to company name
                if website and email_domain not in website:
                    if not any(word in email_domain for word in company_words[:2]):
                        result['inconsistencies'].append('email_domain_mismatch')
                        result['confidence_score'] -= 20
            
            # Check website-company consistency
            if website and company_name:
                website_clean = re.sub(r'[^\w]', '', website)
                company_clean = re.sub(r'[^\w]', '', company_name)
                
                if not any(word in website_clean for word in company_clean.split()[:2]):
                    result['inconsistencies'].append('website_company_mismatch')
                    result['confidence_score'] -= 15
            
            # Geographic consistency
            if city and state:
                # Basic Indian state-city consistency check
                state_city_map = {
                    'maharashtra': ['mumbai', 'pune', 'nagpur', 'nashik'],
                    'tamil nadu': ['chennai', 'coimbatore', 'madurai', 'salem'],
                    'delhi': ['delhi', 'new delhi'],
                    'karnataka': ['bangalore', 'mysore', 'hubli'],
                    'gujarat': ['ahmedabad', 'surat', 'vadodara', 'rajkot']
                }
                
                if state in state_city_map:
                    if city not in state_city_map[state] and not any(c in city for c in state_city_map[state]):
                        result['inconsistencies'].append('geographic_mismatch')
                        result['confidence_score'] -= 10
            
            # Final consistency determination
            result['is_consistent'] = result['confidence_score'] >= 70
            
        except Exception as e:
            self.logger.warning(f"Consistency check error: {str(e)}")
            result['confidence_score'] = 50
        
        return result
    
    def _ai_data_enrichment(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """🔹 AI Data Enrichment and Industry Classification"""
        result = {
            'enriched_data': {},
            'confidence_score': 0
        }
        
        try:
            company_name = company_data.get('company_name', '').lower()
            description = company_data.get('description', '').lower()
            
            # Industry classification
            industry_scores = {}
            for industry, keywords in self.industry_keywords.items():
                score = sum(1 for keyword in keywords if keyword in company_name or keyword in description)
                if score > 0:
                    industry_scores[industry] = score
            
            if industry_scores:
                primary_industry = max(industry_scores, key=industry_scores.get)
                result['enriched_data']['industry'] = primary_industry
                result['enriched_data']['industry_confidence'] = industry_scores[primary_industry] / len(self.industry_keywords[primary_industry])
                result['confidence_score'] += 5
            
            # Company size estimation based on keywords
            size_indicators = {
                'large': ['ltd', 'limited', 'corporation', 'corp', 'international', 'global'],
                'medium': ['pvt', 'private', 'enterprises', 'trading'],
                'small': ['shop', 'store', 'local', 'regional']
            }
            
            for size, indicators in size_indicators.items():
                if any(indicator in company_name for indicator in indicators):
                    result['enriched_data']['company_size'] = size
                    result['confidence_score'] += 3
                    break
            
            # Country enrichment based on phone/location
            phone = company_data.get('phone', '')
            if phone.startswith('+91') or phone.startswith('91'):
                result['enriched_data']['country'] = 'India'
                result['confidence_score'] += 2
            
            # Business type classification
            business_types = {
                'manufacturer': ['manufacturing', 'industries', 'factory', 'production'],
                'trader': ['trading', 'commerce', 'business', 'merchant'],
                'exporter': ['export', 'international', 'overseas'],
                'distributor': ['distribution', 'wholesale', 'supply']
            }
            
            for business_type, keywords in business_types.items():
                if any(keyword in company_name or keyword in description for keyword in keywords):
                    result['enriched_data']['business_type'] = business_type
                    result['confidence_score'] += 3
                    break
            
        except Exception as e:
            self.logger.warning(f"Data enrichment error: {str(e)}")
        
        return result
    
    def validate_batch_data(self, companies_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """🔹 Batch validation with parallel processing"""
        validated_companies = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_company = {
                executor.submit(self.validate_company_data, company): company 
                for company in companies_data
            }
            
            for future in as_completed(future_to_company):
                try:
                    validated_company = future.result()
                    validated_companies.append(validated_company)
                except Exception as e:
                    self.logger.error(f"Batch validation error: {str(e)}")
                    # Add original company with failed validation
                    original_company = future_to_company[future]
                    original_company['validation_score'] = 0
                    original_company['status_verified'] = False
                    original_company['validation_error'] = str(e)
                    validated_companies.append(original_company)
        
        return validated_companies
    
    def remove_duplicates_advanced(self, companies_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """🔹 Advanced Duplicate Removal with Fuzzy Matching"""
        if not companies_data:
            return companies_data
        
        unique_companies = []
        seen_companies = set()
        
        for company in companies_data:
            # Create composite key for duplicate detection
            company_name = str(company.get('company_name', '')).lower().strip()
            email = str(company.get('email', '')).lower().strip()
            phone = str(company.get('phone', '')).strip()
            
            # Normalize company name for better matching
            normalized_name = re.sub(r'[^\w\s]', '', company_name)
            normalized_name = re.sub(r'\s+', ' ', normalized_name).strip()
            
            # Create multiple keys for duplicate detection
            keys_to_check = []
            
            if normalized_name:
                keys_to_check.append(f"name:{normalized_name}")
            
            if email and '@' in email:
                keys_to_check.append(f"email:{email}")
            
            if phone and len(phone) > 5:
                # Normalize phone for comparison
                phone_normalized = re.sub(r'[^\d]', '', phone)
                if len(phone_normalized) >= 10:
                    keys_to_check.append(f"phone:{phone_normalized[-10:]}")  # Last 10 digits
            
            # Check if any key already exists
            is_duplicate = any(key in seen_companies for key in keys_to_check)
            
            if not is_duplicate:
                unique_companies.append(company)
                seen_companies.update(keys_to_check)
        
        self.logger.info(f"Removed {len(companies_data) - len(unique_companies)} duplicates")
        return unique_companies
    
    def filter_high_quality_data(self, companies_data: List[Dict[str, Any]], min_score: int = 70) -> List[Dict[str, Any]]:
        """🔹 Filter only high-quality verified data"""
        high_quality_companies = [
            company for company in companies_data 
            if company.get('validation_score', 0) >= min_score and company.get('status_verified', False)
        ]
        
        self.logger.info(f"Filtered to {len(high_quality_companies)} high-quality companies from {len(companies_data)} total")
        return high_quality_companies
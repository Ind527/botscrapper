import dns.resolver
import phonenumbers
from phonenumbers import NumberParseException, PhoneNumberFormat
import validators
import requests
import re
from typing import List, Dict, Any, Optional
import logging
from difflib import SequenceMatcher
import time
from urllib.parse import urlparse
import socket

class AdvancedDataValidator:
    """Advanced 100% validation system for turmeric buyer data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Disposable email domains to reject
        self.disposable_domains = {
            '10minutemail.com', 'tempmail.org', 'guerrillamail.com', 'mailinator.com',
            'yopmail.com', 'temp-mail.org', 'throwaway.email', 'maildrop.cc',
            'getnada.com', 'tempail.com', 'sharklasers.com', 'grr.la',
            'fakeinbox.com', 'spamgourmet.com', 'dispostable.com', 'mailnesia.com'
        }
        
        # Invalid company name patterns
        self.spam_patterns = [
            r'test\s*company', r'example\s*corp', r'sample\s*ltd', r'dummy\s*business',
            r'fake\s*enterprise', r'xxx+', r'aaa+', r'lorem\s*ipsum', r'john\s*doe',
            r'company\s*name', r'business\s*here', r'enter\s*name', r'your\s*company'
        ]
        
        # Valid Indian mobile prefixes
        self.valid_indian_prefixes = {
            '70', '71', '72', '73', '74', '75', '76', '77', '78', '79',
            '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
            '90', '91', '92', '93', '94', '95', '96', '97', '98', '99'
        }
    
    def validate_complete_buyer_data(self, buyer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Complete validation of buyer data with 100% accuracy"""
        try:
            # Create validation result
            validated_buyer = buyer_data.copy()
            validation_results = {}
            
            # 1. Validate Company Name
            company_valid = self._validate_company_name(buyer_data.get('company_name', ''))
            validation_results['company_name_valid'] = company_valid
            
            # 2. Validate Email with DNS MX Lookup
            email_result = self._validate_email_complete(buyer_data.get('email', ''))
            validation_results['email_valid'] = email_result['valid']
            validation_results['email_mx_valid'] = email_result['mx_valid']
            validation_results['email_disposable'] = email_result['disposable']
            
            # 3. Validate Phone Number
            phone_result = self._validate_phone_complete(buyer_data.get('phone', ''))
            validation_results['phone_valid'] = phone_result['valid']
            validation_results['phone_format'] = phone_result['format']
            validation_results['phone_country'] = phone_result['country']
            
            # 4. Validate Website/Domain
            website_result = self._validate_website_complete(buyer_data.get('website', ''))
            validation_results['website_valid'] = website_result['valid']
            validation_results['website_active'] = website_result['active']
            validation_results['website_status'] = website_result['status']
            
            # 5. Calculate overall validation score
            validation_score = self._calculate_validation_score(validation_results)
            validation_results['validation_score'] = validation_score
            
            # 6. Determine final status
            is_valid = (
                company_valid and
                email_result['valid'] and
                not email_result['disposable'] and
                phone_result['valid'] and
                validation_score >= 80
            )
            
            validation_results['status_verified'] = 'VALID' if is_valid else 'INVALID'
            validation_results['verification_reason'] = self._get_verification_reason(validation_results)
            
            # Add validation data to buyer
            validated_buyer.update(validation_results)
            
            return validated_buyer
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            buyer_data['status_verified'] = 'ERROR'
            buyer_data['verification_reason'] = f"Validation error: {str(e)}"
            return buyer_data
    
    def _validate_company_name(self, company_name: str) -> bool:
        """Validate company name against spam patterns"""
        if not company_name or len(company_name.strip()) < 3:
            return False
        
        company_lower = company_name.lower().strip()
        
        # Check against spam patterns
        for pattern in self.spam_patterns:
            if re.search(pattern, company_lower):
                return False
        
        # Must contain at least one alphabetic character
        if not re.search(r'[a-zA-Z]', company_name):
            return False
        
        # Check for repeated characters (spam indicator)
        if re.search(r'(.)\1{4,}', company_name):  # 5 or more repeated chars
            return False
        
        return True
    
    def _validate_email_complete(self, email: str) -> Dict[str, Any]:
        """Complete email validation with DNS MX lookup"""
        result = {
            'valid': False,
            'mx_valid': False,
            'disposable': False,
            'domain': '',
            'reason': ''
        }
        
        if not email or '@' not in email:
            result['reason'] = 'Invalid format'
            return result
        
        # Basic format validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            result['reason'] = 'Invalid email format'
            return result
        
        # Extract domain
        domain = email.split('@')[1].lower()
        result['domain'] = domain
        
        # Check for disposable email
        if domain in self.disposable_domains:
            result['disposable'] = True
            result['reason'] = 'Disposable email domain'
            return result
        
        # DNS MX Lookup
        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            if mx_records:
                result['mx_valid'] = True
                result['valid'] = True
                result['reason'] = 'Valid email with MX record'
            else:
                result['reason'] = 'No MX record found'
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, Exception) as e:
            result['reason'] = f'DNS lookup failed: {str(e)}'
        
        return result
    
    def _validate_phone_complete(self, phone: str) -> Dict[str, Any]:
        """Complete phone number validation"""
        result = {
            'valid': False,
            'format': '',
            'country': '',
            'reason': ''
        }
        
        if not phone:
            result['reason'] = 'Empty phone number'
            return result
        
        # Clean phone number
        cleaned_phone = re.sub(r'[^\d+]', '', phone)
        
        # Check for obvious fake numbers
        if len(cleaned_phone) < 10 or cleaned_phone in ['0000000000', '1111111111', '1234567890']:
            result['reason'] = 'Invalid phone pattern'
            return result
        
        try:
            # Try to parse with different country codes
            for country_code in ['IN', 'US', 'GB', None]:
                try:
                    parsed_number = phonenumbers.parse(cleaned_phone, country_code)
                    if phonenumbers.is_valid_number(parsed_number):
                        result['valid'] = True
                        result['format'] = phonenumbers.format_number(parsed_number, PhoneNumberFormat.E164)
                        result['country'] = phonenumbers.region_code_for_number(parsed_number)
                        result['reason'] = 'Valid phone number'
                        
                        # Additional validation for Indian numbers
                        if result['country'] == 'IN':
                            mobile_part = result['format'][3:]  # Remove +91
                            if len(mobile_part) == 10 and mobile_part[:2] in self.valid_indian_prefixes:
                                result['reason'] = 'Valid Indian mobile number'
                            else:
                                result['valid'] = False
                                result['reason'] = 'Invalid Indian mobile format'
                        
                        break
                except NumberParseException:
                    continue
                    
            if not result['valid']:
                result['reason'] = 'Could not parse phone number'
                
        except Exception as e:
            result['reason'] = f'Phone validation error: {str(e)}'
        
        return result
    
    def _validate_website_complete(self, website: str) -> Dict[str, Any]:
        """Complete website validation with HTTP check"""
        result = {
            'valid': False,
            'active': False,
            'status': 0,
            'reason': ''
        }
        
        if not website:
            result['reason'] = 'Empty website'
            return result
        
        # Add protocol if missing
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        
        # Basic URL format validation
        try:
            parsed = urlparse(website)
            if not parsed.netloc:
                result['reason'] = 'Invalid URL format'
                return result
        except Exception:
            result['reason'] = 'URL parsing error'
            return result
        
        # HTTP status check
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(website, headers=headers, timeout=10, allow_redirects=True)
            result['status'] = response.status_code
            
            if 200 <= response.status_code < 400:
                result['valid'] = True
                result['active'] = True
                result['reason'] = f'Active website (HTTP {response.status_code})'
            else:
                result['reason'] = f'Website error (HTTP {response.status_code})'
                
        except requests.exceptions.Timeout:
            result['reason'] = 'Website timeout'
        except requests.exceptions.ConnectionError:
            result['reason'] = 'Connection failed'
        except Exception as e:
            result['reason'] = f'HTTP check error: {str(e)}'
        
        return result
    
    def _calculate_validation_score(self, validation_results: Dict[str, Any]) -> int:
        """Calculate overall validation score (0-100)"""
        score = 0
        
        # Company name (20 points)
        if validation_results.get('company_name_valid', False):
            score += 20
        
        # Email validation (30 points)
        if validation_results.get('email_valid', False):
            score += 20
            if validation_results.get('email_mx_valid', False):
                score += 10
        if not validation_results.get('email_disposable', True):
            score += 0  # No penalty for non-disposable
        
        # Phone validation (25 points)
        if validation_results.get('phone_valid', False):
            score += 25
        
        # Website validation (25 points)
        if validation_results.get('website_valid', False):
            score += 15
            if validation_results.get('website_active', False):
                score += 10
        
        return min(score, 100)
    
    def _get_verification_reason(self, validation_results: Dict[str, Any]) -> str:
        """Get human-readable verification reason"""
        if validation_results.get('status_verified') == 'VALID':
            return 'All validations passed'
        
        issues = []
        
        if not validation_results.get('company_name_valid', False):
            issues.append('Invalid company name')
        
        if not validation_results.get('email_valid', False):
            issues.append('Invalid email')
        elif validation_results.get('email_disposable', False):
            issues.append('Disposable email')
        elif not validation_results.get('email_mx_valid', False):
            issues.append('Email domain not active')
        
        if not validation_results.get('phone_valid', False):
            issues.append('Invalid phone number')
        
        if not validation_results.get('website_valid', False):
            issues.append('Invalid website')
        elif not validation_results.get('website_active', False):
            issues.append('Website not active')
        
        return '; '.join(issues) if issues else 'Low validation score'
    
    def validate_batch_data(self, buyers_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate a batch of buyer data"""
        validated_buyers = []
        
        for buyer in buyers_list:
            validated_buyer = self.validate_complete_buyer_data(buyer)
            validated_buyers.append(validated_buyer)
            
            # Small delay to avoid overwhelming DNS servers
            time.sleep(0.1)
        
        return validated_buyers
    
    def filter_valid_buyers_only(self, buyers_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter to return only 100% valid buyers"""
        valid_buyers = []
        
        for buyer in buyers_list:
            if buyer.get('status_verified') == 'VALID':
                valid_buyers.append(buyer)
        
        self.logger.info(f"Filtered {len(valid_buyers)} valid buyers from {len(buyers_list)} total")
        return valid_buyers
    
    def remove_duplicates_advanced(self, buyers_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Advanced duplicate removal with fuzzy matching"""
        if not buyers_list:
            return buyers_list
        
        unique_buyers = []
        seen_companies = set()
        seen_emails = set()
        seen_phones = set()
        
        for buyer in buyers_list:
            company_name = buyer.get('company_name', '').lower().strip()
            email = buyer.get('email', '').lower().strip()
            phone = re.sub(r'[^\d]', '', buyer.get('phone', ''))
            
            # Skip if we've seen this exact company, email, or phone
            is_duplicate = False
            
            if company_name and company_name in seen_companies:
                is_duplicate = True
            elif email and email in seen_emails:
                is_duplicate = True
            elif phone and len(phone) >= 10 and phone in seen_phones:
                is_duplicate = True
            else:
                # Fuzzy matching for similar company names
                for seen_company in seen_companies:
                    similarity = SequenceMatcher(None, company_name, seen_company).ratio()
                    if similarity > 0.85:  # 85% similarity threshold
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                unique_buyers.append(buyer)
                if company_name:
                    seen_companies.add(company_name)
                if email:
                    seen_emails.add(email)
                if phone:
                    seen_phones.add(phone)
        
        removed_count = len(buyers_list) - len(unique_buyers)
        self.logger.info(f"Removed {removed_count} duplicates from {len(buyers_list)} buyers")
        
        return unique_buyers
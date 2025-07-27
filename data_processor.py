import pandas as pd
import re
from datetime import datetime
import logging
from typing import List, Dict, Any

class DataProcessor:
    """Process and clean scraped company data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_data(self, raw_data) -> pd.DataFrame:
        """Process raw scraped data into clean DataFrame"""
        try:
            # Handle different input types
            if isinstance(raw_data, pd.DataFrame):
                df = raw_data.copy()
            elif isinstance(raw_data, list):
                df = pd.DataFrame(raw_data)
            else:
                df = pd.DataFrame([raw_data])
            
            if df.empty:
                return df
            
            # Clean and standardize data
            df = self._clean_company_names(df)
            df = self._clean_phone_numbers(df)
            df = self._clean_email_addresses(df)
            df = self._clean_locations(df)
            df = self._add_metadata(df)
            
            # Remove duplicates
            df = self._remove_duplicates(df)
            
            # Reorder columns
            df = self._reorder_columns(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return pd.DataFrame()
    
    def _clean_company_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize company names"""
        if 'company_name' in df.columns:
            # Remove extra whitespace and normalize
            df['company_name'] = df['company_name'].astype(str).str.strip()
            
            # Remove common prefixes/suffixes for better matching
            df['company_name'] = df['company_name'].str.replace(r'^(M/s\.?|Messrs\.?)\s*', '', regex=True, case=False)
            
            # Capitalize properly
            df['company_name'] = df['company_name'].str.title()
            
            # Remove entries with invalid names
            df = df[df['company_name'].str.len() > 2]
            df = df[~df['company_name'].str.contains(r'(?:nan|none|null)$', case=False, na=False)]
        
        return df
    
    def _clean_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize phone numbers"""
        if 'phone' in df.columns:
            # Remove non-digit characters except +
            df['phone'] = df['phone'].astype(str).str.replace(r'[^\d+]', '', regex=True)
            
            # Standardize Indian phone numbers
            def standardize_phone(phone):
                if pd.isna(phone) or phone in ['nan', 'none', '']:
                    return ''
                
                phone = str(phone).strip()
                
                # Remove country code if present
                if phone.startswith('+91'):
                    phone = phone[3:]
                elif phone.startswith('91') and len(phone) > 10:
                    phone = phone[2:]
                
                # Validate length
                if len(phone) == 10 and phone.isdigit():
                    return '+91-' + phone
                elif len(phone) == 11 and phone.startswith('0'):
                    return '+91-' + phone[1:]
                else:
                    return ''
            
            df['phone'] = df['phone'].apply(standardize_phone)
            
            # Remove invalid phone numbers
            df.loc[df['phone'] == '', 'phone'] = None
        
        return df
    
    def _clean_email_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate email addresses"""
        if 'email' in df.columns:
            # Email validation regex
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            def validate_email(email):
                if pd.isna(email) or email in ['nan', 'none', '']:
                    return ''
                
                email = str(email).strip().lower()
                
                if re.match(email_pattern, email):
                    return email
                else:
                    return ''
            
            df['email'] = df['email'].apply(validate_email)
            
            # Remove invalid emails
            df.loc[df['email'] == '', 'email'] = None
        
        return df
    
    def _clean_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize location data"""
        for col in ['city', 'state']:
            if col in df.columns:
                # Clean and standardize
                df[col] = df[col].astype(str).str.strip().str.title()
                
                # Remove invalid entries
                df.loc[df[col].str.contains(r'(?:nan|none|null)$', case=False, na=False), col] = None
                df.loc[df[col].str.len() <= 1, col] = None
        
        return df
    
    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns"""
        # Add timestamp
        df['date_added'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Add data quality score
        df['data_quality_score'] = self._calculate_quality_score(df)
        
        return df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate data quality score for each row"""
        scores = pd.Series(0, index=df.index)
        
        # Company name (required)
        scores += df['company_name'].notna().astype(int) * 3
        
        # Contact information
        scores += df['phone'].notna().astype(int) * 2
        scores += df['email'].notna().astype(int) * 2
        
        # Location information
        scores += df['city'].notna().astype(int) * 1
        scores += df['state'].notna().astype(int) * 1
        
        # Additional information
        if 'website' in df.columns:
            scores += df['website'].notna().astype(int) * 1
        if 'contact_person' in df.columns:
            scores += df['contact_person'].notna().astype(int) * 1
        
        return scores
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate companies"""
        if df.empty:
            return df
        
        # Remove exact duplicates
        df = df.drop_duplicates()
        
        # Remove duplicates based on company name similarity
        if 'company_name' in df.columns:
            # Sort by data quality score (descending) to keep best records
            if 'data_quality_score' in df.columns:
                df = df.sort_values('data_quality_score', ascending=False)
            
            # Remove duplicates based on company name
            df = df.drop_duplicates(subset=['company_name'], keep='first')
        
        return df.reset_index(drop=True)
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns for better presentation"""
        if df.empty:
            return df
        
        # Preferred column order
        preferred_order = [
            'company_name', 'contact_person', 'city', 'state', 'country',
            'phone', 'email', 'website', 'products', 'description',
            'source', 'company_url', 'data_quality_score', 'date_added'
        ]
        
        # Reorder existing columns
        existing_columns = [col for col in preferred_order if col in df.columns]
        remaining_columns = [col for col in df.columns if col not in existing_columns]
        
        final_order = existing_columns + remaining_columns
        
        return df[final_order]
    
    def merge_dataframes(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """Merge new data with existing data, removing duplicates"""
        try:
            if existing_df.empty:
                return new_df
            
            if new_df.empty:
                return existing_df
            
            # Combine dataframes
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates
            combined_df = self._remove_duplicates(combined_df)
            
            # Sort by data quality score and date
            if 'data_quality_score' in combined_df.columns:
                combined_df = combined_df.sort_values(
                    ['data_quality_score', 'date_added'], 
                    ascending=[False, False]
                )
            
            return combined_df.reset_index(drop=True)
            
        except Exception as e:
            self.logger.error(f"Error merging dataframes: {str(e)}")
            return existing_df

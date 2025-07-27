import pandas as pd
import re
from urllib.parse import urlparse
from typing import Optional
import streamlit as st

def export_to_csv(df: pd.DataFrame) -> str:
    """Export DataFrame to CSV string"""
    try:
        return df.to_csv(index=False)
    except Exception as e:
        st.error(f"Error exporting to CSV: {str(e)}")
        return ""

def validate_url(url: str) -> bool:
    """Validate if a string is a valid URL"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text or pd.isna(text):
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', str(text)).strip()
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\-\.,@()]+', '', text)
    
    return text

def extract_phone_from_text(text: str) -> Optional[str]:
    """Extract phone number from text"""
    if not text or pd.isna(text):
        return None
    
    # Indian phone number patterns
    patterns = [
        r'\+91[\s-]?\d{10}',  # +91 followed by 10 digits
        r'91[\s-]?\d{10}',    # 91 followed by 10 digits
        r'\d{10}',            # 10 digits
        r'\d{11}',            # 11 digits
    ]
    
    for pattern in patterns:
        match = re.search(pattern, str(text))
        if match:
            phone = match.group(0)
            # Clean and format
            phone = re.sub(r'[^\d+]', '', phone)
            if phone.startswith('+91'):
                return phone
            elif phone.startswith('91') and len(phone) > 10:
                return '+91' + phone[2:]
            elif len(phone) == 10:
                return '+91' + phone
    
    return None

def extract_email_from_text(text: str) -> Optional[str]:
    """Extract email address from text"""
    if not text or pd.isna(text):
        return None
    
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, str(text))
    
    if match:
        return match.group(0).lower()
    
    return None

def format_company_name(name: str) -> str:
    """Format company name consistently"""
    if not name or pd.isna(name):
        return ""
    
    name = str(name).strip()
    
    # Remove common prefixes
    name = re.sub(r'^(M/s\.?|Messrs\.?)\s*', '', name, flags=re.IGNORECASE)
    
    # Title case
    name = name.title()
    
    # Fix common abbreviations
    abbreviations = {
        ' Pvt Ltd': ' Pvt. Ltd.',
        ' Private Limited': ' Pvt. Ltd.',
        ' Llp': ' LLP',
        ' Co.': ' Co.',
        ' Corp': ' Corp.',
        ' Inc': ' Inc.',
        ' & Co': ' & Co.',
    }
    
    for old, new in abbreviations.items():
        name = re.sub(re.escape(old), new, name, flags=re.IGNORECASE)
    
    return name

def get_location_parts(location_text: str) -> tuple:
    """Extract city and state from location text"""
    if not location_text or pd.isna(location_text):
        return "", ""
    
    location_text = str(location_text).strip()
    parts = [part.strip() for part in location_text.split(',')]
    
    city = parts[0] if len(parts) > 0 else ""
    state = parts[1] if len(parts) > 1 else ""
    
    return city.title(), state.title()

def calculate_completeness_score(row: pd.Series) -> int:
    """Calculate data completeness score for a company record"""
    score = 0
    
    # Required fields
    if pd.notna(row.get('company_name')) and row.get('company_name'):
        score += 3
    
    # Contact information
    if pd.notna(row.get('phone')) and row.get('phone'):
        score += 2
    if pd.notna(row.get('email')) and row.get('email'):
        score += 2
    
    # Location information
    if pd.notna(row.get('city')) and row.get('city'):
        score += 1
    if pd.notna(row.get('state')) and row.get('state'):
        score += 1
    
    # Additional information
    if pd.notna(row.get('contact_person')) and row.get('contact_person'):
        score += 1
    if pd.notna(row.get('website')) and row.get('website'):
        score += 1
    if pd.notna(row.get('description')) and row.get('description'):
        score += 1
    
    return score

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe file operations"""
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove multiple underscores
    filename = re.sub(r'_+', '_', filename)
    
    # Trim underscores from start and end
    filename = filename.strip('_')
    
    return filename

@st.cache_data
def get_sample_search_terms() -> list:
    """Get sample search terms for turmeric buyers"""
    return [
        "turmeric buyer",
        "turmeric importer",
        "spice buyer",
        "turmeric trader",
        "turmeric distributor",
        "turmeric wholesale",
        "haldi buyer",
        "curcuma buyer",
        "organic turmeric buyer",
        "turmeric powder buyer"
    ]

def validate_search_term(term: str) -> bool:
    """Validate search term"""
    if not term or len(term.strip()) < 3:
        return False
    
    # Check for potentially problematic characters
    if re.search(r'[<>"\']', term):
        return False
    
    return True

def format_data_for_display(df: pd.DataFrame, max_chars: int = 50) -> pd.DataFrame:
    """Format DataFrame for better display in Streamlit"""
    if df.empty:
        return df
    
    display_df = df.copy()
    
    # Truncate long text fields
    text_columns = ['company_name', 'description', 'products', 'contact_person']
    for col in text_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].astype(str).apply(
                lambda x: x[:max_chars] + '...' if len(x) > max_chars else x
            )
    
    # Format phone numbers for display
    if 'phone' in display_df.columns:
        display_df['phone'] = display_df['phone'].fillna('')
    
    # Format email addresses
    if 'email' in display_df.columns:
        display_df['email'] = display_df['email'].fillna('')
    
    return display_df

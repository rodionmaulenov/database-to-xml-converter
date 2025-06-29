from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Optional, List
import re


def normalize_date(date_str: str) -> Optional[str]:
    """
    Normalize various date formats to ISO format (YYYY-MM-DD)
    
    Supported formats:
    - ISO date: 2024-01-01
    - ISO datetime: 2024-01-01T14:30:00 or 2024-01-01 14:30:00
    - US format: 12/31/2024
    - European format: 31-12-2024
    
    Returns:
        Normalized date string or None if invalid
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    
    # Try different date formats
    formats: List[str] = [
        '%Y-%m-%d',           # ISO date
        '%Y-%m-%dT%H:%M:%S',  # ISO datetime with T
        '%Y-%m-%d %H:%M:%S',  # ISO datetime with space
        '%m/%d/%Y',           # US format
        '%d-%m-%Y',           # European format
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    return None


def normalize_amount(amount_str: str) -> Optional[str]:
    """
    Normalize amount to decimal format with exactly 2 decimal places
    
    Supported formats:
    - Whole numbers: 100 → 100.00
    - Single decimal: 100.5 → 100.50
    - Multiple decimals: 100.567 → 100.57 (rounded)
    - European comma decimals: 1,20 → 1.20
    - Negative amounts: -42.1 → -42.10
    
    Returns:
        Normalized amount string or None if invalid
    """
    if not amount_str or not isinstance(amount_str, str):
        return None
    
    amount_str = amount_str.strip()
    
    # Replace European decimal comma with dot
    amount_str = amount_str.replace(',', '.')
    
    try:
        # Parse to Decimal for precise financial calculations
        amount = Decimal(amount_str)
        
        # Round to 2 decimal places using banker's rounding
        rounded = amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        # Handle -0 case
        if rounded == 0:
            return "0.00"
            
        # Format with exactly 2 decimal places
        return f"{rounded:.2f}"
    except (ValueError, TypeError, InvalidOperation):
        return None


def validate_account(account_str: str) -> Optional[str]:
    """
    Validate account number (must be 3-12 digits only)
    
    Returns:
        Account string if valid, None otherwise
    """
    if not account_str or not isinstance(account_str, str):
        return None
    
    account_str = account_str.strip()
    
    # Check if it's only digits and within length constraints
    if re.match(r'^\d{3,12}$', account_str):
        return account_str
    
    return None


def clean_description(desc_str: Optional[str]) -> str:
    """
    Clean description field
    - Trim whitespace
    - Truncate to 255 characters
    - Can be empty
    
    Returns:
        Cleaned description string (can be empty)
    """
    if not desc_str:
        return ""
    
    # Convert to string and trim
    desc_str = str(desc_str).strip()
    
    # Truncate to 255 characters
    if len(desc_str) > 255:
        desc_str = desc_str[:255]
    
    return desc_str
"""
Utility functions for the CivicAtlas scraper
"""

import time
import logging
import re
import functools
from typing import Callable, Any

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('civicatlas_scraper.log'),
            logging.StreamHandler()
        ]
    )

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry function calls on failure
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay on each retry
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logging.getLogger(func.__module__).warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logging.getLogger(func.__module__).error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}: {str(e)}"
                        )
                        
            raise last_exception
            
        return wrapper
    return decorator

def normalize_text(text: str) -> str:
    """
    Normalize text by removing extra whitespace and special characters
    
    Args:
        text: Input text to normalize
        
    Returns:
        Cleaned and normalized text
    """
    if not text:
        return ""
        
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove common unwanted characters but preserve essential punctuation
    text = re.sub(r'[^\w\s\-\.\,\(\)\/]', ' ', text)
    
    # Clean up multiple spaces again
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text

def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable format
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{int(minutes)}m {int(secs)}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {int(secs)}s"

def progress_bar(current: int, total: int, prefix: str = "", length: int = 30) -> str:
    """
    Create a simple text progress bar
    
    Args:
        current: Current progress value
        total: Total/maximum value
        prefix: Text to show before the progress bar
        length: Length of the progress bar in characters
        
    Returns:
        Formatted progress bar string
    """
    if total == 0:
        percent = 100
    else:
        percent = (current / total) * 100
        
    filled = int(length * current // total) if total > 0 else 0
    bar = "█" * filled + "░" * (length - filled)
    
    return f"{prefix} |{bar}| {current}/{total} ({percent:.1f}%)"

def is_valid_url(url: str) -> bool:
    """
    Check if a URL is valid and properly formatted
    
    Args:
        url: URL string to validate
        
    Returns:
        True if URL is valid, False otherwise
    """
    if not url:
        return False
        
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None

def clean_filename(filename: str) -> str:
    """
    Clean a filename by removing invalid characters
    
    Args:
        filename: Original filename
        
    Returns:
        Cleaned filename safe for filesystem use
    """
    # Remove invalid filename characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove extra spaces and dots
    filename = re.sub(r'[\s\.]+', '_', filename)
    
    # Ensure filename is not too long
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename.strip('_')

def extract_numbers_from_text(text: str) -> list:
    """
    Extract all numbers from a text string
    
    Args:
        text: Input text
        
    Returns:
        List of numbers found in the text
    """
    if not text:
        return []
    
    numbers = re.findall(r'\d+', text)
    return [int(num) for num in numbers]

def safe_get_text(element, default: str = "") -> str:
    """
    Safely extract text from a BeautifulSoup element
    
    Args:
        element: BeautifulSoup element
        default: Default value if element is None or empty
        
    Returns:
        Extracted and normalized text
    """
    if element is None:
        return default
        
    try:
        text = element.get_text(strip=True)
        return normalize_text(text) if text else default
    except:
        return default

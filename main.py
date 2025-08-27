#!/usr/bin/env python3
"""
CivicAtlas.in Urban Local Bodies and Wards Scraper
Main entry point for the scraping application
"""

import sys
import time
import logging
from scraper import CivicAtlasScraper
from utils import setup_logging, format_duration

def main():
    """Main function to run the scraper"""
    print("=" * 70)
    print("CivicAtlas.in Urban Local Bodies & Wards Scraper")
    print("=" * 70)
    print()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        start_time = time.time()
        
        # Initialize the scraper
        scraper = CivicAtlasScraper()
        
        print("🚀 Starting comprehensive scraping of CivicAtlas.in...")
        print("   This process will extract all urban local bodies and ward data")
        print("   from all Indian states and union territories.")
        print()
        
        # Run the scraping process
        success = scraper.scrape_all_data()
        
        end_time = time.time()
        duration = format_duration(end_time - start_time)
        
        if success:
            print()
            print("✅ Scraping completed successfully!")
            print(f"⏱️  Total time taken: {duration}")
            print(f"📁 Data saved to: {scraper.output_file}")
            print()
            
            # Display summary statistics
            scraper.display_summary()
            
        else:
            print()
            print("❌ Scraping failed. Please check the logs for details.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted by user. Partial data may be saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
        print(f"\n❌ An unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

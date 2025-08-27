# CivicAtlas Urban Bodies and Wards Scraper

## Overview

This is a Python-based web scraper designed to extract comprehensive data about urban local bodies and wards from CivicAtlas.in. The application systematically scrapes information across all Indian states and union territories, collecting ward-level data including ward numbers, names, urban local body information, districts, and states. The scraper is built with reliability and error handling in mind, implementing retry mechanisms and detailed logging to handle the complexities of web scraping at scale.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Application Structure
The application follows a modular design with three main components:
- **main.py**: Entry point that orchestrates the scraping process and provides user feedback
- **scraper.py**: Core scraping logic implementing the CivicAtlasScraper class
- **utils.py**: Utility functions for logging, retry mechanisms, and text processing

### Data Processing Architecture
The scraper uses a hierarchical data extraction approach:
1. **State-level processing**: Iterates through all Indian states and union territories
2. **Urban body discovery**: Identifies all urban local bodies within each state
3. **Ward extraction**: Extracts detailed ward information for each urban body
4. **Data normalization**: Processes and cleans extracted data before storage

### Error Handling and Reliability
The system implements multiple layers of error handling:
- **Retry decorator**: Configurable retry mechanism with exponential backoff for failed requests
- **Session management**: Persistent HTTP sessions with appropriate headers for reliable web requests
- **Comprehensive logging**: File and console logging with detailed error tracking
- **Statistics tracking**: Real-time monitoring of processing progress and error rates

### Data Storage
The application uses CSV format for data output with predefined schema:
- Ward Number, Ward Name, Urban Local Body Name, Urban Local Body Type, District, State
- Single output file consolidating all extracted data
- Progress tracking and summary statistics

### Web Scraping Strategy
The scraper employs responsible scraping practices:
- **Request throttling**: Built-in delays to avoid overwhelming the target server
- **User-Agent rotation**: Proper browser headers to ensure legitimate request appearance
- **BeautifulSoup parsing**: Robust HTML parsing for data extraction
- **URL handling**: Proper URL joining and validation for navigation

## External Dependencies

### Python Libraries
- **requests**: HTTP client library for web requests and session management
- **beautifulsoup4**: HTML parsing and DOM navigation
- **csv**: Built-in CSV file handling for data output
- **logging**: Built-in logging framework for error tracking and debugging
- **time**: Built-in module for delays and timing operations
- **re**: Built-in regular expressions for text processing
- **urllib.parse**: Built-in URL parsing and manipulation
- **typing**: Type hints for better code documentation
- **os**: Built-in operating system interface
- **functools**: Built-in functional programming utilities

### Target Website
- **CivicAtlas.in**: Primary data source for urban local bodies and ward information across India
- No API integration - relies on web scraping of HTML content
- Requires handling of dynamic navigation and hierarchical data structure

### Data Output
- **Local CSV files**: Self-contained data storage without external database dependencies
- **Log files**: Local file system for error and process logging
- No cloud storage or external database integrations in current architecture
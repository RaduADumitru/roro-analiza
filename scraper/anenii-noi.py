"""
Web scraper for anenii-noi.md website
Extracts title and content from news articles and saves them as JSON files.
"""

import requests
from bs4 import BeautifulSoup
import json
import os
from pathlib import Path
from urllib.parse import urlparse

# Base category URL and number of pages
BASE_CATEGORY_URL = "https://anenii-noi.md/category/stiri/"
NUM_PAGES = 23

# Output directory
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Anenii Noi")


def get_article_urls_from_page(page_url):
    """
    Extract all article URLs from a news list page.
    
    Args:
        page_url: The URL of the list page
        
    Returns:
        list: List of article URLs found on the page
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(page_url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all article links in .blog-post-title a elements
        article_links = soup.select('.blog-post-title a')
        urls = [link.get('href') for link in article_links if link.get('href')]
        
        return urls
    
    except Exception as e:
        print(f"Error fetching article URLs from {page_url}: {str(e)}")
        return []


def extract_article_data(url):
    """
    Extract title and content from an Anenii Noi article page.
    
    Args:
        url: The URL of the article to scrape
        
    Returns:
        dict: Dictionary containing title, content, and metadata
    """
    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title from the posts-heading section
        title_element = soup.select_one('.posts-heading h2')
        title = title_element.get_text(strip=True) if title_element else ""
        
        # Extract content from posts-content-holder
        content_element = soup.select_one('.posts-content-holder')
        if content_element:
            # Get all paragraphs and join them
            paragraphs = content_element.find_all('p')
            content = '\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
            # Remove &nbsp; entities, represented as \xa0 in Python
            content = content.replace('\xa0', ' ')
        else:
            content = ""
        
        return {
            "title": title,
            "content": content,
            "metadata": {}
        }
    
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return None


def save_json(data, filepath):
    """
    Save data to a JSON file with proper formatting.
    
    Args:
        data: Dictionary to save
        filepath: Path to save the file
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    """
    Main function to scrape all URLs and save results.
    """
    # Create output directory if it doesn't exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Collect all article URLs from all pages
    print(f"{'='*60}")
    print(f"Extracting article URLs from {NUM_PAGES} pages...")
    print(f"{'='*60}\n")
    
    all_article_urls = []
    
    for page_num in range(1, NUM_PAGES + 1):
        # Build the page URL
        if page_num == 1:
            page_url = BASE_CATEGORY_URL
        else:
            page_url = f"{BASE_CATEGORY_URL}page/{page_num}/"
        
        print(f"Page {page_num}/{NUM_PAGES}: {page_url}")
        
        # Extract article URLs from this page
        article_urls = get_article_urls_from_page(page_url)
        print(f"  Found {len(article_urls)} articles")
        
        all_article_urls.extend(article_urls)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in all_article_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    print(f"\n{'='*60}")
    print(f"Total unique articles to scrape: {len(unique_urls)}")
    print(f"{'='*60}\n")
    
    # Track the number of successfully scraped articles
    success_count = 0
    
    # Scrape each article
    for idx, url in enumerate(unique_urls):
        print(f"Scraping {idx + 1}/{len(unique_urls)}: {url}")
        
        # Extract article data
        article_data = extract_article_data(url)
        
        if article_data and article_data['title'] and article_data['content']:
            # Generate filename with 4-digit padding
            filename = f"AneniiNoi_{success_count:04d}.json"
            filepath = OUTPUT_DIR / filename
            
            # Set the original_file to the filename being saved
            article_data['metadata']['original_file'] = filename
            
            # Save to JSON
            save_json(article_data, filepath)
            print(f" Saved to {filename}")
            
            success_count += 1
        else:
            print(f" Failed to extract data")
    
    print(f"\n{'='*60}")
    print(f"Successfully scraped {success_count} out of {len(unique_urls)} articles.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

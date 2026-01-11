"""
Web scraper for donduseni.md website
Extracts title and content from news articles and saves them as JSON files.
"""

import requests
from bs4 import BeautifulSoup, Comment
import json
import re
from pathlib import Path
from urllib.parse import urlparse

# Base URL and number of pages
BASE_URL = "https://donduseni.md"
NUM_PAGES = 189

# Output directory
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Donduseni")


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
        
        # Find all article elements
        articles = soup.find_all('article')
        urls = []
        
        for article in articles:
            # Look for links in post-title h2
            post_title = article.find('div', class_='post-title')
            if post_title:
                h2 = post_title.find('h2')
                if h2:
                    link = h2.find('a')
                    if link and link.get('href'):
                        urls.append(link.get('href'))
        
        return urls
    
    except Exception as e:
        print(f"Error fetching article URLs from {page_url}: {str(e)}")
        return []


def extract_article_data(url):
    """
    Extract title and content from a Dondușeni article page.
    
    Args:
        url: The URL of the article to scrape
        
    Returns:
        tuple: (data_dict, status) where status is 'success', 'no_title', 'no_content', or 'error'
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
        
        # Extract title from h1 in post-title div
        title = ""
        post_title_div = soup.find('div', class_='post-title')
        if post_title_div:
            h1 = post_title_div.find('h1')
            if h1:
                title = h1.get_text(strip=True)
        
        # Extract content from entry div
        entry_element = soup.find('div', class_='entry')
        content = ""
        if entry_element:
            # Remove script tags, style tags, and comments
            for script_tag in entry_element.find_all('script'):
                script_tag.decompose()
            
            for style_tag in entry_element.find_all('style'):
                style_tag.decompose()
            
            # Remove HTML comments
            for comment in entry_element.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            
            # Remove social media buttons and widgets
            for social_btn in entry_element.find_all(class_=['fb-like', 'fb-comments', 'g-plusone', 'twitter-share-button']):
                social_btn.decompose()
            
            # Remove pubdate paragraph
            pubdate = entry_element.find('p', class_='pubdate')
            if pubdate:
                pubdate.decompose()
            
            # Remove h20px divs
            for h20px in entry_element.find_all('div', class_='h20px'):
                h20px.decompose()
            
            # Remove fb-comments div
            for fb_comments in entry_element.find_all('div', class_='fb-comments'):
                fb_comments.decompose()
            
            # Remove gallery divs
            for gallery in entry_element.find_all('div', class_=['gallery', 'galleryid']):
                gallery.decompose()
            
            # Get all text from all tags within entry div
            text_parts = []
            for element in entry_element.children:
                if isinstance(element, str):
                    # Direct text nodes
                    text = element.strip()
                    if text:
                        text_parts.append(text)
                else:
                    # Text from tags
                    text = element.get_text(strip=True)
                    # Skip empty text and social media/sharing widgets
                    if text and not text.startswith('data-') and 'facebook' not in text.lower() and 'twitter' not in text.lower() and 'tweet' not in text.lower() and 'afișări' not in text.lower():
                        text_parts.append(text)
            
            content = '\n'.join(text_parts)
            # Remove &nbsp; entities, represented as \xa0 in Python
            content = content.replace('\xa0', ' ')
        
        # Validate extraction
        if not title:
            return None, 'no_title'
        if not content:
            return None, 'no_content'
        
        return {
            "title": title,
            "content": content,
            "metadata": {}
        }, 'success'
    
    except Exception as e:
        print(f"  Error scraping: {str(e)}")
        return None, 'error'


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
            page_url = BASE_URL
        else:
            page_url = f"{BASE_URL}/page/{page_num}/"
        
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
        article_data, status = extract_article_data(url)
        
        if status == 'error':
            print(f"  Failed (network or parsing error)")
        elif status == 'no_title':
            print(f"  Failed (could not extract title)")
        elif status == 'no_content':
            print(f"  Failed (could not extract content)")
        elif status == 'success' and article_data:
            # Generate filename with 4-digit padding
            filename = f"RaionDonduseni_{success_count:04d}.json"
            filepath = OUTPUT_DIR / filename
            
            # Set the original_file to the filename being saved
            article_data['metadata']['original_file'] = filename
            
            # Save to JSON
            save_json(article_data, filepath)
            print(f"  Saved to {filename}")
            
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"Successfully scraped {success_count} out of {len(unique_urls)} articles.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

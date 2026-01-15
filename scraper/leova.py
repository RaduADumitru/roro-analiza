"""
Web scraper for leova.org
Extracts article URLs from paginated listings and saves article content as JSON.
"""

import json
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment


# Base URLs
BASE_URL = "http://www.leova.org"
START_LISTING_URL = "http://www.leova.org/search?&max-results=5"

# Output directory
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Leova")


def fetch_html(url: str, timeout: int = 20):
	"""Fetch HTML content with basic headers; return text or None on failure."""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
	}

	try:
		resp = requests.get(url, headers=headers, timeout=timeout)
		if resp.status_code == 404:
			return None
		resp.raise_for_status()
		resp.encoding = "utf-8"
		return resp.text
	except Exception as exc:
		print(f"  Error fetching {url}: {exc}")
		return None


def extract_listing_urls(listing_url: str):
	"""Return (article_urls, next_page_url) for a listing page."""
	html = fetch_html(listing_url, timeout=15)
	if not html:
		return [], None

	soup = BeautifulSoup(html, "html.parser")

	article_urls = []

	# Extract article links from listing page
	blog_posts_container = soup.find("div", class_=lambda c: c and "blog-posts" in c)
	if blog_posts_container:
		for post in blog_posts_container.find_all("div", class_=lambda c: c and "blog-post" in c):
			link_tag = post.find("a", class_=lambda c: c and "post-image-link" in c)
			if not link_tag:
				title_tag = post.find("h2", class_=lambda c: c and "post-title" in c)
				link_tag = title_tag.find("a", href=True) if title_tag else None
			if link_tag and link_tag.get("href"):
				article_urls.append(urljoin(BASE_URL, link_tag.get("href")))

	next_link_tag = soup.find("a", class_=lambda c: c and "blog-pager-older-link" in c)
	next_page_url = urljoin(BASE_URL, next_link_tag.get("href")) if next_link_tag and next_link_tag.get("href") else None

	return article_urls, next_page_url


def extract_article_data(url: str):
	"""Extract title and cleaned content from an article page."""
	html = fetch_html(url, timeout=25)
	if not html:
		return None, "error"

	soup = BeautifulSoup(html, "html.parser")

	page_title_tag = soup.find("title")
	raw_page_title = page_title_tag.get_text(strip=True) if page_title_tag else ""

	title = ""
	title_tag = soup.find(class_=lambda c: c and ("post-title" in c or "entry-title" in c))
	if title_tag:
		h1 = title_tag.find(["h1", "h2", "h3"]) or title_tag
		title = h1.get_text(strip=True)

	if not title:
		og_title = soup.find("meta", property="og:title")
		if og_title and og_title.get("content"):
			title = og_title["content"].strip()

	if not title and raw_page_title:
		title = raw_page_title.split(" | ")[0].strip() if " | " in raw_page_title else raw_page_title

	content_container = soup.find("div", class_=lambda c: c and "post-body" in c)
	content = ""

	if content_container:
		# Strip unwanted elements before text extraction
		for tag in content_container.find_all(["script", "style", "noscript", "iframe", "form", "button"]):
			tag.decompose()

		for comment in content_container.find_all(string=lambda text: isinstance(text, Comment)):
			comment.extract()

		for social in content_container.find_all(class_=[
			"fb-like",
			"fb-comments",
			"mom-social-share",
			"share_box",
			"ss-vertical",
			"post-sharing",
			"twitter-share-button",
			"g-plusone",
			"social",
		]):
			social.decompose()

		text_parts = [
			line.strip()
			for line in content_container.get_text("\n", strip=True).splitlines()
			if line.strip()
		]

		content = "\n".join(text_parts).replace("\xa0", " ")

	if not title:
		return None, "no_title"
	if not content:
		return None, "no_content"

	metadata = {
		"page_title": raw_page_title,
	}

	return {
		"title": title,
		"content": content,
		"metadata": metadata,
	}, "success"


def save_json(data, filepath: Path):
	"""Save JSON with UTF-8 encoding."""
	filepath.parent.mkdir(parents=True, exist_ok=True)
	with filepath.open("w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False, indent=2)


def main():
	"""Scrape all listing pages, then all articles."""
	OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

	print("=" * 60)
	print("Collecting article URLs...")
	print("=" * 60)

	listing_url = START_LISTING_URL
	seen_listing_urls = set()
	all_article_urls = []

	while listing_url:
		if listing_url in seen_listing_urls:
			print(f"Already visited listing {listing_url}, stopping to avoid loop")
			break

		print(f"Listing: {listing_url}")
		seen_listing_urls.add(listing_url)

		page_urls, next_listing = extract_listing_urls(listing_url)

		if page_urls:
			print(f"  Found {len(page_urls)} article URLs")
			all_article_urls.extend(page_urls)
		else:
			print("  No article URLs found on this page")

		listing_url = next_listing

	# Deduplicate while preserving order
	seen = set()
	unique_urls = []
	for url in all_article_urls:
		if url not in seen:
			seen.add(url)
			unique_urls.append(url)

	print("=" * 60)
	print(f"Total unique articles to scrape: {len(unique_urls)}")
	print("=" * 60)

	success_count = 0

	for idx, url in enumerate(unique_urls, start=1):
		print(f"Scraping {idx}/{len(unique_urls)}: {url}")

		article_data, status = extract_article_data(url)

		if status == "error":
			print("  Failed (network or parsing error)")
			continue
		if status == "no_title":
			print("  Failed (could not extract title)")
			continue
		if status == "no_content":
			print("  Failed (could not extract content)")
			continue

		filename = f"Leova_MD_{success_count:04d}.json"
		filepath = OUTPUT_DIR / filename

		article_data["metadata"]["original_file"] = filename
		article_data["metadata"]["source_url"] = url

		save_json(article_data, filepath)
		print(f"  Saved to {filename}")
		success_count += 1

	print("=" * 60)
	print(f"Successfully scraped {success_count} out of {len(unique_urls)} articles.")
	print("=" * 60)


if __name__ == "__main__":
	main()

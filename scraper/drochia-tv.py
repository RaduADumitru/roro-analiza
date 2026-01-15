"""
Web scraper for drochia.tv website
Extracts title and content from news articles and saves them as JSON files.
"""

import json
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

# Base URLs
BASE_LISTING_URL = "https://drochia.tv/category/stiri"

# Output directory
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Drochia")


def build_page_url(page_num: int) -> str:
	"""Return the listing URL for a given page number."""
	if page_num <= 1:
		return BASE_LISTING_URL
	return f"{BASE_LISTING_URL}/page/{page_num}/"


def get_article_urls_from_page(page_num: int):
	"""Extract all article URLs from a news list page."""
	page_url = build_page_url(page_num)
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
	}

	try:
		response = requests.get(page_url, headers=headers, timeout=15)
		# Stop if the site signals end of pagination
		if response.status_code == 404:
			return None

		response.raise_for_status()
		response.encoding = "utf-8"
	except Exception as exc:  # pragma: no cover - network errors are handled via early stop
		print(f"  Error fetching page {page_url}: {exc}")
		return None

	soup = BeautifulSoup(response.text, "html.parser")

	urls = []

	# Articles on listing pages have h2 > a links inside a bp-head container
	for h2 in soup.find_all("h2"):
		link = h2.find("a")
		href = link.get("href") if link else None
		if href:
			urls.append(urljoin(BASE_LISTING_URL, href))

	return urls


def extract_article_data(url: str):
	"""Extract title and content from a Drochia TV article page."""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
	}

	try:
		response = requests.get(url, headers=headers, timeout=20)
		response.raise_for_status()
		response.encoding = "utf-8"
	except Exception as exc:
		print(f"  Error scraping {url}: {exc}")
		return None, "error"

	soup = BeautifulSoup(response.text, "html.parser")

	# Title extraction: prefer document title tag, fallback to h1 if missing
	title = ""
	title_tag = soup.find("title")
	if title_tag:
		title = title_tag.get_text(strip=True)
		# Remove site name suffix if present
		if " - DrochiaTV" in title:
			title = title.replace(" - DrochiaTV", "").strip()
	if not title:
		title_container = soup.find("h1")
		if title_container:
			title = title_container.get_text(strip=True)

	# Content extraction: try multiple common containers
	content_container = None
	for tag_name, attrs in [
		("div", {"class": "entry"}),
		("div", {"class": "entry-content"}),
		("div", {"class": "post-content"}),
		("div", {"class": "post-entry"}),
		("div", {"class": "single-post-content"}),
		("article", {"class": "post"}),
	]:
		content_container = soup.find(tag_name, attrs=attrs)
		if content_container:
			break

	if not content_container:
		content_container = soup.find("div", class_=True)

	content = ""

	if content_container:
		# Remove unwanted elements
		for tag in content_container.find_all(["script", "style", "noscript", "iframe"]):
			tag.decompose()

		for comment in content_container.find_all(string=lambda text: isinstance(text, Comment)):
			comment.extract()

		for social in content_container.find_all(
			class_=[
				"fb-like",
				"fb-comments",
				"mom-social-share",
				"share_box",
				"ss-vertical",
				"post-sharing",
			]
		):
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

	return {
		"title": title,
		"content": content,
		"metadata": {},
	}, "success"


def save_json(data, filepath: Path):
	"""Save data to a JSON file with proper formatting."""
	filepath.parent.mkdir(parents=True, exist_ok=True)
	with filepath.open("w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False, indent=2)


def main():
	"""Main function to scrape all URLs and save results."""
	OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

	print("=" * 60)
	print("Extracting article URLs...")
	print("=" * 60)

	all_article_urls = []
	page_num = 1

	while True:
		page_urls = get_article_urls_from_page(page_num)
		page_url = build_page_url(page_num)

		if page_urls is None:
			print(f"Page {page_num}: {page_url} -> stop (no response or 404)")
			break

		if not page_urls:
			print(f"Page {page_num}: {page_url} -> no articles found, stopping")
			break

		print(f"Page {page_num}: {page_url} -> found {len(page_urls)} articles")
		all_article_urls.extend(page_urls)
		page_num += 1

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

	for idx, url in enumerate(unique_urls):
		print(f"Scraping {idx + 1}/{len(unique_urls)}: {url}")

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

		filename = f"Drochia_TV_{success_count:04d}.json"
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

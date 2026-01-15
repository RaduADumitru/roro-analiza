"""
Web scraper for cr-falesti.md website.
Extracts title and content from news articles and saves them as JSON files.
"""

import json
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

BASE_URL = "https://www.cr-falesti.md"
BASE_LISTING_URL = BASE_URL  # Listing uses the base with ?start= offset
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Falesti")


def build_page_url(offset: int) -> str:
	"""Return the listing URL for a given offset."""
	if offset <= 0:
		return BASE_LISTING_URL
	return f"{BASE_LISTING_URL}/?start={offset}"


def get_article_urls_from_page(offset: int):
	"""Extract article URLs from a listing page; returns None on hard stop."""
	page_url = build_page_url(offset)
	headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

	try:
		response = requests.get(page_url, headers=headers, timeout=15)
		if response.status_code == 404:
			return None
		response.raise_for_status()
		response.encoding = "utf-8"
	except Exception as exc:
		print(f"  Error fetching page {page_url}: {exc}")
		return None

	soup = BeautifulSoup(response.text, "html.parser")

	urls = []
	for link in soup.select("h2.item-title a"):
		href = link.get("href")
		if href:
			urls.append(urljoin(BASE_URL, href))

	return urls


def extract_article_data(url: str):
	"""Extract title and content from an article page."""
	headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

	try:
		response = requests.get(url, headers=headers, timeout=20)
		response.raise_for_status()
		response.encoding = "utf-8"
	except Exception as exc:
		print(f"  Error scraping {url}: {exc}")
		return None, "error"

	soup = BeautifulSoup(response.text, "html.parser")

	title = ""
	header = soup.find("div", class_="page-header")
	if header:
		h2 = header.find("h2")
		if h2:
			title = h2.get_text(strip=True)

	if not title:
		h1 = soup.find("h1")
		if h1:
			title = h1.get_text(strip=True)

	content_container = soup.find(attrs={"itemprop": "articleBody"})
	if not content_container:
		content_container = soup.find("div", class_="item-page")

	content = ""

	if content_container:
		for tag in content_container.find_all(["script", "style", "noscript", "iframe"]):
			tag.decompose()

		for comment in content_container.find_all(string=lambda text: isinstance(text, Comment)):
			comment.extract()

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
	"""Save data to a JSON file with utf-8 encoding."""
	filepath.parent.mkdir(parents=True, exist_ok=True)
	with filepath.open("w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False, indent=2)


def main():
	OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

	print("=" * 60)
	print("Extracting article URLs...")
	print("=" * 60)

	all_article_urls = []
	offset = 0
	page_size = 5  # Site paginates by 5 items per page via ?start=

	while True:
		page_urls = get_article_urls_from_page(offset)
		page_url = build_page_url(offset)

		if page_urls is None:
			print(f"Page offset {offset}: {page_url} -> stop (no response or 404)")
			break

		if not page_urls:
			print(f"Page offset {offset}: {page_url} -> no articles found, stopping")
			break

		print(f"Page offset {offset}: {page_url} -> found {len(page_urls)} articles")
		all_article_urls.extend(page_urls)
		offset += page_size

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

		filename = f"CR_Falesti_{success_count:04d}.json"
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

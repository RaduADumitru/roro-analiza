"""
Web scraper for dubasari.md website.
Iterates news listing pages, scrapes articles, and stores them as JSON.
"""

import json
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

BASE_URL = "https://dubasari.md"
BASE_LISTING_URL = f"{BASE_URL}/news/list"

# Output directory for Dubasari
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Dubasari")


def build_page_url(page_num: int) -> str:
	"""Return the listing URL for a given page number."""
	return f"{BASE_LISTING_URL}/{page_num}"


def get_article_urls_from_page(page_num: int):
	"""Extract all article URLs from a news list page."""
	page_url = build_page_url(page_num)
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
	}

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

	# Article cards use anchors with class "name" inside listing templates.
	for link in soup.select(".template a.name"):
		href = link.get("href")
		if href and href.startswith("/news/"):
			urls.append(urljoin(BASE_URL, href.strip()))

	# Fallback: use details links if names are missing.
	if not urls:
		for link in soup.select(".template a.details"):
			href = link.get("href")
			if href and href.startswith("/news/"):
				urls.append(urljoin(BASE_URL, href.strip()))

	return urls


def clean_title(raw: str) -> str:
	"""Normalize title and drop site suffix."""
	title = raw.strip()
	suffix = " - Consiliul Raional Dubăsari"
	if title.endswith(suffix):
		title = title[: -len(suffix)].rstrip()
	return title


def extract_article_data(url: str):
	"""Extract title and content from a dubasari.md article page."""
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
	}

	try:
		response = requests.get(url, headers=headers, timeout=20)
		response.raise_for_status()
		response.encoding = "utf-8"
	except Exception as exc:
		print(f"  Error scraping {url}: {exc}")
		return None, "error"

	soup = BeautifulSoup(response.text, "html.parser")

	# Title is usually inside div.name within the article page container.
	title = ""
	title_tag = soup.select_one("#gen_content .page .name")
	if title_tag:
		title = clean_title(title_tag.get_text())

	if not title:
		fallback_title = soup.find("title")
		if fallback_title:
			title = clean_title(fallback_title.get_text())

	# Content is within the article body; prefer the page container under #gen_content.
	content_container = None
	for selector in ["#gen_content .page", "#gen_content", ".home-news"]:
		content_container = soup.select_one(selector)
		if content_container:
			break

	content = ""
	if content_container:
		for tag in content_container.find_all(["script", "style", "noscript", "iframe"]):
			tag.decompose()

		for comment in content_container.find_all(string=lambda text: isinstance(text, Comment)):
			comment.extract()

		# Remove social/share blocks that add noise.
		for noisy in content_container.select(".all-share, .share, .comment-add, .comment-list"):
			noisy.decompose()

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

		filename = f"Dubasari_MD_{success_count:04d}.json"
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

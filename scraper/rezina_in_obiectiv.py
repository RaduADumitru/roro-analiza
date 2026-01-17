import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

BASE_URL = "https://rezinainobiectiv.md"
OUTPUT_DIR = Path("data-cleaned/raioane/RepMoldova/Rezina")


def build_page_url(page_num: int) -> str:
    if page_num <= 1:
        return f"{BASE_URL}/category/stiri/"
    return f"{BASE_URL}/category/stiri/page/{page_num}/"


def is_majorly_cyrillic(text: str) -> bool:
    if not text:
        return False
    cyrillic_pattern = re.compile(r'[\u0400-\u04FF]')
    letters = re.findall(r'[\u0400-\u04FFa-zA-ZăâîșțĂÂÎȘȚ]', text)
    if not letters:
        return False
    cyrillic_count = sum(1 for c in letters if cyrillic_pattern.match(c))
    return (cyrillic_count / len(letters)) > 0.4


def get_article_urls_from_page(page_num: int):
    page_url = build_page_url(page_num)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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

    for article in soup.find_all("article", class_="vce-post"):
        header = article.find("header", class_="entry-header")
        if not header:
            continue
        h2 = header.find("h2", class_="entry-title")
        link = h2.find("a") if h2 else None
        href = link.get("href") if link else None
        if href:
            urls.append(urljoin(BASE_URL, href))

    return urls


def extract_article_data(url: str):
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

    # Title: h1.entry-title
    title = ""
    h1 = soup.find("h1", class_="entry-title")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            title = title_text.split(" - ")[0].strip() if " - " in title_text else title_text

    # Content: div.entry-content
    entry = soup.find("div", class_="entry-content")

    if not entry:
        return None, "no_content"

    # Remove non-textual/unwanted blocks
    for tag in entry.find_all(["script", "style", "noscript", "iframe", "ins"]):
        tag.decompose()
    for comment in entry.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    # Remove unwanted classes and widgets
    for cls in [
        "wp-block-embed",
        "meks-ess-share",
        "meks-easy-social-share",
        "share",
        "post-meta",
        "wp-video",
        "download",
        "related-posts",
        "yarpp-related",
        "sharedaddy",
    ]:
        for node in entry.find_all(class_=lambda c: c and cls in c):
            node.decompose()
    
    for tag_name in ["aside", "figure", "nav"]:
        for node in entry.find_all(tag_name):
            node.decompose()

    text_parts = [
        line.strip()
        for line in entry.get_text("\n", strip=True).splitlines()
        if line.strip()
    ]
    content = "\n".join(text_parts).replace("\xa0", " ")

    if is_majorly_cyrillic(content):
        return None, "cyrillic"
    if not title:
        return None, "no_title"
    if not content:
        return None, "no_content"

    data = {
        "title": title,
        "content": content,
        "metadata": {},
    }

    return data, "success"


def save_json(data, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
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
    for u in all_article_urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    print("=" * 60)
    print(f"Total unique articles to scrape: {len(unique_urls)}")
    print("=" * 60)

    success_count = 0
    skipped_cyrillic = 0

    for idx, url in enumerate(unique_urls):
        print(f"Scraping {idx + 1}/{len(unique_urls)}: {url}")
        article_data, status = extract_article_data(url)

        if status == "cyrillic":
            print("  Skipped (majorly Cyrillic content)")
            skipped_cyrillic += 1
            continue
        if status == "error":
            print("  Failed (network or parsing error)")
            continue
        if status == "no_title":
            print("  Failed (could not extract title)")
            continue
        if status == "no_content":
            print("  Failed (could not extract content)")
            continue

        filename = f"Rezina_In_Obiectiv_{success_count:04d}.json"
        filepath = OUTPUT_DIR / filename

        article_data["metadata"]["original_file"] = filename
        article_data["metadata"]["source_url"] = url

        save_json(article_data, filepath)
        print(f"  Saved to {filename}")
        success_count += 1

    print("=" * 60)
    print(f"Successfully scraped {success_count} out of {len(unique_urls)} articles.")
    print(f"Skipped {skipped_cyrillic} articles (majorly Cyrillic).")
    print("=" * 60)


if __name__ == "__main__":
    main()
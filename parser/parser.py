import asyncio
import json
import os
from urllib.parse import urlparse, urljoin
from playwright.async_api import Page, Response, Route, Browser
import pandas as pd
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm_asyncio
from fake_headers import Headers
visited = set()
semaphore = asyncio.Semaphore(5)
output_file = "xhr_requests.jsonl"
api_filter = ['mc.yandex', '.svg']
url_filter = ['.svg', '.pdf', '.jpg', '.jpeg']

async def intercept_xhr_requests(page: Page, current_url, base_domain: str):
    async def handle_route(route: Route):
        request = route.request
        if (
            request.resource_type in ("xhr", "fetch") and
            not any(f in request.url for f in api_filter) and
            request.url.startswith(base_domain)
        ):
            try:
                data = {
                    "page":page.url,
                    "url": request.url,
                    "method": request.method,
                    "headers": dict(request.headers),
                    "post_data": request.post_data,
                    "page_url": current_url
                }
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"Error logging request: {e}")
        await route.continue_()

    await page.route("**/*", handle_route)

async def extract_links(page:Page, base_url):
    anchors = await page.eval_on_selector_all("a", "elements => elements.map(a => a.href)")
    valid_links = set()
    for link in anchors:
        if not link or link.startswith("mailto:") or link.startswith("javascript:"):
            continue
        if base_url in link:
            valid_links.add(link.split("#")[0])  # убираем якоря
    return valid_links

async def crawl_page(url, browser: Browser, base_domain):
    headers = Headers(browser="chrome", os="win").generate()
    async with semaphore:
        context = await browser.new_context(extra_http_headers=headers)
        page = await context.new_page()
        await intercept_xhr_requests(page, url, base_domain)
        links = set()
        try:
            if any(f in url for f in url_filter):
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps({'page': url}, ensure_ascii=False) + "\n")
            else:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                links = await extract_links(page, base_domain)
            visited.add(url)
        except Exception as e:
            print(f"[!] Failed to crawl {url}: {e}")
        finally:
            await context.close()
        return links


async def crawl_site(start_url, max_pages=10000):
    parsed_url = urlparse(start_url)
    base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"

    to_visit = set([start_url])
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--ignore-certificate-errors"])
        pbar = tqdm_asyncio(total=max_pages)
        total_found = 0
        while to_visit and len(visited) < max_pages:
            tasks = []
            next_batch = set()
            for url in list(to_visit):
                if url not in visited and len(visited) + len(tasks) < max_pages:
                    tasks.append(crawl_page(url, browser, base_domain))
                    to_visit.remove(url)
            results = await asyncio.gather(*tasks)
            for found_links in results:
                if found_links:
                    new_links = found_links - visited - to_visit
                    next_batch |= new_links
                    total_found += len(new_links)
            to_visit |= next_batch - visited
            pbar.update(len(tasks))
            pbar.total = len(visited) + len(to_visit)
            pbar.refresh()
        pbar.close()
        await browser.close()

if __name__ == "__main__":
    import sys
    start_url = sys.argv[1] if len(sys.argv) > 1 else "https://orgp.spb.ru/"
    if os.path.exists(output_file):
        os.remove(output_file)
    asyncio.run(crawl_site(start_url))

import asyncio
import json
import time
from urllib.parse import urljoin, urlparse
import sys
import aiofiles
from playwright.async_api import async_playwright, Response
from pathlib import Path

VISITED = set()
DOMAIN = ""
VISIT_DELAY = 3
MAX_WORKERS = 3
TIME_LIMIT = 120

start_time = None
time_limit = None

start_url = sys.argv[1] if len(sys.argv) > 1 else "https://transport.orgp.spb.ru"
output_file = start_url
if 'https://' in output_file:
    output_file = output_file.removeprefix('https://')
elif 'http://' in output_file:
    output_file = output_file.removeprefix('http://')
else: pass
output_file = output_file.replace('.', '_').replace('/', '-')
output_file = Path(f"../db/temp/{output_file}.jsonl")

async def save_jsonl(entry: dict):
    async with aiofiles.open(output_file, mode="a", encoding="utf-8") as f:
        line = json.dumps(entry, ensure_ascii=False)
        await f.write(line + "\n")

async def handle_response(response:Response):
    try:
        request = response.request
        if request.resource_type not in ("xhr", "fetch"):
            return
        content_type = response.headers.get("content-type", "")
        if not any(ct in content_type for ct in ("application/json", "application/x-www-form-urlencoded")):
            return
        post_data = await request.post_data_json if request.method != "GET" else None
        body = await response.text()
        entry = {
            "url": response.url,
            "status": response.status,
            "method":request.method,
            "request_body":post_data,
            "response_body": json.loads(body)
        }
        await save_jsonl(entry)
    except Exception:
        pass

async def worker(name, queue, browser):
    context = await browser.new_context()
    page = await context.new_page()
    page.on("response", handle_response)

    global start_time, time_limit

    while True:
        # Проверка таймаута
        if time.time() - start_time > time_limit:
            print(f"Worker {name} timeout reached, exiting")
            break

        try:
            url = await asyncio.wait_for(queue.get(), timeout=2)
        except asyncio.TimeoutError:
            # Очередь могла быть пустой — проверим таймаут ещё раз
            if time.time() - start_time > time_limit:
                print(f"Worker {name} timeout on empty queue, exiting")
                break
            else:
                continue

        if url in VISITED:
            queue.task_done()
            continue

        print(f"Worker {name} processing: {url}")
        VISITED.add(url)

        try:
            await page.goto(url, timeout=15000, wait_until="domcontentloaded")
            await asyncio.sleep(VISIT_DELAY)

            anchors = await page.query_selector_all("a[href]")
            for a in anchors:
                href = await a.get_attribute("href")
                if href:
                    abs_url = urljoin(url, href.split("#")[0])
                    if abs_url.startswith(DOMAIN) and abs_url not in VISITED:
                        await queue.put(abs_url)
        except Exception as e:
            print(f"Error {e} on {url}")

        queue.task_done()

    await context.close()
    print(f"Worker {name} done")

async def main(start_url, t_limit=60):
    global DOMAIN, start_time, time_limit
    DOMAIN = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(start_url))
    start_time = time.time()
    time_limit = t_limit

    queue = asyncio.Queue()
    await queue.put(start_url)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        tasks = [asyncio.create_task(worker(f"W{i+1}", queue, browser)) for i in range(MAX_WORKERS)]

        # Ждём, пока все задачи завершатся (само прервётся по таймауту в воркерах)
        await asyncio.gather(*tasks, return_exceptions=True)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main(start_url, TIME_LIMIT))
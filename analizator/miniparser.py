import asyncio
import httpx
from fake_useragent import UserAgent

ua = UserAgent()
def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept": "application/json",
    }

async def fetch(url, client, semaphore, max_retries=3):
    retries = 0
    while retries < max_retries:
        async with semaphore:
            headers = get_headers()
            try:
                resp = await client.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                return await resp.json()
            except Exception as e:
                print(f"Error fetching {url}: {e}, retry {retries+1}")
                retries += 1
    print(f"Failed to fetch {url} after {max_retries} retries")
    return None

async def main(urls):
    semaphore = asyncio.Semaphore(5)
    async with httpx.AsyncClient() as client:
        tasks = [fetch(url, client, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
    ]
    results = asyncio.run(main(urls))
    for url, body in zip(urls, results):
        print(f"URL: {url}\nBody: {body}\n")
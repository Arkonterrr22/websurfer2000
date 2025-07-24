import asyncio
import httpx
import json
from fake_useragent import UserAgent

ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

async def fetch(entry: dict, client: httpx.AsyncClient, semaphore: asyncio.Semaphore, max_retries: int = 3):
    retries = 0
    while retries < max_retries:
        async with semaphore:
            headers = get_headers()
            try:
                method = entry.get("method", "get").lower()
                url = entry["url"]
                data = entry.get("post_data")

                if method == "get":
                    response = await client.get(url, headers=headers, timeout=10)
                elif method == "post":
                    response = await client.post(url, headers=headers, json=data, timeout=10)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                response.raise_for_status()
                return {
                    "url": str(response.url),
                    "status": response.status_code,
                    "method": method.upper(),
                    "request_body": data,
                    "response_body": response.json()
                }

            except Exception as e:
                print(f"Error fetching {entry['url']}: {e}, retry {retries+1}")
                retries += 1

    print(f"Failed to fetch {entry['url']} after {max_retries} retries")
    return None

async def main(requests: list[dict]):
    semaphore = asyncio.Semaphore(5)
    async with httpx.AsyncClient(verify=False) as client:
        tasks = [fetch(entry, client, semaphore) for entry in requests]
        results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    requests_list = [
        {"method": "get", "url": "https://jsonplaceholder.typicode.com/posts/1"},
        {"method": "post", "url": "https://httpbin.org/post", "post_data": {"key": "value"}},
    ]

    results = asyncio.run(main(requests_list))
    for result in results:
        print(json.dumps(result, indent=2, ensure_ascii=False))
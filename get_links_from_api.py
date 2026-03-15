import os
import json
import requests
import time


def scrape_daryo_api():
    api_url = "https://data.daryo.uz/api/v1/site/news-latest/list"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://daryo.uz/",
        "Origin": "https://daryo.uz",
        "Accept": "application/json, text/plain, */*"
    }

    limit = 1000
    offset = 0
    save_dir = "data/links"
    
    # Create the directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    total_news_fetched = 0

    while True:
        print(f"Fetching news from offset {offset}...")
        
        params = {
            "limit": limit,
            "offset": offset,
            "order": "date+desc"
        }

        try:
            response = requests.get(api_url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for the expected structure where actual data is in data['data']
                if isinstance(data, dict) and 'data' in data:
                    news_list = data['data']
                elif isinstance(data, list):
                    news_list = data
                else:
                    print("Unexpected JSON structure.")
                    break
                
                if not news_list:
                    print("No more news fetched. Ending scrape.")
                    break
                
                processed_links = []
                for item in news_list:
                    processed_links.append({
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "category": item.get("category"),
                        "slug": item.get("slug"),
                        "date": item.get("date")
                    })
                
                # Save the processed links to a JSON file named by the offset
                file_path = os.path.join(save_dir, f"{offset}.json")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(processed_links, f, ensure_ascii=False, indent=4)
                    
                total_news_fetched += len(processed_links)
                print(f"Saved {len(processed_links)} items to {file_path}")
                
                # If we fetched fewer items than our limit, we hit the end
                if len(news_list) < limit:
                    print("Fetched less than the limit, meaning we've reached the end.")
                    break
                
                offset += limit
                
            else:
                print(f"Failed to fetch. Status code: {response.status_code}")
                print(response.text)
                break
                
        except Exception as e:
            print(f"An error occurred: {e}")
            break

        # Sleep between requests to avoid rate limits and timeouts on their server
        time.sleep(3)

    print(f"\nFinished scraping! Total news links retrieved: {total_news_fetched}")


if __name__ == "__main__":
    scrape_daryo_api()


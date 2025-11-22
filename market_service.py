import requests
import time
import re
from urllib.parse import quote

class MarketService:
    def __init__(self):
        self._cache = None
        self._last_update = 0
        self._cache_ttl = 600  # Cache for 10 minutes (600 seconds)
        
        self.search_api_url = "https://steamcommunity.com/market/search/render/?query=&start=0&count=12&search_descriptions=0&sort_column=popular&sort_dir=desc&appid=730&norender=1"
        self.price_api_url = "https://steamcommunity.com/market/priceoverview/"

    def get_top_selling_items(self):
        """
        Fetches the top 12 selling CS2 items from the Steam Market.
        """
        current_time = time.time()
        
        if self._cache and (current_time - self._last_update < self._cache_ttl):
            print("[MarketService] Using cached market data for top items")
            return self._cache

        try:
            print("[MarketService] Fetching top items data from Steam...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(self.search_api_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    items = []
                    for item in data.get('results', []):
                        items.append({
                            'name': item.get('hash_name'),
                            'image_url': f"https://community.cloudflare.steamstatic.com/economy/image/{item.get('asset_description', {}).get('icon_url')}",
                            'price_text': item.get('sell_price_text'), 
                            'volume': item.get('sell_listings'),     
                            'sale_price': item.get('sell_price', 0) / 100.0 
                        })
                    
                    self._cache = items
                    self._last_update = current_time
                    return items
            
            print(f"Steam API (search) returned status: {response.status_code}")
            return self._cache or []

        except Exception as e:
            print(f"Error fetching top market data: {e}")
            return self._cache or []

    def get_price_for_item(self, item_name):
        """
        Fetches the current lowest price for a specific item from the Steam Market.
        """
        print(f"[MarketService] Fetching price for '{item_name}'...")
        try:
            # Steam API requires the item name to be URL-encoded
            encoded_item_name = quote(item_name)
            
            params = {
                'appid': 730,  # CS2
                'currency': 1, # USD
                'market_hash_name': encoded_item_name
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(self.price_api_url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    # Price string is like '$1.23 USD' or '¥ 7.80'
                    price_str = data.get('lowest_price') or data.get('median_price')
                    if price_str:
                        # Use regex to find the first number (integer or float)
                        match = re.search(r'[\d\.,]+', price_str)
                        if match:
                            price = float(match.group(0).replace(',', ''))
                            print(f"[MarketService] ✓ Price for '{item_name}': ${price}")
                            return price
                    # API returned success but no price data available
                    # This is common for items not currently listed on market
                    return None
                else:
                    # API returned success=false
                    return None
            
            print(f"[MarketService] Failed to get price for '{item_name}'. Status: {response.status_code}, Response: {response.text[:200]}")
            return None

        except Exception as e:
            print(f"Error fetching price for '{item_name}': {e}")
            return None

market_service = MarketService()
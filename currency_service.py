import requests
import time

class CurrencyService:
    def __init__(self):
        # Simple in-memory cache: { "CNY": {"rate": 7.24, "timestamp": 1712345678} }
        self._cache = {}
        self._cache_ttl = 3600  # Cache TTL: 1 hour (3600 seconds)
        self.api_url = "https://api.frankfurter.app/latest"

    def get_rate(self, to_currency):
        """
        Gets the exchange rate for 1 USD to the target_currency.
        Defaults to 1.0 if the target is USD.
        """
        to_currency = to_currency.upper()
        
        if to_currency == 'USD':
            return 1.0

        # 1. Check cache
        current_time = time.time()
        if to_currency in self._cache:
            data = self._cache[to_currency]
            if current_time - data['timestamp'] < self._cache_ttl:
                print(f"[CurrencyService] Using cached rate for {to_currency}: {data['rate']}")
                return data['rate']

        # 2. If cache missed or expired, fetch from API
        try:
            # Frankfurter API is free and requires no key
            print(f"[CurrencyService] Fetching live rate for {to_currency}...")
            response = requests.get(f"{self.api_url}?from=USD&to={to_currency}")
            data = response.json()
            
            rate = data['rates'].get(to_currency)
            
            if rate:
                # 3. Write to cache
                self._cache[to_currency] = {
                    "rate": rate,
                    "timestamp": current_time
                }
                return rate
            else:
                print(f"Error: Currency {to_currency} not found.")
                return 1.0
                
        except Exception as e:
            print(f"Error fetching currency data: {e}")
            # If the API fails, return 1.0 to avoid errors, or could return stale cache
            return 1.0

# Singleton Pattern: Create a global instance for server.py to use
currency_service = CurrencyService()

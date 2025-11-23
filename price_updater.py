import os
import time
from sqlalchemy import create_engine, text
from market_service import market_service
import datetime

# --- Database Configuration ---
# Make sure to use the same database URI as your main app
DATABASEURI = "postgresql://zw3155:477430@34.139.8.30/proj1part2"
engine = create_engine(DATABASEURI)

# --- Constants ---
STEAM_PLATFORM_ID = 1  # Assuming '1' is the ID for Steam in your Platforms table
UPDATE_INTERVAL_MINUTES = 360  # How often to run the full update cycle (6 hours)
REQUEST_DELAY_SECONDS = 5    # Time to wait between individual API requests to avoid rate limiting

def update_prices():
    """
    Fetches all items from the database, gets their current market price from Steam,
    and inserts a new price snapshot into the MarketSnapshots table.
    """
    print(f"[{datetime.datetime.now()}] Starting price update cycle...")
    
    items_to_update = []
    try:
        with engine.connect() as conn:
            query = text("SELECT item_id, market_name FROM Items")
            result = conn.execute(query).fetchall()
            items_to_update = [{'item_id': row[0], 'market_name': row[1]} for row in result]
            print(f"Found {len(items_to_update)} items to update.")
    except Exception as e:
        print(f"Error fetching items from database: {e}")
        return

    for item in items_to_update:
        item_id = item['item_id']
        market_name = item['market_name']
        
        # 1. Get live price from the market service
        price = market_service.get_price_for_item(market_name)
        
        # 2. If we got a valid price, insert it into the database
        if price is not None:
            try:
                with engine.begin() as conn: # Use .begin() for auto-committing transaction
                    query_max_id = text("SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM MarketSnapshots")
                    new_snapshot_id = conn.execute(query_max_id).scalar()

                    query_insert = text("""
                        INSERT INTO MarketSnapshots (snapshot_id, item_id, platform_id, price, currency, captured_at)
                        VALUES (:sid, :iid, :pid, :price, 'USD', :now)
                    """)
                    
                    conn.execute(query_insert, {
                        "sid": new_snapshot_id,
                        "iid": item_id,
                        "pid": STEAM_PLATFORM_ID,
                        "price": price,
                        "now": datetime.datetime.utcnow()
                    })
                    print(f"Successfully inserted snapshot for item_id {item_id} ({market_name}) at price ${price}")

            except Exception as e:
                print(f"Error inserting snapshot for item_id {item_id}: {e}")
        else:
            print(f"Skipping database insert for item_id {item_id} due to failed price fetch.")

        # 3. Wait a bit before the next request to be respectful to the API
        print(f"Waiting {REQUEST_DELAY_SECONDS} seconds...")
        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"[{datetime.datetime.now()}] Price update cycle finished.")

if __name__ == "__main__":
    print("--- Real-Time Price Updater ---")
    print(f"Running initial update...")
    update_prices() # Run once immediately on start
    
    while True:
        print(f"Next update cycle will start in {UPDATE_INTERVAL_MINUTES} minutes.")
        time.sleep(UPDATE_INTERVAL_MINUTES * 60)
        update_prices()

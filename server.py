import os

from flask import Flask, render_template, request, redirect, url_for, g, flash
from sqlalchemy import create_engine, text

# Initialize the Flask application
app = Flask(__name__)

# --- Database Configuration ---
# ！！重要！！: 把 'your_password' 替换成你的真实数据库密码
DATABASEURI = "postgresql://zw3155:your_password@w4111b.cs.columbia.edu/zw3155"

# Create the database engine
engine = create_engine(DATABASEURI)

def get_db_conn():
    """
    Opens a new database connection if one is not already open.
    """
    if 'db_conn' not in g:
        try:
            g.db_conn = engine.connect()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            g.db_conn = None  # Store None to avoid retries
    return g.db_conn

@app.teardown_appcontext
def close_db_conn(exception):
    """
    Closes the database connection at the end of the request.
    """
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

# --- Routes ---

@app.route('/')
def home():
    """
    Root URL route. Redirects to the main items list page.
    """
    return redirect(url_for('items_list'))


@app.route('/items')
def items_list():
    """
    STEP 1: Displays a list of all items from the Items table.
    """
    conn = None
    items = []
    try:
        conn = engine.connect()
        query = text("SELECT item_id, market_name, game, rarity, exterior FROM Items ORDER BY market_name")
        result = conn.execute(query)
        items = [dict(row) for row in result.fetchall()]
    except Exception as e:
        print(f"Error fetching items: {e}")
    finally:
        if conn:
            conn.close()
            
    return render_template("items.html", items=items)


@app.route('/item/<int:item_id>')
def item_detail(item_id):
    """
    STEP 2: Shows detailed info for a single item, including
    purchase history, sales history, and market snapshots.
    """
    conn = None
    item_details = {}
    purchases = []
    sales = []
    snapshots = []
    
    try:
        conn = engine.connect()
        
        # Query 1: Get basic item details
        query_item = text("SELECT market_name, game, rarity, exterior, extra FROM Items WHERE item_id = :id")
        result_item = conn.execute(query_item, {"id": item_id}).fetchone()
        if result_item:
            item_details = dict(result_item)
        
        # Query 2: Get all purchases for this item
        query_purchases = text("""
            SELECT p.ts, p.price, p.currency, u.display_name, pf.platform_name
            FROM Purchases p
            JOIN Users u ON u.user_id = p.user_id
            JOIN Platforms pf ON pf.platform_id = p.platform_id
            WHERE p.item_id = :id
            ORDER BY p.ts DESC
        """)
        purchases = [dict(row) for row in conn.execute(query_purchases, {"id": item_id}).fetchall()]
        
        # Query 3: Get all sales for this item
        query_sales = text("""
            SELECT s.ts, s.price, s.fee, s.currency, u.display_name, pf.platform_name
            FROM Sales s
            JOIN Users u ON u.user_id = s.user_id
            JOIN Platforms pf ON pf.platform_id = s.platform_id
            WHERE s.item_id = :id
            ORDER BY s.ts DESC
        """)
        sales = [dict(row) for row in conn.execute(query_sales, {"id": item_id}).fetchall()]
        
        # Query 4: Get market snapshots for this item
        query_snapshots = text("""
            SELECT ms.captured_at, ms.price, ms.currency, pf.platform_name
            FROM MarketSnapshots ms
            JOIN Platforms pf ON pf.platform_id = ms.platform_id
            WHERE ms.item_id = :id
            ORDER BY ms.captured_at DESC
        """)
        snapshots = [dict(row) for row in conn.execute(query_snapshots, {"id": item_id}).fetchall()]

    except Exception as e:
        print(f"Error fetching details for item_id {item_id}: {e}")
    
    finally:
        if conn:
            conn.close()
            
    return render_template(
        "item_detail.html",
        item=item_details,
        item_id=item_id,
        purchases=purchases,
        sales=sales,
        snapshots=snapshots
    )


# === User Management Routes ===

@app.route('/users')
def users_list():
    """
    STEP 3 (Part 1): Displays a list of all users in the Users table.
    """
    conn = None
    users = []
    try:
        conn = engine.connect()
        query = text("SELECT user_id, display_name, email, created_at FROM Users ORDER BY user_id")
        result = conn.execute(query)
        users = [dict(row) for row in result.fetchall()]
    except Exception as e:
        print(f"Error fetching users: {e}")
    finally:
        if conn:
            conn.close()
            
    return render_template("users_list.html", users=users)


@app.route('/users/new')
def user_new_form():
    """
    STEP 3 (Part 2): Displays the form to add a new user.
    This is a GET request.
    """
    return render_template("new_user.html")


@app.route('/users/create', methods=['POST'])
def user_create():
    """
    STEP 3 (Part 3): Processes the form submission from new_user.html.
    This is a POST request that handles the INSERT operation.
    """
    user_id = request.form.get('user_id')
    email = request.form.get('email')
    display_name = request.form.get('display_name')
    
    try:
        with engine.begin() as conn:
            query = text("INSERT INTO Users(user_id, email, display_name) VALUES (:id, :email, :name)")
            conn.execute(query, {"id": user_id, "email": email, "name": display_name})
    except Exception as e:
        print(f"Error inserting new user: {e}")
    
    return redirect(url_for('users_list'))


# === Dashboard Route (THIS IS THE FIX) ===

@app.route('/dashboard')
def dashboard():
    """
    STEP 4: Displays the advanced queries from Project Part 2.
    """
    conn = None
    query_1_results = []
    query_2_results = []
    query_3_results = []
    
    query_1 = text("""
        WITH tx AS (
          SELECT user_id, item_id, platform_id, ts, price, 'BUY'  AS side FROM Purchases
          UNION ALL
          SELECT user_id, item_id, platform_id, ts, price, 'SELL' AS side FROM Sales
        )
        SELECT u.display_name, i.market_name, p.platform_name, t.side, t.ts, t.price
        FROM tx t
        JOIN Users     u ON u.user_id     = t.user_id
        JOIN Items     i ON i.item_id     = t.item_id
        JOIN Platforms p ON p.platform_id = t.platform_id
        WHERE t.ts >= CURRENT_DATE - INTERVAL '60 days'
        ORDER BY u.display_name, t.ts DESC
        LIMIT 50;
    """)
    
    query_2 = text("""
        WITH latest_snap AS (
          SELECT DISTINCT ON (item_id, platform_id)
                 item_id, platform_id, price, captured_at
          FROM MarketSnapshots
          ORDER BY item_id, platform_id, captured_at DESC
        ),
        mark AS (
          SELECT item_id, MAX(price) AS mark_price
          FROM latest_snap
          GROUP BY item_id
        ),
        avg_cost AS (
          SELECT user_id, item_id, AVG(price) AS avg_buy
          FROM Purchases
          GROUP BY user_id, item_id
        )
        SELECT u.display_name,
               SUM(m.mark_price)         AS est_market_value,
               ROUND(AVG(a.avg_buy),2)   AS avg_cost_across_items,
               COUNT(a.item_id)          AS distinct_items
        FROM avg_cost a
        JOIN mark m     ON m.item_id = a.item_id
        JOIN Users u    ON u.user_id = a.user_id
        GROUP BY u.display_name
        HAVING COUNT(a.item_id) >= 2
        ORDER BY est_market_value DESC;
    """)
    
    query_3 = text("""
        SELECT
          pf.platform_name,
          COUNT(*) AS n_sales,
          ROUND( AVG(s.price),2) AS avg_sale_price
        FROM Sales s
        JOIN Platforms pf ON pf.platform_id = s.platform_id
        JOIN Items i ON i.item_id = s.item_id
        WHERE s.ts >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY pf.platform_name
        ORDER BY n_sales DESC, avg_sale_price DESC;
    """)

    try:
        conn = engine.connect()
        query_1_results = [dict(row) for row in conn.execute(query_1).fetchall()]
        query_2_results = [dict(row) for row in conn.execute(query_2).fetchall()]
        query_3_results = [dict(row) for row in conn.execute(query_3).fetchall()]
    except Exception as e:
        print(f"Error running dashboard queries: {e}")
    finally:
        if conn:
            conn.close()
            
    return render_template(
        "dashboard.html",
        transactions=query_1_results,
        portfolios=query_2_results,
        platforms=query_3_results
    )

# --- Main Application Runner ---

# === Purchase Management Routes (Get-or-Create Logic) ===

@app.route('/purchases/new')
def purchase_new_form():
    """
    Displays the form to add a new purchase record.
    We also query for existing users and platforms to populate dropdowns.
    """
    conn = get_db_conn()
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))
        
    users = []
    platforms = []
    try:
        users = [dict(row) for row in conn.execute(text("SELECT user_id, display_name FROM Users ORDER BY display_name")).fetchall()]
        platforms = [dict(row) for row in conn.execute(text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")).fetchall()]
    except Exception as e:
        print(f"Error fetching users/platforms: {e}")
        
    return render_template("new_purchase.html", users=users, platforms=platforms)


@app.route('/purchases/create', methods=['POST'])
def purchase_create():
    """
    Processes the form submission from new_purchase.html.
    Implements the "Get-or-Create" logic for an item.
    """
    # 1. Get all form data
    user_id = request.form.get('user_id')
    platform_id = request.form.get('platform_id')
    market_name = request.form.get('market_name')
    exterior = request.form.get('exterior')
    price = request.form.get('price')
    currency = request.form.get('currency')
    ts = request.form.get('ts')
    
    # ！！重要！！:
    # 你的 Purchases 表有一个 'purchase_id' 主键。
    # 我们也需要为它生成一个新 ID。
    
    conn = get_db_conn()
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))

    try:
        # Start a transaction
        with conn.begin():
            # --- Get-or-Create Item Logic ---
            query_item = text("SELECT item_id FROM Items WHERE market_name = :name AND exterior = :ext")
            result = conn.execute(query_item, {"name": market_name, "ext": exterior}).fetchone()
            
            item_id_to_use = None
            
            if result:
                # Case A: Item exists. Use its ID.
                item_id_to_use = result['item_id']
                print(f"Item found. Using existing item_id: {item_id_to_use}")
            else:
                # Case B: Item does not exist. Create it.
                print(f"Item not found. Creating new item: {market_name} ({exterior})")
                
                # 1. Find the next available item_id
                query_max_item_id = text("SELECT MAX(item_id) + 1 FROM Items")
                item_id_to_use = conn.execute(query_max_item_id).scalar()
                
                # 2. Insert the new item into the Items table
                # (We fill non-required fields with NULL or defaults)
                query_insert_item = text("""
                    INSERT INTO Items(item_id, market_name, game, rarity, exterior)
                    VALUES (:id, :name, 'CS2', NULL, :ext)
                """)
                conn.execute(query_insert_item, {"id": item_id_to_use, "name": market_name, "ext": exterior})
                print(f"New item created with item_id: {item_id_to_use}")

            # --- Create Purchase Record Logic ---
            
            # 1. Find the next available purchase_id
            query_max_purchase_id = text("SELECT MAX(purchase_id) + 1 FROM Purchases")
            new_purchase_id = conn.execute(query_max_purchase_id).scalar()
            
            # 2. Insert the new purchase record
            query_insert_purchase = text("""
                INSERT INTO Purchases(purchase_id, user_id, item_id, platform_id, ts, price, currency)
                VALUES (:pid, :uid, :iid, :plid, :ts, :price, :curr)
            """)
            conn.execute(query_insert_purchase, {
                "pid": new_purchase_id,
                "uid": user_id,
                "iid": item_id_to_use,
                "plid": platform_id,
                "ts": ts,
                "price": price,
                "curr": currency
            })
            print(f"Successfully inserted purchase record {new_purchase_id}")
            
        # The transaction is automatically committed here if no errors
        
    except Exception as e:
        print(f"Error during 'Get-or-Create' transaction: {e}")
        # In a real app, you'd show an error page
        # return render_template("error.html", message=str(e))
    
    # Redirect to the detail page for the item we just added/updated
    return redirect(url_for('item_detail', item_id=item_id_to_use))

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8111, debug=True)

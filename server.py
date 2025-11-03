import os
from flask import Flask, render_template, request, redirect, url_for, g, flash
from sqlalchemy import create_engine, text

# Initialize the Flask application
app = Flask(__name__)
# You need a secret key to use flash()
app.secret_key = os.urandom(24) 

# --- Database Configuration ---
# Your password has been added here
DATABASEURI = "postgresql://zw3155:477430@w4111b.cs.columbia.edu/zw3155"

# Create the database engine
engine = create_engine(DATABASEURI)

# --- Database Connection Management (Used by ALL routes) ---

@app.before_request
def get_db_conn():
    """
    Opens a new database connection if one is not already open.
    The connection is stored in 'g', a Flask global context object.
    """
    if 'db_conn' not in g:
        try:
            g.db_conn = engine.connect()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            g.db_conn = None # Store None to avoid retries

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
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))
        
    items = []
    try:
        query = text("SELECT item_id, market_name, game, rarity, exterior FROM Items ORDER BY market_name")
        result = conn.execute(query)
        items = [dict(row) for row in result.fetchall()]
    except Exception as e:
        print(f"Error fetching items: {e}")
        flash(f"Error fetching items: {e}")
            
    return render_template("items.html", items=items)


@app.route('/item/<int:item_id>')
def item_detail(item_id):
    """
    STEP 2: Shows detailed info for a single item.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))
        
    item_details = {}
    purchases = []
    sales = []
    snapshots = []
    
    try:
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
        flash(f"Error fetching item details: {e}")
    
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
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))
        
    users = []
    try:
        query = text("SELECT user_id, display_name, email, created_at FROM Users ORDER BY user_id")
        result = conn.execute(query)
        users = [dict(row) for row in result.fetchall()]
    except Exception as e:
        print(f"Error fetching users: {e}")
        flash(f"Error fetching users: {e}")
            
    return render_template("users_list.html", users=users)


@app.route('/users/new')
def user_new_form():
    """
    STEP 3 (Part 2): Displays the form to add a new user.
    """
    return render_template("new_user.html")


@app.route('/users/create', methods=['POST'])
def user_create():
    """
    STEP 3 (Part 3): Processes the form submission from new_user.html.
    """
    user_id = request.form.get('user_id')
    email = request.form.get('email')
    display_name = request.form.get('display_name')
    
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('users_list'))
        
    try:
        # Use conn.begin() to start a transaction with the connection from 'g'
        with conn.begin():
            query = text("INSERT INTO Users(user_id, email, display_name) VALUES (:id, :email, :name)")
            conn.execute(query, {"id": user_id, "email": email, "name": display_name})
        flash("User created successfully!")
    except Exception as e:
        print(f"Error inserting new user: {e}")
        flash(f"Error inserting new user: {e}")
    
    return redirect(url_for('users_list'))


# === Dashboard Route (Upgraded for Req #4) ===

@app.route('/dashboard')
def dashboard():
    """
    STEP 8 (Upgrade): Displays the advanced queries, including
    a full portfolio dashboard as per Req #4.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))

    query_1_results = []
    portfolio_results = [] # Renamed from query_2_results
    query_3_results = []
    
    # --- Query 1: Recent Transactions (This query is unchanged) ---
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
    
    # --- Query 2 (NEW): Full Portfolio Dashboard Query (Req #4) ---
    query_portfolio = text("""
        WITH latest_market_price AS (
            -- 1. Get the latest known market price for EACH item
            SELECT DISTINCT ON (item_id)
                   item_id,
                   price AS market_price
            FROM MarketSnapshots
            ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            -- 2. Calculate avg cost and total quantity BOUGHT per user per item
            SELECT user_id, item_id,
                   COUNT(*) AS qty_bought,
                   SUM(price) AS total_cost_basis_item,
                   AVG(price) AS avg_buy_cost
            FROM Purchases
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            -- 3. Calculate avg revenue and total quantity SOLD per user per item
            SELECT user_id, item_id,
                   COUNT(*) AS qty_sold,
                   SUM(price) AS total_sale_revenue_item,
                   SUM(fee) AS total_sale_fees_item
            FROM Sales
            GROUP BY user_id, item_id
        ),
        holdings AS (
            -- 4. Calculate current holdings for each user/item
            SELECT
                p.user_id,
                p.item_id,
                (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) AS quantity_held,
                p.avg_buy_cost,
                p.total_cost_basis_item
            FROM
                purchase_summary p
            LEFT JOIN
                sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
            WHERE
                (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) > 0
        ),
        portfolio_calcs AS (
            -- 5. Calculate Realized and Unrealized PnL per user/item
            SELECT
                COALESCE(p.user_id, s.user_id) AS user_id,
                COALESCE(p.item_id, s.item_id) AS item_id,
                
                -- Cost Basis Calcs
                COALESCE(p.total_cost_basis_item, 0) AS total_cost_basis,
                
                -- Holdings Calcs (Unrealized)
                COALESCE(h.quantity_held, 0) AS quantity_held,
                COALESCE(lmp.market_price, 0) AS market_price,
                (COALESCE(lmp.market_price, 0) * COALESCE(h.quantity_held, 0)) AS current_market_value,
                (COALESCE(lmp.market_price, 0) - COALESCE(h.avg_buy_cost, 0)) * COALESCE(h.quantity_held, 0) AS unrealized_pnl,
                
                -- Sales Calcs (Realized)
                COALESCE(s.qty_sold, 0) AS qty_sold,
                COALESCE(s.total_sale_revenue_item, 0) AS total_sale_revenue,
                COALESCE(s.total_sale_fees_item, 0) AS total_sale_fees,
                -- Realized PnL = (Sale Revenue - Fees) - (Cost of Items Sold)
                (COALESCE(s.total_sale_revenue_item, 0) - COALESCE(s.total_sale_fees_item, 0)) - (COALESCE(p.avg_buy_cost, 0) * COALESCE(s.qty_sold, 0)) AS realized_pnl
                
            FROM
                purchase_summary p
            FULL OUTER JOIN
                sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
            LEFT JOIN
                holdings h ON COALESCE(p.user_id, s.user_id) = h.user_id AND COALESCE(p.item_id, s.item_id) = h.item_id
            LEFT JOIN
                latest_market_price lmp ON COALESCE(p.item_id, s.item_id) = lmp.item_id
        )
        -- 6. Final Aggregation per User
        SELECT
            u.display_name,
            SUM(pc.total_cost_basis) AS total_investment,
            SUM(pc.current_market_value) AS total_market_value,
            SUM(pc.realized_pnl) AS total_realized_pnl,
            SUM(pc.unrealized_pnl) AS total_unrealized_pnl,
            (SUM(pc.realized_pnl) + SUM(pc.unrealized_pnl)) AS total_pnl,
            -- ROI = (Total PnL / Total Investment) * 100
            CASE
                WHEN SUM(pc.total_cost_basis) > 0
                THEN ((SUM(pc.realized_pnl) + SUM(pc.unrealized_pnl)) / SUM(pc.total_cost_basis)) * 100
                ELSE 0
            END AS roi_percent
        FROM
            portfolio_calcs pc
        JOIN
            Users u ON pc.user_id = u.user_id
        GROUP BY
            u.user_id, u.display_name
        ORDER BY
            total_market_value DESC;
    """)
    
    # --- Query 3: Platform Sales (This query is unchanged) ---
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
        query_1_results = [dict(row) for row in conn.execute(query_1).fetchall()]
        portfolio_results = [dict(row) for row in conn.execute(query_portfolio).fetchall()]
        query_3_results = [dict(row) for row in conn.execute(query_3).fetchall()]
    except Exception as e:
        print(f"Error running dashboard queries: {e}")
        flash(f"Error running dashboard queries: {e}")
            
    return render_template(
        "dashboard.html",
        transactions=query_1_results,
        portfolios=portfolio_results, # Pass the new portfolio data
        platforms=query_3_results
    )


# === Purchase Management Routes (Get-or-Create Logic) ===

@app.route('/purchases/new')
def purchase_new_form():
    """
    Displays the form to add a new purchase record.
    """
    conn = g.get('db_conn')
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
        flash(f"Error fetching users/platforms: {e}")
        
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
    
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))

    item_id_to_use = None # We need this ID for the final redirect

    try:
        # Start a transaction
        with conn.begin():
            # --- Get-or-Create Item Logic ---
            query_item = text("SELECT item_id FROM Items WHERE market_name = :name AND exterior = :ext")
            result = conn.execute(query_item, {"name": market_name, "ext": exterior}).fetchone()
            
            if result:
                # Case A: Item exists. Use its ID.
                item_id_to_use = result['item_id']
                print(f"Item found. Using existing item_id: {item_id_to_use}")
            else:
                # Case B: Item does not exist. Create it.
                print(f"Item not found. Creating new item: {market_name} ({exterior})")
                
                # BUG FIX: Use COALESCE to handle empty table (MAX() would be NULL)
                query_max_item_id = text("SELECT COALESCE(MAX(item_id), 0) + 1 FROM Items")
                item_id_to_use = conn.execute(query_max_item_id).scalar()
                
                query_insert_item = text("""
                    INSERT INTO Items(item_id, market_name, game, rarity, exterior)
                    VALUES (:id, :name, 'CS2', NULL, :ext)
                """)
                conn.execute(query_insert_item, {"id": item_id_to_use, "name": market_name, "ext": exterior})
                print(f"New item created with item_id: {item_id_to_use}")

            # --- Create Purchase Record Logic ---
            
            # BUG FIX: Use COALESCE to handle empty table
            query_max_purchase_id = text("SELECT COALESCE(MAX(purchase_id), 0) + 1 FROM Purchases")
            new_purchase_id = conn.execute(query_max_purchase_id).scalar()
            
            if not new_purchase_id: new_purchase_id = 1 # Final fallback
            if not item_id_to_use: raise Exception("Item ID could not be determined")
                
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
        flash("Purchase record created successfully!")
        
    except Exception as e:
        print(f"Error during 'Get-or-Create' transaction: {e}")
        flash(f"Transaction failed. Error: {e}")
        return redirect(url_for('purchase_new_form'))
    
    # Redirect to the detail page for the item we just added/updated
    return redirect(url_for('item_detail', item_id=item_id_to_use))


# === Holdings Page Route (Req #2) ===

@app.route('/holdings')
def holdings_redirect():
    """
    A simple redirect. If a user goes to /holdings,
    we'll just show the holdings for User 1 (Alice) as a default.
    """
    return redirect(url_for('holdings_for_user', user_id=1))

@app.route('/holdings/<int:user_id>')
def holdings_for_user(user_id):
    """
    Shows all current holdings for a specific user, calculating
    average cost, market value, and unrealized PnL for each item.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.")
        return redirect(url_for('home'))

    holdings = []
    user_name = ""
    
    # This is a complex query to calculate portfolio holdings
    query = text("""
        WITH latest_market_price AS (
            -- 1. Get the latest known market price for EACH item
            SELECT DISTINCT ON (item_id)
                   item_id,
                   price AS market_price
            FROM MarketSnapshots
            ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            -- 2. Calculate total quantity and average cost for all BUYS
            SELECT user_id, item_id,
                   COUNT(*) AS qty_bought,
                   AVG(price) AS avg_buy_cost
            FROM Purchases
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            -- 3. Calculate total quantity SOLD
            SELECT user_id, item_id,
                   COUNT(*) AS qty_sold
            FROM Sales
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        )
        -- 4. Main query to assemble the portfolio
        SELECT
            p.user_id,
            i.item_id,
            i.market_name,
            i.exterior,
            COALESCE(p.qty_bought, 0) AS total_bought,
            COALESCE(s.qty_sold, 0) AS total_sold,
            (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) AS quantity_held,
            p.avg_buy_cost,
            lmp.market_price,
            -- Unrealized PnL = (Current Price - Avg Cost) * Quantity Held
            (lmp.market_price - p.avg_buy_cost) * (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) AS unrealized_pnl
        FROM
            purchase_summary p
        LEFT JOIN
            sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
        JOIN
            Items i ON p.item_id = i.item_id
        LEFT JOIN
            latest_market_price lmp ON p.item_id = lmp.item_id
        WHERE
            -- Only show items the user *currently holds*
            (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) > 0
        ORDER BY
            i.market_name;
    """)
    
    try:
        # Get the user's name for the page title
        user_name_result = conn.execute(text("SELECT display_name FROM Users WHERE user_id = :uid"), {"uid": user_id}).fetchone()
        if user_name_result:
            user_name = user_name_result['display_name']
        
        # Execute the main holdings query
        result = conn.execute(query, {"uid": user_id})
        holdings = [dict(row) for row in result.fetchall()]
        
    except Exception as e:
        print(f"Error fetching holdings: {e}")
        flash(f"Error fetching holdings: {e}")

    return render_template("holdings.html", holdings=holdings, user_name=user_name, user_id=user_id)


# --- Main Application Runner ---

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8111, debug=True)

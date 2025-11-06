import os
from flask import Flask, render_template, request, redirect, url_for, g, flash
from sqlalchemy import create_engine, text
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

# --- App Initialization ---
app = Flask(__name__)
app.secret_key = os.urandom(24) 

# --- Database Configuration ---
DATABASEURI = "postgresql://zw3155:477430@34.139.8.30/proj1part2"
engine = create_engine(DATABASEURI)

# --- Login Manager Setup ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'

# --- User Class Definition ---
class User(UserMixin):
    def __init__(self, user_id, email, display_name, password_hash):
        self.id = user_id
        self.email = email
        self.display_name = display_name
        self.password_hash = password_hash
    def get_id(self):
        return str(self.id)

@login_manager.user_loader
def load_user(user_id):
    """
    This function is required by flask-login.
    It tells the login manager how to load a user from the database
    given a user_id (from the session cookie).
    """
    # ----------------------------------------------------
    # THE BUG FIX IS HERE:
    # We must use g.get('db_conn'), not get_db_conn()
    conn = g.get('db_conn') 
    # ----------------------------------------------------
    
    if conn is None:
        # This can happen if the db connection fails in @before_request
        print("Error: load_user could not get DB connection from g.")
        return None
        
    try:
        # FIX: Explicitly list columns
        query = text("SELECT user_id, email, display_name, password_hash FROM Users WHERE user_id = :id")
        result_row = conn.execute(query, {"id": int(user_id)}).fetchone()
        
        if result_row:
            # FIX: Access by integer index
            return User(
                user_id=result_row[0],
                email=result_row[1],
                display_name=result_row[2],
                password_hash=result_row[3]
            )
    except Exception as e:
        print(f"Error loading user {user_id}: {e}")
        
    # If user_id is not found or an error occurs
    return None

# --- Database Connection Management ---
@app.before_request
def get_db_conn():
    if 'db_conn' not in g:
        try:
            g.db_conn = engine.connect()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            g.db_conn = None

@app.teardown_appcontext
def close_db_conn(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

# --- Public Routes (Login, Register) ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        conn = g.get('db_conn')
        if conn is None:
            flash("Database connection failed.", "danger")
            return render_template("login.html")

        user_obj = None
        try:
            # FIX: Explicitly list columns, in order
            query = text("SELECT user_id, email, display_name, password_hash FROM Users WHERE email = :email")
            result_row = conn.execute(query, {"email": email}).fetchone()
            
            if result_row:
                # FIX: Access password_hash by index [3]
                if check_password_hash(result_row[3], password):
                    # FIX: Create user object using integer indices
                    user_obj = User(
                        user_id=result_row[0],
                        email=result_row[1],
                        display_name=result_row[2],
                        password_hash=result_row[3]
                    )
            
            if user_obj:
                login_user(user_obj)
                flash(f"Welcome back, {user_obj.display_name}!", "success")
                next_page = request.args.get('next')
                return redirect(next_page or url_for('dashboard'))
            else:
                flash("Invalid email or password. Please try again.", "danger")
                
        except Exception as e:
            print(f"Error during login: {e}")
            flash(f"An error occurred: {e}", "danger")

    return render_template("login.html")

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "success")
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email')
        display_name = request.form.get('display_name')
        password = request.form.get('password')
        
        # --- FIX: Do NOT use g.get('db_conn') for transactions ---
        # We will get a fresh connection from the engine
        
        registration_success = False
        try:
            # --- FIX: Get a fresh connection and begin transaction ---
            with engine.begin() as conn:
                query_check = text("SELECT 1 FROM Users WHERE email = :email OR display_name = :name")
                existing_row = conn.execute(query_check, {"email": email, "name": display_name}).fetchone()
                
                if existing_row:
                    flash("Email or Display Name already in use.", "danger")
                else:
                    password_hash = generate_password_hash(password)
                    query_max_id = text("SELECT COALESCE(MAX(user_id), 0) + 1 FROM Users")
                    new_user_id = conn.execute(query_max_id).scalar()
                    query_insert = text("""
                        INSERT INTO Users(user_id, email, display_name, password_hash)
                        VALUES (:id, :email, :name, :hash)
                    """)
                    conn.execute(query_insert, {
                        "id": new_user_id,
                        "email": email,
                        "name": display_name,
                        "hash": password_hash
                    })
                    flash("Registration successful! Please log in.", "success")
                    registration_success = True
            
            if registration_success:
                return redirect(url_for('login'))
            else:
                return redirect(url_for('register'))

        except Exception as e:
            print(f"Error during registration: {e}")
            flash(f"An error occurred during registration: {e}", "danger")
            
    return render_template("register.html")

# --- Protected Routes (Must be logged in) ---

@app.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    else:
        return redirect(url_for('login'))

@app.route('/items')
@login_required
def items_list():
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    items = []
    try:
        # FIX: Explicitly list columns
        query = text("SELECT item_id, market_name, game, rarity, exterior FROM Items ORDER BY market_name")
        result = conn.execute(query)
        # FIX: Manually build dict from tuple indices
        items = [
            {
                'item_id': row[0],
                'market_name': row[1],
                'game': row[2],
                'rarity': row[3],
                'exterior': row[4]
            } for row in result.fetchall()
        ]
    except Exception as e:
        print(f"Error fetching items: {e}")
        flash(f"Error fetching items: {e}")
    return render_template("items.html", items=items)

@app.route('/item/<int:item_id>')
@login_required
def item_detail(item_id):
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    
    item_details = {}
    purchases = []
    sales = []
    snapshots = []
    
    try:
        # FIX: Explicit columns and manual dict creation
        query_item = text("SELECT market_name, game, rarity, exterior, extra FROM Items WHERE item_id = :id")
        result_item_row = conn.execute(query_item, {"id": item_id}).fetchone()
        if result_item_row:
            item_details = {
                'market_name': result_item_row[0],
                'game': result_item_row[1],
                'rarity': result_item_row[2],
                'exterior': result_item_row[3],
                'extra': result_item_row[4]
            }
        
        # FIX: Explicit columns and manual dict creation
        query_purchases = text("""
            SELECT p.ts, p.price, p.currency, u.display_name, pf.platform_name
            FROM Purchases p
            JOIN Users u ON u.user_id = p.user_id
            JOIN Platforms pf ON pf.platform_id = p.platform_id
            WHERE p.item_id = :id ORDER BY p.ts DESC
        """)
        purchases = [
            {
                'ts': row[0],
                'price': row[1],
                'currency': row[2],
                'display_name': row[3],
                'platform_name': row[4]
            } for row in conn.execute(query_purchases, {"id": item_id}).fetchall()
        ]
        
        # FIX: Explicit columns and manual dict creation
        query_sales = text("""
            SELECT s.ts, s.price, s.fee, s.currency, u.display_name, pf.platform_name
            FROM Sales s
            JOIN Users u ON u.user_id = s.user_id
            JOIN Platforms pf ON pf.platform_id = s.platform_id
            WHERE s.item_id = :id ORDER BY s.ts DESC
        """)
        sales = [
            {
                'ts': row[0],
                'price': row[1],
                'fee': row[2],
                'currency': row[3],
                'display_name': row[4],
                'platform_name': row[5]
            } for row in conn.execute(query_sales, {"id": item_id}).fetchall()
        ]
        
        # FIX: Explicit columns and manual dict creation
        query_snapshots = text("""
            SELECT ms.captured_at, ms.price, ms.currency, pf.platform_name
            FROM MarketSnapshots ms
            JOIN Platforms pf ON pf.platform_id = ms.platform_id
            WHERE ms.item_id = :id ORDER BY ms.captured_at DESC
        """)
        snapshots = [
            {
                'captured_at': row[0],
                'price': row[1],
                'currency': row[2],
                'platform_name': row[3]
            } for row in conn.execute(query_snapshots, {"id": item_id}).fetchall()
        ]

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

@app.route('/dashboard')
@login_required
def dashboard():
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    portfolio_results = []
    
    # This query is complex, but the logic is sound.
    # The fix is to manually build the dict from the final result.
    query_portfolio = text("""
        WITH latest_market_price AS (
            SELECT DISTINCT ON (item_id) item_id, price AS market_price
            FROM MarketSnapshots ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_bought,
                   SUM(price) AS total_cost_basis_item, AVG(price) AS avg_buy_cost
            FROM Purchases
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_sold,
                   SUM(price) AS total_sale_revenue_item, SUM(fee) AS total_sale_fees_item
            FROM Sales
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        ),
        holdings AS (
            SELECT p.user_id, p.item_id,
                   (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) AS quantity_held,
                   p.avg_buy_cost, p.total_cost_basis_item
            FROM purchase_summary p
            LEFT JOIN sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
            WHERE (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) > 0
        ),
        portfolio_calcs AS (
            SELECT
                COALESCE(p.user_id, s.user_id) AS user_id,
                COALESCE(p.item_id, s.item_id) AS item_id,
                COALESCE(p.total_cost_basis_item, 0) AS total_cost_basis,
                COALESCE(h.quantity_held, 0) AS quantity_held,
                COALESCE(lmp.market_price, 0) AS market_price,
                (COALESCE(lmp.market_price, 0) * COALESCE(h.quantity_held, 0)) AS current_market_value,
                (COALESCE(lmp.market_price, 0) - COALESCE(h.avg_buy_cost, 0)) * COALESCE(h.quantity_held, 0) AS unrealized_pnl,
                COALESCE(s.qty_sold, 0) AS qty_sold,
                COALESCE(s.total_sale_revenue_item, 0) AS total_sale_revenue,
                COALESCE(s.total_sale_fees_item, 0) AS total_sale_fees,
                (COALESCE(s.total_sale_revenue_item, 0) - COALESCE(s.total_sale_fees_item, 0)) - (COALESCE(p.avg_buy_cost, 0) * COALESCE(s.qty_sold, 0)) AS realized_pnl
            FROM purchase_summary p
            FULL OUTER JOIN sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
            LEFT JOIN holdings h ON COALESCE(p.user_id, s.user_id) = h.user_id AND COALESCE(p.item_id, s.item_id) = h.item_id
            LEFT JOIN latest_market_price lmp ON COALESCE(p.item_id, s.item_id) = lmp.item_id
            WHERE COALESCE(p.user_id, s.user_id) = :uid
        )
        -- 6. Final Aggregation
        SELECT
            u.display_name,
            SUM(pc.total_cost_basis) AS total_investment,
            SUM(pc.current_market_value) AS total_market_value,
            SUM(pc.realized_pnl) AS total_realized_pnl,
            SUM(pc.unrealized_pnl) AS total_unrealized_pnl,
            (SUM(pc.realized_pnl) + SUM(pc.unrealized_pnl)) AS total_pnl,
            CASE
                WHEN SUM(pc.total_cost_basis) > 0
                THEN ((SUM(pc.realized_pnl) + SUM(pc.unrealized_pnl)) / SUM(pc.total_cost_basis)) * 100
                ELSE 0
            END AS roi_percent
        FROM
            portfolio_calcs pc
        JOIN
            Users u ON pc.user_id = u.user_id
        WHERE
            u.user_id = :uid
        GROUP BY
            u.user_id, u.display_name;
    """)

    try:
        # FIX: Manually build dict from tuple indices
        result = conn.execute(query_portfolio, {"uid": current_user.id}).fetchall()
        portfolio_results = [
            {
                'display_name': row[0],
                'total_investment': row[1],
                'total_market_value': row[2],
                'total_realized_pnl': row[3],
                'total_unrealized_pnl': row[4],
                'total_pnl': row[5],
                'roi_percent': row[6]
            } for row in result
        ]
    except Exception as e:
        print(f"Error running dashboard queries: {e}")
        flash(f"Error running dashboard queries: {e}")
            
    return render_template(
        "dashboard.html",
        portfolios=portfolio_results
    )

@app.route('/purchases/new')
@login_required
def purchase_new_form():
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    platforms = []
    try:
        # FIX: Explicit columns and manual dict creation
        query = text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")
        result = conn.execute(query).fetchall()
        platforms = [
            {
                'platform_id': row[0],
                'platform_name': row[1]
            } for row in result
        ]
    except Exception as e:
        print(f"Error fetching platforms: {e}")
        flash(f"Error fetching platforms: {e}")
    return render_template("new_purchase.html", platforms=platforms)


@app.route('/purchases/create', methods=['POST'])
@login_required
def purchase_create():
    user_id = current_user.id
    platform_id = request.form.get('platform_id')
    market_name = request.form.get('market_name')
    exterior = request.form.get('exterior')
    price = request.form.get('price')
    currency = request.form.get('currency')
    ts = request.form.get('ts')
    
    # --- FIX: Do NOT use g.get('db_conn') for transactions ---
    
    item_id_to_use = None
    try:
        # --- FIX: Get a fresh connection and begin transaction ---
        with engine.begin() as conn:
            query_item = text("SELECT item_id FROM Items WHERE market_name = :name AND exterior = :ext")
            result_row = conn.execute(query_item, {"name": market_name, "ext": exterior}).fetchone()
            
            if result_row:
                item_id_to_use = result_row[0] # Access by index [0]
                print(f"Item found. Using existing item_id: {item_id_to_use}")
            else:
                print(f"Item not found. Creating new item: {market_name} ({exterior})")
                query_max_item_id = text("SELECT COALESCE(MAX(item_id), 0) + 1 FROM Items")
                item_id_to_use = conn.execute(query_max_item_id).scalar()
                query_insert_item = text("""
                    INSERT INTO Items(item_id, market_name, game, rarity, exterior)
                    VALUES (:id, :name, 'CS2', NULL, :ext)
                """)
                conn.execute(query_insert_item, {"id": item_id_to_use, "name": market_name, "ext": exterior})
                print(f"New item created with item_id: {item_id_to_use}")

            query_max_purchase_id = text("SELECT COALESCE(MAX(purchase_id), 0) + 1 FROM Purchases")
            new_purchase_id = conn.execute(query_max_purchase_id).scalar()
            
            if not new_purchase_id: new_purchase_id = 1
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
            
        flash("Purchase record created successfully!")
        
    except Exception as e:
        print(f"Error during 'Get-or-Create' transaction: {e}")
        flash(f"Transaction failed. Error: {e}", "danger")
        return redirect(url_for('purchase_new_form'))
    
    # We must redirect to a URL *outside* the 'try' block
    if item_id_to_use:
        return redirect(url_for('item_detail', item_id=item_id_to_use))
    else:
        # Fallback if something went wrong
        return redirect(url_for('dashboard'))

@app.route('/holdings')
@login_required
def holdings():
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    holdings = []
    
    # This query is complex, but the logic is sound.
    # The fix is to manually build the dict from the final result.
    query = text("""
        WITH latest_market_price AS (
            SELECT DISTINCT ON (item_id)
                   item_id, price AS market_price
            FROM MarketSnapshots
            ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_bought, AVG(price) AS avg_buy_cost
            FROM Purchases
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_sold
            FROM Sales
            WHERE user_id = :uid
            GROUP BY user_id, item_id
        )
        -- 4. Final query: explicit column names
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
            (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) > 0
            AND p.user_id = :uid
        ORDER BY
            i.market_name;
    """)
    
    try:
        result = conn.execute(query, {"uid": current_user.id}).fetchall()
        # FIX: Manually build dict from tuple indices
        holdings = [
            {
                'user_id': row[0],
                'item_id': row[1],
                'market_name': row[2],
                'exterior': row[3],
                'total_bought': row[4],
                'total_sold': row[5],
                'quantity_held': row[6],
                'avg_buy_cost': row[7],
                'market_price': row[8],
                'unrealized_pnl': row[9]
            } for row in result
        ]
        
    except Exception as e:
        print(f"Error fetching holdings: {e}")
        flash(f"Error fetching holdings: {e}", "danger")

    return render_template("holdings.html", holdings=holdings)

# --- Main Application Runner ---
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8111, debug=True)

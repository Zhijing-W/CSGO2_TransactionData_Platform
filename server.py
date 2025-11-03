import os
from flask import Flask, render_template, request, redirect, url_for, g, flash
from sqlalchemy import create_engine, text
# New imports for login
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

# --- App Initialization ---
app = Flask(__name__)
# Secret key is required for sessions and flash messages
app.secret_key = os.urandom(24) 

# --- Database Configuration ---
DATABASEURI = "postgresql://zw3155:477430@34.139.8.30/proj1part2"
engine = create_engine(DATABASEURI)

# --- Login Manager Setup ---
login_manager = LoginManager()
login_manager.init_app(app)
# If a user is not logged in and tries to access a protected page,
# redirect them to the 'login' page.
login_manager.login_view = 'login'
login_manager.login_message_category = 'info' # Optional: for styling flash messages

# --- User Class Definition for Flask-Login ---
class User(UserMixin):
    """
    User model for Flask-Login.
    """
    def __init__(self, user_id, email, display_name, password_hash):
        self.id = user_id
        self.email = email
        self.display_name = display_name
        self.password_hash = password_hash
    
    # flask-login requires this method
    def get_id(self):
        return str(self.id)

@login_manager.user_loader
def load_user(user_id):
    """
    Required callback for flask-login to load a user from session.
    """
    conn = get_db_conn()
    if conn is None:
        return None
    try:
        query = text("SELECT * FROM Users WHERE user_id = :id")
        result = conn.execute(query, {"id": int(user_id)}).fetchone()
        if result:
            return User(
                user_id=result['user_id'],
                email=result['email'],
                display_name=result['display_name'],
                password_hash=result['password_hash']
            )
    except Exception as e:
        print(f"Error loading user {user_id}: {e}")
    return None

# --- Database Connection Management ---
@app.before_request
def get_db_conn():
    """
    Opens a new database connection for each request.
    """
    if 'db_conn' not in g:
        try:
            g.db_conn = engine.connect()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            g.db_conn = None

@app.teardown_appcontext
def close_db_conn(exception):
    """
    Closes the database connection at the end of each request.
    """
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

# --- Public Routes (Login, Register) ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Handles user login.
    GET: Shows the login form.
    POST: Processes the login attempt.
    """
    if current_user.is_authenticated:
        return redirect(url_for('dashboard')) # Already logged in

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        conn = g.get('db_conn')
        if conn is None:
            flash("Database connection failed.", "danger")
            return render_template("login.html")

        user = None
        try:
            # Find the user by their email
            query = text("SELECT * FROM Users WHERE email = :email")
            result = conn.execute(query, {"email": email}).fetchone()
            
            if result:
                # Check if the password hash matches
                if check_password_hash(result['password_hash'], password):
                    user = User(
                        user_id=result['user_id'],
                        email=result['email'],
                        display_name=result['display_name'],
                        password_hash=result['password_hash']
                    )
            
            if user:
                # Password is correct, log the user in
                login_user(user)
                flash(f"Welcome back, {user.display_name}!", "success")
                # Redirect to the page they were trying to access, or dashboard
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
    """
    Logs the current user out.
    """
    logout_user()
    flash("You have been logged out.", "success")
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    """
    Handles new user registration.
    GET: Shows the registration form.
    POST: Processes the new user creation.
    """
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email')
        display_name = request.form.get('display_name')
        password = request.form.get('password')
        
        conn = g.get('db_conn')
        if conn is None:
            flash("Database connection failed.", "danger")
            return render_template("register.html")

        try:
            # Check if email or display name already exists
            query_check = text("SELECT * FROM Users WHERE email = :email OR display_name = :name")
            existing = conn.execute(query_check, {"email": email, "name": display_name}).fetchone()
            
            if existing:
                flash("Email or Display Name already in use.", "danger")
                return redirect(url_for('register'))

            # Hash the password
            password_hash = generate_password_hash(password)
            
            # Get the next user_id
            query_max_id = text("SELECT COALESCE(MAX(user_id), 0) + 1 FROM Users")
            new_user_id = conn.execute(query_max_id).scalar()

            # Insert the new user in a transaction
            with conn.begin():
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
            return redirect(url_for('login'))

        except Exception as e:
            print(f"Error during registration: {e}")
            flash(f"An error occurred during registration: {e}", "danger")
            
    return render_template("register.html")

# --- Protected Routes (Must be logged in) ---

@app.route('/')
def home():
    """
    Root URL route. Redirects to dashboard if logged in, else login.
    """
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    else:
        return redirect(url_for('login'))


@app.route('/items')
@login_required
def items_list():
    """
    Displays a list of all items from the Items table.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
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
@login_required
def item_detail(item_id):
    """
    Shows detailed info for a single item.
    NOTE: This page still shows *all* history for an item,
    which is fine for a marketplace viewer.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
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

@app.route('/dashboard')
@login_required
def dashboard():
    """
    Displays the portfolio dashboard ONLY for the currently logged-in user.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    portfolio_results = []
    
    # This query is now filtered by the logged-in user's ID
    query_portfolio = text("""
        WITH latest_market_price AS (
            SELECT DISTINCT ON (item_id) item_id, price AS market_price
            FROM MarketSnapshots ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_bought,
                   SUM(price) AS total_cost_basis_item, AVG(price) AS avg_buy_cost
            FROM Purchases
            WHERE user_id = :uid -- Filter by current user
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_sold,
                   SUM(price) AS total_sale_revenue_item, SUM(fee) AS total_sale_fees_item
            FROM Sales
            WHERE user_id = :uid -- Filter by current user
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
            WHERE COALESCE(p.user_id, s.user_id) = :uid -- Filter by current user
        )
        -- Final Aggregation for the *single* logged-in user
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
            u.user_id = :uid -- Filter by current user
        GROUP BY
            u.user_id, u.display_name;
    """)

    try:
        portfolio_results = [dict(row) for row in conn.execute(query_portfolio, {"uid": current_user.id}).fetchall()]
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
    """
    Displays the form to add a new purchase record
    FOR THE CURRENTLY LOGGED-IN USER.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
        
    platforms = []
    try:
        # We only need platforms, user is now current_user
        platforms = [dict(row) for row in conn.execute(text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")).fetchall()]
    except Exception as e:
        print(f"Error fetching platforms: {e}")
        flash(f"Error fetching platforms: {e}")
        
    return render_template("new_purchase.html", platforms=platforms)


@app.route('/purchases/create', methods=['POST'])
@login_required
def purchase_create():
    """
    Processes the form submission from new_purchase.html.
    Adds the purchase FOR THE CURRENTLY LOGGED-IN USER.
    Implements the "Get-or-Create" logic for an item.
    """
    # 1. Get all form data
    # user_id is no longer from the form, it's from the session
    user_id = current_user.id
    
    platform_id = request.form.get('platform_id')
    market_name = request.form.get('market_name')
    exterior = request.form.get('exterior')
    price = request.form.get('price')
    currency = request.form.get('currency')
    ts = request.form.get('ts')
    
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    item_id_to_use = None

    try:
        with conn.begin():
            # --- Get-or-Create Item Logic ---
            query_item = text("SELECT item_id FROM Items WHERE market_name = :name AND exterior = :ext")
            result = conn.execute(query_item, {"name": market_name, "ext": exterior}).fetchone()
            
            if result:
                item_id_to_use = result['item_id']
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

            # --- Create Purchase Record Logic ---
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
                "uid": user_id, # Use the logged-in user's ID
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
    
    return redirect(url_for('item_detail', item_id=item_id_to_use))


@app.route('/holdings')
@login_required
def holdings():
    """
    Shows all current holdings ONLY for the currently logged-in user.
    """
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    holdings = []
    
    # This query is now filtered by the logged-in user's ID
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
            WHERE user_id = :uid -- Filter by current user
            GROUP BY user_id, item_id
        ),
        sales_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_sold
            FROM Sales
            WHERE user_id = :uid -- Filter by current user
            GROUP BY user_id, item_id
        )
        SELECT
            p.user_id, i.item_id, i.market_name, i.exterior,
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
            AND p.user_id = :uid -- Filter by current user
        ORDER BY
            i.market_name;
    """)
    
    try:
        result = conn.execute(query, {"uid": current_user.id})
        holdings = [dict(row) for row in result.fetchall()]
        
    except Exception as e:
        print(f"Error fetching holdings: {e}")
        flash(f"Error fetching holdings: {e}", "danger")

    # We don't need user_name anymore, we can get it from current_user in the template
    return render_template("holdings.html", holdings=holdings)

# --- We no longer need the old /users routes ---
# We have /register instead
# We also no longer need the /holdings/<id> route, as /holdings is now dynamic


# --- Main Application Runner ---
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8111, debug=True)

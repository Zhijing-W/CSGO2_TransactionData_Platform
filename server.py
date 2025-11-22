import os
import time
import datetime
import threading
from flask import Flask, render_template, request, redirect, url_for, g, flash
from sqlalchemy import create_engine, text
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from currency_service import currency_service
from market_service import market_service


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
    def __init__(self, user_id, email, display_name, password_hash, is_admin=False):
        self.id = user_id
        self.email = email
        self.display_name = display_name
        self.password_hash = password_hash
        self.is_admin = is_admin
    
    def get_id(self):
        return str(self.id)
    
    def is_administrator(self):
        return self.is_admin

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
    
    conn = get_db_connection()
    if conn is None:
        # This can happen if the db connection fails
        print("Error: load_user could not get DB connection.")
        return None
        
    try:
        # Check if is_admin column exists first to avoid transaction errors
        has_admin_column = False
        try:
            check_column_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='users' AND column_name='is_admin'
            """)
            column_check = conn.execute(check_column_query).fetchone()
            has_admin_column = column_check is not None
        except Exception as e:
            print(f"Error checking for is_admin column in load_user: {e}")
            has_admin_column = False
            # Reconnect if connection failed
            conn = get_db_connection()
            if conn is None:
                return None
        
        # Query user data based on whether is_admin column exists
        if has_admin_column:
            query = text("SELECT user_id, email, display_name, password_hash, COALESCE(is_admin, false) FROM Users WHERE user_id = :id")
        else:
            query = text("SELECT user_id, email, display_name, password_hash FROM Users WHERE user_id = :id")
        
        result_row = conn.execute(query, {"id": int(user_id)}).fetchone()
        if result_row:
            is_admin = result_row[4] if has_admin_column and len(result_row) > 4 else False
            return User(
                user_id=result_row[0],
                email=result_row[1],
                display_name=result_row[2],
                password_hash=result_row[3],
                is_admin=is_admin
            )
    except Exception as e:
        print(f"Error loading user {user_id}: {e}")
        
    # If user_id is not found or an error occurs
    return None

# --- Database Connection Management ---
def get_db_connection():
    """Get or create a database connection, handling transaction errors"""
    if 'db_conn' not in g:
        try:
            g.db_conn = engine.connect()
        except Exception as e:
            print(f"Error connecting to database: {e}")
            g.db_conn = None
    else:
        # Check if connection is still valid, reconnect if needed
        try:
            # Try a simple query to check connection health
            g.db_conn.execute(text("SELECT 1"))
        except Exception as e:
            print(f"Connection error detected, reconnecting: {e}")
            try:
                if g.db_conn:
                    g.db_conn.close()
            except:
                pass
            try:
                g.db_conn = engine.connect()
            except Exception as e2:
                print(f"Error reconnecting to database: {e2}")
                g.db_conn = None
    return g.db_conn

@app.before_request
def get_db_conn():
    get_db_connection()

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
        conn = get_db_connection()
        if conn is None:
            flash("Database connection failed.", "danger")
            return render_template("login.html")

        user_obj = None
        try:
            # Ensure we have a valid connection
            conn = get_db_connection()
            if conn is None:
                flash("Database connection failed.", "danger")
                return render_template("login.html")
            
            # First, check if is_admin column exists by querying information_schema
            # This avoids transaction errors if the column doesn't exist
            has_admin_column = False
            try:
                check_column_query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='users' AND column_name='is_admin'
                """)
                column_check = conn.execute(check_column_query).fetchone()
                has_admin_column = column_check is not None
            except Exception as e:
                print(f"Error checking for is_admin column: {e}")
                has_admin_column = False
                # Reconnect if connection failed
                conn = get_db_connection()
            
            # Now query user data based on whether is_admin column exists
            if has_admin_column:
                query = text("SELECT user_id, email, display_name, password_hash, COALESCE(is_admin, false) FROM Users WHERE email = :email")
            else:
                query = text("SELECT user_id, email, display_name, password_hash FROM Users WHERE email = :email")
            
            result_row = conn.execute(query, {"email": email}).fetchone()
            if result_row:
                if check_password_hash(result_row[3], password):
                    is_admin = result_row[4] if has_admin_column and len(result_row) > 4 else False
                    user_obj = User(
                        user_id=result_row[0],
                        email=result_row[1],
                        display_name=result_row[2],
                        password_hash=result_row[3],
                        is_admin=is_admin
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
    # Renders the public homepage, showing top selling items to all users.
    target_currency = request.args.get('currency', 'USD').upper()
    exchange_rate = currency_service.get_rate(target_currency)
    
    # Get top items and convert prices to target currency
    top_items_raw = market_service.get_top_selling_items()
    top_items = []
    for item in top_items_raw:
        sale_price_usd = item.get('sale_price') or 0
        if sale_price_usd is None:
            sale_price_usd = 0
        converted_price = sale_price_usd * exchange_rate
        top_items.append({
            'name': item.get('name', ''),
            'image_url': item.get('image_url', ''),
            'price_text': item.get('price_text', ''),
            'volume': item.get('volume', 0),
            'sale_price': sale_price_usd,
            'converted_price': converted_price
        })
    
    return render_template('home.html', items=top_items, current_currency=target_currency, exchange_rate=exchange_rate)

@app.route('/items')
@login_required
def items_list():
    # Only admins can access the items catalog
    if not current_user.is_administrator():
        flash("Access denied. This page is only available to administrators.", "danger")
        return redirect(url_for('dashboard'))
    
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
            SELECT p.purchase_id, p.ts, p.price, p.currency, u.display_name, pf.platform_name
            FROM Purchases p
            JOIN Users u ON u.user_id = p.user_id
            JOIN Platforms pf ON pf.platform_id = p.platform_id
            WHERE p.item_id = :id ORDER BY p.ts DESC
        """)
        purchases = [
            {
                'purchase_id': row[0],
                'ts': row[1],
                'price': row[2],
                'currency': row[3],
                'display_name': row[4],
                'platform_name': row[5]
            } for row in conn.execute(query_purchases, {"id": item_id}).fetchall()
        ]
        
        # FIX: Explicit columns and manual dict creation
        query_sales = text("""
            SELECT s.sale_id, s.ts, s.price, s.fee, s.currency, u.display_name, pf.platform_name
            FROM Sales s
            JOIN Users u ON u.user_id = s.user_id
            JOIN Platforms pf ON pf.platform_id = s.platform_id
            WHERE s.item_id = :id ORDER BY s.ts DESC
        """)
        sales = [
            {
                'sale_id': row[0],
                'ts': row[1],
                'price': row[2],
                'fee': row[3],
                'currency': row[4],
                'display_name': row[5],
                'platform_name': row[6]
            } for row in conn.execute(query_sales, {"id": item_id}).fetchall()
        ]
        
        # FIX: Explicit columns and manual dict creation
        query_snapshots = text("""
            SELECT ms.captured_at, ms.price, ms.currency, pf.platform_name
            FROM MarketSnapshots ms
            JOIN Platforms pf ON pf.platform_id = ms.platform_id
            WHERE ms.item_id = :id ORDER BY ms.captured_at ASC
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
    conn = get_db_connection()
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    
    target_currency = request.args.get('currency', 'USD').upper()
    exchange_rate = float(currency_service.get_rate(target_currency))

    # Get all purchases and sales for the user
    purchases = []
    sales = []
    transactions = []  # Combined list for timeline
    
    try:
        # Get all purchases
        query_purchases = text("""
            SELECT p.purchase_id, p.ts, p.price, p.currency, i.market_name, i.exterior, pf.platform_name
            FROM Purchases p
            JOIN Items i ON p.item_id = i.item_id
            JOIN Platforms pf ON pf.platform_id = p.platform_id
            WHERE p.user_id = :uid
            ORDER BY p.ts ASC
        """)
        purchase_rows = conn.execute(query_purchases, {"uid": current_user.id}).fetchall()
        for row in purchase_rows:
            # Convert Decimal to float to avoid type errors
            price_usd = float(row[2]) if row[2] is not None else 0.0
            price_converted = price_usd * exchange_rate
            
            purchase_data = {
                'id': row[0],
                'ts': row[1],
                'price': price_converted,
                'currency': row[3],
                'market_name': row[4],
                'exterior': row[5],
                'platform_name': row[6],
                'type': 'buy'
            }
            purchases.append(purchase_data)
            transactions.append(purchase_data.copy())
        
        # Get all sales
        query_sales = text("""
            SELECT s.sale_id, s.ts, s.price, s.fee, s.currency, i.market_name, i.exterior, pf.platform_name
            FROM Sales s
            JOIN Items i ON s.item_id = i.item_id
            JOIN Platforms pf ON pf.platform_id = s.platform_id
            WHERE s.user_id = :uid
            ORDER BY s.ts ASC
        """)
        sale_rows = conn.execute(query_sales, {"uid": current_user.id}).fetchall()
        for row in sale_rows:
            # Convert Decimal to float to avoid type errors
            price_usd = float(row[2]) if row[2] is not None else 0.0
            fee_usd = float(row[3]) if row[3] is not None else 0.0
            net_revenue = (price_usd - fee_usd) * exchange_rate
            
            sale_data = {
                'id': row[0],
                'ts': row[1],
                'price': price_usd * exchange_rate,
                'fee': fee_usd * exchange_rate,
                'net_revenue': net_revenue,
                'currency': row[4],
                'market_name': row[5],
                'exterior': row[6],
                'platform_name': row[7],
                'type': 'sell'
            }
            sales.append(sale_data)
            transactions.append(sale_data.copy())
        
        # Sort transactions by time
        transactions.sort(key=lambda x: x['ts'])
        
        # Calculate financial metrics - ensure all values are floats
        total_invested = sum(float(p.get('price', 0)) for p in purchases)
        total_revenue = sum(float(s.get('net_revenue', 0)) for s in sales)
        total_fees = sum(float(s.get('fee', 0)) for s in sales)
        realized_pnl = total_revenue - total_invested
        
        # Calculate ROI
        roi_percent = (realized_pnl / total_invested * 100) if total_invested > 0 else 0.0
        
        print(f"[Dashboard] Financial stats - Invested: {total_invested}, Revenue: {total_revenue}, PnL: {realized_pnl}, Purchases: {len(purchases)}, Sales: {len(sales)}")
        
        # Calculate win rate (profitable trades)
        profitable_trades = 0
        total_trades = len(sales)
        # Match sales with their corresponding purchases to calculate per-trade PnL
        # For simplicity, we'll use FIFO matching
        purchase_queue = purchases.copy()
        trade_pnls = []
        for sale in sales:
            # Find matching purchase (FIFO)
            if purchase_queue:
                purchase = purchase_queue.pop(0)
                trade_pnl = float(sale.get('net_revenue', 0)) - float(purchase.get('price', 0))
                trade_pnls.append(trade_pnl)
                if trade_pnl > 0:
                    profitable_trades += 1
        
        win_rate = float((profitable_trades / total_trades * 100) if total_trades > 0 else 0.0)
        avg_trade_pnl = float((sum(trade_pnls) / len(trade_pnls)) if trade_pnls else 0.0)
        
        # Generate K-line data (cumulative portfolio value over time)
        kline_data = []
        if transactions:
            from datetime import datetime, timedelta
            try:
                # Group transactions by date
                transactions_by_date = {}
                for trans in transactions:
                    if trans.get('ts'):
                        trans_date = trans['ts'].date() if hasattr(trans['ts'], 'date') else trans['ts']
                        if isinstance(trans_date, str):
                            trans_date = datetime.strptime(trans_date, '%Y-%m-%d').date()
                        
                        if trans_date not in transactions_by_date:
                            transactions_by_date[trans_date] = []
                        transactions_by_date[trans_date].append(trans)
                
                # Sort dates
                sorted_dates = sorted(transactions_by_date.keys())
                
                if sorted_dates:
                    # Calculate cumulative values
                    cumulative_invested = 0.0
                    cumulative_revenue = 0.0
                    
                    # Start from first transaction date
                    start_date = sorted_dates[0]
                    end_date = sorted_dates[-1]
                    current_date = start_date
                    
                    while current_date <= end_date:
                        # Process transactions on this date
                        if current_date in transactions_by_date:
                            for trans in transactions_by_date[current_date]:
                                if trans['type'] == 'buy':
                                    cumulative_invested += float(trans.get('price', 0))
                                else:  # sell
                                    cumulative_revenue += float(trans.get('net_revenue', trans.get('price', 0)))
                        
                        # Calculate cumulative PnL
                        cumulative_pnl = cumulative_revenue - cumulative_invested
                        
                        kline_data.append({
                            'date': current_date.strftime('%Y-%m-%d'),
                            'invested': round(cumulative_invested, 2),
                            'revenue': round(cumulative_revenue, 2),
                            'pnl': round(cumulative_pnl, 2)
                        })
                        
                        current_date += timedelta(days=1)
            except Exception as kline_error:
                print(f"Error generating K-line data: {kline_error}")
                import traceback
                traceback.print_exc()
                kline_data = []
        
        financial_stats = {
            'total_invested': total_invested,
            'total_revenue': total_revenue,
            'total_fees': total_fees,
            'realized_pnl': realized_pnl,
            'roi_percent': roi_percent,
            'total_trades': total_trades,
            'profitable_trades': profitable_trades,
            'win_rate': win_rate,
            'avg_trade_pnl': avg_trade_pnl,
            'total_purchases': len(purchases),
            'total_sales': len(sales)
        }
            
    except Exception as e:
        print(f"Error running dashboard queries: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Error running dashboard queries: {e}", "danger")
        purchases = []
        sales = []
        transactions = []
        financial_stats = {
            'total_invested': 0.0,
            'total_revenue': 0.0,
            'total_fees': 0.0,
            'realized_pnl': 0.0,
            'roi_percent': 0.0,
            'total_trades': 0,
            'profitable_trades': 0,
            'win_rate': 0.0,
            'avg_trade_pnl': 0.0,
            'total_purchases': 0,
            'total_sales': 0
        }
        kline_data = []
            
    return render_template(
        "dashboard.html",
        purchases=purchases,
        sales=sales,
        transactions=transactions,
        financial_stats=financial_stats,
        kline_data=kline_data,
        current_currency=target_currency,
        exchange_rate=exchange_rate
    )

@app.route('/transactions/new')
@login_required
def transaction_new_form():
    conn = get_db_connection()
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    platforms = []
    holdings = []
    
    try:
        # Get platforms
        query = text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")
        result = conn.execute(query).fetchall()
        platforms = [
            {
                'platform_id': row[0],
                'platform_name': row[1]
            } for row in result
        ]
        
        # Get user's holdings for sale form
        query_holdings = text("""
            WITH purchase_summary AS (
                SELECT user_id, item_id, COUNT(*) AS qty_bought
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
            SELECT
                i.item_id,
                i.market_name,
                i.exterior,
                COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0) AS quantity_held
            FROM
                purchase_summary p
            LEFT JOIN
                sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
            JOIN
                Items i ON p.item_id = i.item_id
            WHERE
                (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) > 0
                AND p.user_id = :uid
            ORDER BY
                i.market_name, i.exterior
        """)
        holdings_result = conn.execute(query_holdings, {"uid": current_user.id}).fetchall()
        holdings = [
            {
                'item_id': row[0],
                'market_name': row[1],
                'exterior': row[2],
                'quantity_held': row[3]
            } for row in holdings_result
        ]
    except Exception as e:
        print(f"Error fetching data: {e}")
        flash(f"Error fetching data: {e}")
    
    # Get pre-fill parameters from query string (for sell action from holdings)
    action = request.args.get('action', '')
    prefill_item_id = request.args.get('item_id', '')
    prefill_market_name = request.args.get('market_name', '')
    prefill_exterior = request.args.get('exterior', '')
    
    return render_template(
        "transaction_form.html", 
        platforms=platforms, 
        holdings=holdings,
        action=action,
        prefill_item_id=prefill_item_id,
        prefill_market_name=prefill_market_name,
        prefill_exterior=prefill_exterior
    )


@app.route('/purchases/create', methods=['POST'])
@login_required
def purchase_create():
    user_id = current_user.id
    platform_id = request.form.get('platform_id')
    market_name = request.form.get('market_name')
    exterior = request.form.get('exterior')
    price_str = request.form.get('price')
    currency = request.form.get('currency')
    ts = request.form.get('ts')
    
    # Format price to 2 decimal places
    try:
        price = float(price_str) if price_str else 0.0
        price = round(price, 2)
    except (ValueError, TypeError):
        flash("Invalid price format.", "danger")
        return redirect(url_for('transaction_new_form'))
    
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


@app.route('/sales/create', methods=['POST'])
@login_required
def sale_create():
    user_id = current_user.id
    platform_id = request.form.get('platform_id')
    market_name = request.form.get('market_name')
    exterior = request.form.get('exterior')
    price_str = request.form.get('price')
    fee_str = request.form.get('fee', '0')
    currency = request.form.get('currency')
    ts = request.form.get('ts')
    
    # Format price and fee to 2 decimal places
    try:
        price = float(price_str) if price_str else 0.0
        price = round(price, 2)
        fee = float(fee_str) if fee_str else 0.0
        fee = round(fee, 2)
    except (ValueError, TypeError):
        flash("Invalid price or fee format.", "danger")
        return redirect(url_for('transaction_new_form'))

    item_id_to_use = None
    try:
        with engine.begin() as conn:
            # Step 1: Find the item_id. Do NOT create a new item.
            query_item = text("SELECT item_id FROM Items WHERE market_name = :name AND exterior = :ext")
            result_row = conn.execute(query_item, {"name": market_name, "ext": exterior}).fetchone()

            if not result_row:
                flash(f"Error: Item '{market_name} ({exterior})' not found in database. You cannot sell an item that hasn't been purchased first.", "danger")
                return redirect(url_for('transaction_new_form'))
            
            item_id_to_use = result_row[0]

            # Step 2: (Crucial Validation) Check if user holds the item
            query_holdings = text("""
                SELECT (COALESCE(p.qty, 0) - COALESCE(s.qty, 0)) AS quantity_held
                FROM (
                    SELECT COUNT(*) as qty FROM Purchases WHERE user_id = :uid AND item_id = :iid
                ) p, (
                    SELECT COUNT(*) as qty FROM Sales WHERE user_id = :uid AND item_id = :iid
                ) s
            """)
            quantity_held = conn.execute(query_holdings, {"uid": user_id, "iid": item_id_to_use}).scalar()

            if quantity_held <= 0:
                flash(f"Error: You do not have '{market_name} ({exterior})' in your holdings to sell.", "danger")
                return redirect(url_for('transaction_new_form'))

            # Step 3: Insert the sale record
            query_max_sale_id = text("SELECT COALESCE(MAX(sale_id), 0) + 1 FROM Sales")
            new_sale_id = conn.execute(query_max_sale_id).scalar()

            query_insert_sale = text("""
                INSERT INTO Sales(sale_id, user_id, item_id, platform_id, ts, price, fee, currency)
                VALUES (:sid, :uid, :iid, :plid, :ts, :price, :fee, :curr)
            """)
            conn.execute(query_insert_sale, {
                "sid": new_sale_id,
                "uid": user_id,
                "iid": item_id_to_use,
                "plid": platform_id,
                "ts": ts,
                "price": price,
                "fee": fee,
                "curr": currency
            })
            print(f"Successfully inserted sale record {new_sale_id}")
            
        flash("Sale record created successfully!")

    except Exception as e:
        print(f"Error during sale creation transaction: {e}")
        flash(f"Transaction failed. Error: {e}", "danger")
        return redirect(url_for('transaction_new_form'))

    return redirect(url_for('item_detail', item_id=item_id_to_use))


@app.route('/purchases/delete/<int:purchase_id>', methods=['POST'])
@login_required
def purchase_delete(purchase_id):
    try:
        with engine.begin() as conn:
            # First, get the purchase details to verify ownership and for redirecting
            query_find = text("SELECT user_id, item_id FROM Purchases WHERE purchase_id = :pid")
            purchase = conn.execute(query_find, {"pid": purchase_id}).fetchone()

            if not purchase:
                flash("Purchase record not found.", "danger")
                return redirect(url_for('dashboard'))

            if purchase[0] != current_user.id:
                flash("You are not authorized to delete this record.", "danger")
                return redirect(url_for('item_detail', item_id=purchase[1]))

            # Now, delete the record
            query_delete = text("DELETE FROM Purchases WHERE purchase_id = :pid")
            conn.execute(query_delete, {"pid": purchase_id})
            
            flash("Purchase record deleted successfully.", "success")
            return redirect(url_for('item_detail', item_id=purchase[1]))

    except Exception as e:
        print(f"Error deleting purchase {purchase_id}: {e}")
        flash("An error occurred while deleting the record.", "danger")
        # A bit of a guess for redirect, dashboard is a safe bet
        return redirect(url_for('dashboard'))


@app.route('/sales/delete/<int:sale_id>', methods=['POST'])
@login_required
def sale_delete(sale_id):
    try:
        with engine.begin() as conn:
            # First, get the sale details
            query_find = text("SELECT user_id, item_id FROM Sales WHERE sale_id = :sid")
            sale = conn.execute(query_find, {"sid": sale_id}).fetchone()

            if not sale:
                flash("Sale record not found.", "danger")
                return redirect(url_for('dashboard'))

            if sale[0] != current_user.id:
                flash("You are not authorized to delete this record.", "danger")
                return redirect(url_for('item_detail', item_id=sale[1]))

            # Now, delete the record
            query_delete = text("DELETE FROM Sales WHERE sale_id = :sid")
            conn.execute(query_delete, {"sid": sale_id})
            
            flash("Sale record deleted successfully.", "success")
            return redirect(url_for('item_detail', item_id=sale[1]))

    except Exception as e:
        print(f"Error deleting sale {sale_id}: {e}")
        flash("An error occurred while deleting the record.", "danger")
        return redirect(url_for('dashboard'))




# --- Transaction EDIT / UPDATE Routes ---

@app.route('/purchases/edit/<int:purchase_id>', methods=['GET'])
@login_required
def purchase_edit_form(purchase_id):
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('dashboard'))

    try:
        # Fetch purchase details and verify ownership
        query_purchase = text("SELECT * FROM Purchases WHERE purchase_id = :pid AND user_id = :uid")
        purchase = conn.execute(query_purchase, {"pid": purchase_id, "uid": current_user.id}).fetchone()

        if not purchase:
            flash("Purchase record not found or you're not authorized to edit it.", "danger")
            return redirect(url_for('dashboard'))

        # Fetch item details for display
        query_item = text("SELECT * FROM Items WHERE item_id = :iid")
        item = conn.execute(query_item, {"iid": purchase.item_id}).fetchone()

        # Fetch platforms for the dropdown
        query_platforms = text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")
        platforms = conn.execute(query_platforms).fetchall()
        
        return render_template("edit_purchase.html", purchase=purchase, item=item, platforms=platforms)

    except Exception as e:
        print(f"Error fetching purchase for edit: {e}")
        flash("An error occurred while fetching the record for editing.", "danger")
        return redirect(url_for('dashboard'))

@app.route('/purchases/update/<int:purchase_id>', methods=['POST'])
@login_required
def purchase_update(purchase_id):
    platform_id = request.form.get('platform_id')
    price = request.form.get('price')
    currency = request.form.get('currency')
    ts = request.form.get('ts')

    try:
        with engine.begin() as conn:
            # First, verify ownership again before updating
            query_find = text("SELECT user_id, item_id FROM Purchases WHERE purchase_id = :pid")
            purchase = conn.execute(query_find, {"pid": purchase_id}).fetchone()

            if not purchase or purchase.user_id != current_user.id:
                flash("Unauthorized to update this record.", "danger")
                return redirect(url_for('dashboard'))

            # Perform the update
            query_update = text("""
                UPDATE Purchases
                SET platform_id = :plid, price = :price, currency = :curr, ts = :ts
                WHERE purchase_id = :pid
            """)
            conn.execute(query_update, {
                "plid": platform_id,
                "price": price,
                "curr": currency,
                "ts": ts,
                "pid": purchase_id
            })
            
            flash("Purchase record updated successfully.", "success")
            return redirect(url_for('item_detail', item_id=purchase.item_id))

    except Exception as e:
        print(f"Error updating purchase {purchase_id}: {e}")
        flash("An error occurred while updating the record.", "danger")
        return redirect(url_for('purchase_edit_form', purchase_id=purchase_id))

@app.route('/sales/edit/<int:sale_id>', methods=['GET'])
@login_required
def sale_edit_form(sale_id):
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('dashboard'))
    try:
        query_sale = text("SELECT * FROM Sales WHERE sale_id = :sid AND user_id = :uid")
        sale = conn.execute(query_sale, {"sid": sale_id, "uid": current_user.id}).fetchone()

        if not sale:
            flash("Sale record not found or you're not authorized to edit it.", "danger")
            return redirect(url_for('dashboard'))

        query_item = text("SELECT * FROM Items WHERE item_id = :iid")
        item = conn.execute(query_item, {"iid": sale.item_id}).fetchone()

        query_platforms = text("SELECT platform_id, platform_name FROM Platforms ORDER BY platform_name")
        platforms = conn.execute(query_platforms).fetchall()
        
        return render_template("edit_sale.html", sale=sale, item=item, platforms=platforms)

    except Exception as e:
        print(f"Error fetching sale for edit: {e}")
        flash("An error occurred while fetching the record for editing.", "danger")
        return redirect(url_for('dashboard'))

@app.route('/sales/update/<int:sale_id>', methods=['POST'])
@login_required
def sale_update(sale_id):
    platform_id = request.form.get('platform_id')
    price = request.form.get('price')
    fee = request.form.get('fee')
    currency = request.form.get('currency')
    ts = request.form.get('ts')

    try:
        with engine.begin() as conn:
            query_find = text("SELECT user_id, item_id FROM Sales WHERE sale_id = :sid")
            sale = conn.execute(query_find, {"sid": sale_id}).fetchone()

            if not sale or sale.user_id != current_user.id:
                flash("Unauthorized to update this record.", "danger")
                return redirect(url_for('dashboard'))

            query_update = text("""
                UPDATE Sales
                SET platform_id = :plid, price = :price, fee = :fee, currency = :curr, ts = :ts
                WHERE sale_id = :sid
            """)
            conn.execute(query_update, {
                "plid": platform_id,
                "price": price,
                "fee": fee,
                "curr": currency,
                "ts": ts,
                "sid": sale_id
            })
            
            flash("Sale record updated successfully.", "success")
            return redirect(url_for('item_detail', item_id=sale.item_id))

    except Exception as e:
        print(f"Error updating sale {sale_id}: {e}")
        flash("An error occurred while updating the record.", "danger")
        return redirect(url_for('sale_edit_form', sale_id=sale_id))

@app.route('/holdings')
@login_required
def holdings():
    conn = get_db_connection()
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))

    target_currency = request.args.get('currency', 'USD').upper()
    exchange_rate = float(currency_service.get_rate(target_currency))
    
    # Get filter parameters
    search_query = request.args.get('search', '').strip()
    rarity_filter = request.args.get('rarity', '')
    exterior_filter = request.args.get('exterior', '')
    price_min = request.args.get('price_min', '')
    price_max = request.args.get('price_max', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    action_filter = request.args.get('action', '')
    sort_by = request.args.get('sort_by', 'name')  # name, price, quantity, date
    sort_order = request.args.get('sort_order', 'asc')  # asc, desc

    holdings = []
    all_rarities = []
    
    # Get all unique rarities for filter dropdown
    try:
        rarity_query = text("SELECT DISTINCT rarity FROM Items WHERE rarity IS NOT NULL ORDER BY rarity")
        rarity_rows = conn.execute(rarity_query).fetchall()
        all_rarities = [row[0] for row in rarity_rows if row[0]]
    except:
        pass
    
    # Query to get all items with holdings and their purchase details
    query_items = text("""
        WITH latest_market_price AS (
            SELECT DISTINCT ON (item_id)
                   item_id, price AS market_price
            FROM MarketSnapshots
            ORDER BY item_id, captured_at DESC
        ),
        purchase_summary AS (
            SELECT user_id, item_id, COUNT(*) AS qty_bought
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
        SELECT
            i.item_id, 
            i.market_name, 
            i.exterior,
            i.rarity,
            (COALESCE(p.qty_bought, 0) - COALESCE(s.qty_sold, 0)) AS quantity_held,
            lmp.market_price
        FROM
            purchase_summary p
        LEFT JOIN
            sales_summary s ON p.user_id = s.user_id AND p.item_id = s.item_id
        JOIN
            Items i ON p.item_id = i.item_id
        LEFT JOIN
            latest_market_price lmp ON p.item_id = lmp.item_id
        WHERE
            p.user_id = :uid
        ORDER BY
            i.market_name;
    """)
    
    # Query to get all purchase details for items in holdings
    query_purchases = text("""
        SELECT 
            p.item_id,
            p.purchase_id,
            p.ts AS purchase_time,
            p.price AS purchase_price,
            p.currency AS purchase_currency,
            pf.platform_name
        FROM Purchases p
        JOIN Platforms pf ON pf.platform_id = p.platform_id
        WHERE p.user_id = :uid
        ORDER BY p.ts DESC
    """)
    
    # Query to get all sale details for items
    query_sales = text("""
        SELECT 
            s.item_id,
            s.sale_id,
            s.ts AS sale_time,
            s.price AS sale_price,
            s.currency AS sale_currency,
            pf.platform_name
        FROM Sales s
        JOIN Platforms pf ON pf.platform_id = s.platform_id
        WHERE s.user_id = :uid
        ORDER BY s.ts DESC
    """)
    
    try:
        # Get all items with holdings
        items_result = conn.execute(query_items, {"uid": current_user.id}).fetchall()
        
        # Get all purchase records
        purchases_result = conn.execute(query_purchases, {"uid": current_user.id}).fetchall()
        
        # Get all sale records
        sales_result = conn.execute(query_sales, {"uid": current_user.id}).fetchall()
        
        # Build holdings dict
        holdings_dict = {}
        
        for row in items_result:
            item_id = row[0]
            market_price_usd = float(row[5]) if row[5] is not None else 0.0
            market_price_converted = market_price_usd * exchange_rate
            
            quantity_held = int(row[4])
            holdings_dict[item_id] = {
                'item_id': item_id,
                'market_name': row[1],
                'exterior': row[2] or 'N/A',
                'rarity': row[3] or 'N/A',
                'quantity_held': quantity_held,
                'market_price': market_price_converted,
                'action': 'hold' if quantity_held > 0 else 'selled',
                'purchases': [],
                'sales': []
            }
        
        # Add purchase records to each holding
        from datetime import datetime
        for row in purchases_result:
            item_id = row[0]
            if item_id in holdings_dict:
                purchase_price_usd = float(row[3]) if row[3] is not None else 0.0
                purchase_price_converted = purchase_price_usd * exchange_rate
                
                # Format purchase_time as string
                purchase_time = row[2]
                if purchase_time:
                    if isinstance(purchase_time, datetime):
                        purchase_time_str = purchase_time.strftime('%Y-%m-%d')
                    else:
                        purchase_time_str = str(purchase_time)
                else:
                    purchase_time_str = 'N/A'
                
                holdings_dict[item_id]['purchases'].append({
                    'purchase_id': row[1],
                    'purchase_time': purchase_time_str,
                    'purchase_time_obj': row[2],  # Keep original for filtering
                    'purchase_price': purchase_price_converted,
                    'purchase_currency': row[4],
                    'platform_name': row[5]
                })
        
        # Add sale records to each holding
        for row in sales_result:
            item_id = row[0]
            if item_id in holdings_dict:
                sale_price_usd = float(row[3]) if row[3] is not None else 0.0
                sale_price_converted = sale_price_usd * exchange_rate
                
                # Format sale_time as string
                sale_time = row[2]
                if sale_time:
                    if isinstance(sale_time, datetime):
                        sale_time_str = sale_time.strftime('%Y-%m-%d')
                    else:
                        sale_time_str = str(sale_time)
                else:
                    sale_time_str = 'N/A'
                
                holdings_dict[item_id]['sales'].append({
                    'sale_id': row[1],
                    'sale_time': sale_time_str,
                    'sale_time_obj': row[2],  # Keep original for filtering
                    'sale_price': sale_price_converted,
                    'sale_currency': row[4],
                    'platform_name': row[5]
                })
        
        # Convert to list and apply filters
        holdings = list(holdings_dict.values())
        
        # Apply filters
        if search_query:
            holdings = [h for h in holdings if search_query.lower() in h['market_name'].lower()]
        
        if action_filter:
            holdings = [h for h in holdings if h.get('action') == action_filter]
        
        if rarity_filter:
            holdings = [h for h in holdings if h.get('rarity') == rarity_filter]
        
        if exterior_filter:
            holdings = [h for h in holdings if h.get('exterior') == exterior_filter]
        
        if price_min:
            try:
                price_min_val = float(price_min) / exchange_rate  # Convert to USD for comparison
                holdings = [h for h in holdings if any(p['purchase_price'] / exchange_rate >= price_min_val for p in h['purchases'])]
            except:
                pass
        
        if price_max:
            try:
                price_max_val = float(price_max) / exchange_rate
                holdings = [h for h in holdings if any(p['purchase_price'] / exchange_rate <= price_max_val for p in h['purchases'])]
            except:
                pass
        
        if date_from:
            try:
                from datetime import datetime
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d').date()
                holdings = [h for h in holdings if any(
                    (p.get('purchase_time_obj').date() if p.get('purchase_time_obj') and hasattr(p.get('purchase_time_obj'), 'date') else None) >= date_from_obj 
                    for p in h['purchases']
                    if p.get('purchase_time_obj')
                )]
            except:
                pass
        
        if date_to:
            try:
                from datetime import datetime
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d').date()
                holdings = [h for h in holdings if any(
                    (p.get('purchase_time_obj').date() if p.get('purchase_time_obj') and hasattr(p.get('purchase_time_obj'), 'date') else None) <= date_to_obj 
                    for p in h['purchases']
                    if p.get('purchase_time_obj')
                )]
            except:
                pass
        
        # Sort purchases by time (newest first) for each holding
        for holding in holdings:
            holding['purchases'].sort(key=lambda x: x.get('purchase_time_obj') or x.get('purchase_time', ''), reverse=True)
        
        # Apply sorting to holdings list
        reverse_order = (sort_order == 'desc')
        if sort_by == 'name':
            holdings.sort(key=lambda x: x.get('market_name', '').lower(), reverse=reverse_order)
        elif sort_by == 'price':
            holdings.sort(key=lambda x: x.get('market_price', 0), reverse=reverse_order)
        elif sort_by == 'quantity':
            holdings.sort(key=lambda x: x.get('quantity_held', 0), reverse=reverse_order)
        elif sort_by == 'date':
            # Sort by most recent purchase date
            def get_latest_purchase_date(holding):
                purchase_dates = [p.get('purchase_time_obj') for p in holding.get('purchases', []) if p.get('purchase_time_obj')]
                if purchase_dates:
                    return max(purchase_dates)
                return None
            holdings.sort(key=get_latest_purchase_date, reverse=reverse_order)
        elif sort_by == 'action':
            holdings.sort(key=lambda x: x.get('action', ''), reverse=reverse_order)
        
    except Exception as e:
        print(f"Error fetching holdings: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Error fetching holdings: {e}", "danger")

    return render_template(
        "holdings.html", 
        holdings=holdings, 
        current_currency=target_currency, 
        exchange_rate=exchange_rate,
        all_rarities=all_rarities,
        search_query=search_query,
        rarity_filter=rarity_filter,
        exterior_filter=exterior_filter,
        price_min=price_min,
        price_max=price_max,
        date_from=date_from,
        date_to=date_to,
        action_filter=action_filter,
        sort_by=sort_by,
        sort_order=sort_order
    )

# --- Admin Routes ---

def admin_required(f):
    """Decorator to require admin access"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_administrator():
            flash("Access denied. Administrator privileges required.", "danger")
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/admin')
@login_required
@admin_required
def admin_dashboard():
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('home'))
    
    stats = {}
    users = []
    items = []
    
    try:
        # Get statistics
        query_stats = text("""
            SELECT 
                (SELECT COUNT(*) FROM Users) as total_users,
                (SELECT COUNT(*) FROM Items) as total_items,
                (SELECT COUNT(*) FROM Purchases) as total_purchases,
                (SELECT COUNT(*) FROM Sales) as total_sales,
                (SELECT COUNT(*) FROM MarketSnapshots) as total_snapshots
        """)
        stats_row = conn.execute(query_stats).fetchone()
        if stats_row:
            stats = {
                'total_users': stats_row[0] or 0,
                'total_items': stats_row[1] or 0,
                'total_purchases': stats_row[2] or 0,
                'total_sales': stats_row[3] or 0,
                'total_snapshots': stats_row[4] or 0
            }
        
        # Check if is_admin column exists
        has_admin_column = False
        try:
            check_column_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='users' AND column_name='is_admin'
            """)
            column_check = conn.execute(check_column_query).fetchone()
            has_admin_column = column_check is not None
        except Exception as e:
            print(f"Error checking for is_admin column in admin_dashboard: {e}")
            has_admin_column = False
        
        # Get all users
        if has_admin_column:
            query_users = text("""
                SELECT user_id, email, display_name, 
                       COALESCE(is_admin, false) as is_admin,
                       (SELECT COUNT(*) FROM Purchases WHERE user_id = Users.user_id) as purchase_count,
                       (SELECT COUNT(*) FROM Sales WHERE user_id = Users.user_id) as sale_count
                FROM Users
                ORDER BY user_id
            """)
        else:
            query_users = text("""
                SELECT user_id, email, display_name, 
                       false as is_admin,
                       (SELECT COUNT(*) FROM Purchases WHERE user_id = Users.user_id) as purchase_count,
                       (SELECT COUNT(*) FROM Sales WHERE user_id = Users.user_id) as sale_count
                FROM Users
                ORDER BY user_id
            """)
        
        user_rows = conn.execute(query_users).fetchall()
        
        for row in user_rows:
            users.append({
                'user_id': row[0],
                'email': row[1],
                'display_name': row[2],
                'is_admin': row[3] if len(row) > 3 else False,
                'purchase_count': row[4] if len(row) > 4 else 0,
                'sale_count': row[5] if len(row) > 5 else 0
            })
        
        # Get all items (limited to 100 for performance)
        query_items = text("""
            SELECT item_id, market_name, game, rarity, exterior,
                   (SELECT COUNT(*) FROM Purchases WHERE item_id = Items.item_id) as purchase_count,
                   (SELECT COUNT(*) FROM Sales WHERE item_id = Items.item_id) as sale_count,
                   (SELECT price FROM MarketSnapshots WHERE item_id = Items.item_id ORDER BY captured_at DESC LIMIT 1) as latest_price
            FROM Items
            ORDER BY item_id DESC
            LIMIT 100
        """)
        item_rows = conn.execute(query_items).fetchall()
        for row in item_rows:
            items.append({
                'item_id': row[0],
                'market_name': row[1],
                'game': row[2],
                'rarity': row[3],
                'exterior': row[4],
                'purchase_count': row[5] if len(row) > 5 else 0,
                'sale_count': row[6] if len(row) > 6 else 0,
                'latest_price': row[7] if len(row) > 7 else None
            })
            
    except Exception as e:
        print(f"Error fetching admin data: {e}")
        flash(f"Error fetching admin data: {e}", "danger")
    
    return render_template("admin_dashboard.html", stats=stats, users=users, items=items)

@app.route('/admin/user/<int:user_id>/toggle_admin', methods=['POST'])
@login_required
@admin_required
def toggle_user_admin(user_id):
    """Toggle admin status for a user"""
    conn = g.get('db_conn')
    if conn is None:
        flash("Database connection failed.", "danger")
        return redirect(url_for('admin_dashboard'))
    
    try:
        with engine.begin() as conn:
            # Check if is_admin column exists
            check_column_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='users' AND column_name='is_admin'
            """)
            column_check = conn.execute(check_column_query).fetchone()
            has_admin_column = column_check is not None
            
            if not has_admin_column:
                flash("The is_admin column does not exist in the Users table. Please run: ALTER TABLE Users ADD COLUMN is_admin BOOLEAN DEFAULT false;", "danger")
                return redirect(url_for('admin_dashboard'))
            
            # First check current status
            query_check = text("SELECT COALESCE(is_admin, false) FROM Users WHERE user_id = :uid")
            current_status = conn.execute(query_check, {"uid": user_id}).scalar()
            new_status = not current_status
            
            query_update = text("UPDATE Users SET is_admin = :status WHERE user_id = :uid")
            conn.execute(query_update, {"status": new_status, "uid": user_id})
            flash(f"User admin status updated successfully.", "success")
    except Exception as e:
        print(f"Error toggling admin status: {e}")
        flash(f"Error updating admin status: {e}", "danger")
    
    return redirect(url_for('admin_dashboard'))

# --- Background Price Updater ---

def run_price_updater():
    """
    This function runs in a background thread and periodically updates
    market prices for all items in the database.
    """
    print("Background price updater thread started.")
    
    STEAM_PLATFORM_ID = 1
    UPDATE_INTERVAL_MINUTES = 360  # 6 hours
    REQUEST_DELAY_SECONDS = 5

    # Give the server a moment to start up before the first run
    time.sleep(10)

    while True:
        print(f"[{datetime.datetime.now()}] Starting price update cycle...")
        
        items_to_update = []
        try:
            # The background thread needs its own database connection
            with engine.connect() as conn:
                query = text("SELECT item_id, market_name FROM Items")
                result = conn.execute(query).fetchall()
                items_to_update = [{'item_id': row[0], 'market_name': row[1]} for row in result]
                print(f"Found {len(items_to_update)} items to update.")
        except Exception as e:
            print(f"Error fetching items from database in background thread: {e}")
            # Sleep for a while before retrying if DB connection fails
            time.sleep(60)
            continue

        success_count = 0
        failed_count = 0

        for item in items_to_update:
            item_id = item['item_id']
            market_name = item['market_name']
            
            price = market_service.get_price_for_item(market_name)
            
            if price is not None:
                try:
                    with engine.begin() as conn:
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
                        success_count += 1

                except Exception as e:
                    print(f"Error inserting snapshot for item_id {item_id}: {e}")
                    failed_count += 1
            else:
                failed_count += 1
                # Only print detailed message for first few failures to reduce log spam
                if failed_count <= 3:
                    print(f"[PriceUpdater] No price available for '{market_name}' (item_id {item_id})")

            time.sleep(REQUEST_DELAY_SECONDS)
        
        print(f"[PriceUpdater] Update cycle complete: {success_count} succeeded, {failed_count} failed/skipped")

        print(f"[{datetime.datetime.now()}] Price update cycle finished. Next run in {UPDATE_INTERVAL_MINUTES} minutes.")
        time.sleep(UPDATE_INTERVAL_MINUTES * 60)

# --- Main Application Runner ---

# Background price updater is disabled - prices are not automatically fetched
# Uncomment the following lines to re-enable automatic price updates:
# updater_thread = threading.Thread(target=run_price_updater, daemon=True)
# updater_thread.start()


if __name__ == "__main__":
    # Background price updater is disabled
    # Uncomment to re-enable:
    # if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    #     print("Starting background price updater thread in MAIN process...")
    #     updater_thread = threading.Thread(target=run_price_updater, daemon=True)
    #     updater_thread.start()
    
    # run Flask
    app.run(host='0.0.0.0', port=8111, debug=True)

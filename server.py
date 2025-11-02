import os
from flask import Flask, render_template, request, redirect, url_for
from sqlalchemy import create_engine, text

# Initialize the Flask application
app = Flask(__name__)

# --- Database Configuration ---
# Replace with your actual database credentials
# Assumes you are the user 'zw3155' and password is 'your_password'
DATABASEURI = "postgresql://zw3155:your_password@w4111b.cs.columbia.edu/zw3155"

# Create the database engine
engine = create_engine(DATABASEURI)

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
    
    # 1. Get data from the submitted form
    user_id = request.form.get('user_id')
    email = request.form.get('email')
    display_name = request.form.get('display_name')
    
    # 2. Use a transaction block to execute the INSERT
    try:
        # engine.begin() creates a connection and starts a transaction
        with engine.begin() as conn:
            query = text("INSERT INTO Users(user_id, email, display_name) VALUES (:id, :email, :name)")
            conn.execute(query, {"id": user_id, "email": email, "name": display_name})
            # The transaction is automatically committed here if no errors
            
    except Exception as e:
        print(f"Error inserting new user: {e}")
        # In a real app, you'd show an error page
        # For this project, printing to console is fine
        # return render_template("error.html", message=str(e))
    
    # 3. Redirect back to the user list page
    return redirect(url_for('users_list'))


# --- Main Application Runner ---

if __name__ == "__main__":
    """
    Runs the Flask application.
    The host '0.0.0.0' makes it accessible from your VM's public IP.
    The port 8111 is required by the project spec.
    """
    app.run(host='0.0.0.0', port=8111, debug=True)
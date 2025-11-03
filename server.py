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
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'

# --- User Class Definition for Flask-Login ---
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
    conn = get_db_conn()
    if conn is None:
        return None
    try:
        query = text("SELECT * FROM Users WHERE user_id = :id")
        result_row = conn.execute(query, {"id": int(user_id)}).fetchone()
        
        if result_row:
            # BUG FIX: Convert row to dict before string access
            result = dict(result_row) 
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

        user = None
        try:
            query = text("SELECT * FROM Users WHERE email = :email")
            result_row = conn.execute(query, {"email": email}).fetchone()
            
            if result_row:
                # BUG FIX: Convert row to dict before string access
                result = dict(result_row)
                if check_password_hash(result['password_hash'], password):
                    user = User(
                        user_id=result['user_id'],
                        email=result['email'],
                        display_name=result['display_name'],
                        password_hash=result['password_hash']
                    )
            
            if user:
                login_user(user)
                flash(f"Welcome back, {user.display_name}!", "success")
                next_page = request.args.get('next')
                return redirect(next_page or url_for('dashboard'))
            else:
                flash("Invalid email or password. Please try again.", "danger")
                
        except Exception as e:
            print(f"Error during

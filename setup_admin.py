#!/usr/bin/env python3
"""
Script to set up admin functionality in the database.
This script will:
1. Add the is_admin column to Users table if it doesn't exist
2. Set a user as admin based on email address

Usage:
    python setup_admin.py <email>
    
Example:
    python setup_admin.py zw3155@columbia.edu
"""

import sys
from sqlalchemy import create_engine, text

# Database Configuration - Update this if needed
DATABASEURI = "postgresql://zw3155:477430@34.139.8.30/proj1part2"

def setup_admin(email):
    """Set up admin column and make a user admin"""
    engine = create_engine(DATABASEURI)
    
    try:
        with engine.begin() as conn:
            # Step 1: Add is_admin column if it doesn't exist
            print("Step 1: Checking if is_admin column exists...")
            try:
                check_column = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='users' AND column_name='is_admin'
                """)
                result = conn.execute(check_column).fetchone()
                
                if result is None:
                    print("  → is_admin column does not exist. Creating it...")
                    add_column = text("ALTER TABLE Users ADD COLUMN is_admin BOOLEAN DEFAULT false")
                    conn.execute(add_column)
                    print("  ✓ is_admin column created successfully!")
                else:
                    print("  ✓ is_admin column already exists.")
            except Exception as e:
                print(f"  ✗ Error checking/creating column: {e}")
                return False
            
            # Step 2: Set user as admin
            print(f"\nStep 2: Setting user '{email}' as admin...")
            try:
                # First check if user exists
                check_user = text("SELECT user_id, email, display_name FROM Users WHERE email = :email")
                user = conn.execute(check_user, {"email": email}).fetchone()
                
                if user is None:
                    print(f"  ✗ Error: User with email '{email}' not found in database.")
                    print("\nAvailable users:")
                    all_users = conn.execute(text("SELECT user_id, email, display_name FROM Users")).fetchall()
                    for u in all_users:
                        print(f"    - {u[1]} ({u[2]})")
                    return False
                
                # Update user to admin
                update_admin = text("UPDATE Users SET is_admin = true WHERE email = :email")
                conn.execute(update_admin, {"email": email})
                print(f"  ✓ User '{email}' ({user[2]}) is now an admin!")
                print("\n✓ Setup complete! The user can now log in and access admin features.")
                return True
                
            except Exception as e:
                print(f"  ✗ Error setting user as admin: {e}")
                return False
                
    except Exception as e:
        print(f"✗ Database connection error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python setup_admin.py <email>")
        print("\nExample:")
        print("  python setup_admin.py zw3155@columbia.edu")
        sys.exit(1)
    
    email = sys.argv[1]
    success = setup_admin(email)
    sys.exit(0 if success else 1)


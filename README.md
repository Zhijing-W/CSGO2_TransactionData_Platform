

# COMS W4111 - Project Part 3 - CS2 Market Tracker

  * **Name:** Zhijing Wu (Philip), Drew Hu
  * **UNI:** zw3155, yh3913

## 1\. Application and Database Information

### PostgreSQL Account

The PostgreSQL account to be used for grading is: **zw3155**

### Live Application URL

The application has been successfully deployed and is running 24/7 on our Google Cloud VM. It can be accessed at the following URL:

**[http://35.196.236.91:8111](https://www.google.com/search?q=http://35.196.236.91:8111)**

external IP will change!!!

## 2\. How to Run The Application (For Reference)

The application is already running in a `screen` session on the VM for grading. If the server needs to be restarted, the steps are:

1.  SSH into the VM: `ssh zw3155@cs4111-instance` (or via Google Cloud Console)
2.  Navigate to the project directory: `cd CSGO2_TransactionData_Platform`
3.  Activate the Python virtual environment: `source ./.virtualenvs/dbproj/bin/activate`
4.  Run the Flask server: `python3 server.py`
5.  The application will be available at `http://<VM_IP>:8111`.

## 3\. Database Connection

  * This application connects to the PostgreSQL database we created and populated in Part 2.
  * The `DATABASEURI` variable within `server.py` contains the credentials to connect.
  * **DB Host:** `34.139.8.30`
  * **DB Name:** `proj1part2`
  * **DB User:** `zw3155`

## 4\. Implemented Functionality (from Part 1 Proposal)

We successfully implemented all core features from the Part 1 proposal, expanding on the original vision to create a secure, multi-user platform where each user's data is private.

  * **Full User Authentication:**

      * Users can register for a new account via the `/register` route. Passwords are securely processed using `werkzeug.security`'s `generate_password_hash` and stored in the `Users` table (which we modified with an `ALTER TABLE` command).
      * Existing users can log in via the `/login` route, which verifies their credentials using `check_password_hash`.
      * The application uses `flask-login` to manage user sessions, remembering who is logged in.

  * **User-Private Data (Core Feature):**

      * Once logged in, a user can **only** view and manage their own financial data.
      * All core application pages (e.g., `/dashboard`, `/holdings`, `/purchases/new`) are protected with `@login_required`.
      * All SQL queries for these pages are strictly filtered using `WHERE user_id = :uid`, with the `:uid` parameter being securely pulled from `current_user.id`.

  * **Add Purchase Record (with "Get-or-Create" Logic):**

      * The `/purchases/new` form allows a logged-in user to add a new purchase to their personal account.
      * The `server.py` backend implements a robust "Get-or-Create" transaction logic:
        1.  It first checks the `Items` table based on the `market_name` and `exterior` (e.g., "AWP | Asiimov" + "Field-Tested").
        2.  **If the item exists**, its `item_id` is retrieved and used.
        3.  **If the item does not exist**, the code safely generates a new `item_id` (using `SELECT COALESCE(MAX(item_id), 0) + 1 FROM Items`), inserts this new item into the `Items` catalog, and **then** inserts the purchase record, all within a single database **transaction (`with conn.begin():`)**.

  * **Item Price History (Req \#3):**

      * Users can browse the global `/items` catalog and click any item to see its detailed history page (e.g., `/item/101`).
      * This page joins data from `Purchases`, `Sales`, and `MarketSnapshots` to show all historical transactions and price points for that item, allowing for price analysis.

  * **Personal Holdings Page (Req \#2):**

      * The `/holdings` page displays **only** the items currently held by the logged-in user (i.e., `qty_bought > qty_sold`).
      * For **each** item, a complex SQL query calculates and displays the user's `Average Buy Cost`, the `Current Market Price` (from the latest snapshot), and the `Unrealized PnL` for that position.

  * **Portfolio Dashboard (Req \#4):**

      * The `/dashboard` route serves as the user's homepage, displaying an **aggregated** financial summary of their entire portfolio.
      * A single, complex SQL query with multiple Common Table Expressions (CTEs) calculates their `Total Investment`, `Total Current Market Value`, `Total Realized PnL`, `Total Unrealized PnL`, `Total PnL`, and `ROI %`.

-----

### Unimplemented Features (Explanation)

  * **"Refresh Button":** The "refresh button" to trigger a new price fetch (mentioned in the Part 1 proposal) was not implemented. This feature would require an external API or web scraper, which falls outside the project's core focus on SQL, database transactions, and web application logic.

-----

## 5\. Most Interesting Database Operations (for Grading)

As required by the Part 3 submission guidelines, here are the two web pages we believe involve the most interesting database operations:

### Page 1: Add New Purchase (Route: `/purchases/create`)

  * **What it is used for:** This is the backend logic for the "Add Purchase" form. It allows a user to log a new purchase, and it ensures that the item they bought either exists in or is added to the master `Items` catalog.
  * **Relation to Database Operations:** This operation is interesting because it performs a "Get-or-Create" logic inside a single, safe **transaction**.
    1.  A `SELECT` query first checks if an item with a matching `market_name` and `exterior` already exists.
    2.  **If it does not exist**, a second query (`SELECT COALESCE(MAX(item_id), 0) + 1...`) safely finds the next available `item_id` (even if the table is empty).
    3.  A third query (`INSERT INTO Items...`) creates the new item.
    4.  A fourth query (`SELECT COALESCE(MAX(purchase_id), 0) + 1...`) finds the next `purchase_id`.
    5.  A final `INSERT INTO Purchases...` query logs the transaction, linking the `user_id` and the (newly found or created) `item_id`.
  * **Why it is interesting:** This demonstrates a complex, multi-step process that must be **atomic**. By wrapping all 5 queries in a `with conn.begin():` block, we ensure that if any step fails (e.g., the `INSERT INTO Purchases` fails), the entire operation is **rolled back**, preventing "orphan" items from being created in the `Items` table without a corresponding purchase.

### Page 2: Portfolio Dashboard (Route: `/dashboard`)

  * **What it is used for:** This page provides the logged-in user with a high-level summary of their entire investment portfolio, including their overall profit/loss and return on investment.
  * **Relation to Database Operations:** This page is generated by a single, complex SQL query that is over 70 lines long. This query uses **six** Common Table Expressions (CTEs) to build up the final report:
    1.  `latest_market_price`: Uses `DISTINCT ON` to find the single most recent price for every item in the `MarketSnapshots` table.
    2.  `purchase_summary`: Aggregates all of a user's purchases by item to find `qty_bought`, `total_cost_basis_item`, and `avg_buy_cost`.
    3.  `sales_summary`: Aggregates all sales to find `qty_sold`, `total_sale_revenue_item`, and `total_sale_fees_item`.
    4.  `holdings`: Joins `purchase_summary` and `sales_summary` to calculate the `quantity_held` for each item.
    5.  `portfolio_calcs`: Uses a `FULL OUTER JOIN` to combine all previous CTEs, calculating `realized_pnl` (from sales) and `unrealized_pnl` (from current holdings) for every item the user has ever touched.
    6.  **Final `SELECT`:** The main query then aggregates all calculations from `portfolio_calcs` into a single row for the user, calculating final `total_pnl` and `roi_percent`.
  * **Why it is interesting:** This single query demonstrates mastery of advanced SQL concepts, including `WITH` clauses for modularity, `DISTINCT ON` for price-finding, `FULL OUTER JOIN` to correctly combine users' purchase and sale histories, and `CASE` statements to prevent division-by-zero errors when calculating `ROI`. It is the most complex operation in the application.

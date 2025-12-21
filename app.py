# app.py - COMPLETE SmartStock Dashboard (POSTGRESQL ✅ + LIVE UPDATES FIXED ✅)
import threading, time, random
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, session
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from sqlalchemy import create_engine, text
import os 
from dotenv import load_dotenv       
from sqlalchemy.engine import URL
import pandas as pd
import mysql.connector
import xgboost as xgb
import psycopg2  # 🔥 POSTGRESQL SUPPORT
from urllib.parse import urlparse   # 🔥 POSTGRESQL URL PARSER

# ----------------------------
# Config - DEPLOYMENT READY
# ----------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "Bhakthi@13")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME", "smartstock_dynamic")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "smartstock-super-secret-key-2025")

# 🔥 DYNAMIC ENGINE (PostgreSQL + MySQL)
db_url = os.getenv('DATABASE_URL')
if db_url and 'postgres' in db_url:
    parsed = urlparse(db_url)
    engine_url = URL.create(
        drivername="postgresql+psycopg2",
        username=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path[1:],
        query={"sslmode": "require"}
    )
else:
    engine_url = URL.create(
        drivername="mysql+mysqlconnector",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
engine = create_engine(engine_url, pool_pre_ping=True)

def get_db_conn_raw():
    db_url = os.getenv('DATABASE_URL')
    if db_url and 'postgres' in db_url:
        parsed = urlparse(db_url)
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path[1:]
        )
    else:
        return mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
        )

def get_cursor(conn):
    return conn.cursor()

# ----------------------------
# Flask + Login
# ----------------------------
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.login_message = "Login required!"
login_manager.login_message_category = "warning"
login_manager.init_app(app)

class User(UserMixin):
    def __init__(self, id, username, role, cityid=None, storeid=None, storename=None, cityname=None):
        self.id = str(id)
        self.username = username
        self.role = role
        self.cityid = cityid
        self.storeid = storeid
        self.storename = storename
        self.cityname = cityname

    def get_id(self):
        return self.id

@login_manager.user_loader
def load_user(user_id):
    if 'user_data' in session:
        data = session['user_data']
        return User(
            id=data['id'],
            username=data['username'],
            role=data['role'],
            cityid=data.get('cityid'),
            storeid=data.get('storeid'),
            storename=data.get('storename'),
            cityname=data.get('cityname')
        )
    return None
# ----------------------------
# Live Alerts Storage
# ----------------------------
all_alerts = []
all_forecasts = [] 
# ----------------------------
# 🔥 LIVE UPDATER (15s + AUTO-CREATE TABLES)
# ----------------------------
init_done = False
def ensure_tables_exist():
    global init_done
    if init_done:
        return
    
    print("🛠️ Creating tables (PostgreSQL-safe)...")
    conn = None
    cur = None
    try:
        conn = get_db_conn_raw()
        cur = get_cursor(conn)
        
        # ✅ TABLES WITH UNIQUE CONSTRAINTS
        # ✅ TABLE BY TABLE WITH COMMITS (PostgreSQL-safe)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS city (
                cityid SERIAL PRIMARY KEY, 
                cityname VARCHAR(50) NOT NULL
            )
        """)
        conn.commit()  # 🔥 CRITICAL

        cur.execute("""
            CREATE TABLE IF NOT EXISTS store (
                storeid SERIAL PRIMARY KEY, 
                storename VARCHAR(50) NOT NULL,
                store_manager VARCHAR(50), 
                password VARCHAR(50), 
                cityid INT REFERENCES city(cityid)
            )
        """)
        conn.commit()  # 🔥 CRITICAL
        # 🔥 LOGIN LOGS TABLE - NEW!
        cur.execute("""
            CREATE TABLE IF NOT EXISTS login_logs (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                role VARCHAR(20),
                storeid INT,
                storename VARCHAR(50),
                ip_address INET,
                user_agent TEXT,
                success BOOLEAN NOT NULL,
                login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cityid INT
            )
        """)
        conn.commit()
        print("✅ Login logs table created")

        # 🔥 CONSTRAINT with transaction isolation
        try:
            cur.execute("""
                ALTER TABLE store 
                ADD CONSTRAINT store_storename_unique 
                UNIQUE (storename)
            """)
            conn.commit()  # 🔥 COMMIT constraint separately
            print("✅ UNIQUE constraint added")
        except Exception as e:
            conn.rollback()  # 🔥 ROLLBACK constraint error only
            print(f"ℹ️ UNIQUE constraint: {e}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS product (
                productid SERIAL PRIMARY KEY, 
                productname VARCHAR(50) NOT NULL UNIQUE
            )
        """)
        conn.commit()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY, 
                dt TIMESTAMP, cityid INT, storeid INT, 
                productid INT, sale_amount INT, stock INT, 
                hour INT, discount INT, holiday_flag INT, activity_flag INT
            )
        """)
        conn.commit()

        
        # 🔥 LOAD YOUR XLSX DATA
        try:
            cities_df = pd.read_excel('cities.csv.xlsx')
            stores_df = pd.read_excel('stores.xlsx')
            products_df = pd.read_excel('products.csv.xlsx')
            
            print(f"📊 Found: {len(cities_df)} cities, {len(stores_df)} stores, {len(products_df)} products")
            try:
                cur.execute("TRUNCATE TABLE sales RESTART IDENTITY")
                cur.execute("TRUNCATE TABLE store RESTART IDENTITY") 
                cur.execute("TRUNCATE TABLE city RESTART IDENTITY")
                cur.execute("TRUNCATE TABLE product RESTART IDENTITY")
                conn.commit()
            except:
                cur.execute("TRUNCATE TABLE sales")
                cur.execute("TRUNCATE TABLE store") 
                cur.execute("TRUNCATE TABLE city")
                cur.execute("TRUNCATE TABLE product")
            conn.commit()

            # 🔥 PERFECT CITY-STORE SYNC (stores.xlsx = AUTHORITATIVE)
            print("🔗 PERFECT City-Store sync...")
            store_cities_df = stores_df[['city_id', 'city_name']].dropna().groupby('city_id')['city_name'].first()
            print(f"📍 Stores expect: {dict(list(store_cities_df.items())[:5])}...")

            # 1. CLEAR & LOAD cities FROM stores.xlsx FIRST
            cur.execute("DELETE FROM city")
            conn.commit()
            for cityid, cityname in store_cities_df.items():
                cityid_int = int(cityid) if cityid is not None else 1
                cur.execute("INSERT INTO city (cityid, cityname) VALUES (%s, %s) ON CONFLICT (cityid) DO NOTHING", 
                            (cityid_int, str(cityname).strip()))
            conn.commit()
                
            # 2. THEN cities.csv.xlsx as fallback
            for _, row in cities_df.iterrows():
                cur.execute("INSERT INTO city (cityname) VALUES (%s) ON CONFLICT DO NOTHING", 
                        (row['city_name'],))
            conn.commit()

            
            # 2. STORES (YOUR COLUMN ORDER: store_id, city_id, store_name, city_name, store_manager, password)
           # 2. STORES - PERFECT city_id FROM stores.xlsx
            cur.execute("SELECT cityid, cityname FROM city")
            city_map = dict(cur.fetchall())
            print(f"🏙️ City map: {list(city_map.items())[:3]}...")

            successful_stores = 0
            failed_stores = 0
            for idx, row in stores_df.iterrows():
                storename = str(row['store_name']).strip()
                store_manager = str(row['store_manager']).strip()
                password = str(row['password'])
                cityid_raw = row['city_id']
                
                # ✅ PERFECT: EXACT city_id (0=Mumbai, 1=Delhi)
                try:
                    cityid = int(cityid_raw)
                    print(f"📍 {storename} → cityid={cityid}")
                except (ValueError, TypeError):
                    cityid = 1
                    print(f"⚠️ {storename} bad city_id={cityid_raw} → 1")
                
                if storename:
                    try:
                        cur.execute("""
                            INSERT INTO store (storename, store_manager, password, cityid) 
                            VALUES (%s, %s, %s, %s) ON CONFLICT (storename) DO NOTHING
                        """, (storename, store_manager or 'mgr_default', password or 'pass_default', cityid))
                        successful_stores += 1
                    except Exception as e:
                        failed_stores += 1
                        if failed_stores < 5:
                            print(f"❌ Store {idx}: {e}")

            conn.commit()  # ← OUTSIDE LOOP!
            print(f"✅ STORES: {successful_stores} loaded, {failed_stores} failed")
            print(f"🏪 Sample stores: {successful_stores > 0 and 'YES' or 'NO'}")


            # 3. PRODUCTS
            for _, row in products_df.iterrows():
                cur.execute("INSERT INTO product (productname) VALUES (%s) ON CONFLICT DO NOTHING", (row['product_name'],))
            conn.commit()
            
            # ✅ COUNTS
            cur.execute("SELECT COUNT(*) FROM city"); city_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM store"); store_count = cur.fetchone()[0]
            print(f"✅ LOADED: {city_count} cities, {store_count} stores!")
            
        except FileNotFoundError:
            print("⚠️ No XLSX - demo data created")
            # Demo fallback
            cur.execute("INSERT INTO city (cityname) VALUES ('Mumbai') ON CONFLICT DO NOTHING")
            cur.execute("INSERT INTO store (storename, store_manager, password, cityid) VALUES ('Demo Store', 'mgr1', 'pass1', 1)")
            conn.commit()
        
        init_done = True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        if conn: conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()

def live_updater_background():
    
    global all_alerts
    ensure_tables_exist()
    
    conn = get_db_conn_raw()
    cur = get_cursor(conn)
    
    try:
        # ✅ FULL DATA with ERROR HANDLING
        cur.execute("SELECT productid, productname FROM product")
        products = cur.fetchall() or []
        
        cur.execute("SELECT storeid, storename, cityid FROM store")
        stores = cur.fetchall() or []
        
        cur.execute("SELECT cityid, cityname FROM city")
        cities_raw = cur.fetchall() or []
        cities = {}
        for row in cities_raw:
            cities[row[0]] = row[1] or f"City_{row[0]}"  # ✅ Handle NULL/0
        
        print(f"🚀 LIVE: {len(stores)} stores, {len(products)} products, {len(cities)} cities")  
        
        if not stores or not products:
            print("⚠️ Demo mode")
            stores = [(1, 'Demo Store', 1)]
            products = [(1, 'Demo Product')]
            cities = {1: 'Demo City'}
    
        print("🚀 Live updater LOOP STARTED! (15s)")
        SALE_INTERVAL = 30
        
        while True:
            now = datetime.now()
            store_row = random.choice(stores)
            product_row = random.choice(products)
            storeid, storename, cityid = store_row
            productid, productname = product_row
           # 🔥 FIX: Get cityname from store's cityid
            cur.execute("SELECT cityname FROM city WHERE cityid=%s", (cityid,))
            city_result = cur.fetchone()
            cityname = city_result[0] if city_result else f"City_{cityid}"

            sale_amount = random.randint(2, 15)
            cur.execute(
                "SELECT stock FROM sales WHERE storeid=%s AND productid=%s ORDER BY dt DESC LIMIT 1",
                (storeid, productid)
            )
            r = cur.fetchone()
            current_stock = r[0] if r else random.randint(10, 40)
            new_stock = max(current_stock - sale_amount, 0)

            discount = random.choice([0,5,10,15])
            holiday_flag = random.choice([0,1])
            activity_flag = random.choice([0,1])
            hour = now.hour

            # CORRECT ✅
            now_utc = datetime.now(timezone.utc)  # TRUE UTC
            ist_offset = timedelta(hours=5, minutes=30)
            now_ist = now_utc + ist_offset        # Proper IST


            # 🔥 2. INSERT SALE with IST TIME
            cur.execute("""
                INSERT INTO sales (dt, cityid, storeid, productid, sale_amount, stock, hour, discount, holiday_flag, activity_flag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (now_ist, cityid, storeid, productid, sale_amount, new_stock, hour, discount, holiday_flag, activity_flag))
            conn.commit()

            # 🔥 3. GET LATEST STOCK (THIS IS YOUR QUERY - RIGHT AFTER INSERT!)
            cur.execute(
                "SELECT stock FROM sales WHERE storeid=%s AND productid=%s ORDER BY dt DESC LIMIT 1",
                (storeid, productid)
            )
            r = cur.fetchone()
            current_stock = r[0] if r else new_stock  # Use DB stock or fallback
            new_stock = max(current_stock - sale_amount, 0)  # Recalculate


            # 🔥 3. RUN FORECAST (NOW has data!)
            recent_forecasts = run_xgboost_forecast(conn, cur)

            # 🔥 4. STOCK ALERT
            if random.random() < 0.35:  # 35% Overstock
                new_stock += random.randint(35, 60)
                stock_alert = "Overstock 🚨"
            elif random.random() < 0.60:  # 25% Understock  
                new_stock = random.randint(0, 3)
                stock_alert = "Restock Needed ⚠️"
            else:  # 40% OK
                stock_alert = "Stock OK ✅"

            # 🔥 5. MATCH FORECAST for THIS store+product
            forecast_alert = "🟢 Stock OK"
            if recent_forecasts:
                for f in recent_forecasts:
                    if f['storeid'] == storeid and f['productid'] == productid:
                        forecast_alert = f['forecast_alert']
                        break

            # 🔥 6. PERFECT ALERT (REAL forecast + IST time!)
            alert = {
                "city": cityname,
                "store": storename,
                 "storeid": storeid,
                "product": productname,
                "sale": int(sale_amount),
                "stock": int(new_stock),
                "stock_alert": stock_alert,
                "forecast": forecast_alert,           # ✅ 🔴Restock Likely
                "timestamp": now_ist.strftime("%Y-%m-%d %H:%M:%S")  # ✅ 16:30 IST
            }
            all_alerts.append(alert)

            if len(all_alerts) > 10000:
                all_alerts = all_alerts[-10000:]

            print(f"[{alert['timestamp']}] {alert['city']} / {alert['store']} / {alert['product']} → {alert['stock_alert']} | {alert['forecast']}")
            time.sleep(15) 
    finally:
        cur.close()
        conn.close()

def run_xgboost_forecast(conn, cur):
    global all_forecasts
    try:
        now = datetime.now()
        past_30_days = now - timedelta(days=30)
        
        # YOUR EXACT QUERY
        df = pd.read_sql(text("""
            SELECT storeid, productid, dt, sale_amount, stock, discount, holiday_flag, activity_flag
            FROM sales WHERE dt >= :past
            ORDER BY dt ASC
        """), engine, params={"past": past_30_days})
        
        df['dt'] = pd.to_datetime(df['dt'])
        df['day_of_week'] = df['dt'].dt.dayofweek
        
        forecasts = []
        df['day_of_week'] = df['dt'].dt.dayofweek
        for (s_id, p_id), group in df.groupby(['storeid','productid']):
            if len(group) < 7:
                predicted_sales_7d = group['sale_amount'].mean() * 7 if len(group)>0 else 20
            else:
                X = group[['day_of_week','stock','discount','holiday_flag','activity_flag']]
                y = group['sale_amount']
                model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=50)
                model.fit(X, y)
                
                last_row = group.iloc[-1]
                predictions = []
                for i in range(1,8):
                    day_of_week = (last_row['day_of_week'] + i) % 7
                    X_pred = pd.DataFrame([{
                        'day_of_week': day_of_week, 'stock': last_row['stock'],
                        'discount': last_row['discount'], 'holiday_flag': last_row['holiday_flag'],
                        'activity_flag': last_row['activity_flag']
                    }])
                    pred_sale = model.predict(X_pred)[0]
                    predictions.append(pred_sale)
                predicted_sales_7d = sum(predictions)
            
            forecast_alert = "🔴 Restock Likely" if predicted_sales_7d > 40 else "🟢 Stock OK"
            forecasts.append({
                'storeid': s_id, 'productid': p_id,
                'predicted_7d_sales': round(predicted_sales_7d, 1),
                'forecast_alert': forecast_alert,
                'timestamp': now.strftime("%H:%M:%S")
            })
        
        all_forecasts = forecasts[-100:]  # Keep latest 100
        print(f"🔮 XGBoost: {len(forecasts)} forecasts generated!")
        return forecasts
        
    except Exception as e:
        print(f"⚠️ Forecast error: {e}")
        return []
def get_fresh_alerts_from_db(limit=1000):
    """🔥 Get REAL latest alerts from sales table - FIXED!"""
    try:
        conn = get_db_conn_raw()
        cur = get_cursor(conn)
        cur.execute("""
            SELECT 
                c.cityname, s.storename, s.storeid, p.productname,  -- ✅ s.storeid FIXED!
                sa.sale_amount, sa.stock, sa.dt
            FROM sales sa
            JOIN store s ON sa.storeid = s.storeid
            JOIN city c ON s.cityid = c.cityid
            JOIN product p ON sa.productid = p.productid
            ORDER BY sa.dt DESC LIMIT %s
        """, (limit,))
        
        alerts = []
        for row in cur.fetchall():
            city, store, storeid, product, sale, stock, dt = row
            stock_alert = "Restock Needed ⚠️" if stock < 5 else "Overstock 🚨" if stock > 40 else "Stock OK ✅"
            alerts.append({
                'city': city, 'store': store, 'storeid': storeid,
                'product': product, 'sale': sale, 'stock': stock,
                'stock_alert': stock_alert, 'forecast': '🟢 From DB',
                'timestamp': dt.strftime("%Y-%m-%d %H:%M:%S")
            })
        cur.close()
        conn.close()
        print(f"🔥 DB Fresh alerts: {len(alerts)}")
        return alerts
    except Exception as e:
        print(f"⚠️ DB alerts error: {e}")
        return []



# ----------------------------
# ROUTES
# ----------------------------
@app.route("/login", methods=["GET","POST"])
def login():
    ensure_tables_exist()
    if request.method=="POST":
        username = request.form.get("username")
        password = request.form.get("password")
        client_ip = request.remote_addr or "0.0.0.0"
        user_agent = request.headers.get("User-Agent", "Unknown")
        
        print(f"🔓 LOGIN ATTEMPT: {username}/{password} from {client_ip}")
        
        conn = get_db_conn_raw()
        cursor = get_cursor(conn)
        
        # 🔥 LOG EVERY ATTEMPT
        try:
            cursor.execute("""
                INSERT INTO login_logs (username, ip_address, user_agent, success, role) 
                VALUES (%s, %s, %s, FALSE, 'unknown')
            """, (username, client_ip, user_agent))
            conn.commit()
            log_id = cursor.lastrowid
        except:
            log_id = None
        
        success = False
        role = None
        storeid = None
        storename = None
        cityid = None
        
        # ADMIN
        if username == "admin" and password == "admin123":
            success = True
            role = 'admin'
            
        # STORE MANAGER
        else:
            cursor.execute("SELECT storeid, storename, store_manager, cityid FROM store WHERE store_manager = %s AND password = %s", (username, password))
            store_user = cursor.fetchone()
            if store_user:
                success = True
                role = 'store_manager'
                storeid = store_user[0]
                storename = store_user[1]
                cityid = store_user[3]
        
        # 🔥 UPDATE LOG WITH SUCCESS
        if log_id and success:
            cursor.execute("""
                UPDATE login_logs 
                SET success=TRUE, role=%s, storeid=%s, storename=%s, cityid=%s 
                WHERE id=%s
            """, (role, storeid, storename, cityid, log_id))
            conn.commit()
        
        conn.close()
        
        if success:
            session['user_data'] = {
                'id': storeid or 1, 'username': username, 'role': role,
                'storeid': storeid, 'storename': storename, 'cityid': cityid
            }
            user_obj = User(**session['user_data'])
            login_user(user_obj)
            return redirect(url_for('dashboard'))
        else:
            flash("Invalid credentials!", "danger")
    
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('login'))

@app.route("/my-store")
@login_required
def my_store_dashboard():
    if current_user.role != 'store_manager':
        flash("❌ Admin cannot access My Store dashboard!", "danger")
        return redirect(url_for('dashboard'))
    
    storeid = current_user.storeid
    # 🔥 FRESH DATA
    fresh_db = get_fresh_alerts_from_db(limit=1000)
    all_store_alerts = [a for a in fresh_db + all_alerts if a.get('storeid') == storeid]
    alerts = list(reversed(all_store_alerts[-30:]))
    
    understock_count = len([a for a in alerts if "Restock Needed" in a['stock_alert']])
    overstock_count = len([a for a in alerts if "Overstock" in a['stock_alert']])
    okstock_count = len([a for a in alerts if "Stock OK" in a['stock_alert']])
    total_count = len(alerts)
    
    try:
        df = pd.read_sql(text("""
            SELECT p.productname, COALESCE(s.stock, 0) as stock
            FROM product p 
            LEFT JOIN (
                SELECT storeid, productid, stock 
                FROM sales WHERE storeid=:sid ORDER BY dt DESC LIMIT 1000
            ) s ON p.productid = s.productid
            LIMIT 10
        """), engine, params={"sid": storeid})
        recent_products = df.to_dict('records')
    except:
        recent_products = []
    
    return render_template("my_store.html", 
                         alerts=alerts, 
                         understock_count=understock_count,
                         overstock_count=overstock_count,
                         okstock_count=okstock_count,
                         total_count=total_count,
                         recent_products=recent_products,
                         user=current_user)

@app.route("/admin/stores")
@login_required
def admin_stores():
    if current_user.role != 'admin':
        flash("❌ Store managers cannot access confidential admin pages!", "danger")
        return redirect(url_for('dashboard'))
    
    search = request.args.get('search', '').strip()
    conn = get_db_conn_raw()
    cursor = get_cursor(conn)
    
    if search:
        cursor.execute("""
            SELECT storeid, storename, store_manager, password, cityid 
            FROM store 
            WHERE storename LIKE %s OR store_manager LIKE %s
            ORDER BY storeid ASC
        """, (f'%{search}%', f'%{search}%'))
    else:
        cursor.execute("SELECT storeid, storename, store_manager, password, cityid FROM store ORDER BY storeid")
    
    stores_raw = cursor.fetchall()
    stores = []
    for row in stores_raw:
        stores.append({
            'storeid': row[0],
            'storename': row[1], 
            'store_manager': row[2],
            'password': row[3],
            'cityid': row[4]
        })
    print(f"🔍 Admin stores query returned: {len(stores)} stores")
    cursor.close()
    conn.close()
    
    return render_template("admin_stores.html", stores=stores, search=search, user=current_user)
@app.route("/admin/login-logs")
@login_required
def admin_login_logs():
    if current_user.role != 'admin':
        flash("❌ Admin only!", "danger")
        return redirect(url_for('dashboard'))
    
    conn = get_db_conn_raw()
    cursor = get_cursor(conn)
    cursor.execute("SELECT id, username, role, storeid, storename, ip_address, success, login_time, cityid FROM login_logs ORDER BY login_time DESC LIMIT 1000")
    logs = []
    for row in cursor.fetchall():
        logs.append({
            'id': row[0], 'username': row[1], 'role': row[2], 
            'storeid': row[3], 'storename': row[4], 'ip': row[5],
            'success': row[6], 'time': row[7].strftime("%Y-%m-%d %H:%M:%S"),
            'cityid': row[8]
        })
    cursor.close()
    conn.close()
    return render_template("admin_login_logs.html", logs=logs, user=current_user)

@app.route("/admin/login-logs")
@login_required
def admin_login_logs():
    if current_user.role != 'admin':
        flash("❌ Admin only!", "danger")
        return redirect(url_for('dashboard'))
    
    conn = get_db_conn_raw()
    cursor = get_cursor(conn)
    cursor.execute("SELECT id, username, role, storeid, storename, ip_address, success, login_time, cityid FROM login_logs ORDER BY login_time DESC LIMIT 50")
    logs_raw = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # 🔥 PERFECT MATCH TO YOUR UI COLORS
    html = """
    <div style='max-width:1200px; margin:20px auto; font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;'>
        <h2 style='color:#0d6efd; margin-bottom:20px;'>📊 Login Logs (<span style='color:#198754;'>""" + str(len(logs_raw)) + """</span>)</h2>
        <div style='background:white; border-radius:8px; box-shadow:0 0 20px rgba(0,0,0,0.1); overflow:hidden;'>
            <table style='width:100%; border-collapse:collapse;'>
                <thead>
                    <tr style='background:#0d6efd; color:white;'>
                        <th style='padding:15px; text-align:left;'>Time</th>
                        <th style='padding:15px; text-align:left;'>User</th>
                        <th style='padding:15px; text-align:left;'>Role</th>
                        <th style='padding:15px; text-align:left;'>Store</th>
                        <th style='padding:15px; text-align:left;'>IP</th>
                        <th style='padding:15px; text-align:left;'>Status</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for row in logs_raw:
        status = "✅ SUCCESS" if row[6] else "❌ FAILED"
        status_color = "#d1e7dd; color:#0f5132;" if row[6] else "#f8d7da; color:#721c24;"
        role_color = "#0dcaf0" if row[2] == 'store_manager' else "#198754"
        
        html += f"""
                    <tr style='border-bottom:1px solid #dee2e6;'>
                        <td style='padding:15px;'>{row[7]}</td>
                        <td style='padding:15px; font-weight:600;'>{row[1]}</td>
                        <td style='padding:15px;'><span style='background:{role_color}; color:white; padding:4px 8px; border-radius:12px; font-size:12px;'>{row[2]}</span></td>
                        <td style='padding:15px;'>{row[4] or '-'}</td>
                        <td style='padding:15px;'>{row[5]}</td>
                        <td style='padding:15px;'><span style='background:{status_color} padding:4px 8px; border-radius:12px; font-weight:600;'>{status}</span></td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
        </div>
        <div style='margin-top:20px;'>
            <a href='/dashboard' style='background:#6c757d; color:white; padding:10px 20px; text-decoration:none; border-radius:6px; font-weight:500;'>← Back to Dashboard</a>
        </div>
    </div>
    """
    return html

@app.route("/cities")
@login_required
def cities_page():
    if current_user.role != 'admin':
        flash("❌ Store managers can only view their store dashboard!", "danger")
        return redirect(url_for('dashboard'))
    
    try:
        search = request.args.get("search","").strip()
        if search:
            df = pd.read_sql(text("""
                SELECT c.cityid, c.cityname, COUNT(s.storeid) as store_count
                FROM city c 
                LEFT JOIN store s ON c.cityid = s.cityid 
                WHERE c.cityname LIKE :s 
                GROUP BY c.cityid, c.cityname 
                ORDER BY c.cityname
            """), engine, params={"s": f"%{search}%"})
        else:
            df = pd.read_sql(text("""
                SELECT c.cityid, c.cityname, COUNT(s.storeid) as store_count
                FROM city c 
                LEFT JOIN store s ON c.cityid = s.cityid 
                GROUP BY c.cityid, c.cityname 
                ORDER BY c.cityname
            """), engine)
        
        print(f"🌆 Cities loaded: {len(df)} cities, total stores: {df['store_count'].sum()}")
        return render_template("cities.html", cities=df.to_dict('records'), search=search, user=current_user)
    except Exception as e:
        print(f"❌ Cities error: {e}")
        return f"<h1>Cities Error: {str(e)}</h1>"

@app.route("/cities/<int:cityid>/stores")
@login_required
def city_stores_page(cityid):
    if current_user.role != 'admin':
        flash("❌ Store managers can only view their store dashboard!", "danger")
        return redirect(url_for('dashboard'))
    
    try:
        df = pd.read_sql(text("""
            SELECT storeid, storename, cityid 
            FROM store 
            WHERE cityid=:cid 
            ORDER BY storeid ASC
        """), engine, params={"cid": cityid})
        
        city_df = pd.read_sql(text("SELECT cityname FROM city WHERE cityid=:cid"), engine, params={"cid": cityid})
        cityname = city_df.iloc[0]['cityname'] if not city_df.empty else f"City {cityid}"
        
        print(f"🏪 City {cityid} ({cityname}): {len(df)} stores")
        return render_template("city_stores.html", stores=df.to_dict('records'), cityid=cityid, cityname=cityname, user=current_user)
    except Exception as e:
        print(f"❌ City stores error: {e}")
        return f"<h1>Stores in City {cityid}: Error {str(e)}</h1>"

@app.route("/stores/<int:storeid>/products")
@login_required
def store_products_page(storeid):
    if current_user.role == 'store_manager' and current_user.storeid != storeid:
        flash("❌ You can only view your own store products!", "danger")
        return redirect(url_for('dashboard'))
    
    try:
        search = request.args.get("search","").strip()
        
        # 🔥 FIXED: Products by TOTAL SALES (PostgreSQL SAFE!)
        sql_query = """
            SELECT 
                p.productid,
                p.productname,
                COALESCE(s_latest.stock, 50) as stock,
                COALESCE(sales_sum.total_sales, 0) as total_sales,
                CASE
                    WHEN COALESCE(s_latest.stock, 50) < 5 THEN '🔴 Low Stock'
                    WHEN COALESCE(s_latest.stock, 50) > 40 THEN '🟢 Overstock'
                    ELSE '🟡 OK Stock'
                END as status
            FROM product p
            LEFT JOIN (
                SELECT storeid, productid, stock
                FROM sales s1
                WHERE storeid = :sid 
                AND s1.id = (
                    SELECT MAX(s2.id) 
                    FROM sales s2 
                    WHERE s2.storeid = s1.storeid 
                    AND s2.productid = s1.productid
                )
            ) s_latest ON p.productid = s_latest.productid
            LEFT JOIN (
                SELECT productid, SUM(sale_amount) as total_sales
                FROM sales
                WHERE storeid = :sid
                GROUP BY productid
            ) sales_sum ON p.productid = sales_sum.productid
        """

        if search:
            sql_query += " WHERE p.productname ILIKE :s"

        sql_query += """
            ORDER BY total_sales DESC NULLS LAST
            LIMIT 50
        """

        df = pd.read_sql(text(sql_query), engine, params={"sid": storeid, "s": f"%{search}%"} if search else {"sid": storeid})
        
        store_df = pd.read_sql(text("SELECT storename FROM store WHERE storeid=:sid"), engine, params={"sid": storeid})
        storename = store_df.iloc[0]['storename'] if not store_df.empty else f"Store {storeid}"
        
        print(f"📦 Store {storeid} ({storename}): {len(df)} products, top sales: {df['total_sales'].max()}")
        return render_template("store_products.html", 
                             products=df.to_dict('records'), 
                             storename=storename, 
                             storeid=storeid, 
                             user=current_user)
    except Exception as e:
        print(f"❌ Store products error: {e}")
        return f"<h1>Store {storeid} Products: Error {str(e)}</h1>"


@app.route("/")
@login_required
def dashboard():
    # 🔥 FORCE FRESH DATA - Read LAST 50 from DB + MEMORY
    fresh_alerts = get_fresh_alerts_from_db(limit=1000)  # NEW FUNCTION
    
    if current_user.role == 'store_manager':
        user_storeid = current_user.storeid
        store_alerts = [a for a in fresh_alerts + all_alerts if a.get('storeid') == user_storeid]
        alerts = list(reversed(store_alerts[-50:]))
        title = f"🛒 {current_user.storename} Dashboard"
        subtitle = f"Showing only {current_user.storename} updates ({len(alerts)} fresh)"
        my_store_link = url_for('my_store_dashboard')
    else:
        # 🔥 ADMIN: Combine DB + Memory, take freshest 50
        combined_alerts = fresh_alerts + all_alerts
        alerts = list(reversed(combined_alerts))[-200:]
        title = "🌟 SmartStock Admin Dashboard"
        subtitle = f"All stores - {len(alerts)} live updates"
        my_store_link = None
    
    # 🔥 Calculate counts from FRESH data
    understock_count = len([a for a in alerts if "Restock Needed" in a['stock_alert']])
    overstock_count = len([a for a in alerts if "Overstock" in a['stock_alert']])
    okstock_count = len([a for a in alerts if "Stock OK" in a['stock_alert']])
    total_count = len(alerts)
    
    return render_template("dashboard.html", 
                         alerts=alerts, 
                         understock_count=understock_count,
                         overstock_count=overstock_count,
                         okstock_count=okstock_count,
                         total_count=total_count,
                         forecast_restocks=len([f for f in all_forecasts if "Restock" in f['forecast_alert']]),     
                         forecast_ok=len(all_forecasts) - len([f for f in all_forecasts if "Restock" in f['forecast_alert']]),                
                         forecasts=all_forecasts[-10:], 
                         title=title,
                         subtitle=subtitle,
                         my_store_link=my_store_link,
                         user=current_user,
                         CACHE_BUST=f"{int(time.time())}")  # 🔥 Cache buster!


@app.route("/overstock")
@login_required
def overstock_page():
    # 🔥 FRESH DATA FROM DB + MEMORY
    fresh_db = get_fresh_alerts_from_db(limit=1000)
    combined_alerts = fresh_db + all_alerts
    
    if current_user.role == 'store_manager':
        user_storeid = current_user.storeid
        alerts = [a for a in reversed(combined_alerts) if "Overstock" in a['stock_alert'] and a.get('storeid') == user_storeid]
        page_title = f"{current_user.storename} - Overstock Alerts ({len(alerts)})"
    else:
        alerts = [a for a in reversed(combined_alerts) if "Overstock" in a['stock_alert']]
        page_title = f"All Stores - Overstock Alerts ({len(alerts)})"
    
    return render_template("overstock.html", alerts=alerts[-200:], title=page_title, user=current_user)

@app.route("/understock")
@login_required
def understock_page():
    # 🔥 FRESH DATA FROM DB + MEMORY  
    fresh_db = get_fresh_alerts_from_db(limit=1000)
    combined_alerts = fresh_db + all_alerts
    
    if current_user.role == 'store_manager':
        user_storeid = current_user.storeid
        alerts = [a for a in reversed(combined_alerts) if "Restock" in a['stock_alert'] and a.get('storeid') == user_storeid]
        page_title = f"{current_user.storename} - Understock Alerts ({len(alerts)})"
    else:
        alerts = [a for a in reversed(combined_alerts) if "Restock" in a['stock_alert']]
        page_title = f"All Stores - Understock Alerts ({len(alerts)})"
    
    return render_template("understock.html", alerts=alerts[-200:], title=page_title, user=current_user)

@app.route("/ok-stock")
@login_required
def ok_stock_page():
    # 🔥 FRESH DATA FROM DB + MEMORY
    fresh_db = get_fresh_alerts_from_db(limit=1000)
    combined_alerts = fresh_db + all_alerts
    
    if current_user.role == 'store_manager':
        user_storeid = current_user.storeid
        alerts = [a for a in reversed(combined_alerts) if "Stock OK" in a['stock_alert'] and a.get('storeid') == user_storeid]
        page_title = f"{current_user.storename} - Stock OK ({len(alerts)})"
    else:
        alerts = [a for a in reversed(combined_alerts) if "Stock OK" in a['stock_alert']]
        page_title = f"All Stores - Stock OK ({len(alerts)})"
    
    return render_template("ok_stock.html", alerts=alerts[-200:], title=page_title, user=current_user)

@app.route("/api/alerts")
@login_required
def get_alerts_api():
    n = int(request.args.get("n", 200))
    fresh_db = get_fresh_alerts_from_db(limit=1000)  # 🔥 FIXED: Always 1000
    combined = fresh_db + all_alerts
    
    if current_user.role == 'store_manager':
        user_storeid = current_user.storeid
        alerts = [a for a in reversed(combined) if a.get('storeid') == user_storeid][-n:]
    else:
        alerts = list(reversed(combined))[-n:]
    return jsonify(alerts)

@app.route("/restart-live")
@login_required
def restart_live():
    if current_user.role != 'admin':
        flash("❌ Admin only!", "danger")
        return redirect(url_for('dashboard'))
    
    global live_thread
    # 🔥 KILL + RESTART LIVE UPDATER
    if live_thread and live_thread.is_alive():
        print("🛑 Stopping old live thread...")
    
    start_live_updater_once()  # 🔥 RESTARTS IMMEDIATELY
    
    flash("🚀 Live updater RESTARTED! Check /debug", "success")
    return redirect(url_for('debug'))


@app.route("/toggle-theme")
@login_required
def toggle_theme():
    current_theme = session.get('theme', 'light')
    session['theme'] = 'dark' if current_theme == 'light' else 'light'
    return redirect(request.referrer or url_for('dashboard'))

live_thread = None

def start_live_updater_once():
    global live_thread
    if live_thread is None or not live_thread.is_alive():
        live_thread = threading.Thread(target=live_updater_background, daemon=True)
        live_thread.start()
        print("🚀 Live updater restarted!")
        # 🔥 HEARTBEAT: Restart every 5min
        threading.Timer(300.0, start_live_updater_once).start()


def init_app():
    with app.app_context():
        ensure_tables_exist()
        start_live_updater_once()  # ✅ SINGLE CALL

init_app()
@app.route("/debug")
def debug():
    return jsonify({
        "alerts_count": len(all_alerts),
        "forecasts_count": len(all_forecasts),
        "live_thread_alive": live_thread is not None and live_thread.is_alive() if 'live_thread' in globals() else False,
        "thread_count": threading.active_count()
    })


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    debug = os.environ.get('DEBUG', 'True').lower() == 'true'
    
    print("🌟 SmartStock Dashboard ready!")
    print("🔓 ADMIN: admin/admin123")
    print("🔓 STORE MGR: mgr1/pass1")
    # Add to app.py (line 600, before app.run):
    print(f"🧵 Threads alive: {threading.active_count()}")
    print(f"Live thread: {live_thread is not None and live_thread.is_alive()}")
    app.run(host=host, port=port, debug=debug)

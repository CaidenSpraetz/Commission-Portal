from flask import Flask, render_template, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
import os
import hashlib
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime
from sqlalchemy import text

# Import Bullhorn API modules (ATS + Back Office helpers)
try:
    from bullhorn_api import (
        BullhornAPI,
        get_bullhorn_commission_data,   # ATS (primarily Permanent)
        get_bbo_commission_data,        # Back Office / Timesheets (Contract/Temp)
        BackOfficeAPI
    )
    API_AVAILABLE = True
except ImportError:
    API_AVAILABLE = False
    print("Warning: bullhorn_api.py not available; API routes will be disabled")

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'commission-portal-secret-key')

# ================= Persisted SQLite on Azure Linux App Service =================
DATA_DIR = os.environ.get('DATA_DIR', '/home/site/wwwroot')
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'commission_portal.db')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{DB_PATH}?check_same_thread=false"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Optional: cap upload size (50 MB)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# Optional: keep original uploaded files for audit/debug
UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ================= Database =================
db = SQLAlchemy(app)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='employee')
    job_function = db.Column(db.String(20), nullable=False, default='recruiter')
    status = db.Column(db.String(10), default='Active')

class CommissionData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_name = db.Column(db.String(100), nullable=False)
    employee_id = db.Column(db.Integer, nullable=True)    # FK-like (not enforced) to Employee.id
    client = db.Column(db.String(200))
    status = db.Column(db.String(50))
    gp = db.Column(db.Float)
    hourly_gp = db.Column(db.Float)
    commission_rate = db.Column(db.String(10))
    commission = db.Column(db.Float)
    month = db.Column(db.String(20))
    day = db.Column(db.Integer)
    year = db.Column(db.Integer)                          # NEW
    placement_id = db.Column(db.String(64))               # NEW (ATS/BBO placement identifier if available)
    unique_key = db.Column(db.String(64), unique=True)    # NEW: upsert guard

# ================= Password Security Helpers =================
def hash_password(password):
    """Hash password - try werkzeug pbkdf2; fallback to SHA256"""
    try:
        from werkzeug.security import generate_password_hash
        return generate_password_hash(password, method='pbkdf2:sha256')
    except Exception:
        return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hash_value):
    """Verify password against hash - try werkzeug; fallback to SHA256 compare"""
    try:
        from werkzeug.security import check_password_hash
        if check_password_hash(hash_value, password):
            return True
    except Exception:
        pass
    return hashlib.sha256(password.encode()).hexdigest() == hash_value

# ================= Generic Utilities =================
def _first_match(row_map, candidates):
    """Return first non-empty value by scanning row_map (lowercased headers -> values)."""
    for key in candidates:
        if key in row_map:
            val = row_map.get(key, "")
            if pd.notna(val) and str(val).strip() != "":
                return val
    return None

def _parse_date(value):
    """Parse many date shapes (strings, pandas timestamps, Excel serials)."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.to_pydatetime()
    if isinstance(value, (int, float)) and 20000 < float(value) < 60000:
        base = pd.Timestamp('1899-12-30')
        try:
            return (base + pd.Timedelta(days=float(value))).to_pydatetime()
        except Exception:
            pass
    try:
        dt = pd.to_datetime(value, errors='coerce', infer_datetime_format=True, utc=False)
        if pd.notna(dt):
            if isinstance(dt, (pd.Series, pd.Index)):
                dt = dt.iloc[0]
            return dt.to_pydatetime()
    except Exception:
        return None
    return None

def _infer_commission_rate(status, gp, provided_rate):
    """
    Returns (rate_str, rate_float). Priority:
    1) provided_rate in file (like '9.75%')
    2) status-based: if contains 'enterprise' -> 9.75%, else 10.00%
    """
    if provided_rate:
        s = str(provided_rate).strip()
        try:
            if s.endswith('%'):
                rf = float(s.rstrip('%')) / 100.0
                return f"{(rf*100):.2f}%", rf
            else:
                rf = float(s)
                if rf > 1:
                    rf = rf / 100.0
                return f"{(rf*100):.2f}%", rf
        except Exception:
            pass
    st = (status or "").lower()
    if "enterprise" in st:
        return "9.75%", 0.0975
    return "10.00%", 0.10

def _unique_key_for(record: dict) -> str:
    """
    Create a stable unique key per logical row to support upserts.
    Includes placement_id when present; otherwise hash core identifiers.
    """
    prefix = "perm" if str(record.get('status', '')).lower().startswith('permanent') else \
             "bbo" if 'contract' in str(record.get('status', '')).lower() else "file"
    base_str = "|".join([
        str(record.get('placement_id') or '').strip(),
        str(record.get('employee_name') or '').strip(),
        str(record.get('client') or '').strip(),
        str(record.get('status') or '').strip(),
        str(record.get('month') or '').strip(),
        str(record.get('year') or '').strip()
    ])
    digest = hashlib.sha256(base_str.encode()).hexdigest()[:24]
    return f"{prefix}:{digest}"

def match_employee_name_to_record(name: str):
    """Find Employee row by display name (case-insensitive)."""
    if not name:
        return None
    return Employee.query.filter(Employee.name.ilike(name)).first()

# ================= Routes =================
@app.route('/')
def index():
    # Ensure we serve the template named 'index.html' from /templates
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'})
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({'success': False, 'error': 'Username and password required'})
    employee = Employee.query.filter_by(username=username, status='Active').first()
    if employee and verify_password(password, employee.password_hash):
        session['user_id'] = employee.id
        session['username'] = employee.username
        session['role'] = employee.role
        session['name'] = employee.name
        return jsonify({'success': True, 'user': {
            'name': employee.name, 'role': employee.role, 'username': employee.username
        }})
    return jsonify({'success': False, 'error': 'Invalid credentials'})

@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True})

@app.route('/api/employees', methods=['GET'])
def get_employees():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    employees = Employee.query.all()
    return jsonify([{
        'id': emp.id,
        'name': emp.name,
        'email': emp.email,
        'username': emp.username,
        'role': emp.role,
        'job_function': emp.job_function,
        'status': emp.status
    } for emp in employees])

@app.route('/api/employees', methods=['POST'])
def add_employee():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'})
    required = ['name', 'email', 'username', 'password', 'role', 'job_function']
    for f in required:
        if not data.get(f):
            return jsonify({'success': False, 'error': f'{f} is required'})
    if Employee.query.filter_by(username=data['username']).first():
        return jsonify({'success': False, 'error': 'Username already exists'})
    password_hash = hash_password(data['password'])
    employee = Employee(
        name=data['name'], email=data['email'], username=data['username'],
        password_hash=password_hash, role=data['role'], job_function=data['job_function']
    )
    try:
        db.session.add(employee)
        db.session.commit()
        return jsonify({'success': True})
    except Exception:
        db.session.rollback()
        return jsonify({'success': False, 'error': 'Database error'})

@app.route('/api/employees/<int:employee_id>', methods=['DELETE'])
def delete_employee(employee_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    employee = Employee.query.get(employee_id)
    if not employee:
        return jsonify({'success': False, 'error': 'Employee not found'})
    try:
        db.session.delete(employee)
        db.session.commit()
        return jsonify({'success': True})
    except Exception:
        db.session.rollback()
        return jsonify({'success': False, 'error': 'Database error'})

@app.route('/api/commission-data', methods=['GET'])
def get_commission_data():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    commission_data = CommissionData.query.order_by(CommissionData.id.desc()).all()
    if session.get('role') == 'employee':
        commission_data = [cd for cd in commission_data if cd.employee_name == session.get('name')]
    return jsonify([{
        'id': cd.id,
        'employee': cd.employee_name,
        'employee_id': cd.employee_id,
        'client': cd.client,
        'status': cd.status,
        'gp': cd.gp,
        'hourly_gp': cd.hourly_gp,
        'commission_rate': cd.commission_rate,
        'commission': cd.commission,
        'month': cd.month,
        'day': cd.day,
        'year': cd.year,
        'placement_id': cd.placement_id,
        'unique_key': cd.unique_key
    } for cd in commission_data])

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'})
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    try:
        filename = secure_filename(file.filename).lower()
        saved_path = os.path.join(UPLOAD_DIR, filename)
        file.stream.seek(0)
        file.save(saved_path)
        if filename.endswith('.csv'):
            df = pd.read_csv(saved_path)
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            df = pd.read_excel(saved_path, engine='openpyxl')
        else:
            return jsonify({'success': False, 'error': 'Unsupported file format'})

        lower_cols = [str(c).strip().lower() for c in df.columns]
        df.columns = lower_cols

        CLIENT_COLS = ['client', 'client name', 'customer', 'company', 'account']
        GP_COLS = ['gp', 'gross profit', 'profit', 'gp amount']
        EMP_COLS = ['employee', 'employee name', 'recruiter', 'sales', 'sales rep', 'owner', 'consultant']
        HOURS_COLS = ['hours', 'total hours', 'hours worked']
        STATUS_COLS = ['status', 'sales status', 'placement status']
        RATE_COLS = ['commission rate', 'rate', 'comm rate']
        DATE_COLS = ['date', 'placement date', 'start date', 'we date', 'week ending', 'invoice date', 'bill date']

        processed_count = 0

        for _, row in df.iterrows():
            row_map = {col: row[col] for col in df.columns}
            client = _first_match(row_map, CLIENT_COLS)
            gp_val = _first_match(row_map, GP_COLS)
            employee_name = _first_match(row_map, EMP_COLS)
            hours_val = _first_match(row_map, HOURS_COLS)
            status_val = _first_match(row_map, STATUS_COLS)
            provided_rate = _first_match(row_map, RATE_COLS)

            date_val = _first_match(row_map, DATE_COLS)
            dt = _parse_date(date_val)
            if dt is None:
                month_name, day_num, year_val = 'Current', 1, datetime.now().year
            else:
                month_name, day_num, year_val = dt.strftime('%B'), int(dt.day), dt.year

            gp = pd.to_numeric(gp_val, errors='coerce') if gp_val is not None else None
            if client and employee_name and gp is not None and not pd.isna(gp):
                hours = pd.to_numeric(hours_val, errors='coerce') if hours_val is not None else None
                if hours is None or pd.isna(hours) or float(hours) <= 0:
                    hours = 40.0
                hourly_gp = float(gp) / float(hours)
                rate_str, rate_float = _infer_commission_rate(status_val, gp, provided_rate)
                commission_amt = round(float(gp) * rate_float, 2)

                record = {
                    'employee_name': str(employee_name),
                    'client': str(client),
                    'status': str(status_val) if status_val else 'New',
                    'gp': float(gp),
                    'hourly_gp': round(hourly_gp, 2),
                    'commission_rate': rate_str,
                    'commission': commission_amt,
                    'month': month_name,
                    'day': day_num,
                    'year': year_val,
                    'placement_id': None
                }
                ukey = _unique_key_for(record)
                emp_row = match_employee_name_to_record(record['employee_name'])
                emp_id = emp_row.id if emp_row else None

                existing = CommissionData.query.filter_by(unique_key=ukey).first()
                if existing:
                    existing.employee_name = record['employee_name']
                    existing.employee_id = emp_id
                    existing.client = record['client']
                    existing.status = record['status']
                    existing.gp = record['gp']
                    existing.hourly_gp = record['hourly_gp']
                    existing.commission_rate = record['commission_rate']
                    existing.commission = record['commission']
                    existing.month = record['month']
                    existing.day = record['day']
                    existing.year = record['year']
                    existing.placement_id = record['placement_id']
                else:
                    db.session.add(CommissionData(
                        employee_name=record['employee_name'],
                        employee_id=emp_id,
                        client=record['client'],
                        status=record['status'],
                        gp=record['gp'],
                        hourly_gp=record['hourly_gp'],
                        commission_rate=record['commission_rate'],
                        commission=record['commission'],
                        month=record['month'],
                        day=record['day'],
                        year=record['year'],
                        placement_id=record['placement_id'],
                        unique_key=ukey
                    ))
                processed_count += 1

        db.session.commit()
        return jsonify({'success': True, 'processed': processed_count})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Processing error: {str(e)}'})

# ================= Bullhorn API Routes =================

@app.route('/api/sync-bullhorn', methods=['POST'])
def sync_bullhorn_data():
    """Sync commission data from ATS (Permanent) and Back Office Timesheets (Contract/Temp)."""
    if not API_AVAILABLE:
        return jsonify({'success': False, 'error': 'API modules not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403

    try:
        data = request.get_json() or {}
        start_date_str = data.get('start_date')
        include_contract_time = data.get('include_contract_time', True)
        include_permanent = data.get('include_permanent', True)

        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'})
        else:
            start_date = datetime(datetime.now().year, 1, 1)

        total_fetched = 0
        processed_count = 0

        # ---- Back Office / Timesheets (Contract/Temp) ----
        if include_contract_time:
            try:
                bbo_rows = get_bbo_commission_data(start_date, datetime.now())
            except Exception as e:
                bbo_rows = []
                print(f"BBO fetch error: {e}")
            total_fetched += len(bbo_rows)

            for record in bbo_rows:
                try:
                    ukey = _unique_key_for(record)
                    emp_row = match_employee_name_to_record(record.get('employee_name'))
                    emp_id = emp_row.id if emp_row else None
                    existing = CommissionData.query.filter_by(unique_key=ukey).first()

                    payload = dict(
                        employee_name=record['employee_name'],
                        employee_id=emp_id,
                        client=record['client'],
                        status=record['status'],
                        gp=float(record['gp'] or 0),
                        hourly_gp=float(record['hourly_gp'] or 0),
                        commission_rate=record['commission_rate'],
                        commission=float(record['commission'] or 0),
                        month=record['month'],
                        day=int(record['day'] or 1),
                        year=int(record.get('year') or datetime.now().year),
                        placement_id=record.get('placement_id'),
                        unique_key=ukey
                    )
                    if existing:
                        for k, v in payload.items():
                            setattr(existing, k, v)
                    else:
                        db.session.add(CommissionData(**payload))
                    processed_count += 1
                except Exception as e:
                    print(f"BBO upsert error: {e}")

        # ---- ATS (Permanent placements) ----
        if include_permanent:
            try:
                ats_rows = get_bullhorn_commission_data(
                    include_contract_time=False,   # we use BBO for contract time
                    include_permanent=True,
                    start_date=start_date
                )
            except Exception as e:
                ats_rows = []
                print(f"ATS fetch error: {e}")
            total_fetched += len(ats_rows)

            for record in ats_rows:
                try:
                    ukey = _unique_key_for(record)
                    emp_row = match_employee_name_to_record(record.get('employee_name'))
                    emp_id = emp_row.id if emp_row else None
                    existing = CommissionData.query.filter_by(unique_key=ukey).first()

                    payload = dict(
                        employee_name=record['employee_name'],
                        employee_id=emp_id,
                        client=record['client'],
                        status=record['status'],
                        gp=float(record['gp'] or 0),
                        hourly_gp=float(record['hourly_gp'] or 0),
                        commission_rate=record['commission_rate'],
                        commission=float(record['commission'] or 0),
                        month=record['month'],
                        day=int(record['day'] or 1),
                        year=int(record.get('year') or datetime.now().year),
                        placement_id=record.get('placement_id'),
                        unique_key=ukey
                    )
                    if existing:
                        for k, v in payload.items():
                            setattr(existing, k, v)
                    else:
                        db.session.add(CommissionData(**payload))
                    processed_count += 1
                except Exception as e:
                    print(f"ATS upsert error: {e}")

        db.session.commit()
        return jsonify({
            'success': True,
            'processed': processed_count,
            'total_fetched': total_fetched
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Sync error: {str(e)}'})

@app.route('/api/test-bullhorn', methods=['GET'])
def test_bullhorn_connection():
    """Test ATS REST connection."""
    if not API_AVAILABLE:
        return jsonify({'success': False, 'error': 'API modules not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        api = BullhornAPI()
        success = api.authenticate()
        if success:
            return jsonify({'success': True, 'message': 'ATS connected', 'rest_url': api.rest_url})
        else:
            return jsonify({'success': False, 'error': 'Failed to authenticate with ATS REST'})
    except Exception as e:
        return jsonify({'success': False, 'error': f'Connection test failed: {str(e)}'})

@app.route('/api/bullhorn-summary', methods=['GET'])
def get_bullhorn_summary():
    """ATS summary (kept similar to prior)."""
    if not API_AVAILABLE:
        return jsonify({'success': False, 'error': 'API modules not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        api = BullhornAPI()
        if not api.authenticate():
            return jsonify({'success': False, 'error': 'Authentication failed'})
        now = datetime.now()
        start_of_year = int(datetime(now.year, 1, 1).timestamp() * 1000)
        perm_fields = ["id"]
        perm_where = f"employmentType='Permanent' AND dateBegin>={start_of_year}"
        _, perm_total = api.fetch_entity("Placement", perm_fields, perm_where, 1)

        # approximate time records count from ATS as before (not authoritative)
        contract_time_fields = ["id"]
        start_month_ms = int(datetime(now.year, now.month, 1).timestamp() * 1000)
        end_month_ms = int(datetime(now.year + (1 if now.month == 12 else 0),
                                    1 if now.month == 12 else now.month + 1, 1).timestamp() * 1000)
        time_where = f"dateWorked>={start_month_ms} AND dateWorked<{end_month_ms}"
        _, all_time_total = api.fetch_entity("PlacementTimeUnit", contract_time_fields, time_where, 1)
        estimated_contract_time_total = int(all_time_total * 0.7) if all_time_total else 0

        return jsonify({'success': True, 'summary': {
            'permanent_placements_ytd': perm_total,
            'contract_time_records_current_month': estimated_contract_time_total,
            'last_sync': None
        }})
    except Exception as e:
        return jsonify({'success': False, 'error': f'Summary error: {str(e)}'})

# -------- Back Office (Timesheets) test & summary --------
@app.route('/api/test-bbo', methods=['GET'])
def test_bbo_connection():
    if not API_AVAILABLE:
        return jsonify({'success': False, 'error': 'API modules not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        client = BackOfficeAPI()
        return jsonify({'success': True}) if client.ping() else jsonify({'success': False, 'error': 'Ping failed'})
    except Exception as e:
        return jsonify({'success': False, 'error': f'BBO test failed: {str(e)}'})

@app.route('/api/bbo-summary', methods=['GET'])
def bbo_summary():
    if not API_AVAILABLE:
        return jsonify({'success': False, 'error': 'API modules not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        # For current month
        now = datetime.now()
        start_of_month = datetime(now.year, now.month, 1)
        rows = get_bbo_commission_data(start_of_month, now)
        entries = len(rows)
        total_hours = 0.0
        for r in rows:
            # We didn't persist hours in CommissionData for BBO, but we can recompute here if needed.
            # If you decide to store hours in CommissionData later, update this accordingly.
            # For summary, treat GP/hourly_gp to derive hours if present:
            gp = float(r.get('gp') or 0.0)
            hourly_gp = float(r.get('hourly_gp') or 0.0)
            if hourly_gp > 0:
                total_hours += gp / hourly_gp
        return jsonify({'success': True, 'summary': {
            'entries_current_month': entries,
            'total_hours_current_month': round(total_hours, 2)
        }})
    except Exception as e:
        return jsonify({'success': False, 'error': f'BBO summary failed: {str(e)}'})

# ================= Init DB & schema guard =================
def ensure_commission_columns():
    """
    Ensure new columns exist for older deployments (SQLite only).
    """
    try:
        res = db.session.execute(text("PRAGMA table_info(commission_data)")).fetchall()
        cols = {row[1] for row in res}
        alter_cmds = []
        if 'employee_id' not in cols:
            alter_cmds.append("ALTER TABLE commission_data ADD COLUMN employee_id INTEGER")
        if 'year' not in cols:
            alter_cmds.append("ALTER TABLE commission_data ADD COLUMN year INTEGER")
        if 'placement_id' not in cols:
            alter_cmds.append("ALTER TABLE commission_data ADD COLUMN placement_id VARCHAR(64)")
        if 'unique_key' not in cols:
            alter_cmds.append("ALTER TABLE commission_data ADD COLUMN unique_key VARCHAR(64)")
        for cmd in alter_cmds:
            db.session.execute(text(cmd))
        if alter_cmds:
            db.session.commit()
    except Exception as e:
        print(f"Schema ensure error (safe to ignore on fresh DB): {e}")

def init_db():
    with app.app_context():
        db.create_all()
        ensure_commission_columns()

        if not Employee.query.filter_by(username='admin').first():
            users = [
                {'name': 'Administrator', 'email': 'admin@company.com', 'username': 'admin', 'password': 'admin123', 'role': 'admin', 'job_function': 'admin'},
                {'name': 'Pam Henard', 'email': 'pam@company.com', 'username': 'phenard', 'password': 'temp123', 'role': 'employee', 'job_function': 'recruiter'},
                {'name': 'Sarah Johnson', 'email': 'sarah@company.com', 'username': 'sjohnson', 'password': 'secure789', 'role': 'manager', 'job_function': 'both'}
            ]
            for u in users:
                user = Employee(
                    name=u['name'], email=u['email'], username=u['username'],
                    password_hash=hash_password(u['password']),
                    role=u['role'], job_function=u['job_function']
                )
                db.session.add(user)

            samples = [
                dict(employee_name='Pam Henard', client='Ajax Building Company', status='Contract (Temporary)',
                     gp=336.96, hourly_gp=6.48, commission_rate='10.00%', commission=33.70, month='August', day=3, year=datetime.now().year, placement_id=None),
                dict(employee_name='Pam Henard', client='Evolent', status='Contract (Temporary)',
                     gp=117.84, hourly_gp=2.95, commission_rate='10.00%', commission=11.78, month='August', day=3, year=datetime.now().year, placement_id=None),
                dict(employee_name='Sarah Johnson', client='TechCorp', status='Permanent',
                     gp=450.00, hourly_gp=0.00, commission_rate='10.00%', commission=45.00, month='August', day=5, year=datetime.now().year, placement_id='P-12345'),
            ]
            for s in samples:
                ukey = _unique_key_for(s)
                db.session.add(CommissionData(
                    employee_name=s['employee_name'], employee_id=match_employee_name_to_record(s['employee_name']).id if match_employee_name_to_record(s['employee_name']) else None,
                    client=s['client'], status=s['status'], gp=s['gp'], hourly_gp=s['hourly_gp'],
                    commission_rate=s['commission_rate'], commission=s['commission'],
                    month=s['month'], day=s['day'], year=s['year'], placement_id=s['placement_id'], unique_key=ukey
                ))
            db.session.commit()

# Initialize database on startup
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(debug=debug_mode, host='0.0.0.0', port=port)

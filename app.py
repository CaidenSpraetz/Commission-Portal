import os
import hashlib
import logging
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, render_template, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename

# ================= Logging =================
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("commission_portal")

# ================= Optional Bullhorn modules =================
try:
    from bullhorn_api import (
        get_bullhorn_commission_data,
        BullhornAPI,
        get_bbo_commission_data,
    )
    try:
        from bullhorn_api import BBOClient as ExternalBBOClient  # optional
    except Exception:
        ExternalBBOClient = None
    BULLHORN_AVAILABLE = True
except Exception as e:
    BULLHORN_AVAILABLE = False
    log.warning("bullhorn_api.py not available; ATS sync routes limited (%s)", e)
    ExternalBBOClient = None

# ================= Flask / App paths =================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'commission-portal-secret-key')

DATA_DIR = os.environ.get('DATA_DIR', '/home/site/wwwroot')
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, 'commission_portal.db')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{DB_PATH}?check_same_thread=false"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Upload settings
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024
UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ================= Database =================
db = SQLAlchemy(app)

class Employee(db.Model):
    __tablename__ = "employee"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), nullable=False)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='employee')
    job_function = db.Column(db.String(20), nullable=False, default='recruiter')
    status = db.Column(db.String(10), default='Active')

class CommissionData(db.Model):
    __tablename__ = "commission_data"
    id = db.Column(db.Integer, primary_key=True)
    employee_name = db.Column(db.String(100), nullable=False)
    client = db.Column(db.String(200))
    status = db.Column(db.String(50))
    gp = db.Column(db.Float)
    hourly_gp = db.Column(db.Float)
    commission_rate = db.Column(db.String(10))
    commission = db.Column(db.Float)
    month = db.Column(db.String(20))
    day = db.Column(db.Integer)

    # Newer schema fields
    year = db.Column(db.Integer, default=datetime.now().year)
    placement_id = db.Column(db.String(50))
    unique_key = db.Column(db.String(120), index=True, unique=True)
    employee_id = db.Column(db.Integer)

# ================== BBO (Back Office) Client – robust endpoint discovery ==================
class BBOClient:
    """
    Env vars you provided:
      - BBO_TIMESHEETS_BASE (preferred) or BBO_REST_DOMAIN
        e.g. https://api.bbo.bullhornstaffing.com
      - BBO_API_KEY
      - (optional) BBO_API_VERSION -> default 'v1.0'
    Supports BOTH styles:
      A) {base}/{version}/Timesheets/Entries
      B) {base}/api/{version_short}/timesheets/entries
    """
    def __init__(self):
        base = os.environ.get("BBO_TIMESHEETS_BASE") or os.environ.get("BBO_REST_DOMAIN") or "https://api.bbo.bullhornstaffing.com"
        # strip trailing slashes & stray colons (some bases come as '...com:')
        self.base_url = base.rstrip("/:").strip()
        self.version = (os.environ.get("BBO_API_VERSION") or "v1.0").strip().strip("/")
        self.api_key = os.environ.get("BBO_API_KEY")
        self.timeout = int(os.environ.get("BBO_TIMEOUT", "30"))

        if not self.api_key:
            raise ValueError("Missing Back Office (Timesheets) env vars: BBO_API_KEY")

        import requests
        self._requests = requests
        self._session = requests.Session()

    def _version_short(self):
        # v1.0 -> v1, 1.0 -> 1, v2 -> v2
        v = self.version.lower()
        if v.startswith("v"):
            vnum = v[1:]
        else:
            vnum = v
        vnum = vnum.split(".")[0] if "." in vnum else vnum
        return f"v{vnum}" if not v.startswith("v") else (f"v{vnum}" if "." in v else v)

    def _candidate_urls(self, path_variants):
        """
        Build a list of candidate full URLs for the given path variants, covering:
          - {base}/{version}/{Path}
          - {base}/api/{vshort}/{path}
          - lower/upper case variations supplied via path_variants
        """
        urls = []
        b = self.base_url
        v = self.version
        vs = self._version_short()

        # If base already includes '/api', also try with and without
        bases = {b}
        if "/api" not in b:
            bases.add(f"{b}/api")
        else:
            bases.add(b.replace("/api", ""))

        for base_candidate in bases:
            for pv in path_variants:
                # Style A
                urls.append(f"{base_candidate}/{v}{pv}")
                # Style B (api/vX)
                urls.append(f"{base_candidate}/{vs}{pv}")

        # Normalize: collapse multiple slashes
        clean = []
        for u in urls:
            u = u.replace("://", "§§").replace("//", "/").replace("§§", "://")
            clean.append(u.rstrip("/"))
        # Deduplicate preserving order
        seen = set()
        out = []
        for u in clean:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _headers(self):
        return {"x-api-key": self.api_key, "Accept": "application/json"}

    def diagnose_connectivity(self):
        """
        Try several endpoints with HEAD then GET.
        Treat any 2xx–4xx as 'reachable' (auth/params may fail but the host is up).
        """
        path_variants = [
            "/Timesheets/Entries", "/timesheets/entries",
            "/Timesheets", "/timesheets",
            "/Documentation/source", "/swagger", "/openapi.json", "/",
        ]
        urls = self._candidate_urls(path_variants)

        last_err = None
        for url in urls:
            try:
                r = self._session.head(url, headers=self._headers(), timeout=self.timeout, allow_redirects=True)
                if 200 <= r.status_code < 500:
                    return {"reachable": True, "url": url, "method": "HEAD", "status": r.status_code}
                # try GET if HEAD is not allowed
                r = self._session.get(url, headers=self._headers(), timeout=self.timeout, allow_redirects=True)
                if 200 <= r.status_code < 500:
                    return {"reachable": True, "url": url, "method": "GET", "status": r.status_code}
                last_err = f"{url} -> {r.status_code}"
            except Exception as e:
                last_err = f"{url} -> {e}"
                continue
        raise RuntimeError(last_err or "No reachable BBO endpoint found")

    def ping(self) -> bool:
        # Backwards-compatible boolean for /api/test-bbo route
        try:
            diag = self.diagnose_connectivity()
            return bool(diag.get("reachable"))
        except Exception:
            return False

    def get_time_entries(self, start_date: datetime, end_date: datetime):
        # Build candidate time-entry endpoints
        path_variants = ["/Timesheets/Entries", "/timesheets/entries"]
        urls = self._candidate_urls(path_variants)

        # Accept common param shapes
        params_variants = [
            {"from": start_date.strftime("%Y-%m-%d"), "to": end_date.strftime("%Y-%m-%d")},
            {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d")},
            {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")},
        ]

        last_err = None
        for url in urls:
            for params in params_variants:
                try:
                    r = self._session.get(url, headers=self._headers(), params=params, timeout=self.timeout)
                    # 200 OK: success
                    if r.status_code == 200:
                        data = r.json()
                        if isinstance(data, dict) and "data" in data:
                            return data["data"]
                        if isinstance(data, list):
                            return data
                        return []
                    # 204 No Content => empty
                    if r.status_code == 204:
                        return []
                    # 4xx often means auth/param issues but connectivity is OK
                    if 400 <= r.status_code < 500:
                        # If it's a parameter error (400), try next param shape
                        last_err = f"{url} {r.status_code} {r.text[:200]}"
                        continue
                    # 5xx: try next URL
                    last_err = f"{url} {r.status_code} {r.text[:200]}"
                except Exception as e:
                    last_err = f"{url} -> {e}"
                    continue
        raise RuntimeError(last_err or "Unable to fetch BBO time entries")

# Prefer BBOClient from bullhorn_api.py if present
if ExternalBBOClient:
    BBOClient = ExternalBBOClient  # type: ignore

# ================= Password Security =================
def hash_password(password):
    try:
        from werkzeug.security import generate_password_hash
        return generate_password_hash(password, method='pbkdf2:sha256')
    except Exception:
        return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hash_value):
    try:
        from werkzeug.security import check_password_hash
        if check_password_hash(hash_value, password):
            return True
    except Exception:
        pass
    return hashlib.sha256(password.encode()).hexdigest() == hash_value

# ================= Helpers =================
def _first_match(row_map, candidates):
    for key in candidates:
        if key in row_map:
            val = row_map.get(key, "")
            if pd.notna(val) and str(val).strip() != "":
                return val
    return None

def _parse_date(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, (pd.Timestamp, )):
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

def _safe_add_column(table: str, column: str, ddl_type: str):
    try:
        res = db.session.execute(db.text(f"PRAGMA table_info({table});")).fetchall()
        cols = [r[1] for r in res]
        if column not in cols:
            db.session.execute(db.text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type};"))
            db.session.commit()
            log.info("SQLite: added missing column %s.%s", table, column)
    except Exception as e:
        db.session.rollback()
        log.warning("SQLite column check/add failed for %s.%s: %s", table, column, e)

def _employee_id_by_name(name: str):
    emp = Employee.query.filter_by(name=name).first()
    return emp.id if emp else None

def _dedupe_key(rec: dict) -> str:
    pid = rec.get("placement_id")
    if pid:
        prefix = "perm" if rec.get("status","").lower().startswith("perm") else "contract"
        return f"{prefix}:{pid}"
    year = rec.get("year") or datetime.now().year
    month = rec.get("month") or "Unknown"
    day = rec.get("day") or 1
    emp = rec.get("employee_name") or ""
    client = rec.get("client") or ""
    status = rec.get("status") or ""
    gp = rec.get("gp") or 0.0
    return f"{year}|{month}|{day}|{emp}|{client}|{status}|{gp:.2f}"

# ================= Routes =================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json() or {}
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
    data = request.get_json() or {}
    required = ['name', 'email', 'username', 'password', 'role', 'job_function']
    for f in required:
        if not data.get(f):
            return jsonify({'success': False, 'error': f'{f} is required'})
    if Employee.query.filter_by(username=data['username']).first():
        return jsonify({'success': False, 'error': 'Username already exists'})
    employee = Employee(
        name=data['name'], email=data['email'], username=data['username'],
        password_hash=hash_password(data['password']),
        role=data['role'], job_function=data['job_function']
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
        'unique_key': cd.unique_key,
        'employee_id': cd.employee_id
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
                month_name = 'Current'
                day_num = 1
                year_num = datetime.now().year
            else:
                month_name = dt.strftime('%B')
                day_num = int(dt.day)
                year_num = dt.year

            gp = pd.to_numeric(gp_val, errors='coerce') if gp_val is not None else None
            if client and employee_name and gp is not None and not pd.isna(gp):
                hours = pd.to_numeric(hours_val, errors='coerce') if hours_val is not None else None
                if hours is None or pd.isna(hours) or float(hours) <= 0:
                    hours = 40.0
                hourly_gp = float(gp) / float(hours)

                rate_str, rate_float = _infer_commission_rate(status_val, gp, provided_rate)
                commission_amt = round(float(gp) * rate_float, 2)

                rec = CommissionData(
                    employee_name=str(employee_name),
                    client=str(client),
                    status=str(status_val) if status_val else 'New',
                    gp=float(gp),
                    hourly_gp=round(hourly_gp, 2),
                    commission_rate=rate_str,
                    commission=commission_amt,
                    month=month_name,
                    day=day_num,
                    year=year_num,
                )
                rec.unique_key = _dedupe_key({
                    "employee_name": rec.employee_name,
                    "client": rec.client,
                    "status": rec.status,
                    "gp": rec.gp,
                    "month": rec.month,
                    "day": rec.day,
                    "year": rec.year,
                    "placement_id": None
                })
                rec.employee_id = _employee_id_by_name(rec.employee_name)

                exists = CommissionData.query.filter_by(unique_key=rec.unique_key).first()
                if not exists:
                    db.session.add(rec)
                    processed_count += 1

        db.session.commit()
        return jsonify({'success': True, 'processed': processed_count})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Processing error: {str(e)}'})

# ================= Bullhorn ATS & Back Office routes =================

@app.route('/api/test-bullhorn', methods=['GET'])
def test_bullhorn_connection():
    if not BULLHORN_AVAILABLE:
        return jsonify({'success': False, 'error': 'Bullhorn API module not available'})
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        api = BullhornAPI()
        success = api.authenticate()
        if success:
            return jsonify({'success': True, 'message': 'Successfully connected to Bullhorn API', 'rest_url': api.rest_url})
        else:
            return jsonify({'success': False, 'error': 'Failed to authenticate with Bullhorn API'})
    except Exception as e:
        return jsonify({'success': False, 'error': f'Connection test failed: {str(e)}'})

@app.route('/api/test-bbo', methods=['GET'])
def test_bbo_connection():
    """Robust BBO connectivity diagnostic (no hard dependency on /Ping or /Health)."""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    try:
        client = BBOClient()
        diag = client.diagnose_connectivity()
        return jsonify({
            'success': True,
            'message': 'BBO is reachable',
            'endpoint': diag.get('url'),
            'http_method': diag.get('method'),
            'status': diag.get('status')
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'BBO test failed: {e}'})

@app.route('/api/bbo-summary', methods=['GET'])
def bbo_summary():
    """Enhanced BBO summary with better error handling for 503 errors."""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    if session.get('role') not in ['admin', 'manager']:
        return jsonify({'error': 'Insufficient permissions'}), 403
    
    try:
        lookback_days = int(request.args.get("days", "30"))
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        if BULLHORN_AVAILABLE:
            # Use enhanced BackOfficeAPI from bullhorn_api.py
            from bullhorn_api import get_bbo_commission_data
            commission_rows = get_bbo_commission_data(
                datetime(start_date.year, start_date.month, start_date.day),
                datetime(end_date.year, end_date.month, end_date.day)
            )
            
            total_entries = len(commission_rows)
            total_hours = sum(row.get("hourly_gp", 0) * 40 for row in commission_rows)  # Estimate
            total_gp = sum(row.get("gp", 0) for row in commission_rows)
            total_commission = sum(row.get("commission", 0) for row in commission_rows)
            
            return jsonify({
                "success": True,
                "summary": {
                    "entries": total_entries,
                    "hours": round(total_hours, 2),
                    "gp_amount": round(total_gp, 2),
                    "commission_amount": round(total_commission, 2),
                    "window": {"start": str(start_date), "end": str(end_date)},
                    "source": "enhanced_api"
                }
            })
        else:
            # Fallback to original BBOClient if bullhorn_api not available
            client = BBOClient()
            entries = client.get_time_entries(
                datetime(start_date.year, start_date.month, start_date.day),
                datetime(end_date.year, end_date.month, end_date.day)
            )
            total = len(entries)
            total_hours = 0.0
            total_bill = 0.0
            total_pay = 0.0
            for e in entries:
                total_hours += float(e.get("hours", 0) or 0)
                total_bill += float(e.get("billAmount", 0) or 0)
                total_pay += float(e.get("payAmount", 0) or 0)
            return jsonify({
                "success": True,
                "summary": {
                    "entries": total,
                    "hours": round(total_hours, 2),
                    "bill_amount": round(total_bill, 2),
                    "pay_amount": round(total_pay, 2),
                    "gp_est": round(total_bill - total_pay, 2),
                    "window": {"start": str(start_date), "end": str(end_date)},
                    "source": "original_client"
                }
            })
            
    except Exception as e:
        error_message = str(e)
        
        # Handle specific error types
        if "503" in error_message or "Service Unavailable" in error_message:
            return jsonify({
                'success': False, 
                'error': 'Back Office service is temporarily unavailable. Please try again later.',
                'error_type': 'service_unavailable',
                'retry_suggested': True
            })
        elif "404" in error_message:
            return jsonify({
                'success': False,
                'error': 'Back Office timesheets endpoint not found. Please verify your tenant configuration.',
                'error_type': 'endpoint_not_found'
            })
        elif "401" in error_message or "403" in error_message:
            return jsonify({
                'success': False,
                'error': 'Back Office authentication failed. Please verify your credentials.',
                'error_type': 'authentication_failed'
            })
        else:
            return jsonify({
                'success': False, 
                'error': f'BBO summary failed: {error_message}',
                'error_type': 'general_error'
            })

@app.route('/api/sync-bullhorn', methods=['POST'])
def sync_bullhorn_data():
    """Enhanced sync with better error handling for BBO service issues."""
    if not BULLHORN_AVAILABLE:
        return jsonify({'success': False, 'error': 'Bullhorn API module not available'})
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

        all_records = []
        sync_warnings = []
        
        # Try to get ATS data (permanent placements)
        if include_permanent:
            try:
                ats_records = get_bullhorn_commission_data(
                    include_contract_time=False,
                    include_permanent=True,
                    start_date=start_date
                )
                all_records.extend(ats_records)
                log.info(f"Successfully fetched {len(ats_records)} ATS records")
            except Exception as e:
                log.error(f"ATS sync failed: {e}")
                sync_warnings.append(f"ATS sync failed: {str(e)}")
        
        # Try to get BBO data (contract/timesheet data)
        if include_contract_time:
            try:
                end_date = datetime.now()
                bbo_records = get_bbo_commission_data(start_date, end_date)
                all_records.extend(bbo_records)
                log.info(f"Successfully fetched {len(bbo_records)} BBO records")
            except Exception as e:
                log.error(f"BBO sync failed: {e}")
                if "503" in str(e) or "Service Unavailable" in str(e):
                    sync_warnings.append("Back Office service temporarily unavailable - contract data skipped")
                else:
                    sync_warnings.append(f"Back Office sync failed: {str(e)}")

        if not all_records and not sync_warnings:
            return jsonify({'success': False, 'error': 'No data retrieved from any APIs'})

        processed = 0
        for rec in all_records:
            try:
                employee_name = rec.get('employee_name') or rec.get('employee') or ""
                client = rec.get('client') or ""
                status = rec.get('status') or "New"
                gp = float(rec.get('gp') or 0.0)
                hourly_gp = float(rec.get('hourly_gp') or 0.0)
                commission_rate = rec.get('commission_rate') or "10.00%"
                commission = float(rec.get('commission') or round(gp * 0.10, 2))
                month = rec.get('month') or (start_date.strftime("%B"))
                day = int(rec.get('day') or 1)
                year = int(rec.get('year') or start_date.year)
                placement_id = rec.get("placement_id")

                unique_key = _dedupe_key({
                    "employee_name": employee_name,
                    "client": client,
                    "status": status,
                    "gp": gp,
                    "month": month,
                    "day": day,
                    "year": year,
                    "placement_id": placement_id
                })

                if CommissionData.query.filter_by(unique_key=unique_key).first():
                    continue

                row = CommissionData(
                    employee_name=employee_name,
                    client=client,
                    status=status,
                    gp=gp,
                    hourly_gp=round(hourly_gp, 2),
                    commission_rate=commission_rate,
                    commission=round(commission, 2),
                    month=month,
                    day=day,
                    year=year,
                    placement_id=str(placement_id) if placement_id else None,
                    unique_key=unique_key,
                    employee_id=_employee_id_by_name(employee_name)
                )
                db.session.add(row)
                processed += 1
            except Exception as rec_error:
                log.warning(f"Error processing record: {rec_error}")
                continue

        db.session.commit()
        
        response_data = {
            'success': True, 
            'processed': processed, 
            'total_fetched': len(all_records)
        }
        
        if sync_warnings:
            response_data['warnings'] = sync_warnings
            
        return jsonify(response_data)

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Sync error: {str(e)}'})

@app.route('/api/bullhorn-summary', methods=['GET'])
def get_bullhorn_summary():
    if not BULLHORN_AVAILABLE:
        return jsonify({'success': False, 'error': 'Bullhorn API module not available'})
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

        return jsonify({
            'success': True,
            'summary': {
                'permanent_placements_ytd': perm_total,
                'contract_time_records_current_month': None,
                'last_sync': None
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'Summary error: {str(e)}'})

# ================= Init DB & seed sample =================
def init_db():
    with app.app_context():
        db.create_all()
        _safe_add_column("commission_data", "year", "INTEGER")
        _safe_add_column("commission_data", "placement_id", "VARCHAR(50)")
        _safe_add_column("commission_data", "unique_key", "VARCHAR(120)")
        _safe_add_column("commission_data", "employee_id", "INTEGER")

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
                CommissionData(employee_name='Pam Henard', client='Ajax Building Company', status='Contract (Contract)', gp=336.96, hourly_gp=6.48, commission_rate='10.00%', commission=33.70, month='August', day=3, year=2025, unique_key='seed:1'),
                CommissionData(employee_name='Pam Henard', client='Evolent', status='Contract (Temporary)', gp=117.84, hourly_gp=2.95, commission_rate='10.00%', commission=11.78, month='August', day=3, year=2025, unique_key='seed:2'),
                CommissionData(employee_name='Sarah Johnson', client='TechCorp', status='Permanent', gp=450.00, hourly_gp=0.00, commission_rate='10.00%', commission=45.00, month='August', day=5, year=2025, unique_key='seed:3')
            ]
            for s in samples:
                db.session.add(s)

            try:
                db.session.commit()
            except Exception:
                db.session.rollback()

init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(debug=debug_mode, host='0.0.0.0', port=port)

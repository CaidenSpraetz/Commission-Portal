from flask import Flask, render_template, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
import os
import hashlib
import pandas as pd
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'commission-portal-secret-key')

# ================= Persisted SQLite on Azure Linux App Service =================
# Anything under /home persists across restarts; /home/site/wwwroot is safe
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
    client = db.Column(db.String(200))
    status = db.Column(db.String(50))
    gp = db.Column(db.Float)
    hourly_gp = db.Column(db.Float)
    commission_rate = db.Column(db.String(10))
    commission = db.Column(db.Float)
    month = db.Column(db.String(20))
    day = db.Column(db.Integer)

# ================= Routes =================
@app.route('/')
def index():
    # Ensure templates/index.html exists in your repo
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

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    employee = Employee.query.filter_by(
        username=username, password_hash=password_hash, status='Active'
    ).first()

    if employee:
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

    password_hash = hashlib.sha256(data['password'].encode()).hexdigest()
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

    commission_data = CommissionData.query.all()
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
        'day': cd.day
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
        # Save original to persisted storage
        filename = secure_filename(file.filename).lower()
        saved_path = os.path.join(UPLOAD_DIR, filename)
        file.stream.seek(0)
        file.save(saved_path)

        # Parse with pandas (explicit engines)
        if filename.endswith('.csv'):
            df = pd.read_csv(saved_path)
        elif filename.endswith('.xlsx'):
            df = pd.read_excel(saved_path, engine='openpyxl')
        elif filename.endswith('.xls'):
            # Only if xlrd==1.2.0 is installed
            import xlrd  # noqa: F401
            df = pd.read_excel(saved_path, engine='xlrd')
        else:
            return jsonify({'success': False, 'error': 'Unsupported file format'})

        df = df.fillna('')

        processed_count = 0
        for _, row in df.iterrows():
            client = None
            gp = None
            employee_name = None

            for col in df.columns:
                col_lower = str(col).lower()
                if 'client' in col_lower and not client:
                    client = row[col]
                elif any(term in col_lower for term in ['gp', 'gross profit', 'profit']) and gp is None:
                    gp = pd.to_numeric(row[col], errors='coerce')
                elif any(term in col_lower for term in ['employee', 'name', 'recruiter', 'sales']) and not employee_name:
                    employee_name = row[col]

            if client and employee_name and gp is not None and not pd.isna(gp):
                commission_rate = '5.00%'
                commission = float(gp) * 0.05
                hourly_gp = float(gp) / 40.0

                commission_data = CommissionData(
                    employee_name=str(employee_name),
                    client=str(client),
                    status='New',
                    gp=float(gp),
                    hourly_gp=hourly_gp,
                    commission_rate=commission_rate,
                    commission=commission,
                    month='Current',
                    day=1
                )
                db.session.add(commission_data)
                processed_count += 1

        db.session.commit()
        return jsonify({'success': True, 'processed': processed_count})

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Processing error: {str(e)}'})

# ================= Init DB & seed sample data =================
def init_db():
    with app.app_context():
        db.create_all()

        if not Employee.query.filter_by(username='admin').first():
            users = [
                {'name': 'Administrator', 'email': 'admin@company.com', 'username': 'admin', 'password': 'admin123', 'role': 'admin', 'job_function': 'admin'},
                {'name': 'Pam Henard', 'email': 'pam@company.com', 'username': 'phenard', 'password': 'temp123', 'role': 'employee', 'job_function': 'recruiter'},
                {'name': 'Sarah Johnson', 'email': 'sarah@company.com', 'username': 'sjohnson', 'password': 'secure789', 'role': 'manager', 'job_function': 'both'}
            ]
            for u in users:
                user = Employee(
                    name=u['name'], email=u['email'], username=u['username'],
                    password_hash=hashlib.sha256(u['password'].encode()).hexdigest(),
                    role=u['role'], job_function=u['job_function']
                )
                db.session.add(user)

            samples = [
                CommissionData(employee_name='Pam Henard', client='Ajax Building Company', status='New', gp=336.96, hourly_gp=6.48, commission_rate='10.00%', commission=33.70, month='August', day=3),
                CommissionData(employee_name='Pam Henard', client='Evolent', status='Enterprise', gp=117.84, hourly_gp=2.95, commission_rate='9.75%', commission=11.49, month='August', day=3),
                CommissionData(employee_name='Sarah Johnson', client='TechCorp', status='New', gp=450.00, hourly_gp=11.25, commission_rate='10.00%', commission=45.00, month='August', day=5)
            ]
            for s in samples:
                db.session.add(s)

            db.session.commit()

# Initialize database on startup
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(debug=debug_mode, host='0.0.0.0', port=port)

from flask import Flask, render_template, request, jsonify, session
from flask_sqlalchemy import SQLAlchemy
import os
import hashlib
import pandas as pd

app = Flask(__name__)
app.secret_key = 'commission-portal-secret-key'

# SQLite database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///commission_portal.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Database Models
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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    employee = Employee.query.filter_by(
        username=username,
        password_hash=password_hash,
        status='Active'
    ).first()
    
    if employee:
        session['user_id'] = employee.id
        session['username'] = employee.username
        session['role'] = employee.role
        session['name'] = employee.name
        
        return jsonify({
            'success': True,
            'user': {
                'name': employee.name,
                'role': employee.role,
                'username': employee.username
            }
        })
    
    return jsonify({'success': False, 'error': 'Invalid credentials'})

@app.route('/api/employees', methods=['GET', 'POST'])
def manage_employees():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if request.method == 'GET':
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
    
    elif request.method == 'POST':
        if session.get('role') not in ['admin', 'manager']:
            return jsonify({'error': 'Insufficient permissions'}), 403
        
        data = request.get_json()
        password_hash = hashlib.sha256(data['password'].encode()).hexdigest()
        
        employee = Employee(
            name=data['name'],
            email=data['email'],
            username=data['username'],
            password_hash=password_hash,
            role=data['role'],
            job_function=data['job_function']
        )
        
        try:
            db.session.add(employee)
            db.session.commit()
            return jsonify({'success': True})
        except:
            return jsonify({'success': False, 'error': 'Username already exists'})

@app.route('/api/commission-data', methods=['GET'])
def get_commission_data():
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    commission_data = CommissionData.query.all()
    
    if session.get('role') == 'employee':
        commission_data = [cd for cd in commission_data if cd.employee_name == session.get('name')]
    
    return jsonify([{
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

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        
        if not Employee.query.filter_by(username='admin').first():
            users = [
                {'name': 'Administrator', 'email': 'admin@company.com', 'username': 'admin', 'password': 'admin123', 'role': 'admin', 'job_function': 'admin'},
                {'name': 'Pam Henard', 'email': 'pam@company.com', 'username': 'phenard', 'password': 'temp123', 'role': 'employee', 'job_function': 'recruiter'},
                {'name': 'Sarah Johnson', 'email': 'sarah@company.com', 'username': 'sjohnson', 'password': 'secure789', 'role': 'manager', 'job_function': 'both'}
            ]
            
            for user_data in users:
                user = Employee(
                    name=user_data['name'],
                    email=user_data['email'],
                    username=user_data['username'],
                    password_hash=hashlib.sha256(user_data['password'].encode()).hexdigest(),
                    role=user_data['role'],
                    job_function=user_data['job_function']
                )
                db.session.add(user)
            
            sample_data = [
                CommissionData(employee_name='Pam Henard', client='Ajax Building Company', status='New', gp=336.96, hourly_gp=6.48, commission_rate='10.00%', commission=33.70, month='August', day=3),
                CommissionData(employee_name='Pam Henard', client='Evolent', status='Enterprise', gp=117.84, hourly_gp=2.95, commission_rate='9.75%', commission=11.49, month='August', day=3)
            ]
            for data in sample_data:
                db.session.add(data)
            
            db.session.commit()
    
    app.run(debug=True, host='0.0.0.0', port=8000)

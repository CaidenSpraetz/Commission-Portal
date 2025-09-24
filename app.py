from flask import Flask, render_template, request, jsonify, session
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
CommissionData(employee_name='Pam Henard', client='Evolent', status='Enterprise', gp=117.84, hourly_gp=2.95, commission_rate='9.75%', commission=11.49, month='August', day=3),
CommissionData(employee_name='Sarah Johnson', client='TechCorp', status='New', gp=450.00, hourly_gp=11.25, commission_rate='10.00%', commission=45.00, month='August', day=5)
]
for data in sample_data:
db.session.add(data)


db.session.commit()


# Initialize database on startup
init_db()


if __name__ == '__main__':
port = int(os.environ.get('PORT', 8000))
debug_mode = os.environ.get('FLASK_ENV') == 'development'
app.run(debug=debug_mode, host='0.0.0.0', port=port)

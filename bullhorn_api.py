# bullhorn_api.py - Complete Bullhorn API Integration with Contract Time Data

import requests
import secrets
import os
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BullhornAPI:
    def __init__(self):
        # Get credentials from Azure environment variables with fallbacks
        self.username = os.environ.get('BULLHORN_USERNAME', '25684.paramount.api')
        self.password = os.environ.get('BULLHORN_PASSWORD', 'k%%26_HK6qF7UP8jk')
        self.client_id = os.environ.get('BULLHORN_CLIENT_ID', 'b6cdadd9-21e2-4691-bff3-a4757c220ccd')
        self.client_secret = os.environ.get('BULLHORN_CLIENT_SECRET', 'hMS0c4xZyPV21G9FrEYhGFFD')
        
        if not all([self.username, self.password, self.client_id, self.client_secret]):
            raise ValueError("Missing Bullhorn API credentials in environment variables")
        
        # API connection variables
        self.rest_url = None
        self.bh_rest_token = None
        self.access_token = None
        self.refresh_token = None
        self.auth_value = None
        self.rest_value = None
        
    def get_data_center(self):
        """Get the correct Data Center for authentication"""
        url = 'https://rest.bullhornstaffing.com/rest-services/loginInfo'
        response = requests.get(url, params={'username': self.username})

        if response.status_code == 200:
            data_center_data = response.json()
            oauth_url = data_center_data.get('oauthUrl', '')
            start_index = oauth_url.find('auth-') + len('auth-')
            end_index = oauth_url.find('.bullhorn')
            
            self.auth_value = oauth_url[start_index:end_index]
            return data_center_data
        else:
            logger.error(f"Failed to retrieve data center. Status code: {response.status_code}")
            return None

    def get_auth_code(self):
        """Get the Authentication Code"""
        state = secrets.token_urlsafe(16)
        url = f'https://auth-{self.auth_value}.bullhornstaffing.com/oauth/authorize'
        params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'action': 'Login',
            'username': self.username,
            'password': self.password,
            'state': state
        }
        
        logger.info("Authenticating with Bullhorn...")
        response = requests.get(url, params=params)
        
        try:
            if response.status_code == 200:
                parsed_url = urlparse(response.url)
                query_parameters = parse_qs(parsed_url.query)
                auth_code = query_parameters.get('code')
                if auth_code:
                    return auth_code[0]
            logger.error(f"Failed to get auth code. Status code: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Exception getting auth code: {e}")
            return None

    def get_access_token(self, auth_code):
        """Get the Access Token"""
        url = f'https://auth-{self.auth_value}.bullhornstaffing.com/oauth/token'
        params = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        logger.info("Requesting access token...")
        response = requests.post(url, data=params)

        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get('access_token')
            self.refresh_token = data.get('refresh_token')
            return True
        else:
            logger.error(f"Failed to get access token. Status code: {response.status_code}")
            return False

    def login(self, data_center_data):
        """Establish connection (login) to Bullhorn REST services"""
        rest_url_full = data_center_data.get('restUrl', '')
        start_index = rest_url_full.find('rest-') + len('rest-')
        end_index = rest_url_full.find('.bullhorn')
        self.rest_value = rest_url_full[start_index:end_index]
        
        url = f'https://rest-{self.rest_value}.bullhornstaffing.com/rest-services/login'
        params = {
            'version': '*',
            'access_token': self.access_token
        }
        
        logger.info("Logging in to Bullhorn REST services...")
        response = requests.post(url, params=params)

        if response.status_code == 200:
            data = response.json()
            self.bh_rest_token = data.get('BhRestToken')
            self.rest_url = data.get('restUrl')
            return True
        else:
            logger.error(f"Failed to login to REST services. Status code: {response.status_code}")
            return False

    def authenticate(self):
        """Complete authentication process"""
        try:
            # Step 1: Get data center
            data_center_data = self.get_data_center()
            if not data_center_data or not self.auth_value:
                return False

            # Step 2: Get auth code
            auth_code = self.get_auth_code()
            if not auth_code:
                return False

            # Step 3: Get access token
            if not self.get_access_token(auth_code):
                return False

            # Step 4: Login to REST services
            if not self.login(data_center_data):
                return False

            logger.info("Bullhorn authentication successful!")
            return True

        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def fetch_entity(self, entity, fields, where_clause=None, count=500, start=0):
        """Fetch data from Bullhorn API for a specific entity"""
        if not self.rest_url or not self.bh_rest_token:
            logger.error("Not authenticated. Please call authenticate() first.")
            return None, 0

        params = {
            "BhRestToken": self.bh_rest_token,
            "fields": ",".join(fields),
            "count": min(count, 500),  # Bullhorn API limit
            "start": start,
        }
        
        if where_clause:
            params["where"] = where_clause

        url = f"{self.rest_url}/query/{entity}"
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                return data.get("data", []), data.get("total", 0)
            else:
                logger.error(f"Error fetching {entity}: {response.status_code} - {response.text}")
                return None, 0
                
        except Exception as e:
            logger.error(f"Exception fetching {entity}: {e}")
            return None, 0

    def fetch_all_pages(self, entity, fields, where_clause=None, max_records=None):
        """Fetch all pages of data for an entity"""
        all_records = []
        start = 0
        count = 500
        
        while True:
            records, total = self.fetch_entity(entity, fields, where_clause, count, start)
            if not records:
                break
            all_records.extend(records)
            if len(records) < count or (max_records and len(all_records) >= max_records):
                break
            start += count
            
        if max_records:
            all_records = all_records[:max_records]
            
        logger.info(f"Fetched {len(all_records)} {entity} records total")
        return all_records

    def get_permanent_placements(self, start_date=None):
        """Get permanent placements data for commission calculations"""
        if not start_date:
            start_date = datetime(datetime.now().year, 1, 1)
        
        start_timestamp = int(start_date.timestamp() * 1000)
        
        fields = [
            "id", "dateBegin", "employmentType", "customDate1", 
            "clientCorporation(id,name)", "candidate(firstName,lastName)", 
            "flatFee", "correlatedCustomText10", "correlatedCustomText1", 
            "customText34", "customText38"
        ]
        
        where_clause = f"employmentType='Permanent' AND dateBegin>={start_timestamp}"
        placements = self.fetch_all_pages("Placement", fields, where_clause)
        return self.format_placement_data(placements, 'Permanent')

    def get_contract_time_data(self, start_date=None, end_date=None):
        """Get Contract Placement Time Data - same structure as TOA but filtered for contract placements"""
        if not start_date:
            now = datetime.now()
            start_date = datetime(now.year, now.month, 1)
        
        if not end_date:
            if start_date.month == 12:
                end_date = datetime(start_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(start_date.year, start_date.month + 1, 1) - timedelta(days=1)
        
        # Convert to milliseconds
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        fields = [
            "id", 
            "placement(id,employmentType,clientCorporation(name),candidate(firstName,lastName))",
            "dateWorked", "hours", "billAmount", "payAmount",
            "placement.correlatedCustomText34", "placement.correlatedCustomText38",
            "placement.correlatedCustomText10", "placement.correlatedCustomText1"
        ]
        
        where_clause = f"dateWorked>={start_timestamp} AND dateWorked<={end_timestamp}"
        all_time_records = self.fetch_all_pages("PlacementTimeUnit", fields, where_clause)
        
        # Filter for only contract placement types
        contract_types = ['Contract', 'Temporary', 'Contract to Hire', 'Temp to Perm']
        contract_time_records = []
        
        for record in all_time_records:
            placement_info = record.get("placement", {})
            employment_type = placement_info.get("employmentType", "")
            if employment_type in contract_types:
                contract_time_records.append(record)
        
        logger.info(f"Filtered {len(contract_time_records)} contract time records from {len(all_time_records)} total time records")
        return self.format_contract_time_data(contract_time_records)

    def format_placement_data(self, placements, placement_type):
        """Format permanent placement data for commission portal"""
        formatted_placements = []
        
        for placement in placements:
            # Skip internal placements
            if placement.get("correlatedCustomText2", "").lower() == "internal":
                continue
            
            # Format dates
            date_begin = self.format_bullhorn_date(placement.get("dateBegin"))
            invoice_date = self.format_bullhorn_date(placement.get("customDate1"))
            year_num = date_begin.year if date_begin else datetime.now().year
            
            # Get employee names
            recruiter = placement.get("customText34", "")
            sales = placement.get("customText38", "")
            primary_employee = sales if sales else recruiter
            if not primary_employee:
                continue
            
            # Get client name
            client_name = ""
            if placement.get("clientCorporation"):
                client_name = placement["clientCorporation"].get("name", "")
            
            # Commission for permanent placements
            flat_fee = float(placement.get("flatFee", 0))
            commission_rate = 0.10  # 10% default for permanent
            commission = flat_fee * commission_rate
            
            formatted_placement = {
                'employee_name': primary_employee,
                'client': client_name,
                'status': 'Permanent',
                'gp': flat_fee,
                'hourly_gp': 0,
                'commission_rate': '10.00%',
                'commission': round(commission, 2),
                'month': date_begin.strftime('%B') if date_begin else 'Unknown',
                'day': date_begin.day if date_begin else 1,
                'year': year_num,
                'placement_id': placement.get("id"),
                'invoice_date': invoice_date.strftime('%Y-%m-%d') if invoice_date else None
            }
            formatted_placements.append(formatted_placement)
        
        return formatted_placements

    def format_contract_time_data(self, contract_time_records):
        """Format contract time data for commission portal"""
        formatted_contract_time = []
        
        # Group contract time records by placement/employee/client for monthly aggregation
        contract_groups = {}
        
        for record in contract_time_records:
            date_worked = self.format_bullhorn_date(record.get("dateWorked"))
            if not date_worked:
                continue
            
            placement_info = record.get("placement", {})
            recruiter = placement_info.get("correlatedCustomText34", "")
            sales = placement_info.get("correlatedCustomText38", "")
            primary_employee = sales if sales else recruiter
            if not primary_employee:
                continue
            
            client_name = placement_info.get("clientCorporation", {}).get("name", "")
            employment_type = placement_info.get("employmentType", "Contract")
            
            month_key = f"{date_worked.year}-{date_worked.month:02d}"
            group_key = f"{primary_employee}|{client_name}|{month_key}|{employment_type}"
            
            if group_key not in contract_groups:
                contract_groups[group_key] = {
                    'employee_name': primary_employee,
                    'client': client_name,
                    'employment_type': employment_type,
                    'month': date_worked.strftime('%B'),
                    'year': date_worked.year,
                    'total_hours': 0.0,
                    'total_bill_amount': 0.0,
                    'total_pay_amount': 0.0,
                    'records': []
                }
            
            group = contract_groups[group_key]
            group['total_hours'] += float(record.get("hours", 0) or 0)
            group['total_bill_amount'] += float(record.get("billAmount", 0) or 0)
            group['total_pay_amount'] += float(record.get("payAmount", 0) or 0)
            group['records'].append(record)
        
        for _, group in contract_groups.items():
            gp = group['total_bill_amount'] - group['total_pay_amount']
            hourly_gp = gp / group['total_hours'] if group['total_hours'] > 0 else 0
            commission_rate = 0.10
            commission = gp * commission_rate
            status = f"Contract ({group['employment_type']})"
            
            formatted_record = {
                'employee_name': group['employee_name'],
                'client': group['client'],
                'status': status,
                'gp': round(gp, 2),
                'hourly_gp': round(hourly_gp, 2),
                'commission_rate': '10.00%',
                'commission': round(commission, 2),
                'month': group['month'],
                'day': 1,
                'year': group['year'],
                'total_hours': round(group['total_hours'], 2),
                'total_bill_amount': round(group['total_bill_amount'], 2)
            }
            formatted_contract_time.append(formatted_record)
        
        return formatted_contract_time

    def format_bullhorn_date(self, timestamp):
        """Convert Bullhorn timestamp to datetime object"""
        if not timestamp:
            return None
        try:
            return datetime.fromtimestamp(timestamp / 1000)
        except (ValueError, TypeError):
            return None

# Helper function to get commission data from Bullhorn
def get_bullhorn_commission_data(include_contract_time=True, include_permanent=True, start_date=None):
    """
    Main function to get all commission data from Bullhorn
    
    Args:
        include_contract_time (bool): Include Contract Time data (replaces TOA)
        include_permanent (bool): Include permanent placement data
        start_date (datetime): Start date for data retrieval
        
    Returns:
        list: Combined commission data
    """
    try:
        api = BullhornAPI()
        if not api.authenticate():
            logger.error("Failed to authenticate with Bullhorn API")
            return []
        
        all_data = []
        if include_permanent:
            logger.info("Fetching permanent placements...")
            permanent_data = api.get_permanent_placements(start_date)
            all_data.extend(permanent_data)
            logger.info(f"Fetched {len(permanent_data)} permanent placements")
        
        if include_contract_time:
            logger.info("Fetching contract time data...")
            contract_time_data = api.get_contract_time_data(start_date)
            all_data.extend(contract_time_data)
            logger.info(f"Fetched {len(contract_time_data)} contract time records")
        
        logger.info(f"Total commission records: {len(all_data)}")
        return all_data
        
    except Exception as e:
        logger.error(f"Error getting Bullhorn commission data: {e}")
        return []

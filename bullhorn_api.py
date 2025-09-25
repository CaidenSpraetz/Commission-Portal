# bullhorn_api.py - ATS + Back Office (Timesheets) client helpers

import os
import base64
import logging
import secrets
import requests
from typing import List, Dict, Any
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================= ATS (Bullhorn REST) =========================
class BullhornAPI:
    def __init__(self):
        # Credentials must be provided via environment variables
        self.username = os.environ.get('BULLHORN_USERNAME')
        self.password = os.environ.get('BULLHORN_PASSWORD')
        self.client_id = os.environ.get('BULLHORN_CLIENT_ID')
        self.client_secret = os.environ.get('BULLHORN_CLIENT_SECRET')

        if not all([self.username, self.password, self.client_id, self.client_secret]):
            raise ValueError("Missing Bullhorn ATS REST credentials in environment variables")

        self.rest_url = None
        self.bh_rest_token = None
        self.access_token = None
        self.refresh_token = None
        self.auth_value = None
        self.rest_value = None

    def get_data_center(self):
        url = 'https://rest.bullhornstaffing.com/rest-services/loginInfo'
        response = requests.get(url, params={'username': self.username}, timeout=30)
        if response.status_code == 200:
            data_center_data = response.json()
            oauth_url = data_center_data.get('oauthUrl', '')
            start_index = oauth_url.find('auth-') + len('auth-')
            end_index = oauth_url.find('.bullhorn')
            self.auth_value = oauth_url[start_index:end_index]
            return data_center_data
        logger.error(f"Failed to retrieve data center. Status code: {response.status_code}")
        return None

    def get_auth_code(self):
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
        logger.info("Authenticating with Bullhorn (ATS)...")
        response = requests.get(url, params=params, allow_redirects=True, timeout=30)
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
        url = f'https://auth-{self.auth_value}.bullhornstaffing.com/oauth/token'
        params = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        logger.info("Requesting ATS access token...")
        response = requests.post(url, data=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get('access_token')
            self.refresh_token = data.get('refresh_token')
            return True
        logger.error(f"Failed to get access token. Status code: {response.status_code}")
        return False

    def login(self, data_center_data):
        rest_url_full = data_center_data.get('restUrl', '')
        start_index = rest_url_full.find('rest-') + len('rest-')
        end_index = rest_url_full.find('.bullhorn')
        self.rest_value = rest_url_full[start_index:end_index]
        url = f'https://rest-{self.rest_value}.bullhornstaffing.com/rest-services/login'
        params = {'version': '*', 'access_token': self.access_token}
        logger.info("Logging in to ATS REST services...")
        response = requests.post(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            self.bh_rest_token = data.get('BhRestToken')
            self.rest_url = data.get('restUrl')
            return True
        logger.error(f"Failed to login to REST services. Status code: {response.status_code}")
        return False

    def authenticate(self):
        try:
            dc = self.get_data_center()
            if not dc or not self.auth_value:
                return False
            auth_code = self.get_auth_code()
            if not auth_code:
                return False
            if not self.get_access_token(auth_code):
                return False
            if not self.login(dc):
                return False
            logger.info("ATS authentication successful")
            return True
        except Exception as e:
            logger.error(f"ATS authentication failed: {e}")
            return False

    def fetch_entity(self, entity, fields, where_clause=None, count=500, start=0):
        if not self.rest_url or not self.bh_rest_token:
            logger.error("Not authenticated. Call authenticate() first.")
            return None, 0
        params = {
            "BhRestToken": self.bh_rest_token,
            "fields": ",".join(fields),
            "count": min(count, 500),
            "start": start,
        }
        if where_clause:
            params["where"] = where_clause
        url = f"{self.rest_url}/query/{entity}"
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data", []), data.get("total", 0)
            logger.error(f"Error fetching {entity}: {resp.status_code} - {resp.text[:200]}")
            return None, 0
        except Exception as e:
            logger.error(f"Exception fetching {entity}: {e}")
            return None, 0

    def fetch_all_pages(self, entity, fields, where_clause=None, max_records=None):
        all_records = []
        start = 0
        count = 500
        while True:
            recs, total = self.fetch_entity(entity, fields, where_clause, count, start)
            if not recs:
                break
            all_records.extend(recs)
            if len(recs) < count or (max_records and len(all_records) >= max_records):
                break
            start += count
        if max_records:
            all_records = all_records[:max_records]
        logger.info(f"Fetched {len(all_records)} {entity} records total")
        return all_records

    def format_bullhorn_date(self, timestamp):
        if not timestamp:
            return None
        try:
            return datetime.fromtimestamp(timestamp / 1000)
        except (ValueError, TypeError):
            return None

    def format_placement_data(self, placements, placement_type):
        formatted = []
        for p in placements:
            if p.get("correlatedCustomText2", "").lower() == "internal":
                continue
            date_begin = self.format_bullhorn_date(p.get("dateBegin"))
            invoice_date = self.format_bullhorn_date(p.get("customDate1"))
            recruiter = p.get("customText34", "")
            sales = p.get("customText38", "")
            primary_employee = sales or recruiter
            if not primary_employee:
                continue
            client_name = p.get("clientCorporation", {}).get("name", "")
            flat_fee = float(p.get("flatFee", 0) or 0)
            commission_rate = 0.10
            commission = round(flat_fee * commission_rate, 2)
            formatted.append({
                'employee_name': primary_employee,
                'client': client_name,
                'status': 'Permanent',
                'gp': flat_fee,
                'hourly_gp': 0.0,
                'commission_rate': '10.00%',
                'commission': commission,
                'month': date_begin.strftime('%B') if date_begin else 'Unknown',
                'day': date_begin.day if date_begin else 1,
                'year': date_begin.year if date_begin else datetime.now().year,
                'placement_id': p.get("id"),
                'invoice_date': invoice_date.strftime('%Y-%m-%d') if invoice_date else None
            })
        return formatted

    def get_permanent_placements(self, start_date=None):
        if not start_date:
            start_date = datetime(datetime.now().year, 1, 1)
        start_ts = int(start_date.timestamp() * 1000)
        fields = [
            "id", "dateBegin", "employmentType", "customDate1",
            "clientCorporation(id,name)", "candidate(firstName,lastName)",
            "flatFee", "correlatedCustomText10", "correlatedCustomText1",
            "correlatedCustomText2", "customText34", "customText38"
        ]
        where_clause = f"employmentType='Permanent' AND dateBegin>={start_ts}"
        placements = self.fetch_all_pages("Placement", fields, where_clause)
        return self.format_placement_data(placements, 'Permanent')

# Public helper for ATS data (we only return Permanent here; contract time is via BBO)
def get_bullhorn_commission_data(include_contract_time=True, include_permanent=True, start_date=None):
    try:
        api = BullhornAPI()
        if not api.authenticate():
            logger.error("ATS auth failed")
            return []
        rows = []
        if include_permanent:
            logger.info("Fetching ATS permanent placements...")
            rows.extend(api.get_permanent_placements(start_date))
        # If you want ATS PlacementTimeUnit as fallback for contract time, add it here.
        return rows
    except Exception as e:
        logger.error(f"ATS commission data error: {e}")
        return []

# ========================= Back Office (Timesheets) =========================
class BackOfficeAPI:
    """
    Minimal Back Office / Timesheets client.
    Many tenants accept API Key + Basic (username:password).
    Adjust endpoints and auth per your BBO docs.
    """
    def __init__(self):
        self.username = os.environ.get("BBO_USERNAME")
        self.password = os.environ.get("BBO_PASSWORD")
        self.api_key  = os.environ.get("BBO_API_KEY")
        self.auth_domain = os.environ.get("BBO_AUTH_DOMAIN")  # reserved for future use
        self.rest_domain = os.environ.get("BBO_REST_DOMAIN")  # e.g., https://rest-east.bullhornstaffing.com/
        self.timesheets_base = os.environ.get("BBO_TIMESHEETS_BASE", (self.rest_domain or "")).rstrip("/")

        if not all([self.username, self.password, self.api_key, self.rest_domain]):
            raise ValueError("Missing Back Office (Timesheets) env vars")

        basic = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        self.base_headers = {
            "X-API-Key": self.api_key,
            "Authorization": f"Basic {basic}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self.timesheets_base}{path}"
        resp = requests.get(url, headers=self.base_headers, params=params, timeout=60)
        if resp.status_code >= 400:
            raise RuntimeError(f"BBO GET {path} failed: {resp.status_code} {resp.text[:500]}")
        return resp.json() if resp.text else {}

    def ping(self) -> bool:
        # Adjust to a harmless health/status endpoint from your BBO docs
        try:
            _ = self._get("/api/v1/health")
            return True
        except Exception as e:
            logger.warning(f"BBO ping failed: {e}")
            return False

    def list_time_entries(self, start_date: datetime, end_date: datetime, page: int = 1, page_size: int = 500) -> Dict[str, Any]:
        """
        Replace the path with the correct one per your BBO API doc.
        Expected to return {'data': [ ... ], 'nextPage': bool/int} or similar.
        """
        params = {
            "dateFrom": start_date.strftime("%Y-%m-%d"),
            "dateTo": end_date.strftime("%Y-%m-%d"),
            "page": page,
            "pageSize": min(page_size, 500),
        }
        return self._get("/api/v1/timesheets/entries", params=params)

    def get_commission_rows_from_timesheets(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        page = 1
        aggregate: Dict[str, Dict[str, Any]] = {}

        while True:
            payload = self.list_time_entries(start_date, end_date, page=page)
            entries = payload.get("data") or payload.get("entries") or []
            if not entries:
                break

            for e in entries:
                date_worked = e.get("dateWorked") or e.get("workDate")
                try:
                    dt = datetime.fromisoformat(str(date_worked)[:10]) if date_worked else None
                except Exception:
                    dt = None
                if not dt:
                    continue

                hours = float(e.get("hours") or e.get("quantity") or 0.0)
                bill_amt = float(e.get("billAmount") or e.get("bill") or 0.0)
                pay_amt  = float(e.get("payAmount")  or e.get("pay")  or 0.0)

                placement = e.get("placement") or {}
                employment_type = placement.get("employmentType") or "Contract"
                client_name = (placement.get("clientCorporation") or {}).get("name") or e.get("clientName") or ""
                primary_employee = (
                    placement.get("correlatedCustomText38")  # Sales
                    or placement.get("correlatedCustomText34")  # Recruiter
                    or e.get("ownerName") or ""
                )
                if not primary_employee:
                    continue

                month_label = dt.strftime("%B")
                year_val = dt.year
                key = f"{primary_employee}|{client_name}|{month_label}|{year_val}|{employment_type}"

                if key not in aggregate:
                    aggregate[key] = {
                        'employee_name': primary_employee,
                        'client': client_name,
                        'employment_type': employment_type,
                        'month': month_label,
                        'year': year_val,
                        'total_hours': 0.0,
                        'bill_sum': 0.0,
                        'pay_sum': 0.0
                    }
                g = aggregate[key]
                g["total_hours"] += hours
                g["bill_sum"] += bill_amt
                g["pay_sum"]  += pay_amt

            next_page = payload.get("nextPage") or payload.get("hasMore")
            if next_page in (True, "true") or (isinstance(next_page, int) and next_page > page):
                page = page + 1 if not isinstance(next_page, int) else next_page
            else:
                break

        for g in aggregate.values():
            gp = g["bill_sum"] - g["pay_sum"]
            hourly_gp = (gp / g["total_hours"]) if g["total_hours"] > 0 else 0.0
            commission_rate = 0.10  # policy default for contract time
            commission = round(gp * commission_rate, 2)
            results.append({
                "employee_name": g["employee_name"],
                "client": g["client"],
                "status": f"Contract ({g['employment_type']})",
                "gp": round(gp, 2),
                "hourly_gp": round(hourly_gp, 2),
                "commission_rate": "10.00%",
                "commission": commission,
                "month": g["month"],
                "day": 1,
                "year": g["year"],
                "placement_id": None  # available if your BBO entry returns a placement id
            })
        return results

def get_bbo_commission_data(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    api = BackOfficeAPI()
    if not api.ping():
        # Not fatal — continue; many tenants have no health endpoint but entries still work
        pass
    return api.get_commission_rows_from_timesheets(start_date, end_date)


# bullhorn_api.py - ATS + Back Office (Timesheets) client helpers (Py3.9-safe)

import os
import base64
import logging
import secrets
import requests
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========================= ATS (Bullhorn REST) =========================
class BullhornAPI:
    def __init__(self):
        self.username = os.environ.get('BULLHORN_USERNAME')
        self.password = os.environ.get('BULLHORN_PASSWORD')
        self.client_id = os.environ.get('BULLHORN_CLIENT_ID')
        self.client_secret = os.environ.get('BULLHORN_CLIENT_SECRET')

        if not all([self.username, self.password, self.client_id, self.client_secret]):
            raise ValueError("Missing Bullhorn ATS REST credentials in environment variables")

        self.rest_url: Optional[str] = None
        self.bh_rest_token: Optional[str] = None
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.auth_value: Optional[str] = None
        self.rest_value: Optional[str] = None

    def get_data_center(self) -> Optional[Dict[str, Any]]:
        url = 'https://rest.bullhornstaffing.com/rest-services/loginInfo'
        response = requests.get(url, params={'username': self.username}, timeout=30)
        if response.status_code == 200:
            dc = response.json()
            oauth_url = dc.get('oauthUrl', '')
            start_index = oauth_url.find('auth-') + len('auth-')
            end_index = oauth_url.find('.bullhorn')
            self.auth_value = oauth_url[start_index:end_index]
            return dc
        logger.error(f"Failed to retrieve data center. Status code: {response.status_code}")
        return None

    def get_auth_code(self) -> Optional[str]:
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
                q = parse_qs(parsed_url.query)
                code = q.get('code')
                if code:
                    return code[0]
            logger.error(f"Failed to get auth code. Status code: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Exception getting auth code: {e}")
            return None

    def get_access_token(self, auth_code: str) -> bool:
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

    def login(self, data_center_data: Dict[str, Any]) -> bool:
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

    def authenticate(self) -> bool:
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

    def fetch_entity(
        self,
        entity: str,
        fields: List[str],
        where_clause: Optional[str] = None,
        count: int = 500,
        start: int = 0
    ):
        if not self.rest_url or not self.bh_rest_token:
            logger.error("Not authenticated. Call authenticate() first.")
            return None, 0
        params: Dict[str, Any] = {
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

    def fetch_all_pages(
        self,
        entity: str,
        fields: List[str],
        where_clause: Optional[str] = None,
        max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        all_records: List[Dict[str, Any]] = []
        start = 0
        count = 500
        while True:
            recs, _ = self.fetch_entity(entity, fields, where_clause, count, start)
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

    @staticmethod
    def _dt_from_ms(ms: Optional[int]) -> Optional[datetime]:
        if not ms:
            return None
        try:
            return datetime.fromtimestamp(ms / 1000)
        except Exception:
            return None

    def format_placement_data(self, placements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        formatted: List[Dict[str, Any]] = []
        for p in placements:
            if str(p.get("correlatedCustomText2", "")).lower() == "internal":
                continue
            date_begin = self._dt_from_ms(p.get("dateBegin"))
            invoice_date = self._dt_from_ms(p.get("customDate1"))
            recruiter = p.get("customText34", "")              # recruiter name text
            sales = p.get("customText38", "")                  # sales name text
            primary_employee = sales or recruiter
            if not primary_employee:
                continue
            client_name = (p.get("clientCorporation") or {}).get("name", "")
            flat_fee = float(p.get("flatFee", 0) or 0)
            commission = round(flat_fee * 0.10, 2)
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

    def get_permanent_placements(self, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
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
        return self.format_placement_data(placements)

def get_bullhorn_commission_data(
    include_contract_time: bool = True,
    include_permanent: bool = True,
    start_date: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Return ATS data (we only return Permanent here; Contract/Temp comes from Back Office)."""
    try:
        api = BullhornAPI()
        if not api.authenticate():
            logger.error("ATS auth failed")
            return []
        rows: List[Dict[str, Any]] = []
        if include_permanent:
            logger.info("Fetching ATS permanent placements...")
            rows.extend(api.get_permanent_placements(start_date))
        return rows
    except Exception as e:
        logger.error(f"ATS commission data error: {e}")
        return []

# ========================= Back Office (Timesheets) =========================
class BackOfficeAPI:
    """
    Enhanced Back Office / Timesheets client with better error handling.

    ENV VARS required:
      BBO_USERNAME, BBO_PASSWORD, BBO_API_KEY, BBO_REST_DOMAIN
    Optional:
      BBO_AUTH_DOMAIN, BBO_TIMESHEETS_BASE (defaults to BBO_REST_DOMAIN)
    """
    def __init__(self):
        self.username = os.environ.get("BBO_USERNAME")
        self.password = os.environ.get("BBO_PASSWORD")
        self.api_key  = os.environ.get("BBO_API_KEY")
        self.auth_domain = os.environ.get("BBO_AUTH_DOMAIN")
        self.rest_domain = os.environ.get("BBO_REST_DOMAIN")
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

    def _get_with_fallback(self, endpoint_variants: List[str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Try multiple endpoint variants and return the first successful response."""
        last_error = None
        
        for endpoint in endpoint_variants:
            # Fix URL construction - ensure proper slash handling
            base = self.timesheets_base.rstrip('/')
            endpoint = endpoint.lstrip('/')
            url = f"{base}/{endpoint}"
            try:
                logger.info(f"Trying BBO endpoint: {url}")
                resp = requests.get(url, headers=self.base_headers, params=params, timeout=60)
                
                if resp.status_code == 200:
                    logger.info(f"BBO endpoint successful: {endpoint}")
                    return resp.json() if resp.text else {}
                elif resp.status_code == 503:
                    last_error = f"Service unavailable (503) at {endpoint}"
                    logger.warning(last_error)
                    continue
                elif resp.status_code == 404:
                    last_error = f"Endpoint not found (404): {endpoint}"
                    logger.warning(last_error)
                    continue
                elif resp.status_code in [401, 403]:
                    last_error = f"Authentication failed ({resp.status_code}) at {endpoint}"
                    logger.error(last_error)
                    continue
                else:
                    last_error = f"HTTP {resp.status_code} at {endpoint}: {resp.text[:200]}"
                    logger.warning(last_error)
                    continue
                    
            except requests.exceptions.Timeout:
                last_error = f"Timeout connecting to {endpoint}"
                logger.warning(last_error)
                continue
            except requests.exceptions.ConnectionError:
                last_error = f"Connection error to {endpoint}"
                logger.warning(last_error)
                continue
            except Exception as e:
                last_error = f"Error with {endpoint}: {str(e)}"
                logger.warning(last_error)
                continue
        
        # If all endpoints failed
        if "503" in str(last_error):
            raise RuntimeError("Back Office service temporarily unavailable (503). Please try again later.")
        elif "404" in str(last_error):
            raise RuntimeError("Back Office timesheets endpoint not found. Please verify your tenant configuration.")
        elif "401" in str(last_error) or "403" in str(last_error):
            raise RuntimeError("Back Office authentication failed. Please verify your credentials.")
        else:
            raise RuntimeError(f"All BBO endpoints failed. Last error: {last_error}")

    def ping(self) -> bool:
        """Test connectivity using various health/status endpoints."""
        health_endpoints = [
            "/health",
            "/api/v1/health", 
            "/api/health",
            "/status",
            "/api/v1/status",
            "/ping"
        ]
        
        try:
            self._get_with_fallback(health_endpoints)
            return True
        except Exception as e:
            logger.warning(f"BBO ping failed (not fatal): {e}")
            return False

    def list_time_entries(
        self, start_date: datetime, end_date: datetime, page: int = 1, page_size: int = 500
    ) -> Dict[str, Any]:
        """
        Fetch time entries with multiple endpoint fallbacks.
        """
        params = {
            "dateFrom": start_date.strftime("%Y-%m-%d"),
            "dateTo": end_date.strftime("%Y-%m-%d"),
            "page": page,
            "pageSize": min(page_size, 500),
        }
        
        # Try multiple common endpoint patterns
        endpoint_variants = [
            "/api/v1/timesheets/entries",
            "/api/v1.0/timesheets/entries",
            "/api/timesheets/entries", 
            "/v1/timesheets/entries",
            "/timesheets/entries",
            "/api/v1/timesheet/entries",
            "/api/timeentries",
            "/api/v1/timeentries"
        ]
        
        return self._get_with_fallback(endpoint_variants, params=params)

    def get_commission_rows_from_timesheets(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch and aggregate timesheet data with enhanced error handling.
        """
        results: List[Dict[str, Any]] = []
        page = 1
        aggregate: Dict[str, Dict[str, Any]] = {}

        try:
            while True:
                try:
                    payload = self.list_time_entries(start_date, end_date, page=page)
                except RuntimeError as e:
                    if "503" in str(e):
                        # Service unavailable - return empty but don't crash
                        logger.warning("BBO service unavailable, returning empty results")
                        return []
                    else:
                        # Re-raise other errors
                        raise e
                
                entries = payload.get("data") or payload.get("entries") or []
                if not entries:
                    break

                for e in entries:
                    try:
                        # Date normalization with better error handling
                        date_worked = e.get("dateWorked") or e.get("workDate")
                        dt = None
                        if date_worked:
                            try:
                                if isinstance(date_worked, str):
                                    dt = datetime.fromisoformat(date_worked[:10])
                                elif hasattr(date_worked, 'year'):  # datetime object
                                    dt = date_worked
                            except Exception:
                                logger.warning(f"Could not parse date: {date_worked}")
                                continue
                        
                        if not dt:
                            continue

                        hours = float(e.get("hours") or e.get("quantity") or 0.0)
                        bill_amt = float(e.get("billAmount") or e.get("bill") or 0.0)
                        pay_amt = float(e.get("payAmount") or e.get("pay") or 0.0)

                        placement = e.get("placement") or {}
                        employment_type = placement.get("employmentType") or "Contract"
                        client_name = (placement.get("clientCorporation") or {}).get("name") or e.get("clientName") or ""
                        primary_employee = (
                            placement.get("correlatedCustomText38")
                            or placement.get("correlatedCustomText34")
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
                        g["pay_sum"] += pay_amt
                        
                    except Exception as entry_error:
                        logger.warning(f"Error processing timesheet entry: {entry_error}")
                        continue

                # Check for next page
                next_page = payload.get("nextPage") or payload.get("hasMore")
                if next_page in (True, "true") or (isinstance(next_page, int) and next_page > page):
                    page = page + 1 if not isinstance(next_page, int) else next_page
                else:
                    break

        except Exception as e:
            logger.error(f"Error fetching BBO timesheet data: {e}")
            # Return partial results if we got some data
            if aggregate:
                logger.info(f"Returning partial BBO results: {len(aggregate)} aggregated records")
            else:
                return []

        # Convert aggregated data to commission records
        for g in aggregate.values():
            try:
                gp = g["bill_sum"] - g["pay_sum"]
                hourly_gp = (gp / g["total_hours"]) if g["total_hours"] > 0 else 0.0
                commission = round(gp * 0.10, 2)
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
                    "placement_id": None
                })
            except Exception as calc_error:
                logger.warning(f"Error calculating commission for {g.get('employee_name')}: {calc_error}")
                continue

        logger.info(f"Successfully processed {len(results)} BBO commission records")
        return results

def get_bbo_commission_data(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """Get Back Office commission data with enhanced error handling."""
    try:
        api = BackOfficeAPI()
        # Don't fail if ping fails - the main endpoint might still work
        try:
            api.ping()
        except Exception:
            logger.info("BBO ping failed but continuing with main request")
        
        return api.get_commission_rows_from_timesheets(start_date, end_date)
    except Exception as e:
        logger.error(f"BBO commission data error: {e}")
        return []

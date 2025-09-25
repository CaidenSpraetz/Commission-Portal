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
            if not self.get_access_token(auth

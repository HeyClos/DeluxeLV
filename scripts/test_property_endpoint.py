#!/usr/bin/env python3
"""
Simple test script to verify Trestle WebAPI Property endpoint access.
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    client_id = os.getenv('TRESTLE_CLIENT_ID')
    client_secret = os.getenv('TRESTLE_CLIENT_SECRET')
    token_url = os.getenv('TRESTLE_TOKEN_URL', 'https://api.cotality.com/trestle/oidc/connect/token')
    api_base = os.getenv('TRESTLE_API_BASE_URL', 'https://api.cotality.com/trestle')
    
    print("=" * 60)
    print("Trestle WebAPI Property Endpoint Test")
    print("=" * 60)
    print(f"\nClient ID: {client_id}")
    print(f"Token URL: {token_url}")
    print(f"API Base: {api_base}")
    
    # Step 1: Authenticate
    print("\n[1] Authenticating...")
    auth_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': 'api'
    }
    
    try:
        auth_response = requests.post(
            token_url,
            data=auth_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )
        
        print(f"    Status: {auth_response.status_code}")
        
        if auth_response.status_code != 200:
            print(f"    ERROR: Authentication failed")
            print(f"    Response: {auth_response.text}")
            return
        
        token_data = auth_response.json()
        access_token = token_data['access_token']
        print(f"    ✓ Got access token (expires in {token_data.get('expires_in', '?')}s)")
        
    except Exception as e:
        print(f"    ERROR: {e}")
        return
    
    # Step 2: Test Property endpoint
    print("\n[2] Testing Property endpoint...")
    property_url = f"{api_base}/odata/Property?$top=1&$select=ListingKey,ListingId,StandardStatus,City,StateOrProvince"
    
    try:
        property_response = requests.get(
            property_url,
            headers={
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            },
            timeout=30
        )
        
        print(f"    URL: {property_url}")
        print(f"    Status: {property_response.status_code}")
        
        if property_response.status_code == 200:
            data = property_response.json()
            records = data.get('value', [])
            print(f"    ✓ SUCCESS! Got {len(records)} record(s)")
            
            if records:
                print("\n    Sample Property Data:")
                for key, value in records[0].items():
                    if not key.startswith('@'):
                        print(f"      {key}: {value}")
        else:
            print(f"    ERROR: {property_response.text[:500]}")
            
    except Exception as e:
        print(f"    ERROR: {e}")
    
    print("\n" + "=" * 60)

if __name__ == '__main__':
    main()

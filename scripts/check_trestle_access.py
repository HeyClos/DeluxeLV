#!/usr/bin/env python3
"""
Script to check available Trestle API endpoints and capabilities.
"""

import os
import sys
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_access_token():
    """Authenticate and get access token."""
    client_id = os.getenv('TRESTLE_CLIENT_ID')
    client_secret = os.getenv('TRESTLE_CLIENT_SECRET')
    token_url = os.getenv('TRESTLE_TOKEN_URL', 'https://api-prod.corelogic.com/trestle/oidc/connect/token')
    
    if not client_id or not client_secret:
        print("ERROR: TRESTLE_CLIENT_ID and TRESTLE_CLIENT_SECRET must be set in .env")
        sys.exit(1)
    
    print(f"Authenticating with Trestle API...")
    print(f"Client ID: {client_id[:8]}...{client_id[-4:] if len(client_id) > 12 else ''}")
    
    # Try multiple token URLs and scope combinations
    token_urls = [
        token_url,
        'https://api-prod.corelogic.com/trestle/oidc/connect/token',
        'https://api.cotality.com/trestle/oidc/connect/token',
        'https://api-uat.corelogic.com/trestle/oidc/connect/token',
    ]
    
    scope_options = ['api', 'ODataApi', '']
    
    for url in token_urls:
        for scope in scope_options:
            print(f"\nTrying: {url}")
            print(f"  Scope: '{scope}' (empty = no scope)")
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
            }
            if scope:
                data['scope'] = scope
            
            try:
                response = requests.post(
                    url,
                    data=data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    timeout=15
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    print(f"  ✓ SUCCESS!")
                    print(f"  Token expires in: {token_data.get('expires_in', 'unknown')} seconds")
                    return token_data['access_token'], url
                else:
                    error_info = response.text[:100] if response.text else 'No response body'
                    print(f"  ✗ Failed ({response.status_code}): {error_info}")
            except requests.RequestException as e:
                print(f"  ✗ Network error: {str(e)[:50]}")
    
    print("\n" + "=" * 60)
    print("ERROR: All authentication attempts failed.")
    print("Please verify your credentials are correct.")
    print("=" * 60)
    sys.exit(1)

def get_metadata(token):
    """Fetch OData metadata to see available entity sets."""
    base_url = os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
    metadata_url = f"{base_url}/$metadata"
    
    print(f"\nFetching metadata from: {metadata_url}")
    
    response = requests.get(
        metadata_url,
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/xml'
        }
    )
    
    if response.status_code != 200:
        print(f"ERROR: Metadata request failed with status {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return None
    
    return response.text

def get_service_document(token, api_base_url=None):
    """Fetch OData service document to see available entity sets."""
    base_url = api_base_url or os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
    
    print(f"\nFetching service document from: {base_url}")
    
    response = requests.get(
        base_url,
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
    )
    
    if response.status_code != 200:
        print(f"ERROR: Service document request failed with status {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return None
    
    return response.json()

def parse_entity_sets(service_doc):
    """Extract entity set names from service document."""
    if not service_doc:
        return []
    
    entity_sets = []
    if 'value' in service_doc:
        for item in service_doc['value']:
            entity_sets.append({
                'name': item.get('name'),
                'url': item.get('url'),
                'kind': item.get('kind', 'EntitySet')
            })
    return entity_sets

def test_entity_access(token, entity_name, api_base_url=None):
    """Test if we can access a specific entity set."""
    base_url = api_base_url or os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
    url = f"{base_url}/{entity_name}?$top=1"
    
    response = requests.get(
        url,
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
    )
    
    return response.status_code == 200, response.status_code

def main():
    print("=" * 60)
    print("Trestle API Access Check")
    print("=" * 60)
    
    # Authenticate
    token, working_url = get_access_token()
    print(f"\nWorking token URL: {working_url}")
    
    # Determine API base URL from token URL
    if 'api-uat' in working_url:
        api_base_url = 'https://api-uat.corelogic.com/trestle/odata'
        print(f"Using UAT API endpoint: {api_base_url}")
    elif 'api-prod' in working_url or 'cotality' in working_url:
        api_base_url = 'https://api-prod.corelogic.com/trestle/odata'
        print(f"Using Production API endpoint: {api_base_url}")
    else:
        api_base_url = os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
        print(f"Using configured API endpoint: {api_base_url}")
    
    # Get service document
    service_doc = get_service_document(token, api_base_url)
    
    if service_doc:
        entity_sets = parse_entity_sets(service_doc)
        
        print(f"\n{'=' * 60}")
        print(f"Available Entity Sets ({len(entity_sets)} found)")
        print("=" * 60)
        
        for entity in entity_sets:
            accessible, status = test_entity_access(token, entity['name'], api_base_url)
            status_icon = "✓" if accessible else "✗"
            print(f"  {status_icon} {entity['name']} (HTTP {status})")
        
        print(f"\n{'=' * 60}")
        print("Summary")
        print("=" * 60)
        accessible_count = sum(1 for e in entity_sets if test_entity_access(token, e['name'], api_base_url)[0])
        print(f"Total entity sets: {len(entity_sets)}")
        print(f"Accessible: {accessible_count}")
        
        # Print recommended .env settings
        print(f"\n{'=' * 60}")
        print("Recommended .env settings:")
        print("=" * 60)
        print(f"TRESTLE_TOKEN_URL={working_url}")
        print(f"TRESTLE_API_BASE_URL={api_base_url}")
    else:
        print("\nCould not retrieve service document. Trying common entity sets...")
        common_entities = ['Property', 'Member', 'Office', 'Media', 'OpenHouse', 'PropertyGreenVerification']
        
        print(f"\n{'=' * 60}")
        print("Testing Common Entity Sets")
        print("=" * 60)
        
        for entity in common_entities:
            accessible, status = test_entity_access(token, entity, api_base_url)
            status_icon = "✓" if accessible else "✗"
            print(f"  {status_icon} {entity} (HTTP {status})")

if __name__ == '__main__':
    main()

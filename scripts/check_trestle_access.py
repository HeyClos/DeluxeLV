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
    print(f"Token URL: {token_url}")
    
    response = requests.post(
        token_url,
        data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'api'
        },
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    
    if response.status_code != 200:
        print(f"ERROR: Authentication failed with status {response.status_code}")
        print(f"Response: {response.text}")
        sys.exit(1)
    
    token_data = response.json()
    print("✓ Authentication successful!")
    return token_data['access_token']

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

def get_service_document(token):
    """Fetch OData service document to see available entity sets."""
    base_url = os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
    
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

def test_entity_access(token, entity_name):
    """Test if we can access a specific entity set."""
    base_url = os.getenv('TRESTLE_API_URL', 'https://api-prod.corelogic.com/trestle/odata')
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
    token = get_access_token()
    
    # Get service document
    service_doc = get_service_document(token)
    
    if service_doc:
        entity_sets = parse_entity_sets(service_doc)
        
        print(f"\n{'=' * 60}")
        print(f"Available Entity Sets ({len(entity_sets)} found)")
        print("=" * 60)
        
        for entity in entity_sets:
            accessible, status = test_entity_access(token, entity['name'])
            status_icon = "✓" if accessible else "✗"
            print(f"  {status_icon} {entity['name']} (HTTP {status})")
        
        print(f"\n{'=' * 60}")
        print("Summary")
        print("=" * 60)
        accessible_count = sum(1 for e in entity_sets if test_entity_access(token, e['name'])[0])
        print(f"Total entity sets: {len(entity_sets)}")
        print(f"Accessible: {accessible_count}")
    else:
        print("\nCould not retrieve service document. Trying common entity sets...")
        common_entities = ['Property', 'Member', 'Office', 'Media', 'OpenHouse', 'PropertyGreenVerification']
        
        print(f"\n{'=' * 60}")
        print("Testing Common Entity Sets")
        print("=" * 60)
        
        for entity in common_entities:
            accessible, status = test_entity_access(token, entity)
            status_icon = "✓" if accessible else "✗"
            print(f"  {status_icon} {entity} (HTTP {status})")

if __name__ == '__main__':
    main()

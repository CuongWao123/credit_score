import requests
import json
from typing import Dict, Any

# Base URL của API
BASE_URL = "http://localhost:6666"

def print_response(endpoint: str, response: requests.Response):
    """Print formatted API response"""
    print("\n" + "="*80)
    print(f"Endpoint: {endpoint}")
    print(f"Status Code: {response.status_code}")
    print("-"*80)
    
    try:
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except:
        print(response.text)
    
    print("="*80)

def test_get_bureau_by_id(reco_id_curr: int = 265640):
    """Test get bureau by current loan ID"""
    print(f"\n🧪 Testing Get Bureau by ID: {reco_id_curr}...")
    response = requests.get(f"{BASE_URL}/bureau/{reco_id_curr}")
    print_response(f"GET /bureau/{reco_id_curr}", response)
    return response.status_code == 200


def run_all_tests():
    """Run all test cases"""
    print("\n" + "🚀 "*20)
    print("Starting Bureau API Tests")
    print("🚀 "*20)
    
    tests = [
        ("Get Bureau by ID", test_get_bureau_by_id),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, "✅ PASSED" if success else "❌ FAILED"))
        except Exception as e:
            results.append((test_name, f"❌ ERROR: {str(e)}"))
    
    # Print summary
    print("\n" + "📊 "*20)
    print("Test Summary")
    print("📊 "*20)
    
    for test_name, result in results:
        print(f"{test_name:.<50} {result}")
    
    passed = sum(1 for _, r in results if "PASSED" in r)
    total = len(results)
    print("\n" + "="*80)
    print(f"Total: {passed}/{total} tests passed")
    print("="*80)


if __name__ == "__main__":
    import sys
    run_all_tests()
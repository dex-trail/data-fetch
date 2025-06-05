#!/usr/bin/env python3
"""
Script to generate event signatures for Ethereum events.
Event signatures are calculated as keccak256 of the canonical event string.
"""

import re
from Crypto.Hash import keccak

def extract_events_from_file(filename):
    """Extract event definitions from the signatures file."""
    events = []
    
    with open(filename, 'r') as f:
        content = f.read()
    
    # Find all event blocks using regex
    event_pattern = r'event\s+(\w+)\s*\((.*?)\)'
    matches = re.findall(event_pattern, content, re.DOTALL)
    
    for event_name, params_str in matches:
        # Parse parameters
        param_lines = [line.strip() for line in params_str.split('\n') if line.strip()]
        param_types = []
        
        for line in param_lines:
            # Remove trailing comma and extract type
            line = line.rstrip(',').strip()
            if line:
                # Extract type (first word before space)
                parts = line.split()
                if parts:
                    param_types.append(parts[0])
        
        events.append({
            'name': event_name,
            'types': param_types
        })
    
    return events

def generate_event_signature(event_name, param_types):
    """Generate the canonical event signature string."""
    params_str = ','.join(param_types)
    return f"{event_name}({params_str})"

def calculate_keccak256(text):
    """Calculate keccak256 hash of the given text."""
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(text.encode('utf-8'))
    return keccak_hash.hexdigest()

def main():
    try:
        # Extract events from the signatures file
        events = extract_events_from_file('signatures')
        
        print("🔐 Event Signature Generator")
        print("="*50)
        
        for event in events:
            # Generate canonical signature
            signature = generate_event_signature(event['name'], event['types'])
            
            # Calculate keccak256 hash
            signature_hash = calculate_keccak256(signature)
            
            print(f"\n📝 Event: {event['name']}")
            print(f"   Signature: {signature}")
            print(f"   Hash: 0x{signature_hash}")
            print(f"   Topic0: 0x{signature_hash[:8]}...")
        
        print(f"\n✅ Generated signatures for {len(events)} events")
        
    except FileNotFoundError:
        print("❌ Error: 'signatures' file not found")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 
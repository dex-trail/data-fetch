#!/usr/bin/env python3
"""
Script to analyze token balances for clustered addresses and identify pool balance
"""

import json
import sys
from decimal import Decimal, getcontext
from typing import Dict, List, Optional
import os

# Import the EtherscanSourceFetcher from fetch_token_source.py
from fetch_token_source import EtherscanSourceFetcher

# Set precision for decimal calculations
getcontext().prec = 50

def load_json_file(filepath: str) -> Optional[Dict]:
    """Load and parse JSON file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in '{filepath}': {e}")
        return None
    except Exception as e:
        print(f"Error loading '{filepath}': {e}")
        return None

def get_total_supply_from_timeline(timeline_data: List[Dict]) -> Optional[int]:
    """Extract total supply from the first record in aggregated timeline (transfer from zero address)"""
    try:
        if not timeline_data or len(timeline_data) == 0:
            return None
        
        first_record = timeline_data[0]
        
        # Check if it's a transfer from zero address
        from_address = first_record.get('from_address', '').lower()
        if from_address == '0x0000000000000000000000000000000000000000':
            # Get the raw value from raw_data if available, otherwise use value
            raw_data = first_record.get('raw_data', {})
            total_supply = raw_data.get('value', first_record.get('value', 0))
            return int(total_supply) if total_supply else None
        
        return None
    except Exception as e:
        print(f"Error extracting total supply: {e}")
        return None

def get_cluster_addresses(clusters_data: Dict) -> List[str]:
    """Extract addresses from the cluster data"""
    try:
        addresses = clusters_data.get('result', {}).get('addresses', [])
        return [addr.lower() for addr in addresses]  # Normalize to lowercase
    except Exception as e:
        print(f"Error extracting cluster addresses: {e}")
        return []

def find_pool_address(balances_data: Dict) -> Optional[str]:
    """
    Find the pool address (typically the address with the highest token balance)
    """
    try:
        balances = balances_data.get('balances', [])
        if not balances:
            return None
        
        # The balances appear to be sorted by token_balance_raw (highest first)
        # The first entry should be the pool
        highest_balance_entry = balances[0]
        return highest_balance_entry.get('address', '').lower()
    except Exception as e:
        print(f"Error finding pool address: {e}")
        return None

def calculate_cluster_balance(cluster_addresses: List[str], balances_data: Dict) -> Dict:
    """Calculate total token balance for cluster addresses"""
    try:
        balances = balances_data.get('balances', [])
        cluster_balances = []
        total_raw_balance = 0
        
        # Create a mapping of address to balance data for faster lookup
        balance_map = {entry['address'].lower(): entry for entry in balances}
        
        for address in cluster_addresses:
            if address in balance_map:
                entry = balance_map[address]
                raw_balance = entry.get('token_balance_raw', 0)
                total_raw_balance += raw_balance
                
                cluster_balances.append({
                    'address': address,
                    'token_balance_raw': raw_balance,
                    'token_balance_formatted': entry.get('token_balance_formatted', '0'),
                    'eth_balance_eth': entry.get('eth_balance_eth', 0)
                })
            else:
                print(f"Warning: Address {address} not found in balance data")
        
        return {
            'total_addresses_in_cluster': len(cluster_addresses),
            'addresses_found_in_balances': len(cluster_balances),
            'total_token_balance_raw': total_raw_balance,
            'total_token_balance_formatted': f"{total_raw_balance:,}",
            'individual_balances': cluster_balances
        }
    except Exception as e:
        print(f"Error calculating cluster balance: {e}")
        return {}

def get_pool_balance(pool_address: str, balances_data: Dict) -> Dict:
    """Get balance information for the pool address"""
    try:
        balances = balances_data.get('balances', [])
        balance_map = {entry['address'].lower(): entry for entry in balances}
        
        if pool_address in balance_map:
            entry = balance_map[pool_address]
            return {
                'pool_address': pool_address,
                'token_balance_raw': entry.get('token_balance_raw', 0),
                'token_balance_formatted': entry.get('token_balance_formatted', '0'),
                'eth_balance_eth': entry.get('eth_balance_eth', 0),
                'eth_balance_wei': entry.get('eth_balance_wei', 0)
            }
        else:
            return {'error': f'Pool address {pool_address} not found in balance data'}
    except Exception as e:
        print(f"Error getting pool balance: {e}")
        return {'error': str(e)}

def analyze_token_data() -> Dict:
    """Main analysis function that returns JSON results"""
    # File paths
    clusters_file = 'output/address_clusters.json'
    balances_file = 'output/token_analysis_alchemy_balances.json'
    timeline_file = 'output/aggregated_timeline.json'
    
    results = {
        'timestamp': None,
        'files_analyzed': {
            'clusters': clusters_file,
            'balances': balances_file,
            'timeline': timeline_file
        },
        'cluster_analysis': {},
        'pool_analysis': {},
        'total_supply_analysis': {},
        'contract_analysis': {},
        'rugpull_analysis': {},
        'comparative_analysis': {},
        'errors': []
    }
    
    # Load data files
    clusters_data = load_json_file(clusters_file)
    if not clusters_data:
        results['errors'].append(f"Failed to load {clusters_file}")
        return results
    
    balances_data = load_json_file(balances_file)
    if not balances_data:
        results['errors'].append(f"Failed to load {balances_file}")
        return results
    
    timeline_data = load_json_file(timeline_file)
    if not timeline_data:
        results['errors'].append(f"Failed to load {timeline_file}")
        return results
    
    # Extract basic metadata
    results['timestamp'] = balances_data.get('metadata', {}).get('timestamp')
    token_address = balances_data.get('metadata', {}).get('token_address')
    total_addresses = balances_data.get('metadata', {}).get('total_addresses', 0)
    
    # Extract cluster addresses
    cluster_addresses = get_cluster_addresses(clusters_data)
    
    # Get total supply from timeline
    total_supply = get_total_supply_from_timeline(timeline_data)
    results['total_supply_analysis'] = {
        'total_supply_raw': total_supply,
        'total_supply_formatted': f"{total_supply:,}" if total_supply else "N/A",
        'source': 'First transfer from zero address in aggregated timeline'
    }
    
    # Calculate cluster balances
    cluster_analysis = calculate_cluster_balance(cluster_addresses, balances_data)
    results['cluster_analysis'] = {
        'cluster_addresses': cluster_addresses,
        'cluster_id': clusters_data.get('result', {}).get('cluster_id'),
        'confidence_level': clusters_data.get('result', {}).get('confidence_level'),
        'reasoning': clusters_data.get('result', {}).get('reasoning'),
        **cluster_analysis
    }
    
    # Find and analyze pool
    pool_address = find_pool_address(balances_data)
    if pool_address:
        pool_analysis = get_pool_balance(pool_address, balances_data)
        results['pool_analysis'] = pool_analysis
    else:
        results['errors'].append("Could not identify pool address")
    
    # Fetch contract source code and perform rugpull analysis
    if token_address:
        try:
            print(f"Fetching contract source code for token: {token_address}")
            
            # Initialize the Etherscan source fetcher
            fetcher = EtherscanSourceFetcher()
            
            # Fetch source code
            source_response = fetcher.fetch_source_code(token_address)
            
            if source_response and 'result' in source_response and len(source_response['result']) > 0:
                contract_info = source_response['result'][0]
                contract_name = contract_info.get('ContractName', 'Unknown')
                source_code = contract_info.get('SourceCode', '')
                
                # Store contract analysis results
                results['contract_analysis'] = {
                    'contract_name': contract_name,
                    'compiler_version': contract_info.get('CompilerVersion', 'N/A'),
                    'optimization_used': contract_info.get('OptimizationUsed') == '1',
                    'runs': contract_info.get('Runs', 'N/A'),
                    'evm_version': contract_info.get('EVMVersion', 'N/A'),
                    'license_type': contract_info.get('LicenseType', 'N/A'),
                    'is_proxy': contract_info.get('Proxy') == '1',
                    'implementation': contract_info.get('Implementation', 'N/A'),
                    'source_code_available': bool(source_code and source_code.strip())
                }
                
                # Perform rugpull analysis if source code is available
                if source_code and source_code.strip():
                    print("Performing rugpull risk analysis...")
                    
                    # Handle multi-file contracts (JSON format)
                    formatted_source_code = source_code
                    if source_code.startswith('{'):
                        try:
                            # Remove extra braces if present
                            if source_code.startswith('{{') and source_code.endswith('}}'):
                                source_code = source_code[1:-1]
                            
                            source_json = json.loads(source_code)
                            
                            if 'sources' in source_json:
                                # Multi-file contract - combine all source files
                                formatted_parts = []
                                for file_path, file_data in source_json['sources'].items():
                                    file_content = file_data.get('content', '')
                                    formatted_parts.append(file_content)
                                formatted_source_code = "\n".join(formatted_parts)
                        except json.JSONDecodeError:
                            # If JSON parsing fails, use as plain text
                            pass
                    
                    # Perform rugpull analysis
                    rugpull_analysis_raw = fetcher.analyze_rugpull_risk(formatted_source_code, contract_name)
                    
                    # Try to parse the rugpull analysis as JSON
                    try:
                        # Strip markdown formatting if present
                        cleaned_analysis = rugpull_analysis_raw.strip()
                        if cleaned_analysis.startswith('```json'):
                            # Remove ```json from the start and ``` from the end
                            cleaned_analysis = cleaned_analysis[7:]  # Remove ```json
                            if cleaned_analysis.endswith('```'):
                                cleaned_analysis = cleaned_analysis[:-3]  # Remove closing ```
                            cleaned_analysis = cleaned_analysis.strip()
                        elif cleaned_analysis.startswith('```'):
                            # Remove generic ``` formatting
                            cleaned_analysis = cleaned_analysis[3:]
                            if cleaned_analysis.endswith('```'):
                                cleaned_analysis = cleaned_analysis[:-3]
                            cleaned_analysis = cleaned_analysis.strip()
                        
                        rugpull_analysis = json.loads(cleaned_analysis)
                        results['rugpull_analysis'] = rugpull_analysis
                    except json.JSONDecodeError as e:
                        # If it's still not valid JSON, store as raw text with error details
                        results['rugpull_analysis'] = {
                            'error': f'Failed to parse rugpull analysis as JSON: {str(e)}',
                            'raw_analysis': rugpull_analysis_raw
                        }
                else:
                    results['rugpull_analysis'] = {
                        'error': 'No source code available for rugpull analysis'
                    }
            else:
                results['contract_analysis'] = {
                    'error': 'Failed to fetch contract source code'
                }
                results['rugpull_analysis'] = {
                    'error': 'Contract source code not available'
                }
        except Exception as e:
            error_msg = f"Error during contract analysis: {str(e)}"
            print(f"Warning: {error_msg}")
            results['errors'].append(error_msg)
            results['contract_analysis'] = {'error': error_msg}
            results['rugpull_analysis'] = {'error': error_msg}
    else:
        results['errors'].append("No token address available for contract analysis")
    
    # Comparative analysis
    if cluster_analysis and pool_address and total_supply:
        pool_analysis = get_pool_balance(pool_address, balances_data)
        
        if 'error' not in pool_analysis:
            cluster_balance = cluster_analysis['total_token_balance_raw']
            pool_balance = pool_analysis['token_balance_raw']
            
            results['comparative_analysis'] = {
                'token_address': token_address,
                'total_addresses_analyzed': total_addresses,
                'cluster_vs_pool_ratio_percent': (cluster_balance / pool_balance * 100) if pool_balance > 0 else 0,
                'pool_vs_cluster_multiplier': (pool_balance / cluster_balance) if cluster_balance > 0 else 0,
                'cluster_vs_total_supply_ratio_percent': (cluster_balance / total_supply * 100) if total_supply > 0 else 0,
                'pool_vs_total_supply_ratio_percent': (pool_balance / total_supply * 100) if total_supply > 0 else 0,
                'cluster_balance_raw': cluster_balance,
                'pool_balance_raw': pool_balance,
                'total_supply_raw': total_supply
            }

    # results = results['comparative_analysis']
    
    return results

def main():
    """Main function"""
    try:
        results = analyze_token_data()
        
        # Output JSON results
        print(json.dumps(results, indent=2, default=str))
        
        # Return 0 for success, 1 if there were errors
        return 1 if results.get('errors') else 0
        
    except Exception as e:
        error_result = {
            'error': f"Fatal error during analysis: {str(e)}",
            'timestamp': None
        }
        print(json.dumps(error_result, indent=2))
        return 1

if __name__ == "__main__":
    sys.exit(main()) 
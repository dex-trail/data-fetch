#!/usr/bin/env python3
"""
Test script to verify the output structure of the analyze_cluster_balances.py modifications
"""
import json

# Mock results structure based on the actual output
mock_results = {
    'cluster_analysis': {
        'confidence_level': 'High',
        'reasoning': '6 addresses show 15 internal links from same-block-identical-value swaps.',
        'total_addresses_in_cluster': 7,
        'other_data': 'should not be in short version'
    },
    'comparative_analysis': {
        'pool_vs_total_supply_ratio_percent': 20.291030951878877,
        'other_data': 'should not be in short version'
    },
    'rugpull_analysis': {
        'rugpull_risk_assessment': {
            'overall_risk_level': 'HIGH',
            'summary_of_concerns': [
                'High centralization of power in the owner address',
                'Owner receives all liquidity pool (LP) tokens upon openTrading',
                'Presence of bots mechanism for blacklisting addresses'
            ],
            'other_data': 'should not be in short version'
        }
    },
    'other_full_data': 'should not be in short version'
}

def create_results_short(results):
    """Create shortened version with only key fields"""
    results_short = {}
    
    # Extract from cluster_analysis
    if 'cluster_analysis' in results:
        cluster_data = results['cluster_analysis']
        if 'confidence_level' in cluster_data:
            results_short['confidence_level'] = cluster_data['confidence_level']
        if 'reasoning' in cluster_data:
            results_short['reasoning'] = cluster_data['reasoning']
        if 'total_addresses_in_cluster' in cluster_data:
            results_short['total_addresses_in_cluster'] = cluster_data['total_addresses_in_cluster']
    
    # Extract from comparative_analysis
    if 'comparative_analysis' in results:
        comparative_data = results['comparative_analysis']
        if 'pool_vs_total_supply_ratio_percent' in comparative_data:
            results_short['pool_vs_total_supply_ratio_percent'] = comparative_data['pool_vs_total_supply_ratio_percent']
    
    # Extract from rugpull_analysis
    if 'rugpull_analysis' in results and isinstance(results['rugpull_analysis'], dict):
        rugpull_data = results['rugpull_analysis']
        if 'rugpull_risk_assessment' in rugpull_data:
            risk_assessment = rugpull_data['rugpull_risk_assessment']
            results_short['rugpull_analysis'] = {}
            if 'overall_risk_level' in risk_assessment:
                results_short['rugpull_analysis']['overall_risk_level'] = risk_assessment['overall_risk_level']
            if 'summary_of_concerns' in risk_assessment:
                results_short['rugpull_analysis']['summary_of_concerns'] = risk_assessment['summary_of_concerns']
    
    return results_short

# Test the structure
results_short = create_results_short(mock_results)

# Output both versions
output = {
    'results': mock_results,
    'results_short': results_short
}

print(json.dumps(output, indent=2, default=str)) 
import hypersync
import asyncio
import json
import pandas as pd
import os
import aiohttp
import networkx as nx
from datetime import datetime
from dotenv import load_dotenv
from hypersync import BlockField, TransactionField, LogField, ClientConfig
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict, Counter
import numpy as np
import argparse

# Load environment variables from .env file
load_dotenv()

# Event signatures and their hashes
EVENT_SIGNATURES = {
    # ERC20 Transfer
    "Transfer": {
        "signature": "Transfer(address indexed from, address indexed to, uint256 value)",
        "hash": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    },
    
    # Uniswap V2
    "V2_Swap": {
        "signature": "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)",
        "hash": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    },
    "V2_Mint": {
        "signature": "Mint(address indexed sender, uint256 amount0, uint256 amount1)",
        "hash": "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"
    },
    "V2_Burn": {
        "signature": "Burn(address indexed sender, uint256 amount0, uint256 amount1, address indexed to)",
        "hash": "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
    },
    
    # Uniswap V3
    "V3_Swap": {
        "signature": "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)",
        "hash": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    },
    "V3_Mint": {
        "signature": "Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
        "hash": "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
    },
    "V3_Burn": {
        "signature": "Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)",
        "hash": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    }
}

class WashTradingDetector:
    """
    Advanced wash trading detection system that analyzes trading patterns
    to identify potential market manipulation and artificial volume.
    """
    
    # Hardcoded addresses for filtering
    UNISWAP_V2_ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"  # Uniswap V2 Router
    
    def __init__(self):
        self.transaction_graph = nx.DiGraph()
        self.unified_timeline = []
        self.wash_trading_patterns = []
        
    def create_unified_timeline(self, transfer_df: pd.DataFrame, swap_v2_df: pd.DataFrame, 
                              swap_v3_df: pd.DataFrame, mint_df: pd.DataFrame, 
                              burn_df: pd.DataFrame) -> pd.DataFrame:
        """Create a unified timeline of all transactions ordered by block number."""
        
        print("   🕐 Creating unified transaction timeline...")
        timeline_data = []
        
        # Process Transfer events
        if not transfer_df.empty:
            for _, row in transfer_df.iterrows():
                timeline_data.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'event_type': 'Transfer',
                    'from_address': self.clean_address(row['from_address']),
                    'to_address': self.clean_address(row['to_address']),
                    'value': float(row['value']) if pd.notna(row['value']) else 0,
                    'token_address': row.get('token_address', ''),
                    'pair_address': None,
                    'raw_data': row.to_dict()
                })
        
        # Process V2 Swap events
        if not swap_v2_df.empty:
            for _, row in swap_v2_df.iterrows():
                timeline_data.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'event_type': 'V2_Swap',
                    'from_address': self.clean_address(row['sender']),
                    'to_address': self.clean_address(row['to']),
                    'value': max(float(row.get('amount0In', 0)), float(row.get('amount1In', 0)),
                               float(row.get('amount0Out', 0)), float(row.get('amount1Out', 0))),
                    'token_address': None,
                    'pair_address': row.get('pair_address', ''),
                    'raw_data': row.to_dict()
                })
        
        # Process V3 Swap events
        if not swap_v3_df.empty:
            for _, row in swap_v3_df.iterrows():
                timeline_data.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'event_type': 'V3_Swap',
                    'from_address': self.clean_address(row['sender']),
                    'to_address': self.clean_address(row['recipient']),
                    'value': max(abs(float(row.get('amount0', 0))), abs(float(row.get('amount1', 0)))),
                    'token_address': None,
                    'pair_address': row.get('pair_address', ''),
                    'raw_data': row.to_dict()
                })
        
        # Process Mint events
        if not mint_df.empty:
            for _, row in mint_df.iterrows():
                from_addr = row.get('sender') or row.get('owner', '')
                timeline_data.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'event_type': 'Mint',
                    'from_address': self.clean_address(from_addr),
                    'to_address': row.get('pair_address', ''),
                    'value': max(float(row.get('amount0', 0)), float(row.get('amount1', 0))),
                    'token_address': None,
                    'pair_address': row.get('pair_address', ''),
                    'raw_data': row.to_dict()
                })
        
        # Process Burn events
        if not burn_df.empty:
            for _, row in burn_df.iterrows():
                to_addr = row.get('to') or row.get('owner', '')
                timeline_data.append({
                    'block_number': row['block_number'],
                    'transaction_hash': row['transaction_hash'],
                    'event_type': 'Burn',
                    'from_address': row.get('pair_address', ''),
                    'to_address': self.clean_address(to_addr),
                    'value': max(float(row.get('amount0', 0)), float(row.get('amount1', 0))),
                    'token_address': None,
                    'pair_address': row.get('pair_address', ''),
                    'raw_data': row.to_dict()
                })
        
        # Create DataFrame and sort by block number
        timeline_df = pd.DataFrame(timeline_data)
        if not timeline_df.empty:
            timeline_df = timeline_df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
            timeline_df['timeline_index'] = range(len(timeline_df))
        
        self.unified_timeline = timeline_df
        print(f"      ✅ Created timeline with {len(timeline_df)} transactions")
        return timeline_df
    
    def create_filtered_timeline(self, timeline_df: pd.DataFrame, token_address: str) -> pd.DataFrame:
        """Create a filtered timeline that removes Transfer events that are part of swap transactions and match mint amounts."""
        
        print("   🔍 Creating filtered timeline (removing transfers that are part of swaps and match mint amounts)...")
        
        if timeline_df.empty:
            return timeline_df.copy()
        
        # Group by transaction hash to find potential swap-related transfers
        tx_groups = timeline_df.groupby('transaction_hash')
        
        filtered_transactions = []
        
        for tx_hash, group in tx_groups:
            # Check if this transaction has both swaps and transfers
            has_swap = any(group['event_type'].str.contains('Swap', na=False))
            has_transfer = any(group['event_type'] == 'Transfer')
            has_mint = any(group['event_type'] == 'Mint')
            
            if has_swap and has_transfer:
                # This transaction has both swaps and transfers
                # Analyze transfers to determine buy/sell and initiators before removing them
                
                swap_events = group[group['event_type'].str.contains('Swap', na=False)]
                transfer_events = group[group['event_type'] == 'Transfer']
                non_transfer_events = group[group['event_type'] != 'Transfer']
                
                # Get pool addresses from swap events
                pool_addresses = set()
                for _, swap in swap_events.iterrows():
                    if 'pair_address' in swap and swap['pair_address']:
                        # Clean pool addresses to ensure consistent case
                        clean_pool_addr = self.clean_address(swap['pair_address'])
                        if clean_pool_addr:  # Only add if cleaning was successful
                            pool_addresses.add(clean_pool_addr)
                
                print(f"      🏊 DEBUG: Cleaned pool addresses: {pool_addresses}")
                
                # Analyze transfers to determine transaction type and initiators
                buy_initiators = []
                sell_initiators = []
                transfer_analysis = []
                
                for _, transfer in transfer_events.iterrows():
                    transfer_from = transfer['from_address']
                    transfer_to = transfer['to_address']
                    transfer_value = transfer['value']
                    
                    # Clean addresses for comparison
                    clean_from = self.clean_address(transfer_from)
                    clean_to = self.clean_address(transfer_to)
                    
                    print(f"      🔍 DEBUG: Comparing addresses:")
                    print(f"         clean_from: '{clean_from}'")
                    print(f"         clean_to: '{clean_to}'")
                    print(f"         pool_addresses: {pool_addresses}")
                    print(f"         clean_from in pool_addresses: {clean_from in pool_addresses}")
                    print(f"         clean_to in pool_addresses: {clean_to in pool_addresses}")
                    
                    transfer_info = {
                        'from': clean_from,
                        'to': clean_to,
                        'value': transfer_value
                    }
                    transfer_analysis.append(transfer_info)
                    print("transfer_analysis")
                    print(transfer_analysis)
                    print("pool_addresses")
                    print(pool_addresses)
                    
                    # Determine if this is a buy or sell
                    if clean_from in pool_addresses:
                        # Transfer FROM pool → BUY (user receiving tokens)
                        buy_initiators.append(clean_to)
                        print(f"      📈 BUY detected: Pool {clean_from[:10]}... → User {clean_to[:10]}... (amount: {transfer_value:,.0f})")
                    elif clean_to in pool_addresses:
                        # Transfer TO pool → SELL (user sending tokens)
                        sell_initiators.append(clean_from)
                        print(f"      📉 SELL detected: User {clean_from[:10]}... → Pool {clean_to[:10]}... (amount: {transfer_value:,.0f})")
                    else:
                        # Neither from nor to is a pool - could be router or other contract
                        # We'll categorize this as unknown for now
                        print(f"      ❓ Unknown transfer: {clean_from[:10]}... → {clean_to[:10]}... (amount: {transfer_value:,.0f})")
                
                # Determine overall transaction type and initiators
                transaction_type = "Unknown"
                initiators = "Unknown"
                
                # Clean token address for comparison
                clean_token_address = self.clean_address(token_address)
                print(f"      🎯 DEBUG: Clean token address for filtering: '{clean_token_address}'")
                
                if buy_initiators and not sell_initiators:
                    transaction_type = "BUY"
                    # Filter out token address from initiators
                    filtered_buy_initiators = [addr for addr in set(buy_initiators) if addr != clean_token_address]
                    initiators = ", ".join(filtered_buy_initiators)
                    print(f"      📈 DEBUG: BUY - Original initiators: {len(set(buy_initiators))}, After filtering token: {len(filtered_buy_initiators)}")
                elif sell_initiators and not buy_initiators:
                    transaction_type = "SELL"
                    # Filter out token address from initiators
                    filtered_sell_initiators = [addr for addr in set(sell_initiators) if addr != clean_token_address]
                    initiators = ", ".join(filtered_sell_initiators)
                    print(f"      📉 DEBUG: SELL - Original initiators: {len(set(sell_initiators))}, After filtering token: {len(filtered_sell_initiators)}")
                elif buy_initiators and sell_initiators:
                    transaction_type = "MIXED"
                    all_initiators = list(set(buy_initiators + sell_initiators))
                    # Filter out token address from initiators
                    filtered_all_initiators = [addr for addr in all_initiators if addr != clean_token_address]
                    initiators = ", ".join(filtered_all_initiators)
                    print(f"      🔀 DEBUG: MIXED - Original initiators: {len(all_initiators)}, After filtering token: {len(filtered_all_initiators)}")
                else:
                    # No clear buy/sell pattern, try to infer from swap participants
                    transaction_type = "SWAP"
                    print(f"      🔄 DEBUG: No clear buy/sell pattern, using swap participants as fallback")
                    
                    # Use swap participants as fallback
                    swap_participants = []
                    for _, swap in swap_events.iterrows():
                        if 'sender' in swap and swap['sender']:
                            participant = self.clean_address(swap['sender'])
                            if participant != clean_token_address:  # Filter out token address
                                swap_participants.append(participant)
                        if 'recipient' in swap and swap['recipient']:
                            participant = self.clean_address(swap['recipient'])
                            if participant != clean_token_address:  # Filter out token address
                                swap_participants.append(participant)
                        if 'to' in swap and swap['to']:
                            participant = self.clean_address(swap['to'])
                            if participant != clean_token_address:  # Filter out token address
                                swap_participants.append(participant)
                    
                    if swap_participants:
                        unique_participants = list(set(swap_participants))
                        initiators = ", ".join(unique_participants)
                        print(f"      🔄 DEBUG: SWAP - Found {len(unique_participants)} unique participants after filtering token address")
                
                # 🚨 CRITICAL ALERT: Check for multiple initiators
                if initiators != "Unknown" and initiators:
                    initiators_list = [addr.strip() for addr in initiators.split(",") if addr.strip()]
                    if len(initiators_list) > 1:
                        print(f"      🚨🚨🚨 CRITICAL ALERT: MULTIPLE INITIATORS DETECTED! 🚨🚨🚨")
                        print(f"         Transaction Type: {transaction_type}")
                        print(f"         Transaction Hash: {tx_hash}")
                        print(f"         Number of Initiators: {len(initiators_list)}")
                        print(f"         Initiators: {initiators}")
                        print(f"         This could indicate COORDINATED MANIPULATION or WASH TRADING!")
                        print(f"         ⚠️⚠️⚠️ REQUIRES IMMEDIATE INVESTIGATION ⚠️⚠️⚠️")
                    else:
                        print(f"      ✅ DEBUG: Single initiator detected: {initiators_list[0][:10]}...")
                else:
                    print(f"      ⚠️ DEBUG: No valid initiators found after filtering")
                
                # Log the transfers we're filtering out
                for _, transfer in transfer_events.iterrows():
                    transfer_from = transfer['from_address']
                    transfer_to = transfer['to_address']
                    print(f"      🚫 Filtering out transfer (part of swap tx): {transfer_from[:10]}...→{transfer_to[:10]}... amount: {transfer['value']:,.0f}")
                
                # Add non-transfer events (swaps, mints, burns) with enhanced information
                for _, event in non_transfer_events.iterrows():
                    event_dict = event.to_dict()
                    
                    # Add transaction analysis to swap events
                    if 'Swap' in event['event_type']:
                        event_dict['transaction_type'] = transaction_type
                        event_dict['initiators'] = initiators
                        event_dict['transfer_count'] = len(transfer_events)
                        event_dict['total_transfer_value'] = sum([t['value'] for t in transfer_analysis])
                        
                        # Add individual transfer details as a summary
                        transfer_summary = "; ".join([
                            f"{t['from'][:8]}...→{t['to'][:8]}...({t['value']:,.0f})" 
                            for t in transfer_analysis[:3]  # Limit to first 3 transfers
                        ])
                        if len(transfer_analysis) > 3:
                            transfer_summary += f"; +{len(transfer_analysis) - 3} more"
                        event_dict['related_transfers'] = transfer_summary
                    
                    filtered_transactions.append(event_dict)
                
            elif has_mint and has_transfer:
                # This transaction has both mints and transfers
                # Filter out transfers that match mint amounts
                
                mint_events = group[group['event_type'] == 'Mint']
                transfer_events = group[group['event_type'] == 'Transfer']
                non_transfer_events = group[group['event_type'] != 'Transfer']
                
                print(f"      🏭 DEBUG: Processing mint transaction {tx_hash[:10]}...")
                print(f"         Found {len(mint_events)} mint events and {len(transfer_events)} transfer events")
                
                # Get mint amounts for comparison
                mint_amounts = set()
                pair_addresses = set()
                
                for _, mint in mint_events.iterrows():
                    # Get the pair address
                    pair_addr = mint.get('pair_address', '')
                    if pair_addr:
                        clean_pair_addr = self.clean_address(pair_addr)
                        if clean_pair_addr:
                            pair_addresses.add(clean_pair_addr)
                    
                    # Get mint amounts (can be amount0 or amount1)
                    amount0 = int(mint.get('amount0', 0)) if pd.notna(mint.get('amount0', 0)) else 0
                    amount1 = int(mint.get('amount1', 0)) if pd.notna(mint.get('amount1', 0)) else 0
                    
                    if amount0 > 0:
                        mint_amounts.add(amount0)
                    if amount1 > 0:
                        mint_amounts.add(amount1)
                
                print(f"         Mint amounts: {[f'{amt:,.0f}' for amt in mint_amounts]}")
                print(f"         Pair addresses: {pair_addresses}")
                
                # Determine which transfers to filter out
                transfers_to_keep = []
                transfers_to_filter = []
                
                for _, transfer in transfer_events.iterrows():
                    transfer_value = int(transfer['value'])
                    
                    # Check if this transfer amount matches any mint amount
                    if transfer_value in mint_amounts:
                        transfers_to_filter.append(transfer)
                        print(f"      🚫 Filtering out transfer (matches mint amount): {transfer['from_address'][:10]}...→{transfer['to_address'][:10]}... amount: {transfer_value:,.0f}")
                    else:
                        transfers_to_keep.append(transfer)
                        print(f"      ✅ Keeping transfer (doesn't match mint): {transfer['from_address'][:10]}...→{transfer['to_address'][:10]}... amount: {transfer_value:,.0f}")
                
                # Analyze remaining transfers for mint transaction analysis
                mint_transaction_type = "MINT"
                mint_initiators = "Unknown"
                
                # Clean addresses for filtering
                clean_token_address = self.clean_address(token_address)
                clean_uniswap_router = self.clean_address(self.UNISWAP_V2_ROUTER)
                
                # Find initiators from remaining transfers (from addresses)
                initiator_candidates = []
                for transfer in transfers_to_keep:
                    clean_from = self.clean_address(transfer['from_address'])
                    
                    # Exclude token address, pair addresses, and Uniswap router
                    if (clean_from and 
                        clean_from != clean_token_address and 
                        clean_from not in pair_addresses and 
                        clean_from != clean_uniswap_router):
                        initiator_candidates.append(clean_from)
                
                if initiator_candidates:
                    unique_initiators = list(set(initiator_candidates))
                    mint_initiators = ", ".join(unique_initiators)
                    print(f"      🏭 MINT - Found {len(unique_initiators)} initiators: {mint_initiators[:50]}...")
                else:
                    print(f"      🏭 MINT - No valid initiators found after filtering")
                
                # Add mint events with analysis
                for _, event in mint_events.iterrows():
                    event_dict = event.to_dict()
                    event_dict['transaction_type'] = mint_transaction_type
                    event_dict['initiators'] = mint_initiators
                    event_dict['transfer_count'] = len(transfers_to_keep)
                    event_dict['filtered_transfer_count'] = len(transfers_to_filter)
                    
                    # Add transfer analysis
                    remaining_transfer_summary = "; ".join([
                        f"{self.clean_address(t['from_address'])[:8]}...→{self.clean_address(t['to_address'])[:8]}...({int(t['value']):,.0f})" 
                        for t in transfers_to_keep[:3]
                    ])
                    if len(transfers_to_keep) > 3:
                        remaining_transfer_summary += f"; +{len(transfers_to_keep) - 3} more"
                    
                    event_dict['related_transfers'] = remaining_transfer_summary
                    filtered_transactions.append(event_dict)
                
                # Add other non-transfer events (burns, etc.)
                for _, event in non_transfer_events.iterrows():
                    if event['event_type'] != 'Mint':  # Don't duplicate mints
                        filtered_transactions.append(event.to_dict())
                
                # Log that we're filtering out ALL transfers in mint transactions
                for _, transfer in transfer_events.iterrows():
                    transfer_from = transfer['from_address']
                    transfer_to = transfer['to_address']
                    print(f"      🚫 Filtering out transfer (part of mint tx): {transfer_from[:10]}...→{transfer_to[:10]}... amount: {transfer['value']:,.0f}")
                
            else:
                # No swap or mint with transfers, keep all events
                for _, event in group.iterrows():
                    filtered_transactions.append(event.to_dict())
        
        # Create filtered DataFrame
        filtered_df = pd.DataFrame(filtered_transactions)
        
        if not filtered_df.empty:
            # Re-sort and re-index
            filtered_df = filtered_df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
            filtered_df['timeline_index'] = range(len(filtered_df))
        
        original_count = len(timeline_df)
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"      ✅ Filtered timeline created:")
        print(f"         Original transactions: {original_count}")
        print(f"         Filtered transactions: {filtered_count}")
        print(f"         Removed transactions: {removed_count}")
        print(f"         Removal strategy: ALL transfers in swap and mint transactions")
        print(f"         Enhanced with: transaction type, initiators, transfer analysis")
        
        return filtered_df
    
    def create_aggregated_timeline(self, filtered_timeline_df: pd.DataFrame) -> pd.DataFrame:
        """Create an aggregated timeline that consolidates transactions with same tx hash, initiator, and direction."""
        
        print("   📊 Creating aggregated timeline (consolidating same tx/initiator/direction)...")
        
        if filtered_timeline_df.empty:
            print("   ⚠️  DEBUG: Filtered timeline is empty, returning empty DataFrame")
            return filtered_timeline_df.copy()
        
        print(f"   📊 DEBUG: Starting with {len(filtered_timeline_df)} transactions in filtered timeline")
        
        # Find events that have the additional analysis fields (swaps and mints)
        analyzable_events = filtered_timeline_df[
            (filtered_timeline_df['transaction_type'].notna()) &
            (filtered_timeline_df['initiators'].notna())
        ].copy()
        
        # Keep non-analyzable events as they are
        non_analyzable_events = filtered_timeline_df[
            (filtered_timeline_df['transaction_type'].isna()) |
            (filtered_timeline_df['initiators'].isna())
        ].copy()
        
        print(f"   📊 DEBUG: Found {len(analyzable_events)} analyzable events (swaps + mints) to aggregate")
        print(f"   📊 DEBUG: Found {len(non_analyzable_events)} non-analyzable events to keep as-is")
        
        if analyzable_events.empty:
            print("   ⚠️  No events with analysis data found for aggregation")
            return filtered_timeline_df.copy()
        
        aggregated_events = []
        
        # Group by transaction_hash, initiators, and transaction_type
        grouping_cols = ['transaction_hash', 'initiators', 'transaction_type']
        
        # Ensure all grouping columns exist
        missing_cols = [col for col in grouping_cols if col not in analyzable_events.columns]
        if missing_cols:
            print(f"   ⚠️  Missing required columns for aggregation: {missing_cols}")
            return filtered_timeline_df.copy()
        
        grouped = analyzable_events.groupby(grouping_cols)
        
        print(f"   📊 DEBUG: Created {len(grouped)} groups for aggregation")
        
        for group_key, group_df in grouped:
            tx_hash, initiators, transaction_type = group_key
            group_size = len(group_df)
            
            print(f"   🔍 DEBUG: Processing group - tx: {tx_hash[:10]}..., initiators: {initiators[:20]}..., type: {transaction_type}, size: {group_size}")
            
            if group_size == 1:
                # Single transaction, keep as is
                aggregated_events.append(group_df.iloc[0].to_dict())
                print(f"      ✅ Single transaction - keeping as-is")
            else:
                # Multiple transactions to aggregate
                print(f"      📊 Aggregating {group_size} {transaction_type} events")
                
                # Use the first row as template
                aggregated_row = group_df.iloc[0].to_dict()
                
                # Sum the values
                total_value = group_df['value'].sum()
                
                # Handle different types of counts based on transaction type
                if transaction_type in ['BUY', 'SELL', 'SWAP', 'MIXED']:
                    # For swap-related events
                    total_transfer_count = group_df['transfer_count'].sum() if 'transfer_count' in group_df.columns else 0
                    total_transfer_value = group_df['total_transfer_value'].sum() if 'total_transfer_value' in group_df.columns else 0
                    
                    aggregated_row['transfer_count'] = total_transfer_count
                    aggregated_row['total_transfer_value'] = total_transfer_value
                    
                elif transaction_type == 'MINT':
                    # For mint events
                    total_transfer_count = group_df['transfer_count'].sum() if 'transfer_count' in group_df.columns else 0
                    total_filtered_transfer_count = group_df['filtered_transfer_count'].sum() if 'filtered_transfer_count' in group_df.columns else 0
                    
                    aggregated_row['transfer_count'] = total_transfer_count
                    aggregated_row['filtered_transfer_count'] = total_filtered_transfer_count
                
                # Combine related transfers
                related_transfers_list = []
                for _, row in group_df.iterrows():
                    if 'related_transfers' in row and pd.notna(row['related_transfers']):
                        related_transfers_list.append(str(row['related_transfers']))
                
                combined_related_transfers = "; ".join(related_transfers_list) if related_transfers_list else ""
                
                # Update aggregated values
                aggregated_row['value'] = total_value
                aggregated_row['related_transfers'] = combined_related_transfers
                aggregated_row['aggregated_count'] = group_size  # Add new field to track aggregation
                aggregated_row['aggregation_note'] = f"Aggregated from {group_size} {transaction_type.lower()} events"
                
                # Add aggregation details
                original_values = group_df['value'].tolist()
                aggregated_row['original_values'] = ", ".join([f"{v:,.0f}" for v in original_values])
                
                aggregated_events.append(aggregated_row)
                
                print(f"      ✅ Aggregated: {group_size} {transaction_type} events → 1 event")
                print(f"         Original values: {[f'{v:,.0f}' for v in original_values]}")
                print(f"         Aggregated value: {total_value:,.0f}")
        
        # Combine aggregated events with non-analyzable events
        all_aggregated = []
        
        # Add aggregated events
        all_aggregated.extend(aggregated_events)
        
        # Add non-analyzable events
        for _, row in non_analyzable_events.iterrows():
            row_dict = row.to_dict()
            row_dict['aggregated_count'] = 1  # Mark as non-aggregated
            row_dict['aggregation_note'] = "Not aggregated (no analysis data)"
            all_aggregated.append(row_dict)
        
        # Create aggregated DataFrame
        aggregated_df = pd.DataFrame(all_aggregated)
        
        if not aggregated_df.empty:
            # Re-sort and re-index
            aggregated_df = aggregated_df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
            aggregated_df['timeline_index'] = range(len(aggregated_df))
        
        original_count = len(filtered_timeline_df)
        aggregated_count = len(aggregated_df)
        reduction_count = original_count - aggregated_count
        
        print(f"   📊 DEBUG: === AGGREGATION SUMMARY ===")
        print(f"      Original filtered transactions: {original_count}")
        print(f"      Aggregated transactions: {aggregated_count}")
        print(f"      Transactions consolidated: {reduction_count}")
        print(f"      Reduction rate: {(reduction_count/original_count*100):.1f}%" if original_count > 0 else "0%")
        print(f"      Analyzable events processed: {len(analyzable_events)}")
        print(f"      Aggregated event groups: {len(grouped)}")
        print(f"      Non-analyzable events kept: {len(non_analyzable_events)}")
        
        # Show sample of aggregated data
        if not aggregated_df.empty:
            aggregated_events = aggregated_df[aggregated_df['aggregated_count'] > 1]
            if not aggregated_events.empty:
                print(f"   🔍 DEBUG: Sample aggregated events:")
                for _, row in aggregated_events.head(3).iterrows():
                    print(f"      - tx: {row['transaction_hash'][:10]}..., type: {row['transaction_type']}, count: {row['aggregated_count']}, value: {row['value']:,.0f}")
        
        print(f"   ✅ DEBUG: Aggregation completed successfully")
        
        return aggregated_df
    
    def clean_address(self, address) -> str:
        """Clean and standardize address format."""
        if not address or address == "N/A":
            return ""
        
        addr_str = str(address)
        if addr_str.startswith('0x'):
            if len(addr_str) == 66:
                # Extract the last 40 characters (20 bytes) + 0x prefix = 42 chars
                return '0x' + addr_str[-40:]
            elif len(addr_str) == 42:
                return addr_str.lower()
        return ""
    
    def build_transaction_graph(self, timeline_df: pd.DataFrame) -> nx.DiGraph:
        """Build a directed graph of address interactions."""
        
        print("   🕸️  Building transaction graph...")
        
        for _, row in timeline_df.iterrows():
            from_addr = row['from_address']
            to_addr = row['to_address']
            
            if from_addr and to_addr and from_addr != to_addr:
                # Add edge with transaction data
                if self.transaction_graph.has_edge(from_addr, to_addr):
                    # Update existing edge
                    edge_data = self.transaction_graph[from_addr][to_addr]
                    edge_data['transaction_count'] += 1
                    edge_data['total_value'] += row['value']
                    edge_data['transactions'].append(row.to_dict())
                else:
                    # Create new edge
                    self.transaction_graph.add_edge(from_addr, to_addr, 
                                                  transaction_count=1,
                                                  total_value=row['value'],
                                                  transactions=[row.to_dict()])
        
        print(f"      ✅ Built graph with {self.transaction_graph.number_of_nodes()} nodes and {self.transaction_graph.number_of_edges()} edges")
        return self.transaction_graph
    
    def detect_circular_trading(self, max_cycle_length: int = 5) -> List[Dict]:
        """Detect circular trading patterns (A→B→C→A)."""
        
        print("   🔄 Detecting circular trading patterns...")
        circular_patterns = []
        
        # Find cycles in the graph
        try:
            cycles = list(nx.simple_cycles(self.transaction_graph))
            
            for cycle in cycles:
                if len(cycle) <= max_cycle_length:
                    # Calculate cycle metrics
                    total_value = 0
                    transaction_count = 0
                    min_block = float('inf')
                    max_block = 0
                    
                    for i in range(len(cycle)):
                        from_addr = cycle[i]
                        to_addr = cycle[(i + 1) % len(cycle)]
                        
                        if self.transaction_graph.has_edge(from_addr, to_addr):
                            edge_data = self.transaction_graph[from_addr][to_addr]
                            total_value += edge_data['total_value']
                            transaction_count += edge_data['transaction_count']
                            
                            # Find block range
                            for tx in edge_data['transactions']:
                                min_block = min(min_block, tx.get('block_number', 0))
                                max_block = max(max_block, tx.get('block_number', 0))
                    
                    block_span = max_block - min_block if max_block > min_block else 0
                    
                    # Calculate suspicion score
                    suspicion_score = self.calculate_circular_suspicion_score(
                        len(cycle), transaction_count, total_value, block_span
                    )
                    
                    circular_patterns.append({
                        'pattern_type': 'Circular Trading',
                        'addresses': cycle,
                        'cycle_length': len(cycle),
                        'transaction_count': transaction_count,
                        'total_value': total_value,
                        'block_span': block_span,
                        'suspicion_score': suspicion_score,
                        'description': f"Circular trading involving {len(cycle)} addresses: {' → '.join(cycle[:3])}{'...' if len(cycle) > 3 else ''}"
                    })
        
        except Exception as e:
            print(f"      ⚠️  Error detecting cycles: {e}")
        
        # Sort by suspicion score
        circular_patterns.sort(key=lambda x: x['suspicion_score'], reverse=True)
        print(f"      ✅ Found {len(circular_patterns)} circular trading patterns")
        
        return circular_patterns
    
    def detect_back_and_forth_trading(self, min_interactions: int = 5, time_window_blocks: int = 1000) -> List[Dict]:
        """Detect back-and-forth trading between address pairs."""
        
        print("   ↔️  Detecting back-and-forth trading patterns...")
        back_forth_patterns = []
        
        # Analyze each pair of connected addresses
        for node1 in self.transaction_graph.nodes():
            for node2 in self.transaction_graph.nodes():
                if node1 != node2:
                    # Check if both directions exist
                    if (self.transaction_graph.has_edge(node1, node2) and 
                        self.transaction_graph.has_edge(node2, node1)):
                        
                        edge1 = self.transaction_graph[node1][node2]
                        edge2 = self.transaction_graph[node2][node1]
                        
                        total_interactions = edge1['transaction_count'] + edge2['transaction_count']
                        
                        if total_interactions >= min_interactions:
                            # Analyze timing patterns
                            all_transactions = edge1['transactions'] + edge2['transactions']
                            all_transactions.sort(key=lambda x: x.get('block_number', 0))
                            
                            # Calculate time clustering
                            time_clusters = self.find_time_clusters(all_transactions, time_window_blocks)
                            
                            # Calculate suspicion score
                            suspicion_score = self.calculate_back_forth_suspicion_score(
                                edge1['transaction_count'], edge2['transaction_count'],
                                edge1['total_value'], edge2['total_value'],
                                len(time_clusters)
                            )
                            
                            back_forth_patterns.append({
                                'pattern_type': 'Back-and-Forth Trading',
                                'addresses': [node1, node2],
                                'interactions_1_to_2': edge1['transaction_count'],
                                'interactions_2_to_1': edge2['transaction_count'],
                                'total_interactions': total_interactions,
                                'value_1_to_2': edge1['total_value'],
                                'value_2_to_1': edge2['total_value'],
                                'time_clusters': len(time_clusters),
                                'suspicion_score': suspicion_score,
                                'description': f"Back-and-forth trading: {total_interactions} interactions between {node1[:8]}... and {node2[:8]}..."
                            })
        
        # Sort by suspicion score and remove duplicates
        back_forth_patterns.sort(key=lambda x: x['suspicion_score'], reverse=True)
        print(f"      ✅ Found {len(back_forth_patterns)} back-and-forth trading patterns")
        
        return back_forth_patterns
    
    def detect_volume_pumping(self, min_volume_threshold: float = 1000000) -> List[Dict]:
        """Detect artificial volume pumping patterns."""
        
        print("   📈 Detecting volume pumping patterns...")
        volume_patterns = []
        
        # Analyze address pairs with high volume
        for edge in self.transaction_graph.edges(data=True):
            from_addr, to_addr, edge_data = edge
            
            if edge_data['total_value'] >= min_volume_threshold:
                # Check for repetitive patterns
                transactions = edge_data['transactions']
                
                # Analyze value patterns
                values = [tx['value'] for tx in transactions]
                value_repetition = len(values) - len(set(values))  # How many repeated values
                
                # Analyze timing patterns
                blocks = [tx.get('block_number', 0) for tx in transactions]
                block_intervals = [blocks[i+1] - blocks[i] for i in range(len(blocks)-1)]
                avg_interval = np.mean(block_intervals) if block_intervals else 0
                
                # Calculate suspicion score
                suspicion_score = self.calculate_volume_suspicion_score(
                    edge_data['total_value'], edge_data['transaction_count'],
                    value_repetition, avg_interval
                )
                
                volume_patterns.append({
                    'pattern_type': 'Volume Pumping',
                    'from_address': from_addr,
                    'to_address': to_addr,
                    'total_value': edge_data['total_value'],
                    'transaction_count': edge_data['transaction_count'],
                    'value_repetition_count': value_repetition,
                    'avg_block_interval': avg_interval,
                    'suspicion_score': suspicion_score,
                    'description': f"High volume ({edge_data['total_value']:,.0f}) in {edge_data['transaction_count']} transactions"
                })
        
        # Sort by suspicion score
        volume_patterns.sort(key=lambda x: x['suspicion_score'], reverse=True)
        print(f"      ✅ Found {len(volume_patterns)} volume pumping patterns")
        
        return volume_patterns
    
    def detect_coordinated_trading(self, time_window_blocks: int = 100) -> List[Dict]:
        """Detect coordinated trading activities."""
        
        print("   🤝 Detecting coordinated trading patterns...")
        coordinated_patterns = []
        
        # Use filtered timeline if available, otherwise fall back to unified timeline
        timeline_to_use = getattr(self, 'filtered_timeline', self.unified_timeline)
        
        if timeline_to_use.empty:
            return coordinated_patterns
        
        # Group transactions by time windows
        timeline_df = timeline_to_use.copy()
        timeline_df['time_group'] = timeline_df['block_number'] // time_window_blocks
        
        for time_group, group_df in timeline_df.groupby('time_group'):
            if len(group_df) >= 5:  # Minimum transactions for coordination
                
                # Find addresses involved in multiple transactions
                address_involvement = Counter()
                for _, row in group_df.iterrows():
                    if row['from_address']:
                        address_involvement[row['from_address']] += 1
                    if row['to_address']:
                        address_involvement[row['to_address']] += 1
                
                # Find highly active addresses in this time window
                highly_active = {addr: count for addr, count in address_involvement.items() 
                               if count >= 3}
                
                if len(highly_active) >= 2:
                    # Calculate coordination metrics
                    total_transactions = len(group_df)
                    involved_addresses = list(highly_active.keys())
                    max_involvement = max(highly_active.values())
                    
                    # Calculate suspicion score
                    suspicion_score = self.calculate_coordination_suspicion_score(
                        len(involved_addresses), max_involvement, total_transactions
                    )
                    
                    coordinated_patterns.append({
                        'pattern_type': 'Coordinated Trading',
                        'time_window_start': time_group * time_window_blocks,
                        'time_window_end': (time_group + 1) * time_window_blocks,
                        'involved_addresses': involved_addresses,
                        'address_count': len(involved_addresses),
                        'total_transactions': total_transactions,
                        'max_address_involvement': max_involvement,
                        'suspicion_score': suspicion_score,
                        'description': f"Coordinated activity: {len(involved_addresses)} addresses in {total_transactions} transactions"
                    })
        
        # Sort by suspicion score
        coordinated_patterns.sort(key=lambda x: x['suspicion_score'], reverse=True)
        print(f"      ✅ Found {len(coordinated_patterns)} coordinated trading patterns")
        
        return coordinated_patterns
    
    def find_time_clusters(self, transactions: List[Dict], window_size: int) -> List[List[Dict]]:
        """Find clusters of transactions within time windows."""
        if not transactions:
            return []
        
        sorted_txs = sorted(transactions, key=lambda x: (x.get('block_number', 0), x.get('transaction_hash', '')))
        clusters = []
        current_cluster = [sorted_txs[0]]
        
        for tx in sorted_txs[1:]:
            if tx.get('block_number', 0) - current_cluster[-1].get('block_number', 0) <= window_size:
                current_cluster.append(tx)
            else:
                if len(current_cluster) > 1:
                    clusters.append(current_cluster)
                current_cluster = [tx]
        
        if len(current_cluster) > 1:
            clusters.append(current_cluster)
        
        return clusters
    
    def calculate_circular_suspicion_score(self, cycle_length: int, tx_count: int, 
                                         total_value: float, block_span: int) -> float:
        """Calculate suspicion score for circular trading patterns."""
        score = 0
        
        # Shorter cycles are more suspicious
        score += (6 - cycle_length) * 20
        
        # More transactions in the cycle
        score += min(tx_count * 5, 50)
        
        # Higher value
        score += min(total_value / 1000000, 30)  # Normalize by 1M
        
        # Shorter time span is more suspicious
        if block_span > 0:
            score += max(0, 20 - block_span / 100)
        
        return min(score, 100)
    
    def calculate_back_forth_suspicion_score(self, count1: int, count2: int, 
                                           value1: float, value2: float, clusters: int) -> float:
        """Calculate suspicion score for back-and-forth trading."""
        score = 0
        
        # Balance of interactions (more balanced = more suspicious)
        balance_ratio = min(count1, count2) / max(count1, count2)
        score += balance_ratio * 30
        
        # Total interaction count
        total_interactions = count1 + count2
        score += min(total_interactions * 2, 40)
        
        # Value balance
        if value1 > 0 and value2 > 0:
            value_balance = min(value1, value2) / max(value1, value2)
            score += value_balance * 20
        
        # Time clustering
        score += min(clusters * 5, 10)
        
        return min(score, 100)
    
    def calculate_volume_suspicion_score(self, total_value: float, tx_count: int, 
                                       repetition: int, avg_interval: float) -> float:
        """Calculate suspicion score for volume pumping."""
        score = 0
        
        # High volume
        score += min(total_value / 10000000, 40)  # Normalize by 10M
        
        # Many transactions
        score += min(tx_count * 3, 30)
        
        # Value repetition
        if tx_count > 0:
            repetition_ratio = repetition / tx_count
            score += repetition_ratio * 20
        
        # Regular intervals are suspicious
        if avg_interval > 0 and avg_interval < 100:
            score += 10
        
        return min(score, 100)
    
    def calculate_coordination_suspicion_score(self, address_count: int, max_involvement: int, 
                                             total_txs: int) -> float:
        """Calculate suspicion score for coordinated trading."""
        score = 0
        
        # More addresses involved
        score += min(address_count * 8, 40)
        
        # High involvement concentration
        involvement_ratio = max_involvement / total_txs
        score += involvement_ratio * 30
        
        # Total activity level
        score += min(total_txs * 2, 30)
        
        return min(score, 100)
    
    def analyze_wash_trading(self, transfer_df: pd.DataFrame, swap_v2_df: pd.DataFrame, 
                           swap_v3_df: pd.DataFrame, mint_df: pd.DataFrame, 
                           burn_df: pd.DataFrame, token_address: str) -> Dict[str, Any]:
        """Main function to analyze all wash trading patterns."""
        
        print("   🕵️  Starting comprehensive wash trading analysis...")
        
        # Create unified timeline
        timeline_df = self.create_unified_timeline(transfer_df, swap_v2_df, swap_v3_df, mint_df, burn_df)
        
        # Create filtered timeline (removes transfers that are part of swaps)
        filtered_timeline_df = self.create_filtered_timeline(timeline_df, token_address)
        
        # Create aggregated timeline (consolidates transactions with same tx/initiator/direction)
        aggregated_timeline_df = self.create_aggregated_timeline(filtered_timeline_df)
        
        # Store filtered timeline for use in coordinated trading detection
        self.filtered_timeline = filtered_timeline_df
        
        # # Build transaction graph using filtered timeline
        # self.build_transaction_graph(filtered_timeline_df)
        
        # # Detect different patterns
        # circular_patterns = self.detect_circular_trading()
        # back_forth_patterns = self.detect_back_and_forth_trading()
        # volume_patterns = self.detect_volume_pumping()
        # coordinated_patterns = self.detect_coordinated_trading()
        
        # # Combine all patterns
        # all_patterns = (circular_patterns + back_forth_patterns + 
        #                volume_patterns + coordinated_patterns)
        
        # # Sort by suspicion score
        # all_patterns.sort(key=lambda x: x['suspicion_score'], reverse=True)
        
        # # Create summary
        # analysis_summary = {
        #     'total_patterns_detected': len(all_patterns),
        #     'circular_trading_patterns': len(circular_patterns),
        #     'back_forth_patterns': len(back_forth_patterns),
        #     'volume_pumping_patterns': len(volume_patterns),
        #     'coordinated_trading_patterns': len(coordinated_patterns),
        #     'high_suspicion_patterns': len([p for p in all_patterns if p['suspicion_score'] >= 70]),
        #     'medium_suspicion_patterns': len([p for p in all_patterns if 40 <= p['suspicion_score'] < 70]),
        #     'graph_stats': {
        #         'total_addresses': self.transaction_graph.number_of_nodes(),
        #         'total_connections': self.transaction_graph.number_of_edges(),
        #         'total_transactions': len(filtered_timeline_df),
        #         'original_transactions': len(timeline_df),
        #         'filtered_transactions': len(filtered_timeline_df)
        #     }
        # }
        
        # print(f"   ✅ Wash trading analysis complete!")
        # print(f"      🔍 Total patterns detected: {len(all_patterns)}")
        # print(f"      🚨 High suspicion patterns: {analysis_summary['high_suspicion_patterns']}")
        # print(f"      ⚠️  Medium suspicion patterns: {analysis_summary['medium_suspicion_patterns']}")
        
        return {
            # 'patterns': all_patterns,
            # 'summary': analysis_summary,
            'timeline': timeline_df,
            'filtered_timeline': filtered_timeline_df,
            'aggregated_timeline': aggregated_timeline_df,
            # 'graph': self.transaction_graph
}

class TokenAnalyticsExcel:
    def __init__(self, output_dir: str = "output", alchemy_api_key: Optional[str] = None):
        self.client = hypersync.HypersyncClient(ClientConfig())
        self.output_dir = output_dir
        self.alchemy_api_key = alchemy_api_key or os.getenv('ALCHEMY_API_KEY')
        self.alchemy_base_url = f"https://eth-mainnet.g.alchemy.com/v2/{self.alchemy_api_key}" if self.alchemy_api_key else None
        self.decoders = {
            "Transfer": hypersync.Decoder([EVENT_SIGNATURES["Transfer"]["signature"]]),
            "V2_Swap": hypersync.Decoder([EVENT_SIGNATURES["V2_Swap"]["signature"]]),
            "V2_Mint": hypersync.Decoder([EVENT_SIGNATURES["V2_Mint"]["signature"]]),
            "V2_Burn": hypersync.Decoder([EVENT_SIGNATURES["V2_Burn"]["signature"]]),
            "V3_Swap": hypersync.Decoder([EVENT_SIGNATURES["V3_Swap"]["signature"]]),
            "V3_Mint": hypersync.Decoder([EVENT_SIGNATURES["V3_Mint"]["signature"]]),
            "V3_Burn": hypersync.Decoder([EVENT_SIGNATURES["V3_Burn"]["signature"]]),
        }
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def load_token_data(self, filename: str) -> List[Dict]:
        """Load token data from JSON file."""
        with open(filename, 'r') as f:
            return json.load(f)
    
    async def fetch_transfer_events(self, token_address: str, from_block: int = 0, to_block: Optional[int] = None) -> Dict[str, Any]:
        """Fetch ALL Transfer events for a specific token."""
        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                hypersync.LogSelection(
                    address=[token_address],
                    topics=[[EVENT_SIGNATURES["Transfer"]["hash"]]]
                )
            ],
            field_selection=hypersync.FieldSelection(
                block=[BlockField.NUMBER, BlockField.TIMESTAMP, BlockField.HASH],
                log=[
                    LogField.LOG_INDEX,
                    LogField.TRANSACTION_INDEX,
                    LogField.TRANSACTION_HASH,
                    LogField.DATA,
                    LogField.ADDRESS,
                    LogField.TOPIC0,
                    LogField.TOPIC1,
                    LogField.TOPIC2,
                    LogField.TOPIC3,
                    LogField.BLOCK_NUMBER,
                ],
                transaction=[
                    TransactionField.BLOCK_NUMBER,
                    TransactionField.TRANSACTION_INDEX,
                    TransactionField.HASH,
                    TransactionField.FROM,
                    TransactionField.TO,
                    TransactionField.VALUE,
                ],
            ),
        )
        
        print(f"🔄 Fetching ALL Transfer events for token {token_address}...")
        res = await self.client.get(query)
        decoded_logs = await self.decoders["Transfer"].decode_logs(res.data.logs)
        
        return {
            "token_address": token_address,
            "event_type": "Transfer",
            "raw_logs": res.data.logs,
            "decoded_logs": decoded_logs,
            "count": len(res.data.logs)
        }
    
    async def fetch_pair_events(self, pair_address: str, pair_version: str, from_block: int = 0, to_block: Optional[int] = None) -> Dict[str, Any]:
        """Fetch ALL Swap, Mint, and Burn events for a specific trading pair."""
        
        # Validate pair address length
        if len(pair_address) != 42:
            print(f"⚠️  Invalid pair address length: {pair_address} (length: {len(pair_address)})")
            return {
                "pair_address": pair_address,
                "pair_version": pair_version,
                "error": f"Invalid address length: {len(pair_address)}, expected 42",
                "events": {},
                "total_logs": 0
            }
        
        # Determine event types based on version
        if "v2" in pair_version.lower():
            event_types = ["V2_Swap", "V2_Mint", "V2_Burn"]
        elif "v3" in pair_version.lower():
            event_types = ["V3_Swap", "V3_Mint", "V3_Burn"]
        elif "v4" in pair_version.lower():
            event_types = ["V3_Swap", "V3_Mint", "V3_Burn"]
        else:
            print(f"⚠️  Unknown pair version: {pair_version}, defaulting to V2")
            event_types = ["V2_Swap", "V2_Mint", "V2_Burn"]
        
        # Create queries for all event types
        log_selections = []
        for event_type in event_types:
            log_selections.append(
                hypersync.LogSelection(
                    address=[pair_address],
                    topics=[[EVENT_SIGNATURES[event_type]["hash"]]]
                )
            )
        
        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=log_selections,
            field_selection=hypersync.FieldSelection(
                block=[BlockField.NUMBER, BlockField.TIMESTAMP, BlockField.HASH],
                log=[
                    LogField.LOG_INDEX,
                    LogField.TRANSACTION_INDEX,
                    LogField.TRANSACTION_HASH,
                    LogField.DATA,
                    LogField.ADDRESS,
                    LogField.TOPIC0,
                    LogField.TOPIC1,
                    LogField.TOPIC2,
                    LogField.TOPIC3,
                    LogField.BLOCK_NUMBER,
                ],
                transaction=[
                    TransactionField.BLOCK_NUMBER,
                    TransactionField.TRANSACTION_INDEX,
                    TransactionField.HASH,
                    TransactionField.FROM,
                    TransactionField.TO,
                    TransactionField.VALUE,
                ],
            ),
        )
        
        print(f"🔄 Fetching ALL pair events for {pair_version} pair {pair_address}...")
        res = await self.client.get(query)
        
        # Separate and decode logs by event type
        events_by_type = {}
        for event_type in event_types:
            event_hash = EVENT_SIGNATURES[event_type]["hash"]
            filtered_logs = [log for log in res.data.logs if log.topics and log.topics[0] == event_hash]
            
            if filtered_logs:
                decoded_logs = await self.decoders[event_type].decode_logs(filtered_logs)
                events_by_type[event_type] = {
                    "raw_logs": filtered_logs,
                    "decoded_logs": decoded_logs,
                    "count": len(filtered_logs)
                }
            else:
                events_by_type[event_type] = {
                    "raw_logs": [],
                    "decoded_logs": [],
                    "count": 0
                }
        
        return {
            "pair_address": pair_address,
            "pair_version": pair_version,
            "events": events_by_type,
            "total_logs": len(res.data.logs)
        }
    
    def process_transfer_events(self, transfer_results: Dict[str, Any]) -> pd.DataFrame:
        """Convert transfer events to pandas DataFrame."""
        transfers_data = []
        
        if transfer_results.get("raw_logs") and transfer_results.get("decoded_logs"):
            for raw_log, decoded_log in zip(transfer_results["raw_logs"], transfer_results["decoded_logs"]):
                if decoded_log is None:
                    continue
                    
                from_address = raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A"
                to_address = raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A"
                value = decoded_log.body[0].val if len(decoded_log.body) > 0 and decoded_log.body[0] else 0
                
                transfers_data.append({
                    "block_number": raw_log.block_number,
                    "transaction_hash": raw_log.transaction_hash,
                    "from_address": from_address,
                    "to_address": to_address,
                    "value": value,
                    "token_address": transfer_results["token_address"]
                })
        
        df = pd.DataFrame(transfers_data)
        if not df.empty:
            df = df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
        return df
    
    def process_swap_events(self, pair_results: Dict[str, Any], event_type: str) -> pd.DataFrame:
        """Convert swap events to pandas DataFrame."""
        swaps_data = []
        
        if "events" not in pair_results or event_type not in pair_results["events"]:
            return pd.DataFrame(swaps_data)
        
        event_data = pair_results["events"][event_type]
        if not event_data.get("raw_logs") or not event_data.get("decoded_logs"):
            return pd.DataFrame(swaps_data)
        
        version = "V2" if "V2" in event_type else "V3"
        
        for raw_log, decoded_log in zip(event_data["raw_logs"], event_data["decoded_logs"]):
            if decoded_log is None:
                continue
            
            swap_data = {
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "pair_address": pair_results["pair_address"],
                "pair_version": pair_results["pair_version"],
            }
            
            if version == "V2":
                swap_data.update({
                    "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                    "to": raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A",
                    "amount0In": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                    "amount1In": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                    "amount0Out": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                    "amount1Out": decoded_log.body[3].val if len(decoded_log.body) > 3 else 0,
                })
            else:  # V3
                swap_data.update({
                    "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                    "recipient": raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A",
                    "amount0": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                    "amount1": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                    "sqrtPriceX96": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                    "liquidity": decoded_log.body[3].val if len(decoded_log.body) > 3 else 0,
                    "tick": decoded_log.body[4].val if len(decoded_log.body) > 4 else 0,
                })
            
            swaps_data.append(swap_data)
        
        df = pd.DataFrame(swaps_data)
        if not df.empty:
            df = df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
        return df
    
    def process_mint_burn_events(self, pair_results: Dict[str, Any], event_type: str) -> pd.DataFrame:
        """Convert mint/burn events to pandas DataFrame."""
        events_data = []
        
        if "events" not in pair_results or event_type not in pair_results["events"]:
            return pd.DataFrame(events_data)
        
        event_data = pair_results["events"][event_type]
        if not event_data.get("raw_logs") or not event_data.get("decoded_logs"):
            return pd.DataFrame(events_data)
        
        version = "V2" if "V2" in event_type else "V3"
        is_mint = "Mint" in event_type
        
        for raw_log, decoded_log in zip(event_data["raw_logs"], event_data["decoded_logs"]):
            if decoded_log is None:
                continue
            
            event_data_row = {
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "pair_address": pair_results["pair_address"],
                "pair_version": pair_results["pair_version"],
                "event_type": "Mint" if is_mint else "Burn"
            }
            
            if version == "V2":
                if is_mint:
                    # V2 Mint: sender (indexed), amount0, amount1
                    event_data_row.update({
                        "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                        "amount0": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                        "amount1": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                    })
                else:
                    # V2 Burn: sender (indexed), amount0, amount1, to (indexed)
                    event_data_row.update({
                        "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                        "to": raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A",
                        "amount0": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                        "amount1": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                    })
            else:  # V3
                if is_mint:
                    # V3 Mint: sender, owner (indexed), tickLower (indexed), tickUpper (indexed), amount, amount0, amount1
                    event_data_row.update({
                        "sender": decoded_log.body[0].val if len(decoded_log.body) > 0 else "N/A",
                        "owner": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                        "tickLower": int(raw_log.topics[2], 16) if len(raw_log.topics) > 2 else 0,
                        "tickUpper": int(raw_log.topics[3], 16) if len(raw_log.topics) > 3 else 0,
                        "amount": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                        "amount0": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                        "amount1": decoded_log.body[3].val if len(decoded_log.body) > 3 else 0,
                    })
                else:
                    # V3 Burn: owner (indexed), tickLower (indexed), tickUpper (indexed), amount, amount0, amount1
                    event_data_row.update({
                        "owner": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                        "tickLower": int(raw_log.topics[2], 16) if len(raw_log.topics) > 2 else 0,
                        "tickUpper": int(raw_log.topics[3], 16) if len(raw_log.topics) > 3 else 0,
                        "amount": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                        "amount0": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                        "amount1": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                    })
            
            events_data.append(event_data_row)
        
        df = pd.DataFrame(events_data)
        if not df.empty:
            df = df.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
        return df
    
    def calculate_token_balances(self, token_address: str, transfer_df: pd.DataFrame, 
                                swap_v2_df: pd.DataFrame, swap_v3_df: pd.DataFrame, 
                                mint_df: pd.DataFrame, burn_df: pd.DataFrame,
                                token_data: Dict[str, Any]) -> pd.DataFrame:
        """Calculate current token balances for each address based on transfers, swaps, mints, and burns."""
        
        print(f"   📊 Calculating token balances for {token_address}...")
        
        # Initialize balance tracking
        balances = defaultdict(int)  # address -> balance
        
        # 1. Process Transfer events
        if not transfer_df.empty:
            print(f"      Processing {len(transfer_df)} transfer events...")
            for _, transfer in transfer_df.iterrows():
                from_addr = transfer['from_address']
                to_addr = transfer['to_address']
                value = int(transfer['value'])
                
                # Decrease sender's balance (except for minting from 0x0)
                if from_addr != "0x0000000000000000000000000000000000000000":
                    balances[from_addr] -= value
                
                # Increase receiver's balance (except for burning to 0x0)
                if to_addr != "0x0000000000000000000000000000000000000000":
                    balances[to_addr] += value
        
        # 2. Process Swap events (more complex - need to determine token position in pairs)
        pairs_data = token_data["token_data"].get("pairs_data", [])
        
        # Create a mapping of pair addresses to token positions
        pair_token_positions = {}
        for pair_info in pairs_data:
            pair_address = pair_info["pairAddress"]
            # Try to determine if our token is token0 or token1 in the pair
            # This would typically require additional contract calls, but we'll make educated guesses
            pair_token_positions[pair_address] = 0  # Assume token0 for now
        
        # Process V2 Swaps
        if not swap_v2_df.empty:
            print(f"      Processing {len(swap_v2_df)} V2 swap events...")
            for _, swap in swap_v2_df.iterrows():
                pair_address = swap['pair_address']
                sender = swap['sender']
                to = swap['to']
                
                # Determine token position (0 or 1) in the pair
                token_position = pair_token_positions.get(pair_address, 0)
                
                if token_position == 0:
                    # Our token is token0
                    amount_in = int(swap['amount0In'])
                    amount_out = int(swap['amount0Out'])
                else:
                    # Our token is token1
                    amount_in = int(swap['amount1In'])
                    amount_out = int(swap['amount1Out'])
                
                # Adjust balances: sender loses amount_in, receiver gains amount_out
                if amount_in > 0:
                    balances[sender] -= amount_in
                if amount_out > 0:
                    balances[to] += amount_out
        
        # Process V3 Swaps
        if not swap_v3_df.empty:
            print(f"      Processing {len(swap_v3_df)} V3 swap events...")
            for _, swap in swap_v3_df.iterrows():
                pair_address = swap['pair_address']
                sender = swap['sender']
                recipient = swap['recipient']
                
                # Determine token position (0 or 1) in the pair
                token_position = pair_token_positions.get(pair_address, 0)
                
                if token_position == 0:
                    # Our token is token0
                    amount = int(swap['amount0'])
                else:
                    # Our token is token1
                    amount = int(swap['amount1'])
                
                # V3 amounts can be positive or negative
                # Positive amount = tokens going to the pool (user losing tokens)
                # Negative amount = tokens coming from the pool (user gaining tokens)
                
                if amount > 0:
                    # User is sending tokens to the pool
                    balances[sender] -= amount
                elif amount < 0:
                    # User is receiving tokens from the pool
                    balances[recipient] += abs(amount)
        
        # 3. Process Mint events (adding liquidity - users send tokens to pool)
        if not mint_df.empty:
            print(f"      Processing {len(mint_df)} mint events...")
            for _, mint in mint_df.iterrows():
                pair_address = mint['pair_address']
                pair_version = mint['pair_version']
                
                # Determine token position (0 or 1) in the pair
                token_position = pair_token_positions.get(pair_address, 0)
                
                if "v2" in pair_version.lower():
                    # V2 Mint: sender provides liquidity
                    sender = mint['sender']
                    
                    if token_position == 0:
                        amount = int(mint['amount0'])
                    else:
                        amount = int(mint['amount1'])
                    
                    # User sends tokens to pool (balance decreases)
                    balances[sender] -= amount
                    
                else:  # V3
                    # V3 Mint: owner/sender provides liquidity
                    # In V3, 'sender' is usually the position manager, 'owner' is the actual user
                    owner = mint.get('owner', mint.get('sender', 'N/A'))
                    
                    if token_position == 0:
                        amount = int(mint['amount0'])
                    else:
                        amount = int(mint['amount1'])
                    
                    # User sends tokens to pool (balance decreases)
                    if owner != 'N/A':
                        balances[owner] -= amount
        
        # 4. Process Burn events (removing liquidity - users receive tokens from pool)
        if not burn_df.empty:
            print(f"      Processing {len(burn_df)} burn events...")
            for _, burn in burn_df.iterrows():
                pair_address = burn['pair_address']
                pair_version = burn['pair_version']
                
                # Determine token position (0 or 1) in the pair
                token_position = pair_token_positions.get(pair_address, 0)
                
                if "v2" in pair_version.lower():
                    # V2 Burn: tokens sent to 'to' address
                    to_addr = burn.get('to', burn.get('sender', 'N/A'))
                    
                    if token_position == 0:
                        amount = int(burn['amount0'])
                    else:
                        amount = int(burn['amount1'])
                    
                    # User receives tokens from pool (balance increases)
                    if to_addr != 'N/A':
                        balances[to_addr] += amount
                        
                else:  # V3
                    # V3 Burn: owner receives the tokens
                    owner = burn.get('owner', 'N/A')
                    
                    if token_position == 0:
                        amount = int(burn['amount0'])
                    else:
                        amount = int(burn['amount1'])
                    
                    # User receives tokens from pool (balance increases)
                    if owner != 'N/A':
                        balances[owner] += amount
        
        # 5. Convert to DataFrame and filter out zero/negative balances
        balance_data = []
        for address, balance in balances.items():
            if balance > 0:  # Only include addresses with positive balances
                balance_data.append({
                    "address": address,
                    "balance": balance,
                    "balance_formatted": f"{balance:,}",  # Human readable format
                    "token_address": token_address
                })
        
        # Sort by balance descending
        balance_df = pd.DataFrame(balance_data)
        if not balance_df.empty:
            balance_df = balance_df.sort_values('balance', ascending=False).reset_index(drop=True)
            print(f"      ✅ Calculated balances for {len(balance_df)} addresses with positive balances")
        else:
            print(f"      ⚠️  No addresses with positive balances found")
        
        return balance_df

    async def analyze_token_to_excel(self, token_data: Dict[str, Any], from_block: int = 0) -> str:
        """Analyze a single token and export all data to Excel file."""
        token_address = token_data["token_data"]["tokenAddress"]
        
        print(f"\n🎯 Analyzing token: {token_address}")
        
        # Create filename based on token address
        filename = f"token_analysis_{token_address}.xlsx"
        filepath = os.path.join(self.output_dir, filename)
        
        # Initialize variables to store dataframes
        transfer_df = pd.DataFrame()
        combined_swaps_v2 = pd.DataFrame()
        combined_swaps_v3 = pd.DataFrame()
        combined_mints = pd.DataFrame()
        combined_burns = pd.DataFrame()
        
        # Initialize Excel writer
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # 1. Fetch and process Transfer events
            try:
                print(f"   📊 Fetching Transfer events...")
                transfer_results = await self.fetch_transfer_events(token_address, from_block)
                transfer_df = self.process_transfer_events(transfer_results)
                
                if not transfer_df.empty:
                    transfer_df.to_excel(writer, sheet_name='Transfers', index=False)
                    print(f"   ✅ Exported {len(transfer_df)} Transfer events to 'Transfers' sheet")
                else:
                    print(f"   ⚠️  No Transfer events found")
                    
            except Exception as e:
                print(f"   ❌ Error processing Transfer events: {e}")
            
            # 2. Process all pairs and their events
            all_swaps_v2 = []
            all_swaps_v3 = []
            all_mints = []
            all_burns = []
            
            pairs_data = token_data["token_data"].get("pairs_data", [])
            
            for pair_info in pairs_data:
                pair_address = pair_info["pairAddress"]
                pair_version = "v" + str(pair_info.get("labels", ["2"])[0]) if pair_info.get("labels") else "v2"
                
                try:
                    print(f"   📊 Fetching events for {pair_version} pair {pair_address}...")
                    pair_events = await self.fetch_pair_events(pair_address, pair_version, from_block)
                    
                    if "error" in pair_events:
                        print(f"   ⚠️  Skipping pair {pair_address}: {pair_events['error']}")
                        continue
                    
                    # Process swap events
                    for event_type in ["V2_Swap", "V3_Swap"]:
                        if event_type in pair_events.get("events", {}):
                            swap_df = self.process_swap_events(pair_events, event_type)
                            if not swap_df.empty:
                                if "V2" in event_type:
                                    all_swaps_v2.append(swap_df)
                                else:
                                    all_swaps_v3.append(swap_df)
                                print(f"      🔄 Found {len(swap_df)} {event_type} events")
                    
                    # Process mint/burn events
                    for event_type in ["V2_Mint", "V3_Mint", "V2_Burn", "V3_Burn"]:
                        if event_type in pair_events.get("events", {}):
                            mb_df = self.process_mint_burn_events(pair_events, event_type)
                            if not mb_df.empty:
                                if "Mint" in event_type:
                                    all_mints.append(mb_df)
                                else:
                                    all_burns.append(mb_df)
                                print(f"      💰 Found {len(mb_df)} {event_type} events")
                    
                except Exception as e:
                    print(f"   ❌ Error processing pair {pair_address}: {e}")
            
            # 3. Combine and export all event types
            if all_swaps_v2:
                combined_swaps_v2 = pd.concat(all_swaps_v2, ignore_index=True)
                combined_swaps_v2 = combined_swaps_v2.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
                combined_swaps_v2.to_excel(writer, sheet_name='Swaps_V2', index=False)
                print(f"   ✅ Exported {len(combined_swaps_v2)} V2 Swap events to 'Swaps_V2' sheet")
            
            if all_swaps_v3:
                combined_swaps_v3 = pd.concat(all_swaps_v3, ignore_index=True)
                combined_swaps_v3 = combined_swaps_v3.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
                combined_swaps_v3.to_excel(writer, sheet_name='Swaps_V3', index=False)
                print(f"   ✅ Exported {len(combined_swaps_v3)} V3 Swap events to 'Swaps_V3' sheet")
            
            if all_mints:
                combined_mints = pd.concat(all_mints, ignore_index=True)
                combined_mints = combined_mints.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
                combined_mints.to_excel(writer, sheet_name='Mints', index=False)
                print(f"   ✅ Exported {len(combined_mints)} Mint events to 'Mints' sheet")
            
            if all_burns:
                combined_burns = pd.concat(all_burns, ignore_index=True)
                combined_burns = combined_burns.sort_values(['block_number', 'transaction_hash']).reset_index(drop=True)
                combined_burns.to_excel(writer, sheet_name='Burns', index=False)
                print(f"   ✅ Exported {len(combined_burns)} Burn events to 'Burns' sheet")
            
            # 4. Calculate and export token balances (now including mints and burns)
            try:
                balance_df = self.calculate_token_balances(
                    token_address, transfer_df, combined_swaps_v2, combined_swaps_v3, 
                    combined_mints, combined_burns, token_data
                )
                
                if not balance_df.empty:
                    balance_df.to_excel(writer, sheet_name='Token_Balances', index=False)
                    print(f"   ✅ Exported {len(balance_df)} token balances to 'Token_Balances' sheet")
                    
                    # Create top holders summary
                    top_holders = balance_df.head(20)  # Top 20 holders
                    top_holders.to_excel(writer, sheet_name='Top_Holders', index=False)
                    print(f"   ✅ Exported top {len(top_holders)} holders to 'Top_Holders' sheet")
                else:
                    print(f"   ⚠️  No token balances to export")
                    
            except Exception as e:
                print(f"   ❌ Error calculating token balances: {e}")
                balance_df = pd.DataFrame()  # Create empty DataFrame for summary
            
            # 5. 🕵️ WASH TRADING ANALYSIS - NEW FEATURE!
            try:
                print(f"\n   🕵️  === WASH TRADING ANALYSIS ===")
                wash_detector = WashTradingDetector()
                wash_analysis = wash_detector.analyze_wash_trading(
                    transfer_df, combined_swaps_v2, combined_swaps_v3, 
                    combined_mints, combined_burns, token_address
                )
                
                # Export wash trading results to Excel
                if True:
                # if wash_analysis['patterns']:
                
                    # # All patterns
                    # patterns_df = pd.DataFrame(wash_analysis['patterns'])
                    # patterns_df.to_excel(writer, sheet_name='Wash_Trading_Patterns', index=False)
                    # print(f"   ✅ Exported {len(patterns_df)} wash trading patterns to 'Wash_Trading_Patterns' sheet")
                    
                    # # High suspicion patterns only
                    # high_suspicion = [p for p in wash_analysis['patterns'] if p['suspicion_score'] >= 70]
                    # if high_suspicion:
                    #     high_suspicion_df = pd.DataFrame(high_suspicion)
                    #     high_suspicion_df.to_excel(writer, sheet_name='High_Risk_Patterns', index=False)
                    #     print(f"   🚨 Exported {len(high_suspicion_df)} high-risk patterns to 'High_Risk_Patterns' sheet")
                    
                    # Export unified timeline
                    if not wash_analysis['timeline'].empty:
                        # Create a comprehensive timeline with all relevant information
                        timeline_export = wash_analysis['timeline'].copy()
                        
                        # Add human-readable value formatting
                        timeline_export['value_formatted'] = timeline_export['value'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x != 0 else "0")
                        
                        # Add short address labels for readability
                        timeline_export['from_short'] = timeline_export['from_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        timeline_export['to_short'] = timeline_export['to_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        
                        # Reorder columns for better readability
                        timeline_columns = ['timeline_index', 'block_number', 'event_type', 'from_short', 'to_short', 
                                          'value_formatted', 'transaction_hash', 'from_address', 'to_address', 
                                          'value', 'token_address', 'pair_address']
                        
                        # Only include columns that exist
                        available_columns = [col for col in timeline_columns if col in timeline_export.columns]
                        timeline_export_final = timeline_export[available_columns]
                        
                        timeline_export_final.to_excel(writer, sheet_name='Unified_Timeline', index=False)
                        print(f"   ✅ Exported comprehensive unified timeline with {len(timeline_export_final)} transactions to 'Unified_Timeline' sheet")
                    else:
                        print(f"   ⚠️  Timeline is empty - no transactions to export")
                    
                    # Export filtered timeline
                    if 'filtered_timeline' in wash_analysis and not wash_analysis['filtered_timeline'].empty:
                        # Create a comprehensive filtered timeline with all relevant information
                        filtered_timeline_export = wash_analysis['filtered_timeline'].copy()
                        
                        # Add human-readable value formatting
                        filtered_timeline_export['value_formatted'] = filtered_timeline_export['value'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x != 0 else "0")
                        
                        # Add short address labels for readability
                        filtered_timeline_export['from_short'] = filtered_timeline_export['from_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        filtered_timeline_export['to_short'] = filtered_timeline_export['to_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        
                        # Reorder columns for better readability - include ALL columns including new transaction analysis ones
                        timeline_columns = ['timeline_index', 'block_number', 'event_type', 'from_short', 'to_short', 
                                          'value_formatted', 'transaction_hash', 'transaction_type', 'initiators', 
                                          'transfer_count', 'total_transfer_value', 'related_transfers',
                                          'from_address', 'to_address', 'value', 'token_address', 'pair_address']
                        
                        # Only include columns that exist
                        available_columns = [col for col in timeline_columns if col in filtered_timeline_export.columns]
                        
                        # Also include any additional columns that might exist but weren't in our predefined list
                        existing_columns = list(filtered_timeline_export.columns)
                        for col in existing_columns:
                            if col not in available_columns:
                                available_columns.append(col)
                        
                        filtered_timeline_export_final = filtered_timeline_export[available_columns]
                        
                        filtered_timeline_export_final.to_excel(writer, sheet_name='Filtered_Timeline', index=False)
                        print(f"   ✅ Exported filtered timeline with {len(filtered_timeline_export_final)} transactions to 'Filtered_Timeline' sheet")
                        print(f"      📊 Filtered out {len(wash_analysis['timeline']) - len(filtered_timeline_export_final)} transactions (transfers that were part of swaps)")
                        print(f"      📋 Columns included: {len(available_columns)} columns")
                        print(f"      🔍 New analysis columns: transaction_type, initiators, transfer_count, total_transfer_value, related_transfers")
                    else:
                        print(f"   ⚠️  Filtered timeline is empty - no transactions to export")
                    
                    # Export aggregated timeline
                    if 'aggregated_timeline' in wash_analysis and not wash_analysis['aggregated_timeline'].empty:
                        # Create a comprehensive aggregated timeline with all relevant information
                        aggregated_timeline_export = wash_analysis['aggregated_timeline'].copy()
                        
                        # Add human-readable value formatting
                        aggregated_timeline_export['value_formatted'] = aggregated_timeline_export['value'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x != 0 else "0")
                        
                        # Add short address labels for readability
                        aggregated_timeline_export['from_short'] = aggregated_timeline_export['from_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        aggregated_timeline_export['to_short'] = aggregated_timeline_export['to_address'].apply(
                            lambda x: f"{x[:8]}...{x[-4:]}" if x and len(str(x)) > 12 else str(x)
                        )
                        
                        # Reorder columns for better readability - include ALL columns including new aggregation ones
                        timeline_columns = ['timeline_index', 'block_number', 'event_type', 'from_address', 'to_address',
                                          'value_formatted', 'value', 'transaction_hash', 'transaction_type', 'initiators',
                                            'token_address', 'pair_address']
                        # timeline_columns = ['timeline_index', 'block_number', 'event_type', 'from_short', 'to_short', 
                        #                   'value_formatted', 'transaction_hash', 'transaction_type', 'initiators', 
                        #                   'transfer_count', 'total_transfer_value', 'related_transfers',
                        #                   'aggregated_count', 'aggregation_note', 'original_values',
                        #                   'from_address', 'to_address', 'value', 'token_address', 'pair_address']
                        
                        # Only include columns that exist
                        available_columns = [col for col in timeline_columns if col in aggregated_timeline_export.columns]
                        
                        # Also include any additional columns that might exist but weren't in our predefined list
                        existing_columns = list(aggregated_timeline_export.columns)
                        for col in existing_columns:
                            if col not in available_columns:
                                available_columns.append(col)
                        
                        aggregated_timeline_export_final = aggregated_timeline_export[available_columns]
                        
                        aggregated_timeline_export_final.to_excel(writer, sheet_name='Aggregated_Timeline', index=False)
                        print(f"   ✅ Exported aggregated timeline with {len(aggregated_timeline_export_final)} transactions to 'Aggregated_Timeline' sheet")
                        
                        # 🆕 SAVE AGGREGATED TIMELINE AS JSON
                        json_filename = f"aggregated_timeline_{token_address}.json"
                        json_filepath = os.path.join(self.output_dir, json_filename)
                        
                        # Convert DataFrame to JSON-serializable format
                        aggregated_json_data = aggregated_timeline_export_final.to_dict('records')
                        
                        # Save as JSON file
                        import json
                        with open(json_filepath, 'w') as json_file:
                            json.dump(aggregated_json_data, json_file, indent=2, default=str)
                        
                        print(f"   ✅ Saved aggregated timeline as JSON: {json_filepath}")
                        
                        # Calculate aggregation statistics
                        aggregated_events = aggregated_timeline_export_final[aggregated_timeline_export_final['aggregated_count'] > 1]
                        original_filtered_count = len(wash_analysis['filtered_timeline']) if 'filtered_timeline' in wash_analysis else 0
                        reduction_count = original_filtered_count - len(aggregated_timeline_export_final)
                        
                        print(f"      📊 Consolidated {reduction_count} transactions through aggregation")
                        print(f"      🔗 Found {len(aggregated_events)} groups with multiple transactions")
                        print(f"      📋 Columns included: {len(available_columns)} columns")
                        print(f"      🆕 New aggregation columns: aggregated_count, aggregation_note, original_values")
                        
                        if not aggregated_events.empty:
                            total_aggregated_transactions = aggregated_events['aggregated_count'].sum()
                            print(f"      📈 Total original transactions represented in aggregated groups: {total_aggregated_transactions}")
                            print(f"      📉 Compression ratio: {len(aggregated_events)}/{total_aggregated_transactions} = {(len(aggregated_events)/total_aggregated_transactions*100):.1f}%")
                    else:
                        print(f"   ⚠️  Aggregated timeline is empty - no transactions to export")
                    
                    # # Create wash trading summary
                    # wash_summary_data = []
                    # summary = wash_analysis['summary']
                    
                    # wash_summary_data.extend([
                    #     {"Metric": "Total Patterns Detected", "Value": summary['total_patterns_detected']},
                    #     {"Metric": "High Suspicion Patterns (≥70)", "Value": summary['high_suspicion_patterns']},
                    #     {"Metric": "Medium Suspicion Patterns (40-69)", "Value": summary['medium_suspicion_patterns']},
                    #     {"Metric": "Circular Trading Patterns", "Value": summary['circular_trading_patterns']},
                    #     {"Metric": "Back-and-Forth Patterns", "Value": summary['back_forth_patterns']},
                    #     {"Metric": "Volume Pumping Patterns", "Value": summary['volume_pumping_patterns']},
                    #     {"Metric": "Coordinated Trading Patterns", "Value": summary['coordinated_trading_patterns']},
                    #     {"Metric": "Total Unique Addresses", "Value": summary['graph_stats']['total_addresses']},
                    #     {"Metric": "Total Address Connections", "Value": summary['graph_stats']['total_connections']},
                    #     {"Metric": "Original Transactions", "Value": summary['graph_stats']['original_transactions']},
                    #     {"Metric": "Filtered Transactions (used for analysis)", "Value": summary['graph_stats']['filtered_transactions']},
                    #     {"Metric": "Aggregated Transactions (consolidated)", "Value": len(wash_analysis['aggregated_timeline']) if 'aggregated_timeline' in wash_analysis else 0},
                    #     {"Metric": "Removed Swap-related Transfers", "Value": summary['graph_stats']['original_transactions'] - summary['graph_stats']['filtered_transactions']},
                    #     {"Metric": "Transactions Consolidated by Aggregation", "Value": summary['graph_stats']['filtered_transactions'] - (len(wash_analysis['aggregated_timeline']) if 'aggregated_timeline' in wash_analysis else 0)}
                    # ])
                    
                    # # Calculate risk assessment
                    # risk_level = "LOW"
                    # if summary['high_suspicion_patterns'] >= 5:
                    #     risk_level = "HIGH"
                    # elif summary['high_suspicion_patterns'] >= 2 or summary['medium_suspicion_patterns'] >= 10:
                    #     risk_level = "MEDIUM"
                    
                    # wash_summary_data.append({"Metric": "Overall Wash Trading Risk", "Value": risk_level})
                    
                    # wash_summary_df = pd.DataFrame(wash_summary_data)
                    # wash_summary_df.to_excel(writer, sheet_name='Wash_Trading_Summary', index=False)
                    # print(f"   ✅ Created wash trading summary - Risk Level: {risk_level}")
                    

                
            except Exception as e:
                print(f"   ❌ Error in wash trading analysis: {e}")
                import traceback
                traceback.print_exc()
            
            # 6. Fetch current balances using Alchemy API
            try:
                print(f"   🔍 Checking Alchemy API configuration...")
                print(f"      API Key available: {'Yes' if self.alchemy_api_key else 'No'}")
                if self.alchemy_api_key:
                    print(f"      API Key length: {len(self.alchemy_api_key)}")
                    print(f"      Base URL: {self.alchemy_base_url[:50]}..." if self.alchemy_base_url else "      Base URL: None")
                
                # Extract unique addresses from all events
                unique_addresses = self.extract_unique_addresses(
                    transfer_df, combined_swaps_v2, combined_swaps_v3, 
                    combined_mints, combined_burns
                )
                
                print(f"   📊 Found {len(unique_addresses)} unique addresses for balance fetching")
                
                if unique_addresses and self.alchemy_api_key:
                    print(f"   🌐 Starting Alchemy API balance fetching...")
                    # Fetch current balances from Alchemy
                    alchemy_balances_df = await self.fetch_balances_batch(unique_addresses, token_address)
                    
                    print(f"   📈 Alchemy API returned {len(alchemy_balances_df)} records")
                    
                    if not alchemy_balances_df.empty:
                        print(f"   📝 Processing Alchemy balance data...")
                        
                        # Sort by token balance (descending)
                        alchemy_balances_df = alchemy_balances_df.sort_values(
                            'token_balance_raw', ascending=False, na_position='last'
                        ).reset_index(drop=True)
                        
                        # Export main Alchemy balances sheet
                        alchemy_balances_df.to_excel(writer, sheet_name='Alchemy_Balances', index=False)
                        print(f"   ✅ Exported {len(alchemy_balances_df)} Alchemy balances to 'Alchemy_Balances' sheet")
                        
                        # Create top token holders from Alchemy data
                        top_token_holders = alchemy_balances_df[
                            alchemy_balances_df['token_balance_raw'] > 0
                        ].head(50)  # Top 50 token holders
                        
                        print(f"   🏆 Found {len(top_token_holders)} addresses with token balances")
                        
                        if not top_token_holders.empty:
                            top_token_holders.to_excel(writer, sheet_name='Top_Token_Holders_Alchemy', index=False)
                            print(f"   ✅ Exported top {len(top_token_holders)} token holders from Alchemy to 'Top_Token_Holders_Alchemy' sheet")
                        
                        # Create ETH-rich addresses
                        eth_rich = alchemy_balances_df[
                            alchemy_balances_df['eth_balance_eth'].notna() & 
                            (alchemy_balances_df['eth_balance_eth'] > 0.1)  # More than 0.1 ETH
                        ].sort_values('eth_balance_eth', ascending=False).head(30)
                        
                        print(f"   💰 Found {len(eth_rich)} ETH-rich addresses (>0.1 ETH)")
                        
                        if not eth_rich.empty:
                            eth_rich.to_excel(writer, sheet_name='ETH_Rich_Addresses', index=False)
                            print(f"   ✅ Exported {len(eth_rich)} ETH-rich addresses to 'ETH_Rich_Addresses' sheet")
                        
                        print(f"   🎉 Alchemy data processing completed successfully!")
                        
                    else:
                        print(f"   ⚠️  No valid Alchemy balances fetched - empty DataFrame returned")
                        alchemy_balances_df = pd.DataFrame()
                elif not self.alchemy_api_key:
                    print(f"   ⚠️  Skipping Alchemy balance fetching - no API key provided")
                    print(f"   💡 Set ALCHEMY_API_KEY in your .env file to enable balance fetching")
                    alchemy_balances_df = pd.DataFrame()
                elif not unique_addresses:
                    print(f"   ⚠️  No unique addresses found for balance fetching")
                    alchemy_balances_df = pd.DataFrame()
                else:
                    print(f"   ⚠️  Unknown condition preventing Alchemy balance fetching")
                    alchemy_balances_df = pd.DataFrame()
                    
            except Exception as e:
                print(f"   ❌ Error fetching Alchemy balances: {e}")
                import traceback
                print(f"   🔧 Full error trace:")
                traceback.print_exc()
                alchemy_balances_df = pd.DataFrame()
            
            # 7. Create summary sheet
            summary_data = {
                "Metric": [
                    "Token Address", 
                    "Transfer Events", 
                    "V2 Swap Events", 
                    "V3 Swap Events", 
                    "Mint Events", 
                    "Burn Events",
                    "Addresses with Balances (calculated)",
                    "Total Supply (from transfers)",
                    "Unique Addresses (all events)",
                    "Alchemy Balances Fetched",
                    "🕵️ Wash Trading Risk Level",
                    "🚨 High Risk Patterns",
                    "⚠️ Medium Risk Patterns"
                ],
                "Value": [
                    token_address,
                    len(transfer_df) if not transfer_df.empty else 0,
                    len(combined_swaps_v2) if not combined_swaps_v2.empty else 0,
                    len(combined_swaps_v3) if not combined_swaps_v3.empty else 0,
                    len(combined_mints) if all_mints else 0,
                    len(combined_burns) if all_burns else 0,
                    len(balance_df) if not balance_df.empty else 0,
                    balance_df['balance'].sum() if not balance_df.empty else 0,
                    len(unique_addresses) if 'unique_addresses' in locals() else 0,
                    len(alchemy_balances_df) if 'alchemy_balances_df' in locals() and not alchemy_balances_df.empty else 0,
                    locals().get('risk_level', 'N/A'),
                    wash_analysis['summary']['high_suspicion_patterns'] if 'wash_analysis' in locals() else 0,
                    wash_analysis['summary']['medium_suspicion_patterns'] if 'wash_analysis' in locals() else 0
                ]
            }
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            print(f"   ✅ Created Summary sheet")
        
        print(f"   💾 Saved complete analysis to: {filepath}")
        return filepath

    def extract_unique_addresses(self, transfer_df: pd.DataFrame, swap_v2_df: pd.DataFrame, 
                               swap_v3_df: pd.DataFrame, mint_df: pd.DataFrame, 
                               burn_df: pd.DataFrame) -> Set[str]:
        """Extract all unique addresses from transfers, swaps, mints, and burns."""
        
        print("   🔍 Extracting unique addresses from all events...")
        addresses = set()
        
        # Extract from transfers
        if not transfer_df.empty:
            addresses.update(transfer_df['from_address'].dropna().tolist())
            addresses.update(transfer_df['to_address'].dropna().tolist())
            print(f"      Found {len(addresses)} unique addresses from transfers")
        
        # Extract from V2 swaps
        if not swap_v2_df.empty:
            addresses.update(swap_v2_df['sender'].dropna().tolist())
            addresses.update(swap_v2_df['to'].dropna().tolist())
            print(f"      Total unique addresses after V2 swaps: {len(addresses)}")
        
        # Extract from V3 swaps
        if not swap_v3_df.empty:
            addresses.update(swap_v3_df['sender'].dropna().tolist())
            addresses.update(swap_v3_df['recipient'].dropna().tolist())
            print(f"      Total unique addresses after V3 swaps: {len(addresses)}")
        
        # Extract from mints
        if not mint_df.empty:
            if 'sender' in mint_df.columns:
                addresses.update(mint_df['sender'].dropna().tolist())
            if 'owner' in mint_df.columns:
                addresses.update(mint_df['owner'].dropna().tolist())
            print(f"      Total unique addresses after mints: {len(addresses)}")
        
        # Extract from burns
        if not burn_df.empty:
            if 'sender' in burn_df.columns:
                addresses.update(burn_df['sender'].dropna().tolist())
            if 'owner' in burn_df.columns:
                addresses.update(burn_df['owner'].dropna().tolist())
            if 'to' in burn_df.columns:
                addresses.update(burn_df['to'].dropna().tolist())
            print(f"      Total unique addresses after burns: {len(addresses)}")
        
        # Filter out null addresses and invalid addresses
        print(f"   🔍 Debugging addresses before filtering:")
        sample_addresses = list(addresses)[:5]  # Show first 5 addresses for debugging
        for addr in sample_addresses:
            print(f"      Address: '{addr}' | Type: {type(addr)} | Length: {len(str(addr)) if addr else 0} | Starts with 0x: {str(addr).startswith('0x') if addr else False}")
        
        # Clean and extract proper addresses
        cleaned_addresses = set()
        for addr in addresses:
            if addr and addr != "N/A":
                addr_str = str(addr)
                if addr_str.startswith('0x'):
                    if len(addr_str) == 66:
                        # Extract the last 40 characters (20 bytes) + 0x prefix = 42 chars
                        clean_addr = '0x' + addr_str[-40:]
                        cleaned_addresses.add(clean_addr)
                    elif len(addr_str) == 42:
                        # Already proper format
                        cleaned_addresses.add(addr_str)
        
        # Filter out zero address
        filtered_addresses = {
            addr for addr in cleaned_addresses 
            if addr != "0x0000000000000000000000000000000000000000"
        }
        
        print(f"   ✅ Final unique addresses count: {len(filtered_addresses)}")
        if len(filtered_addresses) > 0:
            print(f"   📝 Sample filtered addresses: {list(filtered_addresses)[:3]}")
        return filtered_addresses
    
    async def fetch_eth_balance(self, session: aiohttp.ClientSession, address: str) -> Optional[int]:
        """Fetch ETH balance for a single address using Alchemy API."""
        if not self.alchemy_base_url:
            return None
        
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [address, "latest"]
        }
        
        try:
            async with session.post(self.alchemy_base_url, json=payload) as response:
                response_text = await response.text()
                print(f"      ETH balance response status: {response.status}")
                print(f"      ETH balance response: {response_text[:200]}...")
                
                if response.status == 200:
                    data = await response.json()
                    if 'result' in data:
                        # Convert hex to int (wei)
                        return int(data['result'], 16)
                    elif 'error' in data:
                        print(f"      ETH balance API error: {data['error']}")
                return None
        except Exception as e:
            print(f"      Error fetching ETH balance for {address}: {e}")
            return None
    
    async def fetch_token_balance(self, session: aiohttp.ClientSession, address: str, token_address: str) -> Optional[int]:
        """Fetch ERC20 token balance for a single address using Alchemy API."""
        if not self.alchemy_base_url:
            return None
        
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenBalances",
            "params": [address, [token_address]]
        }
        
        try:
            async with session.post(self.alchemy_base_url, json=payload) as response:
                response_text = await response.text()
                print(f"      Token balance response status: {response.status}")
                print(f"      Token balance response: {response_text[:200]}...")
                
                if response.status == 200:
                    data = await response.json()
                    if 'result' in data and 'tokenBalances' in data['result']:
                        token_balances = data['result']['tokenBalances']
                        if token_balances and len(token_balances) > 0:
                            balance_hex = token_balances[0].get('tokenBalance')
                            if balance_hex and balance_hex != '0x':
                                return int(balance_hex, 16)
                        return 0
                    elif 'error' in data:
                        print(f"      Token balance API error: {data['error']}")
                return 0
        except Exception as e:
            print(f"      Error fetching token balance for {address}: {e}")
            return None
    
    async def fetch_balances_batch(self, addresses: Set[str], token_address: str, batch_size: int = 50) -> pd.DataFrame:
        """Fetch ETH and token balances for multiple addresses using Alchemy API."""
        
        if not self.alchemy_api_key:
            print("   ⚠️  No Alchemy API key provided. Set ALCHEMY_API_KEY environment variable or pass it to constructor.")
            return pd.DataFrame()
        
        print(f"   🌐 Fetching balances for {len(addresses)} addresses using Alchemy API...")
        print(f"      Using batch size: {batch_size}")
        
        balance_data = []
        addresses_list = list(addresses)
        
        # Create connector with connection limits
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Process in batches to avoid rate limits
            for i in range(0, len(addresses_list), batch_size):
                batch = addresses_list[i:i + batch_size]
                print(f"      Processing batch {i//batch_size + 1}/{(len(addresses_list) + batch_size - 1)//batch_size}")
                
                # Create tasks for current batch
                tasks = []
                for address in batch:
                    # Fetch both ETH and token balances
                    eth_task = self.fetch_eth_balance(session, address)
                    token_task = self.fetch_token_balance(session, address, token_address)
                    tasks.extend([eth_task, token_task])
                
                # Execute batch
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results (pairs of eth_balance, token_balance)
                for j, address in enumerate(batch):
                    eth_balance = results[j * 2]
                    token_balance = results[j * 2 + 1]
                    
                    # Handle exceptions
                    if isinstance(eth_balance, Exception):
                        eth_balance = None
                    if isinstance(token_balance, Exception):
                        token_balance = None
                    
                    balance_data.append({
                        "address": address,
                        "eth_balance_wei": eth_balance,
                        "eth_balance_eth": eth_balance / 1e18 if eth_balance else None,
                        "token_balance_raw": token_balance,
                        "token_balance_formatted": f"{token_balance:,}" if token_balance else "0",
                        "token_address": token_address
                    })
                
                # Add small delay between batches to respect rate limits
                if i + batch_size < len(addresses_list):
                    await asyncio.sleep(0.5)
        
        balance_df = pd.DataFrame(balance_data)
        
        # Don't filter out addresses - include all results for debugging
        print(f"   📊 Total records processed: {len(balance_df)}")
        if not balance_df.empty:
            print(f"   📈 Records with ETH balance: {len(balance_df[balance_df['eth_balance_wei'].notna()])}")
            print(f"    Records with token balance: {len(balance_df[balance_df['token_balance_raw'].notna()])}")
            print(f"   📈 Records with positive token balance: {len(balance_df[balance_df['token_balance_raw'] > 0])}")
        
        # Return all data for debugging - we'll filter later if needed
        return balance_df

async def test_alchemy_configuration():
    """
    Test function to verify Alchemy API configuration and basic functionality.
    """
    print("🔧 Testing Alchemy API Configuration...")
    print("=" * 50)
    
    # Load environment variables
    load_dotenv()
    
    # Check if API key is available
    api_key = os.getenv('ALCHEMY_API_KEY')
    print(f"1. API Key Status: {'✅ Found' if api_key else '❌ Not found'}")
    
    if api_key:
        print(f"   API Key length: {len(api_key)} characters")
        print(f"   API Key starts with: {api_key[:8]}...")
    
    # Initialize analyzer
    analyzer = TokenAnalyticsExcel()
    print(f"2. Analyzer initialization: {'✅ Success' if analyzer else '❌ Failed'}")
    print(f"   Alchemy API key detected: {'✅ Yes' if analyzer.alchemy_api_key else '❌ No'}")
    print(f"   Base URL configured: {'✅ Yes' if analyzer.alchemy_base_url else '❌ No'}")
    
    if not analyzer.alchemy_api_key:
        print("\n❌ Cannot proceed without API key. Please check your .env file.")
        return
    
    # Test with a simple address
    test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"  # Vitalik's address
    test_token = "0xA0b86a33E6441fb64DF39c7E45c1c89Fd6b14Fb9"  # USDC
    
    print(f"\n3. Testing API calls with address: {test_address}")
    
    try:
        connector = aiohttp.TCPConnector()
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Test ETH balance
            print("   Testing ETH balance fetch...")
            eth_balance = await analyzer.fetch_eth_balance(session, test_address)
            print(f"   ETH balance result: {'✅ Success' if eth_balance is not None else '❌ Failed'}")
            if eth_balance is not None:
                print(f"   ETH balance: {eth_balance / 1e18:.4f} ETH")
            
            # Test token balance
            print("   Testing token balance fetch...")
            token_balance = await analyzer.fetch_token_balance(session, test_address, test_token)
            print(f"   Token balance result: {'✅ Success' if token_balance is not None else '❌ Failed'}")
            if token_balance is not None:
                print(f"   Token balance: {token_balance}")
        
        print("\n4. Testing batch function...")
        test_addresses = {test_address}
        batch_result = await analyzer.fetch_balances_batch(test_addresses, test_token, batch_size=1)
        print(f"   Batch result: {'✅ Success' if not batch_result.empty else '❌ Empty result'}")
        if not batch_result.empty:
            print(f"   Batch returned {len(batch_result)} records")
            print("   Sample data:")
            print(batch_result.head().to_string(index=False))
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 50)
    print("🏁 Test completed!")

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Analyze tokens from new_tokens_data.json and export to Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze all tokens from the JSON file
  python3 token_analytics_excel.py
  
  # Analyze specific token addresses
  python3 token_analytics_excel.py --addresses 0x1234...abcd 0x5678...efgh
  
  # Analyze specific token addresses (case insensitive)
  python3 token_analytics_excel.py -a 0x1234...ABCD 0x5678...efgh
        """
    )
    
    parser.add_argument("--addresses", "-a", nargs="*", type=str,
                        help="Specific token addresses to analyze (case insensitive). "
                             "If not specified, all tokens from the JSON file will be analyzed. "
                             "Example: --addresses 0x1234...abcd 0x5678...efgh")
    
    args = parser.parse_args()
    
    # Initialize analyzer with optional Alchemy API key
    # You can pass the API key directly or set ALCHEMY_API_KEY environment variable
    alchemy_api_key = os.getenv('ALCHEMY_API_KEY')  # or replace with your actual key
    analyzer = TokenAnalyticsExcel(alchemy_api_key=alchemy_api_key)
    
    # Show API key status
    if analyzer.alchemy_api_key:
        print(f"🔑 Alchemy API key configured - balance fetching will be enabled")
    else:
        print(f"⚠️  No Alchemy API key found - balance fetching will be skipped")
        print(f"   💡 To enable balance fetching, set ALCHEMY_API_KEY environment variable")
        print(f"   💡 Or pass alchemy_api_key parameter to TokenAnalyticsExcel constructor")
    
    # Load token data
    try:
        tokens_data = analyzer.load_token_data("new_tokens_data.json")
        print(f"📊 Loaded {len(tokens_data)} tokens from new_tokens_data.json")
    except Exception as e:
        print(f"❌ Error loading token data: {e}")
        return
    
    # Filter tokens based on specified addresses
    if args.addresses:
        # Normalize specified addresses to lowercase for comparison
        specified_addresses = {addr.lower() for addr in args.addresses}
        print(f"🎯 Filtering for specific addresses: {', '.join(args.addresses)}")
        
        # Filter tokens that match the specified addresses
        filtered_tokens = []
        for token_entry in tokens_data:
            token_data = token_entry.get('token_data', {})
            token_address = token_data.get('tokenAddress', '').lower()
            
            if token_address in specified_addresses:
                filtered_tokens.append(token_entry)
        
        if not filtered_tokens:
            print(f"❌ No tokens found matching the specified addresses")
            print(f"   Available token addresses in the file:")
            for token_entry in tokens_data[:10]:  # Show first 10 for reference
                token_data = token_entry.get('token_data', {})
                token_address = token_data.get('tokenAddress', 'Unknown')
                chain_id = token_data.get('chainId', 'Unknown')
                print(f"   - {chain_id}:{token_address}")
            if len(tokens_data) > 10:
                print(f"   ... and {len(tokens_data) - 10} more")
            return
        
        tokens_data = filtered_tokens
        print(f"✅ Found {len(tokens_data)} matching token(s) to analyze")
    else:
        print(f"📝 No specific addresses specified - will analyze all {len(tokens_data)} tokens")
    
    # Analyze each token and export to Excel
    processed_files = []
    max_tokens = len(tokens_data)  # Process all filtered tokens
    
    for i, token_data in enumerate(tokens_data[:max_tokens]):
        try:
            print(f"\n{'='*60}")
            print(f"Processing token {i+1}/{min(max_tokens, len(tokens_data))}")
            
            filepath = await analyzer.analyze_token_to_excel(token_data, from_block=0)
            processed_files.append(filepath)
            
        except Exception as e:
            print(f"❌ Error analyzing token {i+1}: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ Analysis complete! Processed {len(processed_files)} tokens")
    print(f"📂 Excel files saved in: {analyzer.output_dir}/")
    for filepath in processed_files:
        print(f"   - {os.path.basename(filepath)}")
    
    if analyzer.alchemy_api_key:
        print(f"\n🌟 New Excel sheets with Alchemy API data:")
        print(f"   - 'Alchemy_Balances': Current ETH and token balances for all addresses")
        print(f"   - 'Top_Token_Holders_Alchemy': Top 50 token holders by current balance")
        print(f"   - 'ETH_Rich_Addresses': Addresses with >0.1 ETH balance")
    else:
        print(f"\n💡 To get current balance data, set up your Alchemy API key!")
        print(f"   Visit: https://dashboard.alchemy.com/ to get your API key")
        print(f"   Then set: export ALCHEMY_API_KEY='your_api_key_here'")

if __name__ == "__main__":
    import sys
    
    # Check if we want to run the test function
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print("🧪 Running Alchemy API test...")
        asyncio.run(test_alchemy_configuration())
    else:
        asyncio.run(main())
        # Script execution complete
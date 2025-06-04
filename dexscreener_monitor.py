#!/usr/bin/env python3
"""
DexScreener Token Monitor
=========================

A tool to monitor DexScreener API for token profiles and pairs.

Features:
- Multiple monitoring modes:
  1. Monitor latest token profiles for new tokens
  2. Monitor specific token addresses for changes
  3. Run once or continuously
- Fetches token pairs data (chain, dex, pool info)
- Saves complete token data for historical records
- Configurable polling interval
- Multiple alert methods (console, file logging)
- Error handling and retry logic
- Persistent storage of seen tokens
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional, Union
from pathlib import Path
import argparse
from enum import Enum

class MonitorMode(Enum):
    LATEST_TOKENS = "latest"
    SPECIFIC_ADDRESSES = "addresses"

class DexScreenerMonitor:
    def __init__(self, 
                 poll_interval: int = 60,
                 storage_file: str = "seen_tokens.json",
                 data_file: str = "new_tokens_data.json",
                 log_file: str = "dexscreener_monitor.log",
                 monitor_addresses: Optional[List[str]] = None):
        """
        Initialize the DexScreener monitor.
        
        Args:
            poll_interval: Time in seconds between API checks
            storage_file: File to store previously seen tokens
            data_file: File to store complete token data
            log_file: File to store logs
            monitor_addresses: List of specific addresses to monitor (format: "chain:address")
        """
        self.api_url = "https://api.dexscreener.com/token-profiles/latest/v1"
        self.pairs_api_url = "https://api.dexscreener.com/token-pairs/v1"
        self.poll_interval = poll_interval
        self.storage_file = Path(storage_file)
        self.data_file = Path(data_file)
        self.log_file = Path(log_file)
        self.monitor_addresses = monitor_addresses or []
        
        # Determine monitor mode
        self.mode = MonitorMode.SPECIFIC_ADDRESSES if self.monitor_addresses else MonitorMode.LATEST_TOKENS
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Load previously seen tokens/data
        self.seen_tokens: Set[str] = self.load_seen_tokens()
        self.previous_address_data: Dict[str, Dict] = self.load_previous_address_data()
        
        mode_str = "specific addresses" if self.mode == MonitorMode.SPECIFIC_ADDRESSES else "latest tokens"
        self.logger.info(f"Monitor initialized in {mode_str} mode")
        if self.mode == MonitorMode.SPECIFIC_ADDRESSES:
            self.logger.info(f"Monitoring {len(self.monitor_addresses)} addresses: {', '.join(self.monitor_addresses)}")
        else:
            self.logger.info(f"Loaded {len(self.seen_tokens)} previously seen tokens")
    
    def load_seen_tokens(self) -> Set[str]:
        """Load previously seen token addresses from storage file."""
        if not self.storage_file.exists():
            return set()
        
        try:
            with open(self.storage_file, 'r') as f:
                data = json.load(f)
                return set(data.get('seen_tokens', []))
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.warning(f"Could not load seen tokens: {e}")
            return set()
    
    def load_previous_address_data(self) -> Dict[str, Dict]:
        """Load previous data for monitored addresses."""
        if self.mode != MonitorMode.SPECIFIC_ADDRESSES:
            return {}
        
        storage_file = Path(f"address_data_{self.storage_file.stem}.json")
        if not storage_file.exists():
            return {}
        
        try:
            with open(storage_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            self.logger.warning(f"Could not load previous address data: {e}")
            return {}
    
    def save_seen_tokens(self):
        """Save seen token addresses to storage file."""
        try:
            data = {
                'seen_tokens': list(self.seen_tokens),
                'last_updated': datetime.now().isoformat()
            }
            with open(self.storage_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save seen tokens: {e}")
    
    def save_previous_address_data(self):
        """Save previous address data for comparison."""
        if self.mode != MonitorMode.SPECIFIC_ADDRESSES:
            return
        
        try:
            storage_file = Path(f"address_data_{self.storage_file.stem}.json")
            with open(storage_file, 'w') as f:
                json.dump(self.previous_address_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save previous address data: {e}")
    
    def save_token_data(self, new_tokens: List[Dict], data_type: str = "new_tokens"):
        """Save complete token data to data file."""
        try:
            # Load existing data if file exists
            existing_data = []
            if self.data_file.exists():
                try:
                    with open(self.data_file, 'r') as f:
                        existing_data = json.load(f)
                except json.JSONDecodeError:
                    self.logger.warning("Could not read existing token data file, starting fresh")
                    existing_data = []
            
            # Add new tokens with timestamp
            timestamp = datetime.now().isoformat()
            for token in new_tokens:
                token_entry = {
                    'detected_at': timestamp,
                    'data_type': data_type,
                    'token_data': token
                }
                existing_data.append(token_entry)
            
            # Save updated data
            with open(self.data_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            self.logger.info(f"Saved {len(new_tokens)} {data_type} to {self.data_file}")
            
        except Exception as e:
            self.logger.error(f"Could not save token data: {e}")
    
    def fetch_latest_tokens(self) -> Optional[List[Dict]]:
        """Fetch latest token profiles from DexScreener API."""
        try:
            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()
            
            tokens = response.json()
            if not isinstance(tokens, list):
                self.logger.error("API response is not a list")
                return None
                
            self.logger.debug(f"Fetched {len(tokens)} tokens from API")
            return tokens
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode API response: {e}")
            return None
    
    def fetch_token_pairs(self, chain_id: str, token_address: str) -> Optional[List[Dict]]:
        """Fetch token pairs data from DexScreener API."""
        try:
            url = f"{self.pairs_api_url}/{chain_id}/{token_address}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            pairs_data = response.json()
            if not isinstance(pairs_data, list):
                self.logger.warning(f"Token pairs API response is not a list for {chain_id}:{token_address}")
                return None
                
            self.logger.debug(f"Fetched {len(pairs_data)} pairs for token {chain_id}:{token_address}")
            return pairs_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Token pairs API request failed for {chain_id}:{token_address}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode token pairs API response for {chain_id}:{token_address}: {e}")
            return None
    
    def create_token_key(self, token: Dict) -> str:
        """Create a unique key for a token."""
        return f"{token.get('chainId', 'unknown')}:{token.get('tokenAddress', 'unknown')}"
    
    def format_token_info(self, token: Dict) -> str:
        """Format token information for display."""
        chain_id = token.get('chainId', 'Unknown')
        token_address = token.get('tokenAddress', 'Unknown')
        description = token.get('description', 'No description')
        url = token.get('url', 'No URL')
        
        # Extract social links
        links = token.get('links', [])
        social_links = []
        for link in links:
            if link.get('type') in ['twitter', 'telegram']:
                social_links.append(f"{link['type'].title()}: {link['url']}")
            elif link.get('label'):
                social_links.append(f"{link['label']}: {link['url']}")
        
        social_info = "\n    ".join(social_links) if social_links else "No social links"
        
        # Format pairs information if available
        pairs_info = "No pairs data available"
        if 'pairs_data' in token and token['pairs_data']:
            pairs_list = []
            for pair in token['pairs_data'][:5]:  # Show max 5 pairs to avoid clutter
                dex_id = pair.get('dexId', 'Unknown DEX')
                pair_address = pair.get('pairAddress', 'Unknown')
                base_symbol = pair.get('baseToken', {}).get('symbol', 'Unknown')
                quote_symbol = pair.get('quoteToken', {}).get('symbol', 'Unknown')
                price_usd = pair.get('priceUsd', 'Unknown')
                liquidity_usd = pair.get('liquidity', {}).get('usd', 'Unknown')
                
                pair_info = f"DEX: {dex_id.upper()} | Pair: {base_symbol}/{quote_symbol} | Address: {pair_address}"
                if price_usd != 'Unknown':
                    pair_info += f" | Price: ${price_usd}"
                if liquidity_usd != 'Unknown':
                    pair_info += f" | Liquidity: ${liquidity_usd:,.2f}" if isinstance(liquidity_usd, (int, float)) else f" | Liquidity: ${liquidity_usd}"
                
                pairs_list.append(pair_info)
            
            pairs_info = "\n    ".join(pairs_list)
            if len(token['pairs_data']) > 5:
                pairs_info += f"\n    ... and {len(token['pairs_data']) - 5} more pairs"
        
        return f"""
🚀 NEW TOKEN DETECTED!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Chain: {chain_id.upper()}
Token Address: {token_address}
Description: {description}
DexScreener URL: {url}
Social Links:
    {social_info}
Trading Pairs:
    {pairs_info}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    def filter_pairs_data(self, pairs_data: List[Dict]) -> List[Dict]:
        """Filter pairs data to only include Uniswap v2 and v3."""
        if not pairs_data:
            return pairs_data
        
        filtered_pairs = []
        for pair in pairs_data:
            dex_id = pair.get('dexId', '').lower()
            labels = pair.get('labels', [])
            
            # If it's Uniswap, only include v2 and v3
            if dex_id == 'uniswap':
                # Check labels for version information
                has_v2_or_v3 = any(label.lower() in ['v2', 'v3'] for label in labels)
                if has_v2_or_v3:
                    filtered_pairs.append(pair)
                # Skip other Uniswap versions (v1, v4, etc.)
            else:
                # Include all non-Uniswap DEXs
                filtered_pairs.append(pair)
        
        if len(filtered_pairs) != len(pairs_data):
            self.logger.debug(f"Filtered pairs: {len(pairs_data)} -> {len(filtered_pairs)} (removed non-v2/v3 Uniswap pairs)")
        
        return filtered_pairs

    def process_new_tokens(self, tokens: List[Dict]) -> List[Dict]:
        """Process tokens and return list of new ones with additional pairs data (Ethereum only)."""
        new_tokens = []
        current_token_keys = set()
        
        for token in tokens:
            # Only process Ethereum tokens
            chain_id = token.get('chainId', '').lower()
            if chain_id != 'ethereum':
                continue
            
            token_key = self.create_token_key(token)
            current_token_keys.add(token_key)
            
            if token_key not in self.seen_tokens:
                # Fetch additional pairs data for new tokens
                token_address = token.get('tokenAddress')
                
                if chain_id and token_address:
                    self.logger.info(f"Fetching pairs data for new Ethereum token: {token_address}")
                    pairs_data = self.fetch_token_pairs(chain_id, token_address)
                    
                    # Filter pairs to only include Uniswap v2 and v3
                    if pairs_data:
                        pairs_data = self.filter_pairs_data(pairs_data)
                    
                    token['pairs_data'] = pairs_data
                    
                    # Add rate limiting to respect API limits (300 requests per minute)
                    time.sleep(0.2)  # 200ms delay = max 300 requests per minute
                else:
                    self.logger.warning(f"Missing chainId or tokenAddress for token: {token}")
                    token['pairs_data'] = None
                
                new_tokens.append(token)
                self.seen_tokens.add(token_key)
        
        # Save updated seen tokens and token data
        if new_tokens:
            self.save_seen_tokens()
            self.save_token_data(new_tokens)
        
        return new_tokens
    
    def alert_new_tokens(self, new_tokens: List[Dict]):
        """Alert about new tokens found."""
        for token in new_tokens:
            token_info = self.format_token_info(token)
            self.logger.info(token_info)

    def parse_address_input(self, address_input: str) -> tuple[str, str]:
        """Parse address input in format 'chain:address' or just 'address' (defaults to ethereum)."""
        if ':' in address_input:
            chain, address = address_input.split(':', 1)
            return chain.lower(), address.lower()
        else:
            return 'ethereum', address_input.lower()
    
    def fetch_address_data(self, chain_id: str, token_address: str) -> Optional[Dict]:
        """Fetch comprehensive data for a specific address including pairs."""
        try:
            # Fetch pairs data
            pairs_data = self.fetch_token_pairs(chain_id, token_address)
            
            if pairs_data:
                # Filter pairs to only include Uniswap v2 and v3
                pairs_data = self.filter_pairs_data(pairs_data)
                
                # Create a comprehensive token data structure
                token_data = {
                    'chainId': chain_id,
                    'tokenAddress': token_address,
                    'pairs_data': pairs_data,
                    'pair_count': len(pairs_data) if pairs_data else 0,
                    'fetched_at': datetime.now().isoformat()
                }
                
                # Extract token info from first pair if available
                if pairs_data and len(pairs_data) > 0:
                    first_pair = pairs_data[0]
                    base_token = first_pair.get('baseToken', {})
                    quote_token = first_pair.get('quoteToken', {})
                    
                    # Determine which token matches our address
                    target_token = base_token if base_token.get('address', '').lower() == token_address.lower() else quote_token
                    
                    token_data.update({
                        'symbol': target_token.get('symbol', 'Unknown'),
                        'name': target_token.get('name', 'Unknown'),
                        'total_liquidity_usd': sum(
                            pair.get('liquidity', {}).get('usd', 0) 
                            for pair in pairs_data 
                            if isinstance(pair.get('liquidity', {}).get('usd'), (int, float))
                        )
                    })
                
                return token_data
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to fetch data for {chain_id}:{token_address}: {e}")
            return None
    
    def check_address_changes(self, address_key: str, current_data: Dict) -> Dict:
        """Check for changes in address data and return change summary."""
        previous_data = self.previous_address_data.get(address_key, {})
        changes = {}
        
        if not previous_data:
            changes['new_address'] = True
            return changes
        
        # Check for pair count changes
        prev_pair_count = previous_data.get('pair_count', 0)
        curr_pair_count = current_data.get('pair_count', 0)
        if curr_pair_count != prev_pair_count:
            changes['pair_count_change'] = {
                'previous': prev_pair_count,
                'current': curr_pair_count,
                'difference': curr_pair_count - prev_pair_count
            }
        
        # Check for liquidity changes (significant changes only)
        prev_liquidity = previous_data.get('total_liquidity_usd', 0)
        curr_liquidity = current_data.get('total_liquidity_usd', 0)
        if prev_liquidity > 0 and curr_liquidity > 0:
            change_percent = abs(curr_liquidity - prev_liquidity) / prev_liquidity
            if change_percent > 0.1:  # 10% threshold
                changes['liquidity_change'] = {
                    'previous': prev_liquidity,
                    'current': curr_liquidity,
                    'change_percent': change_percent * 100,
                    'direction': 'increase' if curr_liquidity > prev_liquidity else 'decrease'
                }
        
        return changes
    
    def format_address_info(self, address_key: str, token_data: Dict, changes: Dict) -> str:
        """Format address information for display."""
        chain_id = token_data.get('chainId', 'Unknown')
        token_address = token_data.get('tokenAddress', 'Unknown')
        symbol = token_data.get('symbol', 'Unknown')
        name = token_data.get('name', 'Unknown')
        pair_count = token_data.get('pair_count', 0)
        total_liquidity = token_data.get('total_liquidity_usd', 0)
        
        # Format changes
        change_info = []
        if changes.get('new_address'):
            change_info.append("🆕 NEW ADDRESS DETECTED")
        
        if 'pair_count_change' in changes:
            pc = changes['pair_count_change']
            direction = "increased" if pc['difference'] > 0 else "decreased"
            change_info.append(f"📊 Pair count {direction}: {pc['previous']} → {pc['current']} ({pc['difference']:+d})")
        
        if 'liquidity_change' in changes:
            lc = changes['liquidity_change']
            direction_emoji = "📈" if lc['direction'] == 'increase' else "📉"
            change_info.append(f"{direction_emoji} Liquidity {lc['direction']}: ${lc['previous']:,.2f} → ${lc['current']:,.2f} ({lc['change_percent']:+.1f}%)")
        
        changes_text = "\n".join(change_info) if change_info else "No significant changes"
        
        # Format pairs information
        pairs_info = "No pairs data available"
        if token_data.get('pairs_data'):
            pairs_list = []
            for pair in token_data['pairs_data'][:5]:
                dex_id = pair.get('dexId', 'Unknown DEX')
                pair_address = pair.get('pairAddress', 'Unknown')
                base_symbol = pair.get('baseToken', {}).get('symbol', 'Unknown')
                quote_symbol = pair.get('quoteToken', {}).get('symbol', 'Unknown')
                price_usd = pair.get('priceUsd', 'Unknown')
                liquidity_usd = pair.get('liquidity', {}).get('usd', 'Unknown')
                
                pair_info = f"DEX: {dex_id.upper()} | Pair: {base_symbol}/{quote_symbol} | Address: {pair_address}"
                if price_usd != 'Unknown':
                    pair_info += f" | Price: ${price_usd}"
                if liquidity_usd != 'Unknown':
                    if isinstance(liquidity_usd, (int, float)):
                        pair_info += f" | Liquidity: ${liquidity_usd:,.2f}"
                    else:
                        pair_info += f" | Liquidity: ${liquidity_usd}"
                
                pairs_list.append(pair_info)
            
            pairs_info = "\n    ".join(pairs_list)
            if len(token_data['pairs_data']) > 5:
                pairs_info += f"\n    ... and {len(token_data['pairs_data']) - 5} more pairs"
        
        return f"""
🔍 ADDRESS MONITOR UPDATE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Chain: {chain_id.upper()}
Token Address: {token_address}
Symbol: {symbol}
Name: {name}
Total Pairs: {pair_count}
Total Liquidity: ${total_liquidity:,.2f}" if isinstance(total_liquidity, (int, float)) else f"Total Liquidity: ${total_liquidity}

Changes:
{changes_text}

Trading Pairs:
    {pairs_info}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

    def monitor_specific_addresses(self) -> List[Dict]:
        """Monitor specific addresses for changes."""
        address_updates = []
        
        for address_input in self.monitor_addresses:
            try:
                chain_id, token_address = self.parse_address_input(address_input)
                address_key = f"{chain_id}:{token_address}"
                
                self.logger.info(f"Checking address: {address_key}")
                
                # Fetch current data
                current_data = self.fetch_address_data(chain_id, token_address)
                
                if current_data:
                    # Check for changes
                    changes = self.check_address_changes(address_key, current_data)
                    
                    if changes:
                        # Format and display changes
                        update_info = self.format_address_info(address_key, current_data, changes)
                        self.logger.info(update_info)
                        
                        address_updates.append({
                            'address_key': address_key,
                            'data': current_data,
                            'changes': changes
                        })
                    
                    # Update previous data
                    self.previous_address_data[address_key] = current_data
                else:
                    self.logger.warning(f"Could not fetch data for address: {address_key}")
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                self.logger.error(f"Error monitoring address {address_input}: {e}")
        
        # Save updated previous data
        if address_updates:
            self.save_previous_address_data()
        
        return address_updates
    
    def run_once_addresses(self) -> bool:
        """Run one iteration of address monitoring."""
        self.logger.info(f"Checking {len(self.monitor_addresses)} specific addresses...")
        
        updates = self.monitor_specific_addresses()
        
        if updates:
            self.logger.info(f"Found updates for {len(updates)} address(es)!")
            self.save_token_data([update['data'] for update in updates], "address_updates")
        else:
            self.logger.info("No significant changes detected for monitored addresses")
        
        return True
    
    def run_once_latest(self) -> bool:
        """Run one iteration of latest tokens monitoring."""
        self.logger.info("Checking for new tokens...")
        
        tokens = self.fetch_latest_tokens()
        if tokens is None:
            return False
        
        new_tokens = self.process_new_tokens(tokens)
        
        if new_tokens:
            self.logger.info(f"Found {len(new_tokens)} new tokens!")
            self.alert_new_tokens(new_tokens)
        else:
            self.logger.info("No new tokens found")
        
        return True
    
    def run_once(self) -> bool:
        """Run one iteration of the monitor based on the current mode."""
        if self.mode == MonitorMode.SPECIFIC_ADDRESSES:
            return self.run_once_addresses()
        else:
            return self.run_once_latest()
    
    def run_forever(self):
        """Run the monitor continuously."""
        mode_str = "address monitoring" if self.mode == MonitorMode.SPECIFIC_ADDRESSES else "latest token monitoring"
        self.logger.info(f"Starting DexScreener monitor in {mode_str} mode (checking every {self.poll_interval} seconds)")
        
        while True:
            try:
                success = self.run_once()
                if not success:
                    self.logger.warning("Check failed, will retry next cycle")
                
            except KeyboardInterrupt:
                self.logger.info("Monitor stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
            
            time.sleep(self.poll_interval)
    
    def reset_seen_tokens(self):
        """Reset the list of seen tokens (useful for testing)."""
        self.seen_tokens.clear()
        if self.storage_file.exists():
            self.storage_file.unlink()
        if self.data_file.exists():
            self.data_file.unlink()
        self.logger.info("Reset seen tokens list and token data")

def main():
    parser = argparse.ArgumentParser(
        description="Monitor DexScreener for tokens",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor for new tokens (default mode)
  python3 dexscreener_monitor.py
  
  # Monitor specific Ethereum addresses
  python3 dexscreener_monitor.py --addresses 0x1234...abcd 0x5678...efgh
  
  # Monitor addresses on different chains
  python3 dexscreener_monitor.py --addresses ethereum:0x1234...abcd bsc:0x5678...efgh
  
  # Run once and exit
  python3 dexscreener_monitor.py --once --addresses 0x1234...abcd
  
  # Custom polling interval
  python3 dexscreener_monitor.py --interval 30 --addresses 0x1234...abcd
        """
    )
    
    # Mode selection
    parser.add_argument("--addresses", "-a", nargs="+", type=str,
                        help="Monitor specific addresses (format: 'address' or 'chain:address'). "
                             "If no chain specified, defaults to ethereum. "
                             "Example: --addresses 0x1234...abcd ethereum:0x5678...efgh bsc:0x9abc...def0")
    
    # Execution mode
    parser.add_argument("--once", "-o", action="store_true",
                        help="Run once and exit (don't run continuously)")
    
    # Configuration
    parser.add_argument("--interval", "-i", type=int, default=60,
                        help="Polling interval in seconds (default: 60)")
    parser.add_argument("--storage", "-s", type=str, default="seen_tokens.json",
                        help="File to store seen tokens (default: seen_tokens.json)")
    parser.add_argument("--data", "-d", type=str, default="new_tokens_data.json",
                        help="File to store complete token data (default: new_tokens_data.json)")
    parser.add_argument("--log", "-l", type=str, default="dexscreener_monitor.log",
                        help="Log file name (default: dexscreener_monitor.log)")
    
    # Utility
    parser.add_argument("--reset", "-r", action="store_true",
                        help="Reset seen tokens list and token data, then exit")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.addresses and len(args.addresses) == 0:
        parser.error("--addresses requires at least one address")
    
    # Create monitor instance
    monitor = DexScreenerMonitor(
        poll_interval=args.interval,
        storage_file=args.storage,
        data_file=args.data,
        log_file=args.log,
        monitor_addresses=args.addresses
    )
    
    # Handle reset command
    if args.reset:
        monitor.reset_seen_tokens()
        return
    
    # Run monitor
    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()

if __name__ == "__main__":
    main() 
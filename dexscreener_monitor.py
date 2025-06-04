#!/usr/bin/env python3
"""
DexScreener Token Monitor
=========================

A tool to monitor DexScreener API for new token profiles and alert when new tokens appear.

Features:
- Fetches latest token profiles from DexScreener API
- Tracks previously seen tokens to detect new ones
- Fetches additional token pairs data (chain, dex, pool info)
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
from typing import Dict, List, Set, Optional
from pathlib import Path
import argparse

class DexScreenerMonitor:
    def __init__(self, 
                 poll_interval: int = 60,
                 storage_file: str = "seen_tokens.json",
                 data_file: str = "new_tokens_data.json",
                 log_file: str = "dexscreener_monitor.log"):
        """
        Initialize the DexScreener monitor.
        
        Args:
            poll_interval: Time in seconds between API checks
            storage_file: File to store previously seen tokens
            data_file: File to store complete token data
            log_file: File to store logs
        """
        self.api_url = "https://api.dexscreener.com/token-profiles/latest/v1"
        self.pairs_api_url = "https://api.dexscreener.com/token-pairs/v1"
        self.poll_interval = poll_interval
        self.storage_file = Path(storage_file)
        self.data_file = Path(data_file)
        self.log_file = Path(log_file)
        
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
        
        # Load previously seen tokens
        self.seen_tokens: Set[str] = self.load_seen_tokens()
        
        self.logger.info(f"Monitor initialized with {len(self.seen_tokens)} previously seen tokens")
    
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
    
    def save_token_data(self, new_tokens: List[Dict]):
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
                    'token_data': token
                }
                existing_data.append(token_entry)
            
            # Save updated data
            with open(self.data_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            self.logger.info(f"Saved {len(new_tokens)} new token(s) to {self.data_file}")
            
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

    
    def run_once(self) -> bool:
        """Run one iteration of the monitor. Returns True if successful."""
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
    
    def run_forever(self):
        """Run the monitor continuously."""
        self.logger.info(f"Starting DexScreener monitor (checking every {self.poll_interval} seconds)")
        
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
    parser = argparse.ArgumentParser(description="Monitor DexScreener for new tokens")
    parser.add_argument("--interval", "-i", type=int, default=60,
                        help="Polling interval in seconds (default: 60)")
    parser.add_argument("--storage", "-s", type=str, default="seen_tokens.json",
                        help="File to store seen tokens (default: seen_tokens.json)")
    parser.add_argument("--data", "-d", type=str, default="new_tokens_data.json",
                        help="File to store complete token data (default: new_tokens_data.json)")
    parser.add_argument("--log", "-l", type=str, default="dexscreener_monitor.log",
                        help="Log file name (default: dexscreener_monitor.log)")
    parser.add_argument("--reset", "-r", action="store_true",
                        help="Reset seen tokens list and token data, then exit")
    parser.add_argument("--once", "-o", action="store_true",
                        help="Run once and exit (don't run continuously)")
    
    args = parser.parse_args()
    
    monitor = DexScreenerMonitor(
        poll_interval=args.interval,
        storage_file=args.storage,
        data_file=args.data,
        log_file=args.log
    )
    
    if args.reset:
        monitor.reset_seen_tokens()
        return
    
    if args.once:
        monitor.run_once()
    else:
        monitor.run_forever()

if __name__ == "__main__":
    main() 
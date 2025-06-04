import hypersync
import asyncio
import json
import pandas as pd
import os
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from hypersync import BlockField, TransactionField, LogField, ClientConfig
from typing import List, Dict, Any, Optional, Set
from collections import defaultdict

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
        
        return pd.DataFrame(transfers_data)
    
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
        
        return pd.DataFrame(swaps_data)
    
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
        
        return pd.DataFrame(events_data)
    
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
                combined_swaps_v2.to_excel(writer, sheet_name='Swaps_V2', index=False)
                print(f"   ✅ Exported {len(combined_swaps_v2)} V2 Swap events to 'Swaps_V2' sheet")
            
            if all_swaps_v3:
                combined_swaps_v3 = pd.concat(all_swaps_v3, ignore_index=True)
                combined_swaps_v3.to_excel(writer, sheet_name='Swaps_V3', index=False)
                print(f"   ✅ Exported {len(combined_swaps_v3)} V3 Swap events to 'Swaps_V3' sheet")
            
            if all_mints:
                combined_mints = pd.concat(all_mints, ignore_index=True)
                combined_mints.to_excel(writer, sheet_name='Mints', index=False)
                print(f"   ✅ Exported {len(combined_mints)} Mint events to 'Mints' sheet")
            
            if all_burns:
                combined_burns = pd.concat(all_burns, ignore_index=True)
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
            
            # 5. Fetch current balances using Alchemy API
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
            
            # 6. Create summary sheet
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
                    "Alchemy Balances Fetched"
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
                    len(alchemy_balances_df) if 'alchemy_balances_df' in locals() and not alchemy_balances_df.empty else 0
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
            print(f"   📈 Records with token balance: {len(balance_df[balance_df['token_balance_raw'].notna()])}")
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
    
    # Analyze each token and export to Excel
    processed_files = []
    max_tokens = 5  # Process first 5 tokens - adjust as needed
    
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
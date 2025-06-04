import hypersync
import asyncio
import json
from hypersync import BlockField, TransactionField, LogField, ClientConfig
from typing import List, Dict, Any, Optional

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

class TokenAnalytics:
    def __init__(self):
        self.client = hypersync.HypersyncClient(ClientConfig())
        self.decoders = {
            "Transfer": hypersync.Decoder([EVENT_SIGNATURES["Transfer"]["signature"]]),
            "V2_Swap": hypersync.Decoder([EVENT_SIGNATURES["V2_Swap"]["signature"]]),
            "V2_Mint": hypersync.Decoder([EVENT_SIGNATURES["V2_Mint"]["signature"]]),
            "V2_Burn": hypersync.Decoder([EVENT_SIGNATURES["V2_Burn"]["signature"]]),
            "V3_Swap": hypersync.Decoder([EVENT_SIGNATURES["V3_Swap"]["signature"]]),
            "V3_Mint": hypersync.Decoder([EVENT_SIGNATURES["V3_Mint"]["signature"]]),
            "V3_Burn": hypersync.Decoder([EVENT_SIGNATURES["V3_Burn"]["signature"]]),
        }
    
    def load_token_data(self, filename: str) -> List[Dict]:
        """Load token data from JSON file."""
        with open(filename, 'r') as f:
            return json.load(f)
    
    async def fetch_transfer_events(self, token_address: str, from_block: int = 0, to_block: Optional[int] = None) -> Dict[str, Any]:
        """Fetch Transfer events for a specific token."""
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
                    LogField.BLOCK_NUMBER,  # Add block number to log fields
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
        
        print(f"🔄 Fetching Transfer events for token {token_address}...")
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
        """Fetch Swap, Mint, and Burn events for a specific trading pair."""
        
        # Validate pair address length (should be 42 characters for Ethereum addresses: 0x + 40 hex chars)
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
            # V4 uses similar events to V3 for now
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
                    LogField.BLOCK_NUMBER,  # Add block number to log fields
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
        
        print(f"🔄 Fetching pair events for {pair_version} pair {pair_address}...")
        res = await self.client.get(query)
        
        # Separate and decode logs by event type
        events_by_type = {}
        for event_type in event_types:
            event_hash = EVENT_SIGNATURES[event_type]["hash"]
            # Filter logs by topic0 (event signature hash)
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
    
    def decode_transfer_event(self, raw_log, decoded_log) -> Dict[str, Any]:
        """Decode a Transfer event into human-readable format."""
        if decoded_log is None:
            return {"error": "Failed to decode"}
        
        # For Transfer: indexed from (topic1), indexed to (topic2), value (data)
        from_address = raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A"
        to_address = raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A"
        value = decoded_log.body[0].val if len(decoded_log.body) > 0 and decoded_log.body[0] else 0
        
        return {
            "block": raw_log.block_number,
            "transaction": raw_log.transaction_hash,
            "from": from_address,
            "to": to_address,
            "value": value
        }
    
    def decode_swap_event(self, raw_log, decoded_log, version: str) -> Dict[str, Any]:
        """Decode a Swap event into human-readable format."""
        if decoded_log is None:
            return {"error": "Failed to decode"}
        
        result = {
            "block": raw_log.block_number,
            "transaction": raw_log.transaction_hash,
        }
        
        if "V2" in version:
            # V2 Swap: indexed sender (topic1), amounts in data, indexed to (topic2)
            result.update({
                "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                "to": raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A",
                "amount0In": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                "amount1In": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                "amount0Out": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                "amount1Out": decoded_log.body[3].val if len(decoded_log.body) > 3 else 0,
            })
        else:  # V3
            # V3 Swap: indexed sender (topic1), indexed recipient (topic2), amounts in data
            result.update({
                "sender": raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A",
                "recipient": raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A",
                "amount0": decoded_log.body[0].val if len(decoded_log.body) > 0 else 0,
                "amount1": decoded_log.body[1].val if len(decoded_log.body) > 1 else 0,
                "sqrtPriceX96": decoded_log.body[2].val if len(decoded_log.body) > 2 else 0,
                "liquidity": decoded_log.body[3].val if len(decoded_log.body) > 3 else 0,
                "tick": decoded_log.body[4].val if len(decoded_log.body) > 4 else 0,
            })
        
        return result
    
    async def analyze_token(self, token_data: Dict[str, Any], from_block: int = 0, limit_events: int = 10000) -> Dict[str, Any]:
        """Analyze a single token: fetch transfers and pair events."""
        token_address = token_data["token_data"]["tokenAddress"]
        
        print(f"\n🎯 Analyzing token: {token_address}")
        
        # Fetch transfer events
        transfer_count = 0
        transfer_error = None
        sample_transfers = []
        
        try:
            transfer_results = await self.fetch_transfer_events(token_address, from_block)
            transfer_count = transfer_results['count']
            print(f"   ✅ Found {transfer_count} Transfer events")
            
            # Sample and decode some events for display
            if transfer_results.get("decoded_logs"):
                for i, (raw_log, decoded_log) in enumerate(zip(transfer_results["raw_logs"], transfer_results["decoded_logs"])):
                    if i >= limit_events:
                        break
                    sample_transfers.append(self.decode_transfer_event(raw_log, decoded_log))
                    
        except Exception as e:
            print(f"   ❌ Error fetching Transfer events: {e}")
            transfer_error = str(e)
        
        # Fetch pair events for each trading pair
        clean_pair_results = []
        sample_swaps = []
        pairs_data = token_data["token_data"].get("pairs_data", [])
        
        for pair_info in pairs_data:
            pair_address = pair_info["pairAddress"]
            pair_version = "v" + str(pair_info.get("labels", ["2"])[0]) if pair_info.get("labels") else "v2"
            
            try:
                pair_events = await self.fetch_pair_events(pair_address, pair_version, from_block)
                total_events = sum(event_data["count"] for event_data in pair_events["events"].values()) if "events" in pair_events else 0
                print(f"   ✅ Pair {pair_address} ({pair_version}): {total_events} total events")
                
                # Create clean pair data (without raw objects)
                clean_pair_data = {
                    "pair_address": pair_events["pair_address"],
                    "pair_version": pair_events["pair_version"],
                    "total_logs": pair_events["total_logs"],
                    "events_summary": {}
                }
                
                # Add event counts only
                if "events" in pair_events:
                    for event_type, event_data in pair_events["events"].items():
                        clean_pair_data["events_summary"][event_type] = {
                            "count": event_data["count"]
                        }
                        
                        # Sample swap events
                        if "Swap" in event_type and event_data["decoded_logs"] and len(sample_swaps) < limit_events:
                            version = "V2" if "V2" in event_type else "V3"
                            for i, (raw_log, decoded_log) in enumerate(zip(event_data["raw_logs"], event_data["decoded_logs"])):
                                if i >= limit_events or len(sample_swaps) >= limit_events:
                                    break
                                sample_swaps.append(self.decode_swap_event(raw_log, decoded_log, version))
                
                if "error" in pair_events:
                    clean_pair_data["error"] = pair_events["error"]
                
                clean_pair_results.append(clean_pair_data)
                
            except Exception as e:
                print(f"   ❌ Error fetching pair events for {pair_address}: {e}")
                clean_pair_results.append({
                    "pair_address": pair_address, 
                    "error": str(e)
                })
        
        return {
            "token_address": token_address,
            "transfer_events": {
                "count": transfer_count,
                "token_address": token_address,
                "event_type": "Transfer",
                "error": transfer_error
            },
            "pair_events": clean_pair_results,
            "sample_transfers": sample_transfers[:limit_events],
            "sample_swaps": sample_swaps[:limit_events]
        }

async def main():
    analyzer = TokenAnalytics()
    
    # Load token data
    try:
        tokens_data = analyzer.load_token_data("new_tokens_data.json")
        print(f"📊 Loaded {len(tokens_data)} tokens from new_tokens_data.json")
    except Exception as e:
        print(f"❌ Error loading token data: {e}")
        return
    
    # Analyze each token (limit to first few for testing)
    analysis_results = []
    max_tokens = 3  # Limit for testing - change this to process more tokens
    
    for i, token_data in enumerate(tokens_data[:max_tokens]):
        try:
            result = await analyzer.analyze_token(token_data, from_block=0, limit_events=5)
            analysis_results.append(result)
            
            # Display sample results
            print(f"\n📈 Sample Results for {result['token_address']}:")
            
            if result["sample_transfers"]:
                print("   🔄 Sample Transfers:")
                for transfer in result["sample_transfers"]:
                    if "error" not in transfer:
                        print(f"      Block {transfer['block']}: {transfer['value']} tokens from {transfer['from'][:10]}... to {transfer['to'][:10]}...")
            
            if result["sample_swaps"]:
                print("   💱 Sample Swaps:")
                for swap in result["sample_swaps"]:
                    if "error" not in swap:
                        if "amount0In" in swap:  # V2
                            print(f"      Block {swap['block']}: V2 Swap - In: {swap['amount0In']},{swap['amount1In']} Out: {swap['amount0Out']},{swap['amount1Out']}")
                        else:  # V3
                            print(f"      Block {swap['block']}: V3 Swap - Amounts: {swap['amount0']},{swap['amount1']} Tick: {swap['tick']}")
            
        except Exception as e:
            print(f"❌ Error analyzing token {i+1}: {e}")
    
    print(f"\n✅ Analysis complete! Processed {len(analysis_results)} tokens")
    
    # Save results to file
    with open("token_analysis_results.json", "w") as f:
        json.dump(analysis_results, f, indent=2, default=str)
    print("💾 Results saved to token_analysis_results.json")

if __name__ == "__main__":
    asyncio.run(main()) 
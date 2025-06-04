import hypersync
import asyncio
from hypersync import BlockField, TransactionField, LogField, ClientConfig

# returns all logs of swap events from a uniswap v3 pool within a block range and decodes them

async def main():
    # Create hypersync client using the ethereum mainnet hypersync endpoint (default)
    client = hypersync.HypersyncClient(ClientConfig())

    # Uniswap V3 pool address - update this to your desired pool
    pool = "0xa339d4c41ad791e27a10cd0f9a80deec815b79ee"

    # Uniswap V3 Swap event signature and topic0 hash
    # Swap(address sender, address recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
    event_topic_0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    
    # Define the query using the more comprehensive Query structure
    query = hypersync.Query(
        from_block=0,
        # to_block=20_333_826,  # Uncomment and set if you want to limit the range
        logs=[
            hypersync.LogSelection(
                address=[pool],
                topics=[
                    [event_topic_0]
                ]
            )
        ],
        # Select the fields we are interested in
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
            ],
            transaction=[
                TransactionField.BLOCK_NUMBER,
                TransactionField.TRANSACTION_INDEX,
                TransactionField.HASH,
                TransactionField.FROM,
                TransactionField.TO,
                TransactionField.VALUE,
                TransactionField.INPUT,
            ],
        ),
    )

    # Create decoder for Uniswap V3 Swap events
    decoder = hypersync.Decoder([
        "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
    ])

    print("Running the query...")

    # Run the query once, the query is automatically paginated so it will return when it reaches some limit (time, response size etc.)
    # there is a next_block field on the response object so we can set the from_block of our query to this value and continue our query until
    # res.next_block is equal to res.archive_height or query.to_block in case we specified an end block.
    res = await client.get(query)

    print(f"Query returned {len(res.data.logs)} logs of swap events from contract {pool}")

    # Decode the logs
    decoded_logs = await decoder.decode_logs(res.data.logs)
    
    # Process and display the decoded logs
    for i, log in enumerate(decoded_logs):
        if log is None:
            print(f"Log {i}: Failed to decode")
            continue
        
        # Get the corresponding raw log for additional info
        raw_log = res.data.logs[i]
        
        # Debug: Print the structure of the decoded log
        print(f"\nLog {i} debug info:")
        print(f"  log.body length: {len(log.body) if log.body else 0}")
        print(f"  raw_log topics: {raw_log.topics if hasattr(raw_log, 'topics') else 'No topics'}")
        if log.body:
            for j, param in enumerate(log.body):
                print(f"  log.body[{j}]: {param.val if param else 'None'}")
        
        # Extract parameters correctly:
        # Indexed parameters are in topics, non-indexed are in log.body
        # topics[0] is the event signature hash, topics[1] is first indexed param, etc.
        sender = raw_log.topics[1] if len(raw_log.topics) > 1 else "N/A"  # topic1 = sender (indexed)
        recipient = raw_log.topics[2] if len(raw_log.topics) > 2 else "N/A"  # topic2 = recipient (indexed)
        
        # Non-indexed parameters are in log.body
        amount0 = log.body[0].val if len(log.body) > 0 and log.body[0] else 0
        amount1 = log.body[1].val if len(log.body) > 1 and log.body[1] else 0
        sqrt_price_x96 = log.body[2].val if len(log.body) > 2 and log.body[2] else 0
        liquidity = log.body[3].val if len(log.body) > 3 and log.body[3] else 0
        tick = log.body[4].val if len(log.body) > 4 and log.body[4] else 0
        
        print(f"\n--- Swap Event {i+1} ---")
        print(f"Block: {raw_log.block_number}")
        print(f"Transaction: {raw_log.transaction_hash}")
        print(f"Sender: {sender}")
        print(f"Recipient: {recipient}")
        print(f"Amount0: {amount0}")
        print(f"Amount1: {amount1}")
        print(f"SqrtPriceX96: {sqrt_price_x96}")
        print(f"Liquidity: {liquidity}")
        print(f"Tick: {tick}")
        
        # Stop after first few logs for debugging
        if i >= 2:
            print(f"\n... stopping after first {i+1} logs for debugging ...")
            break

    print(f"\nTotal processed: {len([log for log in decoded_logs if log is not None])} valid swap events")

asyncio.run(main())
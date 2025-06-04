import pandas as pd
from collections import defaultdict
from io import StringIO
import networkx as nx
from community import best_partition # This is from the python-louvain library

def analyze_rugpuller_cluster(data_string: str, token_address_to_exclude: str, pair_address_to_exclude: str):
    """
    Analyzes transaction data to identify one primary rug puller trading cluster.
    Uses graph-based community detection.
    """
    try:
        df = pd.read_csv(StringIO(data_string), sep='\t', dtype=str)
        df.columns = df.columns.str.strip()
    except Exception as e:
        return {"error": f"Failed to parse CSV data: {e}"}

    # --- 1. Data Preprocessing ---
    df['value_formatted'] = df['value_formatted'].str.replace(',', '', regex=False).astype(float)
    df['initiators'] = df['initiators'].str.lower().fillna('')
    df['from_address'] = df['from_address'].str.lower().fillna('')
    df['to_address'] = df['to_address'].str.lower().fillna('')
    df['block_number'] = pd.to_numeric(df['block_number'], errors='coerce').fillna(0).astype(int)

    excluded_addresses = set()
    if token_address_to_exclude: excluded_addresses.add(token_address_to_exclude.lower())
    if pair_address_to_exclude: excluded_addresses.add(pair_address_to_exclude.lower())

    G = nx.Graph()
    source_addresses = set()
    all_swappers = set()
    address_swap_actions = defaultdict(list)
    address_attributes = defaultdict(lambda: {"is_source": False, "is_swapper": False, "swap_count": 0})

    # Identify Sources & Swappers, Collect Swap Actions
    # Source from initial transfer
    initial_transfers = df[(df['event_type'] == 'Transfer') & (df['from_address'] == '0x0000000000000000000000000000000000000000')]
    for _, row in initial_transfers.iterrows():
        recipient = row['to_address']
        if recipient and recipient != 'nan' and recipient not in excluded_addresses:
            source_addresses.add(recipient)
            address_attributes[recipient]['is_source'] = True
            if recipient not in G: G.add_node(recipient, type='source')

    # Source from Mint events
    mint_events = df[df['event_type'] == 'Mint']
    for _, row in mint_events.iterrows():
        minter = row['initiators']
        if minter and minter != 'nan' and minter not in excluded_addresses:
            source_addresses.add(minter)
            address_attributes[minter]['is_source'] = True
            if minter not in G: G.add_node(minter, type='source')
            
            # If mint logs a recipient different from initiator (and not excluded)
            mint_recipient = row['to_address']
            if mint_recipient and mint_recipient != 'nan' and mint_recipient not in excluded_addresses and mint_recipient != minter:
                if minter not in G: G.add_node(minter) # Ensure minter node exists
                if mint_recipient not in G: G.add_node(mint_recipient)
                G.add_edge(minter, mint_recipient, weight=5, type='mint_funding', block=row['block_number'])


    v2_swaps_df = df[df['event_type'] == 'V2_Swap']
    for _, row in v2_swaps_df.iterrows():
        initiator = row['initiators']
        if initiator and initiator != 'nan' and initiator not in excluded_addresses:
            all_swappers.add(initiator)
            address_attributes[initiator]['is_swapper'] = True
            address_attributes[initiator]['swap_count'] += 1
            if initiator not in G: G.add_node(initiator, type='swapper')
            address_swap_actions[initiator].append({
                "block": row['block_number'], "type": row['transaction_type'], "value": row['value_formatted']
            })

    # --- 2. Graph Construction - Add Edges ---
    # Direct Funding from Source
    transfers_df = df[df['event_type'] == 'Transfer']
    for _, row in transfers_df.iterrows():
        from_addr, to_addr = row['from_address'], row['to_address']
        if from_addr in source_addresses and to_addr and to_addr != 'nan' and to_addr not in excluded_addresses and from_addr != to_addr:
            if from_addr not in G: G.add_node(from_addr) # Ensure source node exists
            if to_addr not in G: G.add_node(to_addr)
            G.add_edge(from_addr, to_addr, weight=5.0, type='source_funding', block=row['block_number']) # High weight for source funding

    # Coordinated Swaps (Same Block, Identical Action)
    grouped_swaps = v2_swaps_df.groupby(['block_number', 'transaction_type', 'value_formatted'])
    for group_key, group_df in grouped_swaps:
        block, tx_type, val = group_key
        unique_initiators_in_group = group_df['initiators'].dropna().unique()
        
        valid_initiators = [i for i in unique_initiators_in_group if i and i != 'nan' and i not in excluded_addresses]
        if len(valid_initiators) > 1:
            # Add edges between all pairs in this coordinated group
            for i in range(len(valid_initiators)):
                for j in range(i + 1, len(valid_initiators)):
                    addr1, addr2 = valid_initiators[i], valid_initiators[j]
                    if addr1 not in G: G.add_node(addr1)
                    if addr2 not in G: G.add_node(addr2)
                    G.add_edge(addr1, addr2, weight=10.0, type='coordinated_swap', block=block, action_details=f"{tx_type}_{val}") # Very high weight

    # --- 3. Community Detection ---
    if not G.nodes() or not G.edges():
        return {"message": "Graph has no nodes or edges; cannot perform community detection.", "confidence_level": "None"}

    try:
        partition = best_partition(G, weight='weight', random_state=42) # random_state for reproducibility
        # Invert partition to get clusters: {community_id: [addresses]}
        clusters_raw = defaultdict(list)
        for node, community_id in partition.items():
            clusters_raw[community_id].append(node)
        
        communities = [addrs for addrs in clusters_raw.values() if len(addrs) > 0]
    except Exception as e:
         return {"message": f"Community detection failed: {e}", "confidence_level": "None"}


    # --- 4. Identify the "Rug Puller Trading Cluster" ---
    best_cluster_info = None
    max_score = -1

    for i, cluster_addresses in enumerate(communities):
        if not cluster_addresses: continue

        current_score = 0
        reasoning_parts = []
        
        num_members = len(cluster_addresses)
        cluster_is_source_linked = any(address_attributes[addr]['is_source'] for addr in cluster_addresses) or \
                                   any(any(data['type'] == 'source_funding' for u,v,data in G.edges(addr, data=True)) for addr in cluster_addresses if G.has_node(addr))


        total_buys = 0
        total_sells = 0
        coordinated_swap_participation_count = 0
        
        # Check for "coordinated_swap" links *within* the cluster
        subgraph = G.subgraph(cluster_addresses)
        for u, v, data in subgraph.edges(data=True):
            if data.get('type') == 'coordinated_swap':
                coordinated_swap_participation_count +=1 # Count edges, or unique events
        
        if coordinated_swap_participation_count > 0:
            current_score += num_members * 2 # Bonus for size of coordinated group
            current_score += coordinated_swap_participation_count * 2.5 # Bonus for number of internal coord links
            reasoning_parts.append(f"{num_members} addresses show {coordinated_swap_participation_count} internal links from same-block-identical-value swaps.")

        for addr in cluster_addresses:
            for swap in address_swap_actions.get(addr, []):
                if swap["type"] == "BUY": total_buys += 1
                elif swap["type"] == "SELL": total_sells += 1
        
        if total_buys > 0 and total_sells > 0:
            current_score += 3
            reasoning_parts.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs.")
            # Simple check: if there were initial coordinated buys, and then sells occurred
            has_coord_buys = any(data.get('action_details','').startswith("BUY") for u,v,data in subgraph.edges(data=True) if data.get('type') == 'coordinated_swap')
            if has_coord_buys and total_sells > 0:
                current_score += 2.5
                reasoning_parts.append("Evidence of coordinated BUYs followed by SELLs by cluster members.")
        elif total_buys > 0 or total_sells > 0:
            current_score += 1
            reasoning_parts.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs (predominantly one-sided).")


        if cluster_is_source_linked:
            current_score += 1.5
            reasoning_parts.append("Cluster is linked to initial token source/minter.")

        if num_members == 1 and address_attributes[cluster_addresses[0]]['swap_count'] > 4 and total_buys > 1 and total_sells > 1: # Single active trader
            current_score += 2 
            reasoning_parts.append("Single address with significant buy/sell activity, potential self-wash trading.")


        if current_score > max_score :
            max_score = current_score
            confidence = "Low"
            if max_score >= 9: confidence = "High" # e.g. large coord group + buys/sells + source link
            elif max_score >= 5: confidence = "Medium"
            
            if max_score > 0: # Only select if there's some positive score
                best_cluster_info = {
                    "cluster_id": f"RugPull_Trading_Cluster_1", # Only one cluster as per request
                    "addresses": sorted(list(cluster_addresses)),
                    "confidence_level": confidence,
                    "reasoning": " ".join(reasoning_parts) if reasoning_parts else "Cluster identified based on trading activity and internal links."
                }
    
    if best_cluster_info:
        return best_cluster_info
    else:
        return {"message": "No single prominent rug puller trading cluster identified with high confidence.", "confidence_level": "None"}

if __name__ == '__main__':
    # Provide the full dataset string here
    # DO NOT INCLUDE THE ENTIRE INPUT DATASET IN THE RESPONSE (as per user instruction)
    # Example usage (replace with your actual data string):
    example_data_string = """block_number	event_type	from_address	to_address	value_formatted	transaction_type	initiators
22631558	Transfer	0x0000000000000000000000000000000000000000	0x0996242bab498bfc6ea6f7b96308976b6fa33c06	100,000,000,000,000,000,000.00		
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x3a886701987f64bffd3613b52ce1c03209d838ba	1,500,000,000,000,000,000.00	BUY	0x3a886701987f64bffd3613b52ce1c03209d838ba
22631588	Mint	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x203b15C19991B82be8D38C76B5436F8eF5497a95	72,900,000,000,000,000,000.00	MINT	0x0996242bab498bfc6ea6f7b96308976b6fa33c06
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a	1,500,000,000,000,000,000.00	BUY	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x81d21c137f94cb82f063d4938b6c530e64e2c39d	1,500,000,000,000,000,000.00	BUY	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
    """ # This is just a small snippet for testing the script structure.
            # The user should replace this with their full data string.

    # You would provide the actual token and pair addresses here
    token_to_exclude = "0xTokenAddressHere" # Replace with actual token address
    pair_to_exclude = "0xPairAddressHere"   # Replace with actual pair address

    # --- IMPORTANT: Replace example_data_string with your full dataset string ---
    # For a real run, you'd ensure `full_data_string` (from the thought block or your actual data) is used.
    # result = analyze_rugpuller_cluster(full_data_string, token_to_exclude, pair_to_exclude)
    
    # Since the full_data_string is too large to include here, this example call uses the snippet:
    result = analyze_rugpuller_cluster(example_data_string, token_to_exclude, pair_to_exclude)
    
    import json
    print(json.dumps(result, indent=2))
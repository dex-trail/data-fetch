import pandas as pd
from collections import defaultdict
from io import StringIO
import networkx as nx
from community import best_partition # This is from the python-louvain library
import logging
import json

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_rugpuller_cluster(data_string: str, token_address_to_exclude: str, pair_address_to_exclude: str):
    """
    Analyzes transaction data to identify one primary rug puller owner cluster,
    ensuring all source addresses are included. Uses graph-based community detection.
    """
    logger.info("=== Starting rugpuller cluster analysis ===")
    logger.info(f"Token to exclude: {token_address_to_exclude}")
    logger.info(f"Pair to exclude: {pair_address_to_exclude}")
    
    try:
        df = pd.read_csv(StringIO(data_string), sep='\t', dtype=str)
        df.columns = df.columns.str.strip()
        logger.info(f"Successfully parsed CSV data with {len(df)} rows and columns: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Failed to parse CSV data: {e}")
        return {"error": f"Failed to parse CSV data: {e}"}

    # --- 1. Data Preprocessing ---
    logger.info("=== Starting data preprocessing ===")
    df['value_formatted'] = df['value_formatted'].str.replace(',', '', regex=False).astype(float)
    df['initiators'] = df['initiators'].str.lower().fillna('')
    df['from_address'] = df['from_address'].str.lower().fillna('')
    df['to_address'] = df['to_address'].str.lower().fillna('')
    df['block_number'] = pd.to_numeric(df['block_number'], errors='coerce').fillna(0).astype(int)
    logger.info("Completed data type conversions and normalization")

    excluded_addresses = set()
    if token_address_to_exclude: excluded_addresses.add(token_address_to_exclude.lower())
    if pair_address_to_exclude: excluded_addresses.add(pair_address_to_exclude.lower())
    logger.info(f"Excluded addresses: {excluded_addresses}")

    G = nx.Graph()
    source_addresses = set()
    all_swappers = set()
    address_swap_actions = defaultdict(list)
    address_attributes = defaultdict(lambda: {"is_source": False, "is_swapper": False, "swap_count": 0})

    # Identify Sources & Swappers, Collect Swap Actions
    logger.info("=== Identifying source addresses from initial transfers ===")
    # Source from initial transfer
    initial_transfers = df[(df['event_type'] == 'Transfer') & (df['from_address'] == '0x0000000000000000000000000000000000000000')]
    logger.info(f"Found {len(initial_transfers)} initial transfer events")
    
    for idx, row in initial_transfers.iterrows():
        recipient = row['to_address']
        if recipient and recipient != 'nan' and recipient not in excluded_addresses:
            source_addresses.add(recipient)
            address_attributes[recipient]['is_source'] = True
            if recipient not in G: G.add_node(recipient, type='source')
            logger.debug(f"Added source address from initial transfer: {recipient}")

    logger.info(f"Found {len(source_addresses)} source addresses from initial transfers")

    # Source from Mint events
    logger.info("=== Identifying source addresses from Mint events ===")
    mint_events = df[df['event_type'] == 'Mint']
    logger.info(f"Found {len(mint_events)} mint events")
    
    for idx, row in mint_events.iterrows():
        minter = row['initiators']
        if minter and minter != 'nan' and minter not in excluded_addresses:
            was_new_source = minter not in source_addresses
            source_addresses.add(minter)
            address_attributes[minter]['is_source'] = True
            if minter not in G: G.add_node(minter, type='source')
            logger.debug(f"Added source address from mint event: {minter} (new: {was_new_source})")
            
    logger.info(f"Total source addresses after mint events: {len(source_addresses)}")
    logger.info(f"All source addresses: {sorted(list(source_addresses))}")
    
    logger.info("=== Identifying swappers from V2_Swap events ===")
    v2_swaps_df = df[df['event_type'] == 'V2_Swap']
    logger.info(f"Found {len(v2_swaps_df)} V2_Swap events")
    
    for idx, row in v2_swaps_df.iterrows():
        initiator = row['initiators']
        if initiator and initiator != 'nan' and initiator not in excluded_addresses:
            all_swappers.add(initiator)
            address_attributes[initiator]['is_swapper'] = True
            address_attributes[initiator]['swap_count'] += 1
            if initiator not in G: G.add_node(initiator, type='swapper')
            address_swap_actions[initiator].append({
                "block": row['block_number'], "type": row['transaction_type'], "value": row['value_formatted']
            })
            logger.debug(f"Processed swap by {initiator}: {row['transaction_type']} of {row['value_formatted']} at block {row['block_number']}")

    logger.info(f"Found {len(all_swappers)} unique swapper addresses")
    logger.info(f"All swapper addresses: {sorted(list(all_swappers))}")

    # Log swap count statistics
    swap_counts = {addr: attrs['swap_count'] for addr, attrs in address_attributes.items() if attrs['swap_count'] > 0}
    logger.info(f"Swap count distribution: {dict(sorted(swap_counts.items(), key=lambda x: x[1], reverse=True))}")

    # --- 2. Graph Construction - Add Edges ---
    logger.info("=== Starting graph construction ===")
    logger.info(f"Current graph stats - Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")
    
    logger.info("Adding source funding edges from Transfer events...")
    transfers_df = df[df['event_type'] == 'Transfer']
    source_funding_edges = 0
    
    for idx, row in transfers_df.iterrows():
        from_addr, to_addr = row['from_address'], row['to_address']
        if from_addr in source_addresses and to_addr and to_addr != 'nan' and to_addr not in excluded_addresses and from_addr != to_addr:
            if from_addr not in G: G.add_node(from_addr)
            if to_addr not in G: G.add_node(to_addr)
            G.add_edge(from_addr, to_addr, weight=5.0, type='source_funding', block=row['block_number'])
            source_funding_edges += 1
            logger.debug(f"Added source funding edge: {from_addr} -> {to_addr} at block {row['block_number']}")

    logger.info(f"Added {source_funding_edges} source funding edges")

    logger.info("Adding coordinated swap edges...")
    grouped_swaps = v2_swaps_df.groupby(['block_number', 'transaction_type', 'value_formatted'])
    coordinated_swap_edges = 0
    
    for group_key, group_df in grouped_swaps:
        block, tx_type, val = group_key
        unique_initiators_in_group = group_df['initiators'].dropna().unique()
        
        valid_initiators = [i for i in unique_initiators_in_group if i and i != 'nan' and i not in excluded_addresses]
        
        if len(valid_initiators) > 1:
            logger.debug(f"Found coordinated swap group at block {block}: {len(valid_initiators)} initiators doing {tx_type} of {val}")
            logger.debug(f"Initiators: {valid_initiators}")
            
            for i in range(len(valid_initiators)):
                for j in range(i + 1, len(valid_initiators)):
                    addr1, addr2 = valid_initiators[i], valid_initiators[j]
                    if addr1 not in G: G.add_node(addr1)
                    if addr2 not in G: G.add_node(addr2)
                    G.add_edge(addr1, addr2, weight=10.0, type='coordinated_swap', block=block, action_details=f"{tx_type}_{val}")
                    coordinated_swap_edges += 1
                    logger.debug(f"Added coordinated swap edge: {addr1} <-> {addr2}")

    logger.info(f"Added {coordinated_swap_edges} coordinated swap edges")
    logger.info(f"Final graph stats - Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

    # Log graph structure details
    logger.info("=== Graph structure analysis ===")
    node_types = {}
    for node, data in G.nodes(data=True):
        node_type = data.get('type', 'unknown')
        node_types[node_type] = node_types.get(node_type, 0) + 1
    logger.info(f"Node types: {node_types}")

    edge_types = {}
    for u, v, data in G.edges(data=True):
        edge_type = data.get('type', 'unknown')
        edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
    logger.info(f"Edge types: {edge_types}")

    # --- 3. Community Detection ---
    logger.info("=== Starting community detection ===")
    if not G.nodes() or not G.edges():
        logger.warning("Graph has no nodes or edges for community detection")
        if source_addresses:
            logger.info("Falling back to source addresses as the cluster")
            return {
                "cluster_id": "RugPull_Owner_Cluster_1",
                "addresses": sorted(list(source_addresses)),
                "confidence_level": "Medium",
                "reasoning": "Owner's cluster consists of identified source addresses. Insufficient transactional links for further community detection."
            }
        else:
            logger.error("No source addresses found either")
            return {"message": "Graph has no nodes or edges, and no source addresses were identified; cannot perform community detection.", "confidence_level": "None"}

    try:
        logger.info("Running Louvain community detection...")
        partition = best_partition(G, weight='weight', random_state=42)
        logger.info(f"Community detection completed. Found partition with {len(set(partition.values()))} communities")
        
        clusters_raw = defaultdict(list)
        for node, community_id in partition.items():
            clusters_raw[community_id].append(node)
        communities = [addrs for addrs in clusters_raw.values() if len(addrs) > 0]
        
        logger.info(f"Communities found: {len(communities)}")
        for i, community in enumerate(communities):
            logger.info(f"Community {i}: {len(community)} members - {sorted(community)}")
            
    except Exception as e:
        logger.error(f"Community detection failed: {e}")
        if source_addresses:
            logger.info("Falling back to source addresses as the cluster")
            return {
                "cluster_id": "RugPull_Owner_Cluster_1",
                "addresses": sorted(list(source_addresses)),
                "confidence_level": "Medium",
                "reasoning": f"Community detection failed ({e}). Owner's cluster identified based on source addresses."
            }
        else:
            return {"message": f"Community detection failed: {e}, and no source addresses identified.", "confidence_level": "None"}

    # --- 4. Identify the "Rug Puller Trading Cluster" ---
    logger.info("=== Evaluating communities for rug puller trading cluster ===")
    best_cluster_info_trading = None # Renamed to avoid confusion with final output
    max_score = -1

    for i, cluster_addresses in enumerate(communities):
        if not cluster_addresses: continue
        
        logger.info(f"--- Analyzing Community {i} with {len(cluster_addresses)} members ---")
        logger.info(f"Community {i} members: {sorted(cluster_addresses)}")

        current_score = 0
        reasoning_parts = []
        
        num_members = len(cluster_addresses)
        cluster_is_source_linked = any(address_attributes[addr]['is_source'] for addr in cluster_addresses) or \
                                   any(any(data['type'] == 'source_funding' for u,v,data in G.edges(addr, data=True)) for addr in cluster_addresses if G.has_node(addr))

        logger.debug(f"Community {i} - Source linked: {cluster_is_source_linked}")

        total_buys = 0
        total_sells = 0
        coordinated_swap_participation_count = 0
        
        subgraph = G.subgraph(cluster_addresses)
        for u, v, data in subgraph.edges(data=True):
            if data.get('type') == 'coordinated_swap':
                coordinated_swap_participation_count += 1
                logger.debug(f"Community {i} - Coordinated swap edge: {u} <-> {v} ({data.get('action_details', 'N/A')})")
        
        logger.debug(f"Community {i} - Coordinated swap edges: {coordinated_swap_participation_count}")
        
        if coordinated_swap_participation_count > 0:
            score_addition = num_members * 2 + coordinated_swap_participation_count * 2.5
            current_score += score_addition
            reasoning_parts.append(f"{num_members} addresses show {coordinated_swap_participation_count} internal links from same-block-identical-value swaps.")
            logger.debug(f"Community {i} - Added {score_addition} points for coordinated swaps")

        for addr in cluster_addresses:
            addr_buys = addr_sells = 0
            for swap in address_swap_actions.get(addr, []):
                if swap["type"] == "BUY": 
                    total_buys += 1
                    addr_buys += 1
                elif swap["type"] == "SELL": 
                    total_sells += 1
                    addr_sells += 1
            if addr_buys > 0 or addr_sells > 0:
                logger.debug(f"Community {i} - Address {addr}: {addr_buys} buys, {addr_sells} sells")
        
        logger.debug(f"Community {i} - Total: {total_buys} buys, {total_sells} sells")
        
        if total_buys > 0 and total_sells > 0:
            current_score += 3
            reasoning_parts.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs.")
            logger.debug(f"Community {i} - Added 3 points for having both buys and sells")
            
            has_coord_buys = any(data.get('action_details','').startswith("BUY") for u,v,data in subgraph.edges(data=True) if data.get('type') == 'coordinated_swap')
            if has_coord_buys and total_sells > 0:
                current_score += 2.5
                reasoning_parts.append("Evidence of coordinated BUYs followed by SELLs by cluster members.")
                logger.debug(f"Community {i} - Added 2.5 points for coordinated buys followed by sells")
        elif total_buys > 0 or total_sells > 0:
            current_score += 1
            reasoning_parts.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs (predominantly one-sided).")
            logger.debug(f"Community {i} - Added 1 point for one-sided trading")

        if cluster_is_source_linked:
            current_score += 1.5
            reasoning_parts.append("Cluster is linked to initial token source/minter.")
            logger.debug(f"Community {i} - Added 1.5 points for source linkage")

        if num_members == 1 and address_attributes[cluster_addresses[0]]['swap_count'] > 4 and total_buys > 1 and total_sells > 1:
            current_score += 2 
            reasoning_parts.append("Single address with significant buy/sell activity, potential self-wash trading.")
            logger.debug(f"Community {i} - Added 2 points for potential wash trading")

        logger.info(f"Community {i} final score: {current_score}")
        logger.info(f"Community {i} reasoning: {' '.join(reasoning_parts) if reasoning_parts else 'No specific reasoning'}")

        if current_score > max_score:
            max_score = current_score
            confidence = "Low"
            if max_score >= 9: confidence = "High"
            elif max_score >= 5: confidence = "Medium"
            
            logger.info(f"Community {i} is now the best cluster with score {current_score}")
            
            if max_score > 0:
                best_cluster_info_trading = {
                    "addresses": list(cluster_addresses), # Use list directly
                    "confidence_level": confidence,
                    "reasoning": " ".join(reasoning_parts) if reasoning_parts else "Cluster identified based on trading activity and internal links."
                }
    
    logger.info(f"Best trading cluster found with max score: {max_score}")
    if best_cluster_info_trading:
        logger.info(f"Best trading cluster: {sorted(best_cluster_info_trading['addresses'])}")
        logger.info(f"Best trading cluster confidence: {best_cluster_info_trading['confidence_level']}")
    
    # --- 5. Consolidate Owner's Cluster (Trading Cluster + All Source Addresses) ---
    logger.info("=== Consolidating final owner's cluster ===")
    final_cluster_addresses = set()
    final_reasoning_parts = []
    
    if best_cluster_info_trading:
        final_cluster_addresses.update(best_cluster_info_trading["addresses"])
        if best_cluster_info_trading.get("reasoning"):
            final_reasoning_parts.append(best_cluster_info_trading["reasoning"])
        current_confidence_from_trading_cluster = best_cluster_info_trading["confidence_level"]
        logger.info(f"Added {len(best_cluster_info_trading['addresses'])} addresses from best trading cluster")
    else:
        current_confidence_from_trading_cluster = "None" # No trading cluster found
        logger.info("No best trading cluster found")

    # Ensure all source addresses are included
    if source_addresses:
        missing_sources = source_addresses - final_cluster_addresses
        if missing_sources:
            final_cluster_addresses.update(missing_sources)
            final_reasoning_parts.append(f"All identified source addresses ({len(missing_sources)} unique) included in the owner's cluster.")
            logger.info(f"Added {len(missing_sources)} missing source addresses: {sorted(list(missing_sources))}")
        else:
            logger.info("All source addresses were already included in the trading cluster")
        
        # If no trading cluster was found, but sources exist, they form the cluster.
        if not best_cluster_info_trading: # and source_addresses is non-empty (implicit from outer if)
            # final_cluster_addresses already updated if it was empty and missing_sources was just source_addresses
            if not final_reasoning_parts: # Add reasoning if not already added (e.g. if missing_sources was empty because final_cluster_addresses was already populated by them)
                 final_reasoning_parts.append("Owner's cluster primarily identified based on source addresses as no dominant trading activity cluster was found.")
            logger.info("Using source addresses as primary cluster basis")
    
    if not final_cluster_addresses:
        # This means no best_cluster_info_trading AND no source_addresses
        logger.error("No owner cluster could be identified")
        return {"message": "No owner cluster identified (no trading activity cluster and no source addresses found).", "confidence_level": "None"}

    # Determine final confidence
    final_confidence = "None"
    if best_cluster_info_trading:
        final_confidence = current_confidence_from_trading_cluster
    elif source_addresses: # No best_cluster_info_trading, but source_addresses exist and form the cluster
        final_confidence = "Medium"
    else: # Should not be reached if final_cluster_addresses is populated
        final_confidence = "Low" 

    final_reasoning_str = " ".join(filter(None, final_reasoning_parts))
    if not final_reasoning_str:
        if source_addresses and final_cluster_addresses.issubset(source_addresses) and len(final_cluster_addresses) == len(source_addresses):
            final_reasoning_str = "Cluster consists solely of identified source addresses."
        elif best_cluster_info_trading:
             final_reasoning_str = "Cluster identified based on trading activity and internal links, and includes all source addresses."
        else: # Fallback, should ideally have more specific reasoning
            final_reasoning_str = "Owner cluster identified."

    logger.info("=== Final results ===")
    logger.info(f"Final cluster size: {len(final_cluster_addresses)}")
    logger.info(f"Final cluster addresses: {sorted(list(final_cluster_addresses))}")
    logger.info(f"Final confidence level: {final_confidence}")
    logger.info(f"Final reasoning: {final_reasoning_str}")
            
    return {
        "cluster_id": "RugPull_Owner_Cluster_1",
        "addresses": sorted(list(final_cluster_addresses)),
        "confidence_level": final_confidence,
        "reasoning": final_reasoning_str
    }

if __name__ == '__main__':
    example_data_string = """block_number	event_type	from_address	to_address	value_formatted	transaction_type	initiators
22631558	Transfer	0x0000000000000000000000000000000000000000	0x0996242bab498bfc6ea6f7b96308976b6fa33c06	100,000,000,000,000,000,000.00		
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x3a886701987f64bffd3613b52ce1c03209d838ba	1,500,000,000,000,000,000.00	BUY	0x3a886701987f64bffd3613b52ce1c03209d838ba
22631588	Mint	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x203b15C19991B82be8D38C76B5436F8eF5497a95	72,900,000,000,000,000,000.00	MINT	0x0996242bab498bfc6ea6f7b96308976b6fa33c06
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a	1,500,000,000,000,000,000.00	BUY	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x81d21c137f94cb82f063d4938b6c530e64e2c39d	1,500,000,000,000,000,000.00	BUY	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
    """

    token_to_exclude = "0xTokenAddressHere" 
    pair_to_exclude = "0xPairAddressHere"   
    
    result = analyze_rugpuller_cluster(example_data_string, token_to_exclude, pair_to_exclude)
    
    print(json.dumps(result, indent=2))
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
    Also analyzes links of most active traders to this owner cluster.
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
    all_swappers = set() # To store all unique swapper addresses not in excluded_addresses
    address_swap_actions = defaultdict(list)
    address_attributes = defaultdict(lambda: {"is_source": False, "is_swapper": False, "swap_count": 0})

    # Identify Sources & Swappers, Collect Swap Actions
    logger.info("=== Identifying source addresses from initial transfers ===")
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
            all_swappers.add(initiator) # Collect all valid swappers
            address_attributes[initiator]['is_swapper'] = True
            address_attributes[initiator]['swap_count'] += 1
            if initiator not in G: G.add_node(initiator, type='swapper')
            address_swap_actions[initiator].append({
                "block": row['block_number'], "type": row['transaction_type'], "value": row['value_formatted']
            })
            logger.debug(f"Processed swap by {initiator}: {row['transaction_type']} of {row['value_formatted']} at block {row['block_number']}")

    logger.info(f"Found {len(all_swappers)} unique swapper addresses (candidates for active traders)")
    
    swap_counts = {addr: attrs['swap_count'] for addr, attrs in address_attributes.items() if attrs['swap_count'] > 0}
    logger.info(f"Swap count distribution (top 10): {dict(sorted(swap_counts.items(), key=lambda x: x[1], reverse=True)[:10])}")

    # --- 2. Graph Construction - Add Edges ---
    logger.info("=== Starting graph construction ===")
    logger.info(f"Current graph stats - Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")
    
    logger.info("Adding source funding edges from Transfer events...")
    transfers_df = df[df['event_type'] == 'Transfer'] # This will be used later too
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

    logger.info("=== Graph structure analysis ===")
    node_types = defaultdict(int)
    for node, data in G.nodes(data=True):
        node_types[data.get('type', 'unknown')] += 1
    logger.info(f"Node types: {dict(node_types)}")

    edge_types = defaultdict(int)
    for u, v, data in G.edges(data=True):
        edge_types[data.get('type', 'unknown')] += 1
    logger.info(f"Edge types: {dict(edge_types)}")

    # --- 3. Community Detection ---
    logger.info("=== Starting community detection ===")
    if not G.nodes() or not G.edges():
        logger.warning("Graph has no nodes or edges for community detection")
        if source_addresses:
            logger.info("Falling back to source addresses as the cluster")
            # Prepare result for active trader analysis part
            main_result = {
                "cluster_id": "RugPull_Owner_Cluster_1",
                "addresses": sorted(list(source_addresses)),
                "confidence_level": "Medium",
                "reasoning": "Owner's cluster consists of identified source addresses. Insufficient transactional links for further community detection."
            }
            # Fall through to active trader analysis section
        else:
            logger.error("No source addresses found either")
            return {"message": "Graph has no nodes or edges, and no source addresses were identified; cannot perform community detection.", "confidence_level": "None"}
    else: # Graph has nodes and edges, proceed with community detection
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
                logger.debug(f"Community {i}: {len(community)} members - {sorted(community)}")
                
        except Exception as e:
            logger.error(f"Community detection failed: {e}")
            if source_addresses:
                logger.info("Falling back to source addresses as the cluster due to community detection failure")
                main_result = { # Prepare result for active trader analysis part
                    "cluster_id": "RugPull_Owner_Cluster_1",
                    "addresses": sorted(list(source_addresses)),
                    "confidence_level": "Medium",
                    "reasoning": f"Community detection failed ({e}). Owner's cluster identified based on source addresses."
                }
                # Fall through to active trader analysis section
            else:
                return {"message": f"Community detection failed: {e}, and no source addresses identified.", "confidence_level": "None"}

    # --- 4. Identify the "Rug Puller Trading Cluster" (if community detection happened) ---
    # This section will be skipped if main_result is already defined due to graph/community issues but sources existed
    if 'main_result' not in locals(): 
        logger.info("=== Evaluating communities for rug puller trading cluster ===")
        best_cluster_info_trading = None 
        max_score = -1

        for i, cluster_addresses_list in enumerate(communities): # Ensure it's a list for G.subgraph
            if not cluster_addresses_list: continue
            
            logger.info(f"--- Analyzing Community {i} with {len(cluster_addresses_list)} members ---")
            logger.debug(f"Community {i} members: {sorted(cluster_addresses_list)}")

            current_score = 0
            reasoning_parts_trading = [] # Use a distinct name
            
            num_members = len(cluster_addresses_list)
            # G.edges requires nodes to be in G. For subgraph, nodes are inherently from G.
            cluster_is_source_linked = any(address_attributes[addr]['is_source'] for addr in cluster_addresses_list) or \
                                    any(any(data['type'] == 'source_funding' for u,v,data in G.edges(addr, data=True)) for addr in cluster_addresses_list if addr in G)


            logger.debug(f"Community {i} - Source linked: {cluster_is_source_linked}")

            total_buys = 0
            total_sells = 0
            coordinated_swap_participation_count = 0
            
            # Ensure all addresses in cluster_addresses_list are in G before creating subgraph
            valid_subgraph_nodes = [addr for addr in cluster_addresses_list if addr in G]
            if not valid_subgraph_nodes:
                 logger.debug(f"Community {i} - No members found in graph G, skipping subgraph analysis.")
            else:
                subgraph = G.subgraph(valid_subgraph_nodes)
                for u, v, data in subgraph.edges(data=True):
                    if data.get('type') == 'coordinated_swap':
                        coordinated_swap_participation_count += 1
                        logger.debug(f"Community {i} - Coordinated swap edge: {u} <-> {v} ({data.get('action_details', 'N/A')})")
            
            logger.debug(f"Community {i} - Coordinated swap edges: {coordinated_swap_participation_count}")
            
            if coordinated_swap_participation_count > 0:
                score_addition = num_members * 2 + coordinated_swap_participation_count * 2.5
                current_score += score_addition
                reasoning_parts_trading.append(f"{num_members} addresses show {coordinated_swap_participation_count} internal links from same-block-identical-value swaps.")
                logger.debug(f"Community {i} - Added {score_addition} points for coordinated swaps")

            for addr in cluster_addresses_list:
                addr_buys = addr_sells = 0
                for swap in address_swap_actions.get(addr, []):
                    if swap["type"] == "BUY": 
                        total_buys += 1; addr_buys += 1
                    elif swap["type"] == "SELL": 
                        total_sells += 1; addr_sells += 1
                if addr_buys > 0 or addr_sells > 0:
                    logger.debug(f"Community {i} - Address {addr}: {addr_buys} buys, {addr_sells} sells")
            
            logger.debug(f"Community {i} - Total: {total_buys} buys, {total_sells} sells")
            
            if total_buys > 0 and total_sells > 0:
                current_score += 3
                reasoning_parts_trading.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs.")
                logger.debug(f"Community {i} - Added 3 points for having both buys and sells")
                
                has_coord_buys = False
                if valid_subgraph_nodes: # only check subgraph if it was created
                    subgraph_check = G.subgraph(valid_subgraph_nodes) # re-subgraph if needed, or pass 'subgraph'
                    has_coord_buys = any(data.get('action_details','').startswith("BUY") for u,v,data in subgraph_check.edges(data=True) if data.get('type') == 'coordinated_swap')

                if has_coord_buys and total_sells > 0:
                    current_score += 2.5
                    reasoning_parts_trading.append("Evidence of coordinated BUYs followed by SELLs by cluster members.")
                    logger.debug(f"Community {i} - Added 2.5 points for coordinated buys followed by sells")
            elif total_buys > 0 or total_sells > 0:
                current_score += 1
                reasoning_parts_trading.append(f"Cluster members performed {total_buys} BUYs and {total_sells} SELLs (predominantly one-sided).")
                logger.debug(f"Community {i} - Added 1 point for one-sided trading")

            if cluster_is_source_linked:
                current_score += 1.5
                reasoning_parts_trading.append("Cluster is linked to initial token source/minter.")
                logger.debug(f"Community {i} - Added 1.5 points for source linkage")

            if num_members == 1 and address_attributes[cluster_addresses_list[0]]['swap_count'] > 4 and total_buys > 1 and total_sells > 1:
                current_score += 2 
                reasoning_parts_trading.append("Single address with significant buy/sell activity, potential self-wash trading.")
                logger.debug(f"Community {i} - Added 2 points for potential wash trading")

            logger.info(f"Community {i} final score: {current_score}")
            logger.debug(f"Community {i} reasoning: {' '.join(reasoning_parts_trading) if reasoning_parts_trading else 'No specific reasoning'}")

            if current_score > max_score :
                max_score = current_score
                confidence_trading = "Low" # distinct name
                if max_score >= 9: confidence_trading = "High"
                elif max_score >= 5: confidence_trading = "Medium"
                
                logger.info(f"Community {i} is now the best cluster with score {current_score}")
                
                if max_score > 0:
                    best_cluster_info_trading = {
                        "addresses": list(cluster_addresses_list), 
                        "confidence_level": confidence_trading,
                        "reasoning": " ".join(reasoning_parts_trading) if reasoning_parts_trading else "Cluster identified based on trading activity and internal links."
                    }
        
        logger.info(f"Best trading cluster found with max score: {max_score}")
        if best_cluster_info_trading:
            logger.info(f"Best trading cluster: {sorted(best_cluster_info_trading['addresses'])}")
            logger.info(f"Best trading cluster confidence: {best_cluster_info_trading['confidence_level']}")
        
        # --- 5. Consolidate Owner's Cluster (Trading Cluster + All Source Addresses) ---
        logger.info("=== Consolidating final owner's cluster ===")
        final_cluster_addresses_set = set() # Use a distinct name
        final_reasoning_parts_list = [] # Use a distinct name
        
        if best_cluster_info_trading:
            final_cluster_addresses_set.update(best_cluster_info_trading["addresses"])
            if best_cluster_info_trading.get("reasoning"):
                final_reasoning_parts_list.append(best_cluster_info_trading["reasoning"])
            current_confidence_from_trading_cluster = best_cluster_info_trading["confidence_level"]
            logger.info(f"Added {len(best_cluster_info_trading['addresses'])} addresses from best trading cluster")
        else:
            current_confidence_from_trading_cluster = "None" 
            logger.info("No best trading cluster found")

        if source_addresses:
            missing_sources = source_addresses - final_cluster_addresses_set
            if missing_sources:
                final_cluster_addresses_set.update(missing_sources)
                final_reasoning_parts_list.append(f"All identified source addresses ({len(missing_sources)} unique) included in the owner's cluster.")
                logger.info(f"Added {len(missing_sources)} missing source addresses: {sorted(list(missing_sources))}")
            elif best_cluster_info_trading: # Only log if there was a trading cluster to begin with
                logger.info("All source addresses were already included in the trading cluster or no new sources to add.")
            
            if not best_cluster_info_trading: 
                if not final_reasoning_parts_list : # Ensure it's added only if no other reasoning exists
                     final_reasoning_parts_list.append("Owner's cluster primarily identified based on source addresses as no dominant trading activity cluster was found.")
                logger.info("Using source addresses as primary cluster basis as no trading cluster was found.")
        
        if not final_cluster_addresses_set:
            logger.error("No owner cluster could be identified (no trading cluster and no source addresses)")
            return {"message": "No owner cluster identified (no trading activity cluster and no source addresses found).", "confidence_level": "None"}

        final_confidence_level = "None"
        if best_cluster_info_trading:
            final_confidence_level = current_confidence_from_trading_cluster
        elif source_addresses: 
            final_confidence_level = "Medium"
        else: 
            final_confidence_level = "Low" 

        final_reasoning_text = " ".join(filter(None, final_reasoning_parts_list))
        if not final_reasoning_text:
            if source_addresses and final_cluster_addresses_set.issubset(source_addresses) and len(final_cluster_addresses_set) == len(source_addresses):
                final_reasoning_text = "Cluster consists solely of identified source addresses."
            elif best_cluster_info_trading:
                 final_reasoning_text = "Cluster identified based on trading activity and internal links, and includes all source addresses."
            else: 
                final_reasoning_text = "Owner cluster identified."
        
        main_result = {
            "cluster_id": "RugPull_Owner_Cluster_1",
            "addresses": sorted(list(final_cluster_addresses_set)),
            "confidence_level": final_confidence_level,
            "reasoning": final_reasoning_text
        }

    # End of if 'main_result' not in locals():
    # By this point, 'main_result' is defined, either from fallback or from full analysis.
        
    logger.info("=== Final results for Owner's Cluster ===")
    logger.info(f"Owner's cluster size: {len(main_result['addresses'])}")
    logger.info(f"Owner's cluster addresses: {main_result['addresses']}")
    logger.info(f"Owner's confidence level: {main_result['confidence_level']}")
    logger.info(f"Owner's reasoning: {main_result['reasoning']}")

    # --- 6. Analyze Most Active Traders ---
    logger.info("=== Analyzing Most Active Traders Not in Owner's Cluster ===")
    active_trader_analysis_results = []
    owner_cluster_set = set(main_result["addresses"]) 
    
    swapper_counts_list = [] # Use a distinct name
    for addr in all_swappers: 
        if addr not in owner_cluster_set: # Exclude those already in the owner's cluster
            swapper_counts_list.append((addr, address_attributes[addr]['swap_count']))
    
    sorted_swappers_list = sorted(swapper_counts_list, key=lambda x: x[1], reverse=True)
    
    N_TOP_TRADERS = 5 
    top_active_traders_list = [addr for addr, count in sorted_swappers_list[:N_TOP_TRADERS] if count > 0]

    if not top_active_traders_list:
        logger.info("No significant active traders found outside the owner's cluster to analyze.")
    else:
        logger.info(f"Top {len(top_active_traders_list)} active traders (not in owner's cluster):")
        for i, addr in enumerate(top_active_traders_list):
            logger.info(f"  {i+1}. Address: {addr}, Swap Count: {address_attributes[addr]['swap_count']}")

        for active_trader_addr in top_active_traders_list:
            trader_links_info = { # Use a distinct name
                "address": active_trader_addr,
                "swap_count": address_attributes[active_trader_addr]['swap_count'],
                "funded_by_owner_cluster": [],
                "funded_owner_cluster": [],
                "coordinated_swap_with_owner_cluster": []
            }
            logger.info(f"--- Analyzing links for active trader: {active_trader_addr} ---")

            for _, row in transfers_df.iterrows(): # Using the transfers_df from earlier
                from_a, to_a = row['from_address'], row['to_address']
                value, block = row['value_formatted'], row['block_number']
                
                if from_a in owner_cluster_set and to_a == active_trader_addr:
                    link_detail = {"from": from_a, "value": value, "block": block}
                    trader_links_info["funded_by_owner_cluster"].append(link_detail)
                    logger.info(f"Link: {active_trader_addr} funded by owner's cluster member {from_a} (Value: {value}, Block: {block})")
                
                if from_a == active_trader_addr and to_a in owner_cluster_set:
                    link_detail = {"to": to_a, "value": value, "block": block}
                    trader_links_info["funded_owner_cluster"].append(link_detail)
                    logger.info(f"Link: {active_trader_addr} funded owner's cluster member {to_a} (Value: {value}, Block: {block})")

            if active_trader_addr in G: 
                for owner_member_addr in owner_cluster_set:
                    if owner_member_addr in G and G.has_edge(active_trader_addr, owner_member_addr):
                        edge_data = G.edges[active_trader_addr, owner_member_addr]
                        if edge_data.get('type') == 'coordinated_swap':
                            link_detail = {
                                "with_member": owner_member_addr,
                                "block": edge_data.get('block'),
                                "action_details": edge_data.get('action_details')
                            }
                            trader_links_info["coordinated_swap_with_owner_cluster"].append(link_detail)
                            logger.info(f"Link: {active_trader_addr} had coordinated swap with {owner_member_addr} (Block: {link_detail['block']}, Action: {link_detail['action_details']})")
            else:
                logger.debug(f"Active trader {active_trader_addr} not found in graph G, skipping graph-based link checks for it.")
            
            active_trader_analysis_results.append(trader_links_info)

    main_result["active_trader_analysis"] = active_trader_analysis_results
    logger.info("=== Completed analysis of most active traders ===")
            
    return main_result

if __name__ == '__main__':
    example_data_string = """block_number	event_type	from_address	to_address	value_formatted	transaction_type	initiators
22631558	Transfer	0x0000000000000000000000000000000000000000	0x0996242bab498bfc6ea6f7b96308976b6fa33c06	100,000,000,000,000,000,000.00		
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x3a886701987f64bffd3613b52ce1c03209d838ba	1,500,000,000,000,000,000.00	BUY	0x3a886701987f64bffd3613b52ce1c03209d838ba
22631588	Mint	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x203b15C19991B82be8D38C76B5436F8eF5497a95	72,900,000,000,000,000,000.00	MINT	0x0996242bab498bfc6ea6f7b96308976b6fa33c06
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a	1,500,000,000,000,000,000.00	BUY	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
22631588	V2_Swap	0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49	0x81d21c137f94cb82f063d4938b6c530e64e2c39d	1,500,000,000,000,000,000.00	BUY	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x81d21c137f94cb82f063d4938b6c530e64e2c39d
22631596	V2_Swap	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	0x7a250d5630b4cf539739df2c5dacb4c659f2488d	2,215,000,000,000,000,000.00	SELL	0x610d60e5f2754d2c4f3de517cbe3cbe17ba58e9a
# Add an active trader not in the original cluster for testing the new section
22631600	V2_Swap	0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef	0xactiveTrader1	100.00	BUY	0xactiveTrader1
22631601	V2_Swap	0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef	0xactiveTrader1	100.00	SELL	0xactiveTrader1
22631602	V2_Swap	0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef	0xactiveTrader1	100.00	BUY	0xactiveTrader1
22631603	Transfer	0x0996242bab498bfc6ea6f7b96308976b6fa33c06	0xactiveTrader1	50.00		
    """

    token_to_exclude = "0xTokenAddressHere" 
    pair_to_exclude = "0xPairAddressHere"   
    
    # Modify example to include 0xdeadbeef... as an excluded address to make 0xactiveTrader1 more prominent if it wasn't for exclusion
    # For now, let 0xactiveTrader1 just be an independent active trader.
    
    result = analyze_rugpuller_cluster(example_data_string, token_to_exclude, pair_to_exclude)
    
    print(json.dumps(result, indent=2))
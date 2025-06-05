#!/usr/bin/env python3
"""
Address Clustering Analyzer using Graph-based Community Detection

This script takes the aggregated timeline JSON output from token_analytics_excel.py
and uses graph-based community detection to identify clusters of addresses
that likely belong to the same entity (rug puller trading clusters).
"""

import json
import os
import sys
from typing import Dict, Any, List
import asyncio
from datetime import datetime
import argparse
import pandas as pd
import io
from dotenv import load_dotenv

# Import the graph analysis function
from graph_a import analyze_rugpuller_cluster

load_dotenv()


class AddressClusteringAnalyzer:
    """
    Analyzes blockchain transaction data to identify address clusters using graph-based community detection
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize the analyzer
        
        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"✅ Initialized Address Clustering Analyzer")
        print(f"📁 Output directory: {output_dir}")
    
    def load_aggregated_timeline(self, json_file: str) -> List[Dict[str, Any]]:
        """
        Load the aggregated timeline JSON data
        
        Args:
            json_file: Path to the aggregated timeline JSON file
            
        Returns:
            List of transaction records
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                timeline_data = json.load(f)
            
            print(f"✅ Loaded {len(timeline_data)} transactions from {json_file}")
            return timeline_data
        
        except FileNotFoundError:
            print(f"❌ Error: JSON file {json_file} not found")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error loading JSON file: {e}")
            sys.exit(1)
    
    def convert_timeline_to_csv_string(self, timeline_data: List[Dict[str, Any]]) -> str:
        """
        Convert timeline data to CSV string format expected by the graph analysis function
        
        Args:
            timeline_data: List of transaction records
            
        Returns:
            Tab-separated CSV string
        """
        # Required columns for the graph analysis function
        required_columns = [
            'block_number', 'event_type', 'from_address', 'to_address', 
            'value_formatted', 'transaction_type', 'initiators'
        ]
        
        # Prepare data for CSV conversion
        csv_data = []
        for record in timeline_data:
            csv_row = {}
            # Map fields from timeline data to expected format
            csv_row['block_number'] = record.get('block_number', '')
            csv_row['event_type'] = record.get('event_type', '')
            csv_row['from_address'] = record.get('from_address', '')
            csv_row['to_address'] = record.get('to_address', '')
            csv_row['value_formatted'] = record.get('value_formatted', '')
            csv_row['transaction_type'] = record.get('transaction_type', '')
            csv_row['initiators'] = record.get('initiators', '')
            
            # Only add if we have some meaningful data
            if any(str(csv_row[col]) not in ['', 'nan', 'null', 'None'] for col in required_columns):
                csv_data.append(csv_row)
        
        if not csv_data:
            print("⚠️  Warning: No valid data found for CSV conversion")
            return ""
        
        # Convert to DataFrame and then to tab-separated CSV string
        df = pd.DataFrame(csv_data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep='\t')
        csv_string = csv_buffer.getvalue()
        
        print(f"✅ Converted {len(csv_data)} records to CSV format")
        print(f"📏 CSV data length: {len(csv_string)} characters")
        
        return csv_string
    
    def extract_addresses_from_timeline(self, timeline_data: List[Dict[str, Any]]) -> tuple:
        """
        Extract token and pair addresses from the timeline data
        
        Args:
            timeline_data: List of transaction records
            
        Returns:
            Tuple of (token_address, pair_address)
        """
        token_address = None
        pair_address = None
        
        for record in timeline_data:
            if not token_address and record.get('token_address'):
                token_address = record['token_address']
            if not pair_address and record.get('pair_address'):
                pair_address = record['pair_address']
            
            # Break early if we found both
            if token_address and pair_address:
                break
        
        print(f"🔍 Extracted addresses - Token: {token_address}, Pair: {pair_address}")
        return token_address or "", pair_address or ""
    
    def analyze_with_graph(self, csv_data: str, token_address: str, pair_address: str) -> Dict[str, Any]:
        """
        Analyze the data using the graph-based clustering function
        
        Args:
            csv_data: Tab-separated CSV string of transaction data
            token_address: Token address to exclude from analysis
            pair_address: Pair address to exclude from analysis
            
        Returns:
            Analysis result dictionary
        """
        try:
            print("📊 Running graph-based community detection analysis...")
            print("⏳ Analyzing transaction patterns and identifying clusters...")
            
            # Call the graph analysis function
            result = analyze_rugpuller_cluster(csv_data, token_address, pair_address)
            
            print("✅ Graph analysis completed")
            return result
                
        except Exception as e:
            print(f"❌ Error in graph analysis: {e}")
            import traceback
            traceback.print_exc()
            return {
                "error": f"Graph analysis failed: {str(e)}",
                "confidence_level": "None"
            }
    
    def save_analysis_result(self, analysis_result: Dict[str, Any], token_address: str) -> str:
        """
        Save the analysis result as JSON
        
        Args:
            analysis_result: The clustering analysis result from graph function
            token_address: The token address being analyzed
            
        Returns:
            Path to the saved result file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_filename = f"address_clusters_{token_address}_{timestamp}.json"
        result_filepath = os.path.join(self.output_dir, result_filename)
        
        try:
            # Prepare the complete result data
            result_data = {
                "timestamp": timestamp,
                "token_address": token_address,
                "analysis_type": "graph_based_clustering",
                "analysis_method": "community_detection",
                "result": analysis_result
            }
            
            # Save the result
            with open(result_filepath, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Saved clustering analysis to: {result_filepath}")
            
            # Print summary of results
            if "addresses" in analysis_result:
                print(f"🎯 Found cluster with {len(analysis_result['addresses'])} addresses")
                print(f"🔒 Confidence level: {analysis_result.get('confidence_level', 'Unknown')}")
                if analysis_result.get('reasoning'):
                    print(f"💡 Reasoning: {analysis_result['reasoning']}")
            elif "message" in analysis_result:
                print(f"📋 Result: {analysis_result['message']}")
            
            return result_filepath
            
        except Exception as e:
            print(f"❌ Error saving analysis result: {e}")
            raise
    
    async def analyze_token_clusters(self, json_file: str) -> str:
        """
        Main method to analyze address clusters for a token
        
        Args:
            json_file: Path to the aggregated timeline JSON file
            
        Returns:
            Path to the saved analysis result
        """
        print(f"\n🎯 Starting Graph-based Address Clustering Analysis")
        print(f"📄 Input JSON: {json_file}")
        
        # Load timeline data
        timeline_data = self.load_aggregated_timeline(json_file)
        
        # Extract addresses for exclusion
        token_address, pair_addr = self.extract_addresses_from_timeline(timeline_data)
        
        # Convert to CSV format
        csv_data = self.convert_timeline_to_csv_string(timeline_data)
        
        if not csv_data:
            print("❌ Error: No valid data to analyze")
            return ""
        
        # Analyze with graph function
        analysis_result = self.analyze_with_graph(csv_data, token_address, pair_addr)
        
        # Save results
        result_filepath = self.save_analysis_result(analysis_result, token_address)
        
        print(f"\n🎉 Analysis completed!")
        print(f"📊 Results saved to: {result_filepath}")
        
        return result_filepath


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Analyze blockchain address clusters using graph-based community detection')
    parser.add_argument('json_file', help='Path to the aggregated timeline JSON file')
    parser.add_argument('--output-dir', default='output', help='Output directory for results (default: output)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.json_file):
        print(f"❌ Error: Input JSON file not found: {args.json_file}")
        sys.exit(1)
    
    try:
        # Initialize analyzer
        analyzer = AddressClusteringAnalyzer(args.output_dir)
        
        # Run analysis
        result_file = await analyzer.analyze_token_clusters(args.json_file)
        
        if result_file:
            print(f"\n✅ Address clustering analysis completed successfully!")
            print(f"📄 Results: {result_file}")
        else:
            print(f"\n❌ Analysis failed - no results generated")
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 

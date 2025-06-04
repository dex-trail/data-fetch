import os
import requests
import json
from dotenv import load_dotenv
from typing import List, Dict, Optional

# Load environment variables from .env
load_dotenv()

class SolanaTokenTransferFetcher:
    def __init__(self):
        """Initialize the Solana token transfer fetcher with Alchemy API."""
        self.api_key = os.getenv('ALCHEMY_API_KEY')
        if not self.api_key:
            raise ValueError("ALCHEMY_API_KEY not found in environment variables. Please check your .env file.")
        
        # Alchemy Solana mainnet endpoint
        self.base_url = f"https://solana-mainnet.g.alchemy.com/v2/{self.api_key}"
        
        self.headers = {
            'Content-Type': 'application/json',
        }
    
    def get_token_transfers(self, token_address: str, limit: int = 100, before: Optional[str] = None) -> Dict:
        """
        Fetch token transfers for a specific token address.
        
        Args:
            token_address (str): The token mint address on Solana
            limit (int): Maximum number of transfers to return (default: 100, max: 1000)
            before (str, optional): Signature to fetch transfers before (for pagination)
            
        Returns:
            Dict: Response containing token transfers data
        """
        
        # Alchemy Enhanced API for token transfers
        url = f"{self.base_url}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                token_address,
                {
                    "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                },
                {
                    "encoding": "jsonParsed"
                }
            ]
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching token transfers: {e}")
            return {"error": str(e)}
    
    def get_asset_transfers(self, token_address: str, limit: int = 100) -> Dict:
        """
        Get asset transfers using Alchemy's Enhanced API.
        
        Args:
            token_address (str): The token mint address
            limit (int): Number of transfers to fetch
            
        Returns:
            Dict: Asset transfers data
        """
        
        # Using Alchemy Enhanced API endpoint
        enhanced_url = f"https://solana-mainnet.g.alchemy.com/v2/{self.api_key}/enhanced"
        
        payload = {
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": {
                "fromBlock": "0x0",
                "toBlock": "latest",
                "contractAddresses": [token_address],
                "category": ["token"],
                "maxCount": hex(limit),
                "order": "desc"
            },
            "id": 1
        }
        
        try:
            response = requests.post(enhanced_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching asset transfers: {e}")
            return {"error": str(e)}
    
    def get_signatures_for_address(self, token_address: str, limit: int = 100) -> Dict:
        """
        Get transaction signatures for a token address.
        
        Args:
            token_address (str): The token mint address
            limit (int): Number of signatures to fetch
            
        Returns:
            Dict: Transaction signatures data
        """
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                token_address,
                {
                    "limit": limit,
                    "commitment": "confirmed"
                }
            ]
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching signatures: {e}")
            return {"error": str(e)}
    
    def parse_transaction_for_token_transfers(self, signature: str) -> Dict:
        """
        Parse a specific transaction to extract token transfer information.
        
        Args:
            signature (str): Transaction signature
            
        Returns:
            Dict: Parsed transaction data
        """
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed"
                }
            ]
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching transaction: {e}")
            return {"error": str(e)}

def main():
    """Main function to demonstrate usage."""
    try:
        # Initialize the fetcher
        fetcher = SolanaTokenTransferFetcher()
        
        # Example token address (USDC on Solana)
        # Replace with your desired token address
        token_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
        
        print(f"Fetching token transfers for: {token_address}")
        print("-" * 60)
        
        # Method 1: Get signatures for the token address
        print("1. Fetching transaction signatures...")
        signatures_response = fetcher.get_signatures_for_address(token_address, limit=10)
        
        if "result" in signatures_response:
            signatures = signatures_response["result"]
            print(f"Found {len(signatures)} recent transactions")
            
            # Parse first few transactions for token transfers
            print("\n2. Parsing transactions for token transfers...")
            for i, sig_info in enumerate(signatures[:3]):  # Limit to first 3 for demo
                signature = sig_info["signature"]
                print(f"\nTransaction {i+1}: {signature}")
                
                transaction_data = fetcher.parse_transaction_for_token_transfers(signature)
                
                if "result" in transaction_data and transaction_data["result"]:
                    tx = transaction_data["result"]
                    meta = tx.get("meta", {})
                    
                    # Check for token balance changes
                    if "postTokenBalances" in meta and "preTokenBalances" in meta:
                        pre_balances = {bal["accountIndex"]: bal for bal in meta["preTokenBalances"]}
                        post_balances = {bal["accountIndex"]: bal for bal in meta["postTokenBalances"]}
                        
                        print("  Token balance changes:")
                        for account_index, post_bal in post_balances.items():
                            pre_bal = pre_balances.get(account_index, {})
                            pre_amount = int(pre_bal.get("uiTokenAmount", {}).get("amount", 0))
                            post_amount = int(post_bal.get("uiTokenAmount", {}).get("amount", 0))
                            
                            if pre_amount != post_amount:
                                change = post_amount - pre_amount
                                decimals = post_bal.get("uiTokenAmount", {}).get("decimals", 0)
                                ui_change = change / (10 ** decimals)
                                print(f"    Account {post_bal.get('owner', 'Unknown')}: {ui_change:,.6f}")
                
                print("  Status:", "Success" if meta.get("err") is None else "Failed")
                print("  Slot:", tx.get("slot", "Unknown"))
                
        else:
            print("Error or no signatures found:", signatures_response)
            
    except ValueError as e:
        print(f"Configuration Error: {e}")
        print("Please create a .env file with your ALCHEMY_API_KEY")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Token Source Code Fetcher

This script fetches the source code of a token contract from Etherscan API.
Usage:
    python fetch_token_source.py <token_address>
    python fetch_token_source.py <token_address> --api-key <your_api_key>
"""

import argparse
import json
import os
import sys
import requests
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from google import genai

# Load environment variables from .env file
load_dotenv()

class EtherscanSourceFetcher:
    """Class to handle Etherscan API interactions for fetching contract source code."""
    
    def __init__(self, api_key: Optional[str] = None, genai_api_key: Optional[str] = None):
        """
        Initialize the Etherscan source fetcher.
        
        Args:
            api_key: Etherscan API key. If not provided, will try to get from environment
                    or use YourApiKeyToken (rate limited)
            genai_api_key: Google GenAI API key for rugpull analysis
        """
        self.base_url = "https://api.etherscan.io/api"
        self.api_key = api_key or os.getenv('ETHERSCAN_API_KEY', 'YourApiKeyToken')
        self.genai_api_key = genai_api_key or os.getenv('GEMINI_API_KEY')
        
        # Initialize GenAI client if API key is available
        if self.genai_api_key:
            self.genai_client = genai.Client(api_key=self.genai_api_key)
        else:
            self.genai_client = None
        
    def is_valid_address(self, address: str) -> bool:
        """
        Validate if the provided string is a valid Ethereum address.
        
        Args:
            address: The address to validate
            
        Returns:
            bool: True if valid Ethereum address, False otherwise
        """
        if not isinstance(address, str):
            return False
        
        # Remove 0x prefix if present
        if address.startswith('0x'):
            address = address[2:]
        
        # Check if it's 40 characters long and contains only hex characters
        if len(address) != 40:
            return False
        
        try:
            int(address, 16)
            return True
        except ValueError:
            return False
    
    def fetch_source_code(self, token_address: str) -> Dict[str, Any]:
        """
        Fetch the source code for a given token address from Etherscan.
        
        Args:
            token_address: The Ethereum address of the token contract
            
        Returns:
            Dict containing the API response with source code information
        """
        if not self.is_valid_address(token_address):
            raise ValueError(f"Invalid Ethereum address: {token_address}")
        
        # Ensure address has 0x prefix
        if not token_address.startswith('0x'):
            token_address = '0x' + token_address
            
        params = {
            'module': 'contract',
            'action': 'getsourcecode',
            'address': token_address,
            'apikey': self.api_key
        }
        
        try:
            print(f"Fetching source code for address: {token_address}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data['status'] != '1':
                raise Exception(f"API Error: {data.get('message', 'Unknown error')}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Network error while fetching data: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse API response: {str(e)}")
    
    def analyze_rugpull_risk(self, source_code: str, contract_name: str = "Unknown") -> str:
        """
        Analyze the source code for potential rugpull indicators using LLM.
        
        Args:
            source_code: The contract source code to analyze
            contract_name: Name of the contract
            
        Returns:
            Analysis result from the LLM
        """
        if not self.genai_client:
            return "Error: GenAI API key not provided. Cannot perform rugpull analysis."
        
        if not source_code or source_code.strip() == "":
            return "Error: No source code available for analysis."
        
        # Create a comprehensive prompt for rugpull analysis
        prompt = f"""
You are a Solidity smart contract analysis assistant. Your task is to analyze the provided Solidity code for potential rugpull indicators and security risks, focusing on patterns that could be used to drain funds or manipulate token holders.

**Output ONLY a valid JSON object.** Do not include any introductory text, explanations, or summaries outside of the JSON structure.

The JSON output must conform to the following structure:

```json
{{
  "contract_name": "{contract_name}",
  "rugpull_risk_assessment": {{
    "overall_risk_level": "MINIMAL | LOW | MEDIUM | HIGH",
    "summary_of_concerns": [
      // Array of strings, each describing a key concern. Example: "High centralization of power in owner address."
      // If no major concerns, provide a statement like "No major immediate concerns identified based on the analyzed indicators."
    ],
    "investor_recommendations": "STRING_PLACEHOLDER" // Actionable advice for potential investors based on the findings.
  }},
  "specific_indicators_analysis": [
    // Array of objects, one for each of the 10 indicators listed below.
    // For each indicator:
    // - "id": A unique identifier for the indicator (use the provided IDs).
    // - "name": The full name of the indicator.
    // - "is_present": Boolean (true if the indicator is detected, false otherwise).
    // - "description": A brief explanation of the finding. If present, detail how it's implemented or why it's a concern. If not present, state "Not detected" or similar.
    // - "relevant_code_snippets": Array of strings, each a relevant code snippet from the contract. Empty array if not present or not applicable.
    {{
      "id": "unrestricted_minting",
      "name": "Mint functions without restrictions",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "owner_fund_drain",
      "name": "Owner-only functions that can drain funds",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "blacklist_whitelist_freeze",
      "name": "Blacklist/whitelist mechanisms that can freeze user funds",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "arbitrary_variable_fees",
      "name": "Variable fees that can be changed arbitrarily",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "proxy_code_change",
      "name": "Proxy patterns that allow code changes",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "developer_biased_timelocks",
      "name": "Time-locked withdrawals or vesting that benefits only developers",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "unusual_transfer_restrictions",
      "name": "Unusual transfer restrictions",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "hidden_backdoors_emergency",
      "name": "Hidden backdoors or emergency functions",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "centralized_control",
      "name": "Centralized control mechanisms",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }},
    {{
      "id": "liquidity_manipulation",
      "name": "Liquidity manipulation capabilities",
      "is_present": false,
      "description": "STRING_PLACEHOLDER",
      "relevant_code_snippets": []
    }}
  ]
}}
```

Contract Code:
```solidity
{source_code[:20000]}  // Limit to first 8000 characters to avoid token limits
```
"""

        try:
            print("Analyzing source code for rugpull indicators...")
            response = self.genai_client.models.generate_content(
                model="gemini-2.5-flash-preview-05-20", 
                contents=prompt
            )
            return response.text
        except Exception as e:
            return f"Error during LLM analysis: {str(e)}"

    def format_source_code(self, api_response: Dict[str, Any], include_rugpull_analysis: bool = False) -> str:
        """
        Format the source code response for display.
        
        Args:
            api_response: The response from Etherscan API
            include_rugpull_analysis: Whether to include rugpull risk analysis
            
        Returns:
            Formatted string with source code information
        """
        result = api_response['result'][0]
        
        output = []
        output.append("=" * 80)
        output.append("TOKEN CONTRACT SOURCE CODE")
        output.append("=" * 80)
        
        # Basic contract information
        contract_name = result.get('ContractName', 'N/A')
        output.append(f"Contract Name: {contract_name}")
        output.append(f"Compiler Version: {result.get('CompilerVersion', 'N/A')}")
        output.append(f"Optimization: {'Enabled' if result.get('OptimizationUsed') == '1' else 'Disabled'}")
        output.append(f"Runs: {result.get('Runs', 'N/A')}")
        output.append(f"Constructor Arguments: {result.get('ConstructorArguments', 'N/A')}")
        output.append(f"EVM Version: {result.get('EVMVersion', 'N/A')}")
        output.append(f"Library: {result.get('Library', 'N/A')}")
        output.append(f"License Type: {result.get('LicenseType', 'N/A')}")
        output.append(f"Proxy: {'Yes' if result.get('Proxy') == '1' else 'No'}")
        output.append(f"Implementation: {result.get('Implementation', 'N/A')}")
        output.append("")
        
        # Source code
        source_code = result.get('SourceCode', '')
        formatted_source_code = ""
        
        if source_code:
            output.append("SOURCE CODE:")
            output.append("-" * 40)
            
            # Handle multi-file contracts (JSON format)
            if source_code.startswith('{'):
                try:
                    # Remove extra braces if present
                    if source_code.startswith('{{') and source_code.endswith('}}'):
                        source_code = source_code[1:-1]
                    
                    source_json = json.loads(source_code)
                    
                    if 'sources' in source_json:
                        # Multi-file contract
                        formatted_parts = []
                        for file_path, file_data in source_json['sources'].items():
                            file_content = file_data.get('content', '')
                            output.append(f"\n--- FILE: {file_path} ---")
                            output.append(file_content)
                            formatted_parts.append(file_content)
                        formatted_source_code = "\n".join(formatted_parts)
                    else:
                        # Single file in JSON format
                        output.append(source_code)
                        formatted_source_code = source_code
                        
                except json.JSONDecodeError:
                    # If JSON parsing fails, treat as plain text
                    output.append(source_code)
                    formatted_source_code = source_code
            else:
                # Plain text source code
                output.append(source_code)
                formatted_source_code = source_code
        else:
            output.append("No source code available (contract may not be verified)")
        
        # Add rugpull analysis if requested
        if include_rugpull_analysis and formatted_source_code:
            output.append("\n" + "=" * 80)
            output.append("RUGPULL RISK ANALYSIS")
            output.append("=" * 80)
            
            analysis = self.analyze_rugpull_risk(formatted_source_code, contract_name)
            output.append(analysis)
        
        output.append("\n" + "=" * 80)
        
        return "\n".join(output)


def main():
    """Main function to handle command line arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Fetch source code of a token contract from Etherscan",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_token_source.py 0xA0b86a33E6441d3ab5a0db3A06FF2dFC9b8A7c1e
  python fetch_token_source.py 0xA0b86a33E6441d3ab5a0db3A06FF2dFC9b8A7c1e --api-key YOUR_API_KEY
  python fetch_token_source.py 0xA0b86a33E6441d3ab5a0db3A06FF2dFC9b8A7c1e --output contract_source.sol
  python fetch_token_source.py 0xA0b86a33E6441d3ab5a0db3A06FF2dFC9b8A7c1e --analyze-rugpull
        """
    )
    
    parser.add_argument(
        'token_address',
        help='The Ethereum address of the token contract'
    )
    
    parser.add_argument(
        '--api-key',
        help='Etherscan API key (can also be set via ETHERSCAN_API_KEY environment variable)'
    )
    
    parser.add_argument(
        '--genai-api-key',
        help='Google GenAI API key for rugpull analysis (can also be set via GEMINI_API_KEY environment variable)'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output file to save the source code (optional)'
    )
    
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output raw JSON response instead of formatted text'
    )
    
    parser.add_argument(
        '--analyze-rugpull',
        action='store_true',
        help='Analyze the source code for potential rugpull indicators using LLM'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize the fetcher
        fetcher = EtherscanSourceFetcher(api_key=args.api_key, genai_api_key=args.genai_api_key)
        
        # Fetch the source code
        response = fetcher.fetch_source_code(args.token_address)
        
        if args.json:
            # Output raw JSON
            output_text = json.dumps(response, indent=2)
        else:
            # Format the output
            output_text = fetcher.format_source_code(response, include_rugpull_analysis=args.analyze_rugpull)
        
        # Save to file or print to stdout
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output_text)
            print(f"Source code saved to: {args.output}")
        else:
            print(output_text)
            
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main() 
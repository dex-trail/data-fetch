# Address Clustering Analysis with Gemini AI

This project combines blockchain transaction analysis with AI-powered address clustering to identify addresses that likely belong to the same entity.

## Overview

The system works in two stages:

1. **Data Collection & Processing** (`token_analytics_excel.py`) - Analyzes blockchain transactions and exports both Excel and JSON data
2. **AI-Powered Clustering** (`address_clustering_analyzer.py`) - Uses Google Gemini's experimental thinking model to identify address clusters

## Prerequisites

### Required API Keys

1. **Alchemy API Key** (for blockchain data):
   - Sign up at [Alchemy](https://alchemy.com)
   - Create a new app and get your API key
   - Set as environment variable: `ALCHEMY_API_KEY=your_key_here`

2. **Google Gemini API Key** (for AI analysis):
   - Sign up at [Google AI Studio](https://makersuite.google.com/app/apikey)
   - Create an API key
   - Set as environment variable: `GEMINI_API_KEY=your_key_here`

### Dependencies

Install the required packages:

```bash
# For token analytics (existing requirements)
pip install pandas openpyxl aiohttp python-dotenv networkx

# For address clustering analysis
pip install -r clustering_requirements.txt
```

## Usage

### Step 1: Analyze Token and Generate Data

Run the token analytics script to collect blockchain data and generate the aggregated timeline JSON:

```bash
python token_analytics_excel.py
```

This will:
- Fetch transfer, swap, mint, and burn events for the specified token
- Create an Excel file with all the analysis
- **NEW:** Also save the aggregated timeline as JSON (e.g., `output/aggregated_timeline_0x123...abc.json`)

### Step 2: Analyze Address Clusters with AI

Use the generated JSON file with the clustering analyzer:

```bash
# Basic usage
python address_clustering_analyzer.py output/aggregated_timeline_0x123...abc.json

# With custom prompt file
python address_clustering_analyzer.py output/aggregated_timeline_0x123...abc.json --prompt prompt.txt

# With custom output directory
python address_clustering_analyzer.py output/aggregated_timeline_0x123...abc.json --output-dir results

# With API key as argument (instead of environment variable)
python address_clustering_analyzer.py output/aggregated_timeline_0x123...abc.json --api-key YOUR_GEMINI_API_KEY
```

### Expected Output

The clustering analyzer will create a JSON file with results like:

```json
[
  {
    "cluster_id": "Cluster_1",
    "addresses": [
      "0x1234567890abcdef1234567890abcdef12345678",
      "0xabcdef1234567890abcdef1234567890abcdef12"
    ],
    "confidence_level": "High",
    "reasoning": "Linked by direct transfer from Source Address and coordinated V2_Swaps in block 12345"
  },
  {
    "cluster_id": "Cluster_2", 
    "addresses": [
      "0x9876543210fedcba9876543210fedcba98765432"
    ],
    "confidence_level": "Medium",
    "reasoning": "Single address showing self-interaction swap pattern"
  }
]
```

## Clustering Methodology

The analysis follows a structured approach:

### Phase 1: Identify Key Seed Addresses
- Find original token sources (minters, initial recipients)
- Map direct funding/token movements
- Track direct transfers between active addresses

### Phase 2: Cluster Based on Coordinated Activity
- Same block, identical swap actions
- Coordinated buy-sell patterns
- Self-interaction patterns

### Phase 3: Consolidate and Assign Confidence
- **High Confidence**: Multiple strong heuristics (direct funding + coordinated swaps)
- **Medium Confidence**: Clear patterns but fewer corroborating signals
- **Low Confidence**: Single weak heuristic or isolated patterns

## File Outputs

### From `token_analytics_excel.py`:
- `token_analysis_TOKEN_ADDRESS.xlsx` - Complete Excel analysis
- `aggregated_timeline_TOKEN_ADDRESS.json` - **NEW:** JSON data for clustering analysis

### From `address_clustering_analyzer.py`:
- `address_clusters_TOKEN_ADDRESS_TIMESTAMP.json` - AI-generated clustering results

## Environment Variables

Create a `.env` file in your project directory:

```bash
ALCHEMY_API_KEY=your_alchemy_api_key_here
GEMINI_API_KEY=your_gemini_api_key_here
```

## Error Handling

The scripts include comprehensive error handling:

- **Missing API keys**: Clear error messages with setup instructions
- **Invalid JSON**: Saves raw responses for manual inspection
- **Network issues**: Retry logic and detailed error reporting
- **File not found**: Validates all input files before processing

## Tips for Best Results

1. **Data Quality**: Ensure your token has sufficient transaction history
2. **API Limits**: Be aware of rate limits for both Alchemy and Gemini APIs
3. **Prompt Customization**: Modify `prompt.txt` for different analysis focuses
4. **Batch Processing**: For multiple tokens, run the analytics script for each token first, then batch process the JSON files

## Troubleshooting

### Common Issues

1. **"No aggregated timeline found"**
   - Ensure the token analytics script completed successfully
   - Check that the JSON file exists in the output directory

2. **"Gemini API error"**
   - Verify your API key is correct and has sufficient quota
   - Check if the experimental thinking model is available in your region

3. **"Empty clustering results"**
   - The AI might not have found significant clusters
   - Try adjusting the confidence thresholds in the prompt
   - Ensure your transaction data has sufficient inter-address activity

### Debug Mode

For debugging, check the console output - both scripts provide detailed logging of their progress and any issues encountered.

## Advanced Usage

### Custom Prompts

You can modify `prompt.txt` to:
- Adjust confidence level criteria
- Focus on specific types of clustering patterns
- Change the output format requirements

### Integration

The JSON output can be easily integrated into other tools:
- Import into graph analysis software
- Feed into additional ML models
- Create visualizations with network analysis tools

## License

This project is provided as-is for research and analysis purposes. 
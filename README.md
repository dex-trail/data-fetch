# DexScreener Token Monitor

A Python tool that monitors the DexScreener API for new token profiles and alerts you when new tokens appear.

## Features

- 🔄 **Continuous Monitoring**: Automatically checks for new tokens at configurable intervals
- 💾 **Persistent Storage**: Remembers previously seen tokens across restarts
- 📊 **Complete Data Storage**: Saves full token data for historical analysis
- 📝 **Comprehensive Logging**: Logs all activity to both console and file
- 🚨 **Rich Alerts**: Detailed notifications with token information and social links
- ⚙️ **Configurable**: Customizable polling intervals and storage options
- 🛡️ **Error Handling**: Robust error handling with retry logic
- 🔧 **Command Line Interface**: Easy to use with various options

## Requirements

- Python 3.7+
- `requests` library

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Make the script executable (optional):
```bash
chmod +x dexscreener_monitor.py
```

## Usage

### Basic Usage

Run the monitor continuously (checks every 60 seconds by default):
```bash
python dexscreener_monitor.py
```

### Command Line Options

```bash
python dexscreener_monitor.py [OPTIONS]
```

**Options:**
- `--interval, -i`: Polling interval in seconds (default: 60)
- `--storage, -s`: File to store seen tokens (default: seen_tokens.json)
- `--data, -d`: File to store complete token data (default: new_tokens_data.json)
- `--log, -l`: Log file name (default: dexscreener_monitor.log)
- `--reset, -r`: Reset seen tokens list and token data, then exit
- `--once, -o`: Run once and exit (don't run continuously)

### Examples

Monitor with custom interval (30 seconds):
```bash
python dexscreener_monitor.py --interval 30
```

Run once and exit:
```bash
python dexscreener_monitor.py --once
```

Reset seen tokens (useful for testing):
```bash
python dexscreener_monitor.py --reset
```

Use custom storage, data, and log files:
```bash
python dexscreener_monitor.py --storage my_tokens.json --data my_token_data.json --log my_monitor.log
```

## Output

When new tokens are detected, you'll see detailed alerts like this:

```
🚀 NEW TOKEN DETECTED!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Chain: ETHEREUM
Token Address: 0x1234567890abcdef1234567890abcdef12345678
Description: A revolutionary new DeFi token
DexScreener URL: https://dexscreener.com/ethereum/0x1234567890abcdef1234567890abcdef12345678
Social Links:
    Twitter: https://x.com/newtoken
    Website: https://newtoken.com
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Files Created

The tool creates the following files:

- `seen_tokens.json`: Stores previously seen token addresses (for tracking)
- `new_tokens_data.json`: Stores complete token data with timestamps for all detected tokens
- `dexscreener_monitor.log`: Log file with all monitoring activity

## Token Data Storage

The tool saves complete token information in `new_tokens_data.json` with the following structure:

```json
[
  {
    "detected_at": "2024-06-04T13:29:41.212000",
    "token_data": {
      "url": "https://dexscreener.com/ethereum/0x1234...",
      "chainId": "ethereum",
      "tokenAddress": "0x1234567890abcdef...",
      "icon": "https://dd.dexscreener.com/ds-data/tokens/...",
      "header": "https://dd.dexscreener.com/ds-data/tokens/...",
      "description": "Token description",
      "links": [
        {
          "type": "twitter",
          "url": "https://x.com/tokenhandle"
        }
      ]
    }
  }
]
```

This allows you to:
- **Analyze historical trends** of new token launches
- **Export data** for further analysis or research
- **Track token information** that may change over time
- **Build databases** of new cryptocurrency projects

## API Information

The tool uses the DexScreener API endpoint:
```
https://api.dexscreener.com/token-profiles/latest/v1
```

This endpoint returns the latest token profiles that have been added to DexScreener.

## Stopping the Monitor

To stop the continuous monitoring, press `Ctrl+C` in the terminal.

## Customization

You can easily extend the tool by modifying the `alert_new_tokens` method to add additional notification methods such as:

- Email notifications
- Discord/Slack webhooks
- Desktop notifications
- Sound alerts
- Database storage

## Troubleshooting

### Common Issues

1. **Network Errors**: The tool will log network errors and retry on the next cycle
2. **API Rate Limits**: If you encounter rate limits, increase the polling interval
3. **Storage Issues**: Check file permissions if you see storage-related errors

### Debug Mode

To enable debug logging, modify the logging level in the script:
```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## License

This tool is provided as-is for educational and personal use.

## Contributing

Feel free to submit issues and pull requests to improve the tool. 
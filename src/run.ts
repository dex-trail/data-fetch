import 'dotenv/config';
import path from 'node:path';
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

async function getClient() {
  const client = new Client({ name: "dextrail-intel-runner", version: "0.0.1" });
  await client.connect(new StdioClientTransport({
    command: "/Users/mkejji/.nvm/versions/node/v22.12.0/bin/node",
    args: [path.join(__dirname, '..', 'build', 'index.js')],
    env: {
      PRIVATE_KEY: process.env.PRIVATE_KEY!,
    }
  }));
  return client;
}

async function main() {
  const client = await getClient();

  console.log(await client.callTool({
    name: "getTokenInfos",
    arguments: {
      tokenAddress: '0x8EfdE11f3E0Df0e7Da7feCDf489ed4D27Db59351'
    }
  }));
  console.log(await client.callTool({
    name: "analyzeToken",
    arguments: {
      tokenAddress: '0x8EfdE11f3E0Df0e7Da7feCDf489ed4D27Db59351'
    }
  }));
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

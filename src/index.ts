import path from 'node:path';
import { readFile } from 'node:fs/promises';

// @ts-ignore
import { BLOCKCHAIN_IDS } from 'dkg.js/constants';

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import z from "zod";

import { exec } from 'node:child_process';
import { CustomDkgClient } from './custom-dkg-client.mjs';
import { KCMulti } from './kcmulti.mjs';
import { ethers } from 'ethers';
import 'dotenv/config.js';
// @ts-ignore
import { kcTools } from 'assertion-tools';
import { ERC20 } from './erc20abi';

interface ExecResult {
  stdout: Buffer<ArrayBufferLike>;
  stderr: Buffer<ArrayBufferLike>;
}

const scriptsPath = path.join(__dirname, '..', 'scripts');
const pythonPath = path.join(scriptsPath, 'venv', 'bin', 'python');

const currentBigTask = {
  busy: false,
  promise: Promise.resolve() as Promise<void>,
};

function enqueueBigTask(task: Promise<void>) {
  if (!currentBigTask.busy) {
    currentBigTask.promise = task.then(() => {
      currentBigTask.busy = false;
    });
    currentBigTask.busy = true;
    return;
  }
  currentBigTask.promise = currentBigTask.promise
    .then(() => {
      currentBigTask.busy = true;
    })
    .then(() => task)
    .then(() => {
      currentBigTask.busy = false;
    });
}

// @ts-ignore
BigInt.prototype.toJSON = function () {
  return this.toString();
};

const execAsync = (cmd: any, options: any): Promise<ExecResult> => {
  return new Promise((resolve, reject) => {
    exec(cmd, options, (err, stdout, stderr) => {
      if (err) return reject(err);
      resolve({ stdout, stderr });
    });
  });
};

const OT_NODE_HOSTNAME = 'https://v6-pegasus-node-02.origin-trail.network';
const OT_NODE_PORT = '8900';

const options = {
  epochsNum: 2,
  minimumNumberOfFinalizationConfirmations: 3,
  minimumNumberOfNodeReplications: 1,
};

const DkgClient = new CustomDkgClient({
  endpoint: OT_NODE_HOSTNAME,
  port: OT_NODE_PORT,
  blockchain: {
    name: BLOCKCHAIN_IDS.NEUROWEB_TESTNET,
    privateKey: process.env.PRIVATE_KEY,
  },
  maxNumberOfRetries: 300,
  frequency: 2,
  contentType: 'all',
  nodeApiVersion: '/v1',
});

const queue: any = {
  node: {
    tasks: [],
    processing: false,
  },
  blockchain: {
    tasks: [],
    processing: false,
  },
};

setInterval(async () => {
  try {
    if (queue.node.processing) return;
    queue.node.processing = true;
    while (queue.node.tasks.length > 0) {
      const task = queue.node.tasks.shift();
      queue.blockchain.tasks.push(await publishToNode(task).catch(() => null));
    }
    queue.node.processing = false;
  } catch {
    queue.node.processing = false;
  }
}, 10_000);

setInterval(async () => {
  try {
    if (queue.blockchain.processing) return;
    queue.blockchain.processing = true;
    await registerAsset(queue.blockchain.tasks.filter(Boolean));
    queue.blockchain.tasks.length = 0;
    queue.blockchain.processing = false;
  } catch {
    queue.blockchain.processing = false;
  }
}, 30_000);

const provider = new ethers.JsonRpcProvider(OT_NODE_HOSTNAME);
const wallet = new ethers.Wallet(process.env.PRIVATE_KEY!, provider);
const contract = new ethers.Contract(KCMulti.address, KCMulti.abi, wallet);
const server = new McpServer({
  name: "dextrail-intel-agent",
  version: "0.0.1",
});

async function publishToNode(content: any) {
  const {
    endpoint,
    port,
    authToken,
    datasetRoot,
    dataset,
    datasetSize,
    blockchain,
    hashFunctionId,
    minimumNumberOfNodeReplications,
    maxNumberOfRetries,
    frequency,
    epochsNum,
    tokenAmount,
    immutable,
    contentAssetStorageAddress,
    minimumNumberOfFinalizationConfirmations,
    payer,
  } = await DkgClient.validateAndProcess(content, options);

  const publicationResult = await DkgClient.publish({
    endpoint,
    port,
    authToken,
    datasetRoot,
    datasetSize,
    dataset,
    blockchain,
    hashFunctionId,
    minimumNumberOfNodeReplications,
    maxNumberOfRetries,
    frequency,
    tokenAmount,
    epochsNum,
  });
  if (publicationResult === null) {
    throw new Error("Failed to publish");
  }
  const {
    estimatedPublishingCost,
    publisherNodeIdentityId,
    publisherNodeR,
    publisherNodeVS,
    identityIds,
    r,
    vs,
    operationId,
    operationResult,
  } = publicationResult;
  console.log("[NeuroWeb] Published Asset to the node");

  return {
    publishOperationResult: operationResult,
    publishOperationId: operationId,
    datasetRoot,
    dataset,
    datasetSize,
    epochsNum,
    publisherNodeIdentityId,
    estimatedPublishingCost,
    blockchain,
    immutable,
    publisherNodeR,
    publisherNodeVS,
    identityIds,
    r,
    vs,
    contentAssetStorageAddress,
    minimumNumberOfFinalizationConfirmations,
    endpoint,
    port,
    authToken,
    maxNumberOfRetries,
    frequency,
    payer,
  };
}

async function registerAsset(results: Array<any>) {
  const tx = await contract.multiPublish(results.map(res => ({
    publishOperationId: res.publishOperationId,
    merkleRoot: res.datasetRoot,
    knowledgeAssetsAmount: kcTools.countDistinctSubjects(res.dataset.public),
    byteSize: res.datasetSize,
    epochs: res.epochsNum,
    tokenAmount: res.estimatedPublishingCost.toString(),
    isImmutable: res.immutable,
    paymaster: res.payer,
    publisherNodeIdentityId: res.publisherNodeIdentityId,
    publisherNodeR: res.publisherNodeR,
    publisherNodeVS: res.publisherNodeVS,
    identityIds: res.identityIds,
    r: res.r,
    vs: res.vs,
  })));
  console.log(`Asset minted ${tx.hash}`);
  return await tx.wait();
}

server.tool(
  "getTokenInfos",
  "Fetches data about a token, notably its trading pairs and creates the necessary file for further analysis",
  {
    tokenAddress: z.string(),
  },
  async ({ tokenAddress }) => {
    await execAsync(`${pythonPath} dexscreener_monitor.py --addresses ${tokenAddress} --once`, {
      cwd: scriptsPath,
      maxBuffer: 1024 * 1024 * 5,
    });

    const text = await readFile(path.join(scriptsPath, 'new_tokens_data.json'), 'utf-8');
    try {
      const data = JSON.parse(text).pop().token_data;
      const contract = new ethers.Contract(data.tokenAddress, ERC20, provider);
      const pair = data.pairs_data[0];
      queue.node.tasks.push({
        "@context": "http://schema.org",
        "@id": `urn:dextrail:v1:pool:${data.chainId}:${pair.pairAddress}`,
        "@type": "TradingPool",
        "address": pair.pairAddress,
        "chainId": data.chainId,
      });
      queue.node.tasks.push({
        "@context": "http://schema.org",
        "@id": `urn:dextrail:v1:token:${data.chainId}:${data.tokenAddress}`,
        "@type": "Token",
        "address": data.tokenAddress,
        "name": data.name,
        "symbol": data.symbol,
        "chainId": data.chainId,
        "totalSupply": await contract.totalSupply(),
        "pool": {
          "@id": `urn:dextrail:v1:pool:${data.chainId}:${pair.pairAddress}`,
        },
        "url": `https://dexscreener.com/${data.chainId}/${data.tokenAddress}`,
        "socialLinks": [
          pair?.info?.websites?.length ? {
            "@type": "website",
            "url": pair.info.websites[0].url,
          } : undefined,
          ...pair?.info?.socials?.maps((d: any) => ({
            "@type": d.type,
            url: d.url,
          }))
        ].filter(Boolean),
      });
    } catch {}
    return {
      content: [
        {
          type: "text",
          text,
        },
      ],
    };
  }
);

server.tool(
  "analyzeToken",
  "Analyze the token transfers to determine whether there are clusters or adresses that seem like they work together and might actually belong to the same person. This tool CANNOT be run before the getTokenInfos one for that given token.",
  {
    tokenAddress: z.string(),
  },
  async ({ tokenAddress }) => {
    await execAsync(`${pythonPath} address_clustering_analyzer.py output/aggregated_timeline_${tokenAddress}.json`, {
      cwd: scriptsPath,
      maxBuffer: 1024 * 1024 * 5,
    });
    let clusters: any = null;
    try {
      clusters = JSON.parse(await readFile(path.join(scriptsPath, 'output/address_clusters.json'), 'utf-8'));
      for (const address of clusters.result.addresses) {
        queue.node.tasks.push({
          "@context": "http://schema.org",
          "@id": `urn:dextrail:v1:eoa:ethereum:${address}`,
          "@type": "ExternallyOwnedAccount",
          address,
          chain_id: 'ethereum',
        });
      }
    } catch {}
    const { stdout, stderr } = await execAsync(`${pythonPath} analyze_cluster_balances.py output/address_clusters.json output/token_analysis_alchemy_balances_${tokenAddress}.json output/aggregated_timeline_${tokenAddress}.json`, {
      cwd: scriptsPath,
      maxBuffer: 1024 * 1024 * 5,
    });
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify({
            clusters,
            summary: { stdout, stderr },
          }),
        },
      ],
    };
  }
);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("DexTrail MCP Server running on Std I/O");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});

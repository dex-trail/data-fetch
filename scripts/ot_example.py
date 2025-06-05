import json
import time

from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dkg.constants import BlockchainIds

node_provider = NodeHTTPProvider(
    endpoint_uri="https://v6-pegasus-node-02.origin-trail.network:8900",
    api_version="v1",
)

blockchain_provider = BlockchainProvider(
    BlockchainIds.NEUROWEB_TESTNET.value,
)

config = {
    "max_number_of_retries": 300,
    "frequency": 2,
}
dkg = DKG(node_provider, blockchain_provider, config)

content = {
    "public": {
        "@context": "https://www.schema.org",
        "@id": "urn:first-dkg-ka:info:hello-dkg",
        "@type": "CreativeWork",
        "name": "Hello DKG",  # 🎯 Remember this name for querying!
        "description": "My first Knowledge Asset on the Decentralized Knowledge Graph!"
    }
}

print("Publishing Knowledge Asset...")

create_asset_result = dkg.asset.create(content, {
    "epochs_num": 2,
    "minimum_number_of_finalization_confirmations": 3,
    "minimum_number_of_node_replications": 1
})

print("Success! Your Knowledge Asset has been published:")
print(json.dumps(create_asset_result, indent=4))
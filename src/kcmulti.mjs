export const KCMulti = {
  address: '0x7c8d7299a90cBc2d0293a21C08EeeC5429525f45',
  abi:
    [
      {
        inputs: [],
        stateMutability: "nonpayable",
        type: "constructor"
      },
      {
        inputs: [],
        name: "KNOWLEDGE_COLLECTION",
        outputs: [
          {
            internalType: "address",
            name: "",
            type: "address"
          }
        ],
        stateMutability: "view",
        type: "function"
      },
      {
        inputs: [],
        name: "TRAC_TROKEN",
        outputs: [
          {
            internalType: "address",
            name: "",
            type: "address"
          }
        ],
        stateMutability: "view",
        type: "function"
      },
      {
        inputs: [
          {
            components: [
              {
                internalType: "string",
                name: "publishOperationId",
                type: "string"
              },
              {
                internalType: "bytes32",
                name: "merkleRoot",
                type: "bytes32"
              },
              {
                internalType: "uint256",
                name: "knowledgeAssetsAmount",
                type: "uint256"
              },
              {
                internalType: "uint88",
                name: "byteSize",
                type: "uint88"
              },
              {
                internalType: "uint40",
                name: "epochs",
                type: "uint40"
              },
              {
                internalType: "uint96",
                name: "tokenAmount",
                type: "uint96"
              },
              {
                internalType: "bool",
                name: "isImmutable",
                type: "bool"
              },
              {
                internalType: "address",
                name: "paymaster",
                type: "address"
              },
              {
                internalType: "uint72",
                name: "publisherNodeIdentityId",
                type: "uint72"
              },
              {
                internalType: "bytes32",
                name: "publisherNodeR",
                type: "bytes32"
              },
              {
                internalType: "bytes32",
                name: "publisherNodeVS",
                type: "bytes32"
              },
              {
                internalType: "uint72[]",
                name: "identityIds",
                type: "uint72[]"
              },
              {
                internalType: "bytes32[]",
                name: "r",
                type: "bytes32[]"
              },
              {
                internalType: "bytes32[]",
                name: "vs",
                type: "bytes32[]"
              }
            ],
            internalType: "struct IKnowledgeCollection.CreateKCData[]",
            name: "dataArray",
            type: "tuple[]"
          }
        ],
        name: "multiPublish",
        outputs: [],
        stateMutability: "nonpayable",
        type: "function"
      }
    ]
}
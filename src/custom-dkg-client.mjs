import path from 'node:path';
import DKG from 'dkg.js';
import { kaTools, kcTools } from 'assertion-tools';
import { ethers, getBytes, hashMessage } from 'ethers';

const CHUNK_BYTE_SIZE = 32;
const OPERATION_STATUSES = {
  PENDING: 'PENDING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
};
const OPERATIONS = {
  PUBLISH: 'publish',
  GET: 'get',
  LOCAL_STORE: 'local-store',
  QUERY: 'query',
  PUBLISH_PARANET: 'publishParanet',
  FINALITY: 'finality',
};

const PRIVATE_ASSERTION_PREDICATE =
  'https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot';

const PRIVATE_RESOURCE_PREDICATE =
  'https://ontology.origintrail.io/dkg/1.0#representsPrivateResource';
const PRIVATE_HASH_SUBJECT_PREFIX = 'https://ontology.origintrail.io/dkg/1.0#metadata-hash:';

function deriveUAL(blockchain, contract, kcTokenId, kaTokenId) {
  const ual = `did:dkg:${blockchain.toLowerCase()}/${contract.toLowerCase()}/${kcTokenId}`;
  return kaTokenId ? `${ual}/${kaTokenId}` : ual;
}


function getOperationStatusObject(operationResult, operationId) {
  const operationData = operationResult.data?.errorType
    ? { status: operationResult.status, ...operationResult.data }
    : { status: operationResult.status };

  return {
    operationId,
    ...operationData,
  };
}
export class CustomDkgClient extends DKG {
  constructor(config) {
    super(config);

  }

  async validateAndProcess(content, options) {
    this.asset.validationService.validateJsonldOrNquads(content);
    const {
      blockchain,
      endpoint,
      port,
      maxNumberOfRetries,
      frequency,
      epochsNum,
      hashFunctionId,
      scoreFunctionId,
      immutable,
      tokenAmount,
      authToken,
      payer,
      minimumNumberOfFinalizationConfirmations,
      minimumNumberOfNodeReplications,
    } = this.asset.inputService.getAssetCreateArguments(options);

    this.asset.validationService.validateAssetCreate(
      content,
      blockchain,
      endpoint,
      port,
      maxNumberOfRetries,
      frequency,
      epochsNum,
      hashFunctionId,
      scoreFunctionId,
      immutable,
      tokenAmount,
      authToken,
      payer,
      minimumNumberOfFinalizationConfirmations,
      minimumNumberOfNodeReplications,
    );

    let dataset = {};
    if (typeof content === 'string') {
      dataset.public = this.asset.processContent(content);
    } else if (
      typeof content.public === 'string' ||
      (!content.public && content.private && typeof content.private === 'string')
    ) {
      if (content.public) {
        dataset.public = this.asset.processContent(content.public);
      } else {
        dataset.public = [];
      }
      if (content.private && typeof content.private === 'string') {
        dataset.private = this.asset.processContent(content.private);
      }
    } else {
      dataset = await kcTools.formatDataset(content);
    }

    let publicTriplesGrouped = [];
    // Assign IDs to blank nodes

    dataset.public = kcTools.generateMissingIdsForBlankNodes(dataset.public);

    if (dataset.private?.length) {
      dataset.private = kcTools.generateMissingIdsForBlankNodes(dataset.private);

      // Group private triples by subject and flatten
      const privateTriplesGrouped = kcTools.groupNquadsBySubject(dataset.private, true);
      dataset.private = privateTriplesGrouped.flat();

      // Compute private root and add to public
      const privateRoot = kcTools.calculateMerkleRoot(dataset.private);
      dataset.public.push(
        `<${kaTools.generateNamedNode()}> <${PRIVATE_ASSERTION_PREDICATE}> "${privateRoot}" .`,
      );

      // Group public triples by subject
      publicTriplesGrouped = kcTools.groupNquadsBySubject(dataset.public, true);

      // Create a map of public subject -> index for quick lookup
      const publicSubjectMap = new Map();
      for (let i = 0; i < publicTriplesGrouped.length; i += 1) {
        const [publicSubject] = publicTriplesGrouped[i][0].split(' ');
        publicSubjectMap.set(publicSubject, i);
      }

      const privateTripleSubjectHashesGroupedWithoutPublicPair = [];

      // Integrate private subjects into public or store separately if no match to be appended later
      for (const privateTriples of privateTriplesGrouped) {
        const [privateSubject] = privateTriples[0].split(' ');
        const privateSubjectHash = ethers.solidityPackedSha256(
          ['string'],
          [privateSubject.slice(1, -1)],
        );

        if (publicSubjectMap.has(privateSubject)) {
          // If there's a public pair, insert a representation in that group
          const publicIndex = publicSubjectMap.get(privateSubject);
          this.asset.insertTripleSorted(
            publicTriplesGrouped[publicIndex],
            `${privateSubject} <${PRIVATE_RESOURCE_PREDICATE}> <${kaTools.generateNamedNode()}> .`,
          );
        } else {
          // If no public pair, maintain separate list, inserting sorted by hash
          this.asset.insertTripleSorted(
            privateTripleSubjectHashesGroupedWithoutPublicPair,
            `${`<${PRIVATE_HASH_SUBJECT_PREFIX}${privateSubjectHash}>`} <${PRIVATE_RESOURCE_PREDICATE}> <${kaTools.generateNamedNode()}> .`,
          );
        }
      }

      // Append any non-paired private subjects at the end
      for (const triple of privateTripleSubjectHashesGroupedWithoutPublicPair) {
        publicTriplesGrouped.push([triple]);
      }

      dataset.public = publicTriplesGrouped.flat();
    } else {
      // No private triples, just group and flatten public
      publicTriplesGrouped = kcTools.groupNquadsBySubject(dataset.public, true);
      dataset.public = publicTriplesGrouped.flat();
    }

    const numberOfChunks = kcTools.calculateNumberOfChunks(dataset.public, CHUNK_BYTE_SIZE);
    const datasetSize = numberOfChunks * CHUNK_BYTE_SIZE;

    this.asset.validationService.validateAssertionSizeInBytes(datasetSize);
    const datasetRoot = kcTools.calculateMerkleRoot(dataset.public);

    const contentAssetStorageAddress = await this.asset.blockchainService.getContractAddress(
      'KnowledgeCollectionStorage',
      blockchain,
    );
    return {
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
    }
  }

  async publish({
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
  }) {
    const operationId = await this.asset.nodeApiService.publish(
      endpoint,
      port,
      authToken,
      datasetRoot,
      dataset,
      blockchain.name,
      hashFunctionId,
      minimumNumberOfNodeReplications,
    );
    const operationResult = await this.asset.nodeApiService.getOperationResult(
      endpoint,
      port,
      authToken,
      OPERATIONS.PUBLISH,
      maxNumberOfRetries,
      frequency,
      operationId,
    );
    if (
      operationResult.status !== OPERATION_STATUSES.COMPLETED &&
      !operationResult.data.minAcksReached
    ) {
      return null;
    }
    const { signatures } = operationResult.data;

    const {
      identityId: publisherNodeIdentityId,
      r: publisherNodeR,
      vs: publisherNodeVS,
    } = operationResult.data.publisherNodeSignature;

    const identityIds = [];
    const r = [];
    const vs = [];
    await Promise.all(
      signatures.map(async (signature) => {
        try {
          const signerAddress = ethers.recoverAddress(
            hashMessage(getBytes(datasetRoot)),
            signature,
          );

          const keyIsOperationalWallet =
            await this.asset.blockchainService.keyIsOperationalWallet(
              blockchain,
              signature.identityId,
              signerAddress,
            );
          if (keyIsOperationalWallet) {
            identityIds.push(signature.identityId);
            r.push(signature.r);
            vs.push(signature.vs);
          }
        } catch {
          // If error happened continue
        }
      }),
    );

    let estimatedPublishingCost;
    if (tokenAmount) {
      estimatedPublishingCost = tokenAmount;
    } else {
      const timeUntilNextEpoch = await this.asset.blockchainService.timeUntilNextEpoch(blockchain);
      const epochLength = await this.asset.blockchainService.epochLength(blockchain);
      const stakeWeightedAverageAsk = await this.asset.blockchainService.getStakeWeightedAverageAsk(
        blockchain,
      );
      estimatedPublishingCost =
        (BigInt(stakeWeightedAverageAsk) *
          (BigInt(epochsNum) * BigInt(1e18) +
            (BigInt(timeUntilNextEpoch) * BigInt(1e18)) / BigInt(epochLength)) *
          BigInt(datasetSize)) /
        BigInt(1024) /
        BigInt(1e18);
    }

    return {
      estimatedPublishingCost,
      publisherNodeIdentityId,
      publisherNodeR,
      publisherNodeVS,
      identityIds,
      r,
      vs,
      operationId,
      operationResult,
    }
  }

  async createKnowledgeCollection({
    publishOperationResult,
    publishOperationId,
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
  }) {
    let knowledgeCollectionId;
    let mintKnowledgeCollectionReceipt;

    ({ knowledgeCollectionId, receipt: mintKnowledgeCollectionReceipt } =
      await this.asset.blockchainService.createKnowledgeCollection(
        {
          publishOperationId,
          merkleRoot: datasetRoot,
          knowledgeAssetsAmount: kcTools.countDistinctSubjects(dataset.public),
          byteSize: datasetSize,
          epochs: epochsNum,
          tokenAmount: estimatedPublishingCost.toString(),
          isImmutable: immutable,
          paymaster: payer,
          publisherNodeIdentityId,
          publisherNodeR,
          publisherNodeVS,
          identityIds,
          r,
          vs,
        },
        null,
        null,
        blockchain,
      ));

    const UAL = deriveUAL(blockchain.name, contentAssetStorageAddress, knowledgeCollectionId);

    let finalityStatusResult = 0;
    if (minimumNumberOfFinalizationConfirmations > 0) {
      finalityStatusResult = await this.asset.nodeApiService.finalityStatus(
        endpoint,
        port,
        authToken,
        UAL,
        minimumNumberOfFinalizationConfirmations,
        maxNumberOfRetries,
        frequency,
      );
    }

    return {
      UAL,
      datasetRoot,
      signatures: publishOperationResult.data.signatures,
      operation: {
        mintKnowledgeCollection: mintKnowledgeCollectionReceipt,
        publish: getOperationStatusObject(publishOperationResult, publishOperationId),
        finality: {
          status:
            finalityStatusResult >= minimumNumberOfFinalizationConfirmations
              ? 'FINALIZED'
              : 'NOT FINALIZED',
        },
        numberOfConfirmations: finalityStatusResult,
        requiredConfirmations: minimumNumberOfFinalizationConfirmations,
      },
    };
  }
}

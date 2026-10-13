import { randomUUID } from 'node:crypto';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import topology from '../../../../contracts/messaging/topology.json' with { type: 'json' };

export const SQS_ENDPOINT = process.env['JWP_SQS_ENDPOINT'] ?? 'http://127.0.0.1:54566';

export type QueueContract = (typeof topology.queues)[number];

export function sqsClient(): SQSClient {
  return new SQSClient({
    endpoint: SQS_ENDPOINT,
    region: process.env['AWS_REGION'] ?? 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    maxAttempts: 1,
  });
}


export async function requireSqs(client: SQSClient): Promise<void> {
  try {
    await client.send(new ListQueuesCommand({}), { abortSignal: AbortSignal.timeout(5000) });
  } catch (error) {
    throw new Error(
      `SQS unreachable at ${SQS_ENDPOINT}; start it with "docker compose -p jwp-sqs -f compose.sqs.yml up -d --wait"`,
      { cause: error },
    );
  }
}

export function contractFor(name: string): QueueContract {
  const queue = topology.queues.find(candidate => candidate.name === name);
  if (!queue) throw new Error(`queue ${name} is not in contracts/messaging/topology.json`);
  return queue;
}

export async function queueUrl(client: SQSClient, name: string): Promise<string> {
  const { QueueUrl } = await client.send(new GetQueueUrlCommand({ QueueName: name }));
  if (!QueueUrl) throw new Error(`queue ${name} has no URL`);
  return QueueUrl;
}

export async function queueAttributes(client: SQSClient, url: string): Promise<Record<string, string>> {
  const { Attributes } = await client.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['All'] }));
  return Attributes ?? {};
}

export interface QueuePair {
  readonly url: string;
  readonly dlqUrl: string;
  dispose(): Promise<void>;
}






export async function isolatedPair(client: SQSClient, sourceName: string): Promise<QueuePair> {
  const source = contractFor(sourceName);
  if (!('deadLetterQueue' in source)) throw new Error(`${sourceName} has no dead-letter queue`);
  const dlq = contractFor(source.deadLetterQueue);
  const suffix = randomUUID().slice(0, 8);
  const dlqUrl = await createQueue(client, `test-${suffix}-${dlq.name}`, sqsAttributes(dlq.attributes));
  const { QueueArn } = await queueAttributes(client, dlqUrl);
  const { maxReceiveCount, ...attributes } = source.attributes;
  const url = await createQueue(client, `test-${suffix}-${source.name}`, {
    ...attributes,
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: QueueArn, maxReceiveCount }),
  });
  return {
    url,
    dlqUrl,
    async dispose() {
      await client.send(new DeleteQueueCommand({ QueueUrl: url }));
      await client.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
    },
  };
}

function sqsAttributes(attributes: Readonly<Record<string, string>>): Record<string, string> {
  const { maxReceiveCount: _ignored, ...rest } = attributes as Record<string, string>;
  return rest;
}

async function createQueue(client: SQSClient, name: string, attributes: Record<string, string>): Promise<string> {
  const { QueueUrl } = await client.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }));
  if (!QueueUrl) throw new Error(`queue ${name} was not created`);
  return QueueUrl;
}

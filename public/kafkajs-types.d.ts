// Trimmed ambient KafkaJS 2.2.4 type surface for the in-browser TSPlayground
// compile-check (see src/components/TSPlayground.astro header comment for
// why this file exists instead of a CDN fetch of kafkajs's real .d.ts).
//
// Every member below is copied verbatim from the real
// tools/probe/node_modules/kafkajs/types/index.d.ts (2.2.4) — nothing here is
// hand-invented. What was DROPPED, and why: the real file opens with
// `/// <reference types="node" />` + `import * as tls from 'tls'` +
// `import * as net from 'net'`, which pulls in almost the entire @types/node
// dependency graph (net, tls, dns, crypto, stream, buffer, events, url, ...)
// once you follow every `/// <reference lib=...>`/`types=...` chain — dozens
// of files, fragile to fetch from a CDN and mostly irrelevant to the
// producer/consumer/admin snippets this course actually type-checks. So:
// fields typed with `tls.ConnectionOptions` / `net.Socket` (`ssl`,
// `socketFactory`), `SASLOptions`/`Mechanism` (`sasl`), and config knobs typed
// with `Cluster`/`Logger`/`PartitionAssigner` (`partitionAssigners`,
// `logCreator`) are omitted. Everything else — class/type/interface shape,
// method signatures, defaults implied by optionality — matches the real
// 2.2.4 d.ts exactly.

declare module 'kafkajs' {
  export class Kafka {
    constructor(config: KafkaConfig)
    producer(config?: ProducerConfig): Producer
    consumer(config: ConsumerConfig): Consumer
    admin(config?: AdminConfig): Admin
    logger(): Logger
  }

  export type BrokersFunction = () => string[] | Promise<string[]>

  export interface RetryOptions {
    maxRetryTime?: number
    initialRetryTime?: number
    factor?: number
    multiplier?: number
    retries?: number
  }

  // ssl / sasl / socketFactory / logCreator dropped — see file header.
  export interface KafkaConfig {
    brokers: string[] | BrokersFunction
    clientId?: string
    connectionTimeout?: number
    authenticationTimeout?: number
    reauthenticationThreshold?: number
    requestTimeout?: number
    enforceRequestTimeout?: boolean
    retry?: RetryOptions
    logLevel?: logLevel
  }

  export type ICustomPartitioner = () => (args: PartitionerArgs) => number

  export interface PartitionerArgs {
    topic: string
    partitionMetadata: PartitionMetadata[]
    message: Message
  }

  export type PartitionMetadata = {
    partitionErrorCode: number
    partitionId: number
    leader: number
    replicas: number[]
    isr: number[]
    offlineReplicas?: number[]
  }

  export const Partitioners: {
    DefaultPartitioner: ICustomPartitioner
    LegacyPartitioner: ICustomPartitioner
    /** @deprecated Use DefaultPartitioner instead */
    JavaCompatiblePartitioner: ICustomPartitioner
  }

  // partitionAssigners dropped (typed with Cluster/Logger) — see file header.
  export interface ProducerConfig {
    createPartitioner?: ICustomPartitioner
    retry?: RetryOptions
    metadataMaxAge?: number
    allowAutoTopicCreation?: boolean
    idempotent?: boolean
    transactionalId?: string
    transactionTimeout?: number
    maxInFlightRequests?: number
  }

  // partitionAssigners dropped (typed with Cluster/Logger/Assigner) — see file header.
  export interface ConsumerConfig {
    groupId: string
    metadataMaxAge?: number
    sessionTimeout?: number
    rebalanceTimeout?: number
    heartbeatInterval?: number
    maxBytesPerPartition?: number
    minBytes?: number
    maxBytes?: number
    maxWaitTimeInMs?: number
    retry?: RetryOptions & { restartOnFailure?: (err: Error) => Promise<boolean> }
    allowAutoTopicCreation?: boolean
    maxInFlightRequests?: number
    readUncommitted?: boolean
    rackId?: string
  }

  export interface AdminConfig {
    retry?: RetryOptions
  }

  export interface IHeaders {
    [key: string]: Buffer | string | (Buffer | string)[] | undefined
  }

  export interface Message {
    key?: Buffer | string | null
    value: Buffer | string | null
    partition?: number
    headers?: IHeaders
    timestamp?: string
  }

  export interface ProducerRecord {
    topic: string
    messages: Message[]
    acks?: number
    timeout?: number
    compression?: CompressionTypes
  }

  export type RecordMetadata = {
    topicName: string
    partition: number
    errorCode: number
    offset?: string
    timestamp?: string
    baseOffset?: string
    logAppendTime?: string
    logStartOffset?: string
  }

  export interface TopicMessages {
    topic: string
    messages: Message[]
  }

  export interface ProducerBatch {
    acks?: number
    timeout?: number
    compression?: CompressionTypes
    topicMessages?: TopicMessages[]
  }

  export interface PartitionOffset {
    partition: number
    offset: string
  }

  export interface TopicOffsets {
    topic: string
    partitions: PartitionOffset[]
  }

  export interface Offsets {
    topics: TopicOffsets[]
  }

  type Sender = {
    send(record: ProducerRecord): Promise<RecordMetadata[]>
    sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]>
  }

  export type Producer = Sender & {
    connect(): Promise<void>
    disconnect(): Promise<void>
    isIdempotent(): boolean
    transaction(): Promise<Transaction>
    logger(): Logger
  }

  export type Transaction = Sender & {
    sendOffsets(offsets: Offsets & { consumerGroupId: string }): Promise<void>
    commit(): Promise<void>
    abort(): Promise<void>
    isActive(): boolean
  }

  export type TopicPartition = {
    topic: string
    partition: number
  }
  export type TopicPartitionOffset = TopicPartition & {
    offset: string
  }
  export type TopicPartitionOffsetAndMetadata = TopicPartitionOffset & {
    metadata?: string | null
  }
  export type TopicPartitions = { topic: string; partitions: number[] }

  /** @deprecated Replaced by ConsumerSubscribeTopics */
  export type ConsumerSubscribeTopic = { topic: string | RegExp; fromBeginning?: boolean }
  export type ConsumerSubscribeTopics = { topics: (string | RegExp)[]; fromBeginning?: boolean }

  interface MessageSetEntry {
    key: Buffer | null
    value: Buffer | null
    timestamp: string
    attributes: number
    offset: string
    size: number
    headers?: never
  }

  interface RecordBatchEntry {
    key: Buffer | null
    value: Buffer | null
    timestamp: string
    attributes: number
    offset: string
    headers: IHeaders
    size?: never
  }

  export type KafkaMessage = MessageSetEntry | RecordBatchEntry

  export type Batch = {
    topic: string
    partition: number
    highWatermark: string
    messages: KafkaMessage[]
    isEmpty(): boolean
    firstOffset(): string | null
    lastOffset(): string
    offsetLag(): string
    offsetLagLow(): string
  }

  export interface OffsetsByTopicPartition {
    topics: TopicOffsets[]
  }

  export interface EachMessagePayload {
    topic: string
    partition: number
    message: KafkaMessage
    heartbeat(): Promise<void>
    pause(): () => void
  }

  export interface EachBatchPayload {
    batch: Batch
    resolveOffset(offset: string): void
    heartbeat(): Promise<void>
    pause(): () => void
    commitOffsetsIfNecessary(offsets?: Offsets): Promise<void>
    uncommittedOffsets(): OffsetsByTopicPartition
    isRunning(): boolean
    isStale(): boolean
  }

  export type EachBatchHandler = (payload: EachBatchPayload) => Promise<void>
  export type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>

  export type ConsumerRunConfig = {
    autoCommit?: boolean
    autoCommitInterval?: number | null
    autoCommitThreshold?: number | null
    eachBatchAutoResolve?: boolean
    partitionsConsumedConcurrently?: number
    eachBatch?: EachBatchHandler
    eachMessage?: EachMessageHandler
  }

  export type GroupDescription = {
    groupId: string
    members: unknown[]
    protocol: string
    protocolType: string
    state: 'Unknown' | 'PreparingRebalance' | 'CompletingRebalance' | 'Stable' | 'Dead' | 'Empty'
  }

  export type Consumer = {
    connect(): Promise<void>
    disconnect(): Promise<void>
    subscribe(subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic): Promise<void>
    stop(): Promise<void>
    run(config?: ConsumerRunConfig): Promise<void>
    commitOffsets(topicPartitions: Array<TopicPartitionOffsetAndMetadata>): Promise<void>
    seek(topicPartitionOffset: TopicPartitionOffset): void
    describeGroup(): Promise<GroupDescription>
    pause(topics: Array<{ topic: string; partitions?: number[] }>): void
    paused(): TopicPartitions[]
    resume(topics: Array<{ topic: string; partitions?: number[] }>): void
    logger(): Logger
  }

  export interface ITopicConfig {
    topic: string
    numPartitions?: number
    replicationFactor?: number
  }

  export interface ITopicPartitionConfig {
    topic: string
    count: number
    assignments?: Array<Array<number>>
  }

  // Trimmed to the methods this course's lessons actually call — the real
  // Admin type has ~20 more (ACLs, config describe/alter, reassignments...).
  export type Admin = {
    connect(): Promise<void>
    disconnect(): Promise<void>
    listTopics(): Promise<string[]>
    createTopics(options: {
      validateOnly?: boolean
      waitForLeaders?: boolean
      timeout?: number
      topics: ITopicConfig[]
    }): Promise<boolean>
    deleteTopics(options: { topics: string[]; timeout?: number }): Promise<void>
    createPartitions(options: {
      validateOnly?: boolean
      timeout?: number
      topicPartitions: ITopicPartitionConfig[]
    }): Promise<boolean>
    logger(): Logger
  }

  export enum CompressionTypes {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
    ZSTD = 4,
  }

  export enum logLevel {
    NOTHING = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 4,
    DEBUG = 5,
  }

  export interface LogEntry {
    namespace: string
    level: logLevel
    label: string
    log: {
      timestamp: string
      message: string
      [key: string]: unknown
    }
  }

  export type Logger = {
    info(message: string, extra?: object): void
    error(message: string, extra?: object): void
    warn(message: string, extra?: object): void
    debug(message: string, extra?: object): void
    namespace(namespace: string, logLevel?: logLevel): Logger
    setLogLevel(logLevel: logLevel): void
  }

  export interface KafkaJSErrorMetadata {
    retriable?: boolean
    helpUrl?: string
    cause?: Error
  }

  export class KafkaJSError extends Error {
    readonly message: Error['message']
    readonly name: string
    readonly retriable: boolean
    readonly helpUrl?: string
    readonly cause?: Error

    constructor(e: Error | string, metadata?: KafkaJSErrorMetadata)
  }

  export class KafkaJSNonRetriableError extends KafkaJSError {
    constructor(e: Error | string)
  }

  export class KafkaJSProtocolError extends KafkaJSError {
    readonly code: number
    readonly type: string
    constructor(e: Error | string)
  }

  export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
    readonly topic: string
    readonly partition: number
  }

  export class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
    readonly stack: string
    readonly retryCount: number
    readonly retryTime: number
  }

  export class KafkaJSConnectionError extends KafkaJSError {
    readonly broker: string
  }

  export class KafkaJSRequestTimeoutError extends KafkaJSError {
    readonly broker: string
    readonly correlationId: number
  }

  export class KafkaJSAggregateError extends Error {
    readonly errors: (Error | string)[]
    constructor(message: Error | string, errors: (Error | string)[])
  }
}

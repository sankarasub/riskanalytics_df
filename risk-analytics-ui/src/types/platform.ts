export interface PlatformConfig {
  execution_mode: string;
  spark_mode: string;
  catalog: CatalogConfig;
  storage: StorageConfig;
  orchestration: string;
  streaming: string;
  kafka?: KafkaConfig;
  airflow?: AirflowConfig;
}

export interface CatalogConfig {
  name: string;
  type: string;
  uri?: string;
  namespace: string;
  stage_namespace: string;
  ods_namespace: string;
  warehouse: string;
}

export interface StorageConfig {
  type: string;
  endpoint?: string;
  path?: string;
  bucket?: string;
  access_key?: string;
  secret_key?: string;
}

export interface KafkaConfig {
  bootstrap_servers: string | null;
  topics: {
    ingest_prefix: string | null;
    trigger_topic: string | null;
  };
}

export interface AirflowConfig {
  api_url: string | null;
  username: string | null;
  password: string | null;
}

export interface HealthStatus {
  api: string;
  spark: string;
  nessie: string;
  storage: string;
  execution_mode: string;
}

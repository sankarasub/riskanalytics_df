import api from './api';

export interface PipelineRequest {
  target: string;
  as_of_date: string;
  params?: Record<string, any>;
}

export interface PipelineStatus {
  status: string;
  current_dag?: string;
  progress?: number;
  error?: string;
}

export interface PipelineExecutionResult {
  status: string;
  target: string;
  message: string;
}

export const pipelineService = {
  async executePipeline(request: PipelineRequest): Promise<PipelineExecutionResult> {
    const response = await api.post<PipelineExecutionResult>('/pipeline/execute', request);
    return response.data;
  },

  async getPipelineStatus(): Promise<PipelineStatus> {
    const response = await api.get<PipelineStatus>('/pipeline/status');
    return response.data;
  },

  async triggerBootstrap(asOfDate: string): Promise<PipelineExecutionResult> {
    return this.executePipeline({
      target: 'bootstrap',
      as_of_date: asOfDate
    });
  },

  async triggerOrchestration(asOfDate: string): Promise<PipelineExecutionResult> {
    return this.executePipeline({
      target: 'orchestration',
      as_of_date: asOfDate
    });
  },

  async triggerRiskMetrics(asOfDate: string, dataModel: string = 'source-to-ods'): Promise<PipelineExecutionResult> {
    return this.executePipeline({
      target: 'riskmetrics',
      as_of_date: asOfDate,
      params: { data_model: dataModel }
    });
  },

  async triggerStage(entity: string, source: string, asOfDate: string): Promise<PipelineExecutionResult> {
    return this.executePipeline({
      target: 'stage',
      as_of_date: asOfDate,
      params: { entity, source }
    });
  },

  async triggerOds(entity: string, source: string, asOfDate: string): Promise<PipelineExecutionResult> {
    return this.executePipeline({
      target: 'ods',
      as_of_date: asOfDate,
      params: { entity, source }
    });
  }
};
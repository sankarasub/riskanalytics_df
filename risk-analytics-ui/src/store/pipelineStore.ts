import { create } from 'zustand';
import { PipelineStatus, PipelineExecutionResult } from '../services/pipelineService';
import { pipelineService } from '../services/pipelineService';

interface PipelineState {
  status: PipelineStatus | null;
  loading: boolean;
  error: string | null;
  fetchStatus: () => Promise<void>;
  executePipeline: (target: string, asOfDate: string, params?: Record<string, any>) => Promise<PipelineExecutionResult>;
  triggerBootstrap: (asOfDate: string) => Promise<PipelineExecutionResult>;
  triggerOrchestration: (asOfDate: string) => Promise<PipelineExecutionResult>;
  triggerRiskMetrics: (asOfDate: string, dataModel?: string) => Promise<PipelineExecutionResult>;
  triggerStage: (entity: string, source: string, asOfDate: string) => Promise<PipelineExecutionResult>;
  triggerOds: (entity: string, source: string, asOfDate: string) => Promise<PipelineExecutionResult>;
}

export const usePipelineStore = create<PipelineState>((set) => ({
  status: null,
  loading: false,
  error: null,

  fetchStatus: async () => {
    set({ loading: true, error: null });
    try {
      const status = await pipelineService.getPipelineStatus();
      set({ status, loading: false });
    } catch (error) {
      set({ error: 'Failed to fetch pipeline status', loading: false });
    }
  },

  executePipeline: async (target, asOfDate, params) => {
    set({ loading: true, error: null });
    try {
      const result = await pipelineService.executePipeline({ target, as_of_date: asOfDate, params });
      // Update status after execution
      const status = await pipelineService.getPipelineStatus();
      set({ status, loading: false });
      return result;
    } catch (error) {
      set({ error: 'Failed to execute pipeline', loading: false });
      throw error;
    }
  },

  triggerBootstrap: async (asOfDate) => {
    return await pipelineService.triggerBootstrap(asOfDate);
  },

  triggerOrchestration: async (asOfDate) => {
    return await pipelineService.triggerOrchestration(asOfDate);
  },

  triggerRiskMetrics: async (asOfDate, dataModel) => {
    return await pipelineService.triggerRiskMetrics(asOfDate, dataModel);
  },

  triggerStage: async (entity, source, asOfDate) => {
    return await pipelineService.triggerStage(entity, source, asOfDate);
  },

  triggerOds: async (entity, source, asOfDate) => {
    return await pipelineService.triggerOds(entity, source, asOfDate);
  }
}));
import api from './api';
import { HealthStatus, PlatformConfig } from '../types/platform';

export const platformService = {
  async getHealth(): Promise<HealthStatus> {
    const response = await api.get<HealthStatus>('/platform/health');
    return response.data;
  },

  async getConfig(): Promise<PlatformConfig> {
    const response = await api.get<PlatformConfig>('/platform/config');
    return response.data;
  },

  async updateConfig(config: Partial<PlatformConfig>): Promise<{ status: string }> {
    const response = await api.post<{ status: string }>('/platform/config', config);
    return response.data;
  }
};
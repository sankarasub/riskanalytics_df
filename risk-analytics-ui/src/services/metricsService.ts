import api from './api';

export interface RiskMetricsSummary {
  totalPFE?: number;
  var?: number;
  nettingExposure?: number;
  recordCount?: number;
  exposureByCustomer?: Array<{
    customer: string;
    exposure: number;
  }>;
  detailedMetrics?: Array<{
    customer: string;
    pfe: number;
    var: number;
    nettingSet: string;
  }>;
}

export interface HistoricalMetrics {
  date: string;
  totalPFE: number;
  var: number;
  recordCount: number;
}

export const riskMetricsService = {
  async getMetricsSummary(asOfDate: string, customerId?: string): Promise<RiskMetricsSummary> {
    const params: any = { as_of_date: asOfDate };
    if (customerId) params.customer_id = customerId;
    
    const response = await api.get<RiskMetricsSummary>('/metrics/summary', { params });
    return response.data;
  },

  async getHistoricalMetrics(customerId?: string, limit: number = 30): Promise<HistoricalMetrics[]> {
    const params: any = { limit };
    if (customerId) params.customer_id = customerId;
    
    const response = await api.get<HistoricalMetrics[]>('/metrics/historical', { params });
    return response.data;
  }
};
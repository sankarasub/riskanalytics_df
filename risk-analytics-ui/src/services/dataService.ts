import api from './api';

export interface TableInfo {
  name: string;
  namespace?: string;
  rows?: number;
}

export interface TableData {
  schema: Array<{
    name: string;
    type: string;
    nullable: boolean;
  }>;
  data: Record<string, any>[];
  total: number;
  limit: number;
  offset: number;
}

export const dataService = {
  async listTables(): Promise<TableInfo[]> {
    const response = await api.get<{ tables: string[] }>('/data/tables');
    return response.data.tables.map(name => ({ name }));
  },

  async getTableData(tableName: string, limit: number = 100, offset: number = 0, filters?: string): Promise<TableData> {
    const params: any = { limit, offset };
    if (filters) params.filters = filters;
    
    const response = await api.get<TableData>(`/data/table/${tableName}`, { params });
    return response.data;
  },

  async getTableSchema(tableName: string): Promise<Array<{ name: string; type: string; nullable: boolean }>> {
    const response = await api.get<{ schema: Array<{ name: string; type: string; nullable: boolean }> }>(`/data/table/${tableName}/schema`);
    return response.data.schema;
  }
};
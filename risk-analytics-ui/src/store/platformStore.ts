import { create } from 'zustand';
import { HealthStatus, PlatformConfig } from '../types/platform';
import { platformService } from '../services/platformService';

interface PlatformState {
  health: HealthStatus | null;
  config: PlatformConfig | null;
  loading: boolean;
  error: string | null;
  fetchHealth: () => Promise<void>;
  fetchConfig: () => Promise<void>;
  updateConfig: (config: Partial<PlatformConfig>) => Promise<void>;
}

export const usePlatformStore = create<PlatformState>((set) => ({
  health: null,
  config: null,
  loading: false,
  error: null,

  fetchHealth: async () => {
    set({ loading: true, error: null });
    try {
      const health = await platformService.getHealth();
      set({ health, loading: false });
    } catch (error) {
      set({ error: 'Failed to fetch health status', loading: false });
    }
  },

  fetchConfig: async () => {
    set({ loading: true, error: null });
    try {
      const config = await platformService.getConfig();
      set({ config, loading: false });
    } catch (error) {
      set({ error: 'Failed to fetch configuration', loading: false });
    }
  },

  updateConfig: async (config) => {
    set({ loading: true, error: null });
    try {
      await platformService.updateConfig(config);
      const newConfig = await platformService.getConfig();
      set({ config: newConfig, loading: false });
    } catch (error) {
      set({ error: 'Failed to update configuration', loading: false });
    }
  },
}));

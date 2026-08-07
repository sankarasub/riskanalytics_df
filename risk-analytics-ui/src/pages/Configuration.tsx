import { Box, Typography, Grid, Card, CardContent, TextField, Button, Alert, Divider, Chip } from '@mui/material';
import { useEffect, useState } from 'react';
import { usePlatformStore } from '../store/platformStore';
import ExecutionModeSelector from '../components/platform/ExecutionModeSelector';

const Configuration = () => {
  const { config, loading, error, fetchConfig, updateConfig } = usePlatformStore();
  const [editableConfig, setEditableConfig] = useState<any>(null);
  const [editMode, setEditMode] = useState(false);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  useEffect(() => {
    if (config) {
      setEditableConfig({ ...config });
    }
  }, [config]);

  const handleSave = async () => {
    try {
      await updateConfig(editableConfig);
      setEditMode(false);
      await fetchConfig();
    } catch (err) {
      console.error('Failed to save configuration:', err);
    }
  };

  const handleCancel = () => {
    setEditableConfig({ ...config });
    setEditMode(false);
  };

  const handleFieldChange = (section: string, field: string, value: any) => {
    setEditableConfig((prev: any) => ({
      ...prev,
      [section]: {
        ...prev[section],
        [field]: value
      }
    }));
  };

  if (!config) {
    return (
      <Box>
        <Typography variant="h4" gutterBottom>
          Configuration
        </Typography>
        {loading && <Typography>Loading configuration...</Typography>}
        {error && <Alert severity="error">{error}</Alert>}
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Platform Configuration
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        <Grid item xs={12} md={4}>
          <ExecutionModeSelector 
            value={editableConfig?.execution_mode || 'docker'} 
            onChange={(mode) => handleFieldChange('root', 'execution_mode', mode)}
            disabled={!editMode || loading}
          />
        </Grid>

        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
                <Typography variant="h6">
                  Current Configuration
                </Typography>
                <Box>
                  {!editMode ? (
                    <Button 
                      variant="contained" 
                      onClick={() => setEditMode(true)}
                      disabled={loading}
                    >
                      Edit
                    </Button>
                  ) : (
                    <>
                      <Button 
                        variant="outlined" 
                        onClick={handleCancel}
                        disabled={loading}
                        sx={{ mr: 1 }}
                      >
                        Cancel
                      </Button>
                      <Button 
                        variant="contained" 
                        onClick={handleSave}
                        disabled={loading}
                      >
                        Save
                      </Button>
                    </>
                  )}
                </Box>
              </Box>

              <Divider sx={{ mb: 2 }} />

              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Execution Mode"
                    value={editableConfig?.execution_mode || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>
                
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Spark Mode"
                    value={editableConfig?.spark_mode || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom>
                    Catalog Configuration
                  </Typography>
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Catalog Name"
                    value={editableConfig?.catalog?.name || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Catalog Type"
                    value={editableConfig?.catalog?.type || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12}>
                  <TextField
                    fullWidth
                    label="Catalog URI"
                    value={editableConfig?.catalog?.uri || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={4}>
                  <TextField
                    fullWidth
                    label="Namespace"
                    value={editableConfig?.catalog?.namespace || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={4}>
                  <TextField
                    fullWidth
                    label="Stage Namespace"
                    value={editableConfig?.catalog?.stage_namespace || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={4}>
                  <TextField
                    fullWidth
                    label="ODS Namespace"
                    value={editableConfig?.catalog?.ods_namespace || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom>
                    Storage Configuration
                  </Typography>
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Storage Type"
                    value={editableConfig?.storage?.type || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Endpoint/Path"
                    value={editableConfig?.storage?.endpoint || editableConfig?.storage?.path || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Bucket"
                    value={editableConfig?.storage?.bucket || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom>
                    Orchestration & Streaming
                  </Typography>
                </Grid>

                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="Orchestration"
                    value={editableConfig?.orchestration || ''}
                    disabled={!editMode || loading}
                    sx={{ mb: 2 }}
                  />
                </Grid>

                <Grid item xs={12} sm={6}>
                  <Box display="flex" alignItems="center" gap={2}>
                    <TextField
                      fullWidth
                      label="Streaming"
                      value={editableConfig?.streaming || ''}
                      disabled={!editMode || loading}
                      sx={{ mb: 2 }}
                    />
                    <Chip 
                      label={editableConfig?.streaming === 'enabled' ? 'Active' : 'Inactive'} 
                      color={editableConfig?.streaming === 'enabled' ? 'success' : 'default'}
                    />
                  </Box>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Configuration;

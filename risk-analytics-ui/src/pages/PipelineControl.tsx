import { Box, Typography, Grid, Card, CardContent, Button, TextField, MenuItem, LinearProgress, Alert } from '@mui/material';
import { useState } from 'react';
import { usePipelineStore } from '../store/pipelineStore';
import { usePlatformStore } from '../store/platformStore';
import ExecutionModeSelector from '../components/platform/ExecutionModeSelector';

const PipelineControl = () => {
  const { status, loading, error, fetchStatus, triggerBootstrap, triggerOrchestration, triggerRiskMetrics, triggerStage, triggerOds } = usePipelineStore();
  const { config, updateConfig } = usePlatformStore();
  
  const [asOfDate, setAsOfDate] = useState(new Date().toISOString().split('T')[0]);
  const [entity, setEntity] = useState('customer');
  const [source, setSource] = useState('sourcea');
  const [dataModel, setDataModel] = useState('source-to-ods');
  const [tempMode, setTempMode] = useState(config?.execution_mode || 'docker');

  const handleModeChange = async (mode: string) => {
    setTempMode(mode);
    // Note: This would require backend restart in real implementation
    // For now, just update the UI state
  };

  const handleBootstrap = async () => {
    try {
      await triggerBootstrap(asOfDate);
      await fetchStatus();
    } catch (err) {
      console.error('Bootstrap failed:', err);
    }
  };

  const handleOrchestration = async () => {
    try {
      await triggerOrchestration(asOfDate);
      await fetchStatus();
    } catch (err) {
      console.error('Orchestration failed:', err);
    }
  };

  const handleRiskMetrics = async () => {
    try {
      await triggerRiskMetrics(asOfDate, dataModel);
      await fetchStatus();
    } catch (err) {
      console.error('Risk metrics failed:', err);
    }
  };

  const handleStage = async () => {
    try {
      await triggerStage(entity, source, asOfDate);
      await fetchStatus();
    } catch (err) {
      console.error('Stage failed:', err);
    }
  };

  const handleOds = async () => {
    try {
      await triggerOds(entity, source, asOfDate);
      await fetchStatus();
    } catch (err) {
      console.error('ODS failed:', err);
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Pipeline Control
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        <Grid item xs={12} md={4}>
          <ExecutionModeSelector 
            value={tempMode} 
            onChange={handleModeChange}
            disabled={loading}
          />
        </Grid>

        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Pipeline Parameters
              </Typography>
              
              <Grid container spacing={2}>
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    label="As of Date"
                    type="date"
                    value={asOfDate}
                    onChange={(e) => setAsOfDate(e.target.value)}
                    InputLabelProps={{ shrink: true }}
                    disabled={loading}
                  />
                </Grid>
                
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    select
                    label="Entity"
                    value={entity}
                    onChange={(e) => setEntity(e.target.value)}
                    disabled={loading}
                  >
                    <MenuItem value="customer">Customer</MenuItem>
                    <MenuItem value="asset">Asset</MenuItem>
                    <MenuItem value="collateral">Collateral</MenuItem>
                    <MenuItem value="deals">Deals</MenuItem>
                  </TextField>
                </Grid>
                
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    select
                    label="Source"
                    value={source}
                    onChange={(e) => setSource(e.target.value)}
                    disabled={loading}
                  >
                    <MenuItem value="sourcea">Source A</MenuItem>
                    <MenuItem value="sourceb">Source B</MenuItem>
                  </TextField>
                </Grid>
                
                <Grid item xs={12} sm={6}>
                  <TextField
                    fullWidth
                    select
                    label="Data Model"
                    value={dataModel}
                    onChange={(e) => setDataModel(e.target.value)}
                    disabled={loading}
                  >
                    <MenuItem value="source-to-ods">Source to ODS</MenuItem>
                  </TextField>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Pipeline Actions
              </Typography>
              
              <Box display="flex" gap={2} flexWrap="wrap">
                <Button 
                  variant="contained" 
                  onClick={handleBootstrap}
                  disabled={loading}
                >
                  Bootstrap
                </Button>
                <Button 
                  variant="contained" 
                  onClick={handleOrchestration}
                  disabled={loading}
                >
                  Orchestration
                </Button>
                <Button 
                  variant="contained" 
                  onClick={handleStage}
                  disabled={loading}
                >
                  Stage
                </Button>
                <Button 
                  variant="contained" 
                  onClick={handleOds}
                  disabled={loading}
                >
                  ODS
                </Button>
                <Button 
                  variant="contained" 
                  color="secondary"
                  onClick={handleRiskMetrics}
                  disabled={loading}
                >
                  Risk Metrics
                </Button>
              </Box>

              {loading && (
                <Box sx={{ mt: 2 }}>
                  <LinearProgress />
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        {status && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Pipeline Status
                </Typography>
                <Typography variant="body1">
                  Status: {status.status}
                </Typography>
                {status.current_dag && (
                  <Typography variant="body2">
                    Current DAG: {status.current_dag}
                  </Typography>
                )}
                {status.progress !== undefined && (
                  <Typography variant="body2">
                    Progress: {status.progress}%
                  </Typography>
                )}
                {status.error && (
                  <Alert severity="error" sx={{ mt: 1 }}>
                    {status.error}
                  </Alert>
                )}
              </CardContent>
            </Card>
          </Grid>
        )}
      </Grid>
    </Box>
  );
};

export default PipelineControl;

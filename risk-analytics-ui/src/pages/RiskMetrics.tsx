import { Box, Typography, Grid, Card, CardContent, TextField, Button, LinearProgress, Alert } from '@mui/material';
import { useState, useEffect } from 'react';
import { riskMetricsService, RiskMetricsSummary } from '../services/metricsService';

const RiskMetrics = () => {
  const [asOfDate, setAsOfDate] = useState(new Date().toISOString().split('T')[0]);
  const [customerId, setCustomerId] = useState('');
  const [metrics, setMetrics] = useState<RiskMetricsSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchMetrics = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await riskMetricsService.getMetricsSummary(asOfDate, customerId || undefined);
      setMetrics(data);
    } catch (err) {
      setError('Failed to fetch risk metrics');
      console.error('Error fetching metrics:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
  }, [asOfDate]);

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Risk Metrics Dashboard
      </Typography>

      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={4}>
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
        
        <Grid item xs={12} sm={4}>
          <TextField
            fullWidth
            label="Customer ID (optional)"
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            disabled={loading}
          />
        </Grid>
        
        <Grid item xs={12} sm={4}>
          <Button 
            variant="contained" 
            onClick={fetchMetrics}
            disabled={loading}
            sx={{ height: '56px' }}
          >
            Refresh
          </Button>
        </Grid>
      </Grid>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading && <LinearProgress />}

      {metrics && (
        <Grid container spacing={3}>
          <Grid item xs={12} sm={3}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Total PFE
                </Typography>
                <Typography variant="h4">
                  {metrics.totalPFE?.toLocaleString() ?? 'N/A'}
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid item xs={12} sm={3}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Value at Risk
                </Typography>
                <Typography variant="h4">
                  {metrics.var?.toLocaleString() ?? 'N/A'}
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid item xs={12} sm={3}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Netting Exposure
                </Typography>
                <Typography variant="h4">
                  {metrics.nettingExposure?.toLocaleString() ?? 'N/A'}
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          <Grid item xs={12} sm={3}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Record Count
                </Typography>
                <Typography variant="h4">
                  {metrics.recordCount?.toLocaleString() ?? 'N/A'}
                </Typography>
              </CardContent>
            </Card>
          </Grid>

          {metrics.exposureByCustomer && metrics.exposureByCustomer.length > 0 && (
            <Grid item xs={12}>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Exposure by Customer
                  </Typography>
                  <Box sx={{ maxHeight: 300, overflow: 'auto' }}>
                    {metrics.exposureByCustomer.map((item, index) => (
                      <Box key={index} sx={{ display: 'flex', justifyContent: 'space-between', py: 1, borderBottom: '1px solid #eee' }}>
                        <Typography>{item.customer}</Typography>
                        <Typography fontWeight="bold">
                          {item.exposure.toLocaleString()}
                        </Typography>
                      </Box>
                    ))}
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          )}

          {metrics.detailedMetrics && metrics.detailedMetrics.length > 0 && (
            <Grid item xs={12}>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Detailed Metrics
                  </Typography>
                  <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                    {metrics.detailedMetrics.map((item, index) => (
                      <Box key={index} sx={{ display: 'flex', justifyContent: 'space-between', py: 1, borderBottom: '1px solid #eee' }}>
                        <Box>
                          <Typography fontWeight="bold">{item.customer}</Typography>
                          <Typography variant="caption" color="textSecondary">
                            {item.nettingSet}
                          </Typography>
                        </Box>
                        <Box textAlign="right">
                          <Typography>PFE: {item.pfe.toLocaleString()}</Typography>
                          <Typography variant="caption">VaR: {item.var.toLocaleString()}</Typography>
                        </Box>
                      </Box>
                    ))}
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          )}
        </Grid>
      )}
    </Box>
  );
};

export default RiskMetrics;

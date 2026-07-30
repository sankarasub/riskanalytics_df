import { Box, Typography, Grid, Card, CardContent, LinearProgress } from '@mui/material';
import { useEffect } from 'react';
import { usePlatformStore } from '../store/platformStore';
import HealthIndicator from '../components/platform/HealthIndicator';

const Dashboard = () => {
  const { health, config, loading, fetchHealth, fetchConfig } = usePlatformStore();

  useEffect(() => {
    fetchHealth();
    fetchConfig();
    const interval = setInterval(() => {
      fetchHealth();
    }, 30000); // Refresh every 30 seconds
    return () => clearInterval(interval);
  }, [fetchHealth, fetchConfig]);

  if (loading && !health) {
    return <LinearProgress />;
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Platform Dashboard
      </Typography>

      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Platform Health
              </Typography>
              <HealthIndicator health={health} />
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Execution Mode
              </Typography>
              <Typography variant="body1">
                {config?.execution_mode || 'Unknown'}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Spark: {config?.spark_mode}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Catalog: {config?.catalog?.type}
              </Typography>
              <Typography variant="body2" color="textSecondary">
                Storage: {config?.storage?.type}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard;

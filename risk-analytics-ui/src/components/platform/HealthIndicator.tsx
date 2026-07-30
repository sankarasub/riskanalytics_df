import { Box, Chip, Typography } from '@mui/material';
import { CheckCircle, Error, Warning } from '@mui/icons-material';

interface HealthIndicatorProps {
  health: {
    api: string;
    spark: string;
    nessie: string;
    storage: string;
  } | null;
}

const HealthIndicator = ({ health }: HealthIndicatorProps) => {
  if (!health) {
    return <Typography color="error">Unable to determine health status</Typography>;
  }

  const services = [
    { name: 'API', status: health.api },
    { name: 'Spark', status: health.spark },
    { name: 'Nessie', status: health.nessie },
    { name: 'Storage', status: health.storage },
  ];

  const getStatusIcon = (status: string) => {
    if (status === 'healthy') return <CheckCircle color="success" />;
    if (status === 'unhealthy') return <Error color="error" />;
    return <Warning color="warning" />;
  };

  const getStatusColor = (status: string) => {
    if (status === 'healthy') return 'success';
    if (status === 'unhealthy') return 'error';
    return 'warning';
  };

  return (
    <Box>
      {services.map((service) => (
        <Box key={service.name} display="flex" alignItems="center" mb={1}>
          {getStatusIcon(service.status)}
          <Typography variant="body1" sx={{ ml: 1 }}>
            {service.name}
          </Typography>
          <Chip
            label={service.status}
            color={getStatusColor(service.status) as any}
            size="small"
            sx={{ ml: 'auto' }}
          />
        </Box>
      ))}
    </Box>
  );
};

export default HealthIndicator;

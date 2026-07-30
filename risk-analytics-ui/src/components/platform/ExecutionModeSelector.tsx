import { Box, Typography, RadioGroup, FormControlLabel, Radio, Card, CardContent } from '@mui/material';
import ComputerIcon from '@mui/icons-material/Computer';
import CloudIcon from '@mui/icons-material/Cloud';
import StorageIcon from '@mui/icons-material/Storage';

interface ExecutionModeSelectorProps {
  value: string;
  onChange: (mode: string) => void;
  disabled?: boolean;
}

const modes = [
  {
    value: 'local',
    label: 'Local',
    description: 'Local Spark, local files, no external services',
    icon: <ComputerIcon />
  },
  {
    value: 'hybrid',
    label: 'Hybrid',
    description: 'Local Spark, remote catalog/storage',
    icon: <CloudIcon />
  },
  {
    value: 'docker',
    label: 'Docker',
    description: 'Full Docker stack (requires Docker)',
    icon: <StorageIcon />
  }
];

const ExecutionModeSelector: React.FC<ExecutionModeSelectorProps> = ({ value, onChange, disabled = false }) => {
  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Execution Mode
        </Typography>
        <RadioGroup value={value} onChange={(e) => onChange(e.target.value)}>
          {modes.map((mode) => (
            <FormControlLabel
              key={mode.value}
              value={mode.value}
              control={<Radio />}
              disabled={disabled}
              label={
                <Box>
                  <Box display="flex" alignItems="center" gap={1}>
                    {mode.icon}
                    <Typography fontWeight="bold">{mode.label}</Typography>
                  </Box>
                  <Typography variant="caption" color="textSecondary">
                    {mode.description}
                  </Typography>
                </Box>
              }
            />
          ))}
        </RadioGroup>
      </CardContent>
    </Card>
  );
};

export default ExecutionModeSelector;
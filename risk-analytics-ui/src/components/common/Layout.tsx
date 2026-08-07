import { Box, Container, AppBar, Toolbar, Typography, Tabs, Tab } from '@mui/material';
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

const Layout = ({ children }: { children: React.ReactNode }) => {
  const navigate = useNavigate();
  const [currentTab, setCurrentTab] = useState('/');

  const handleTabChange = (_: React.SyntheticEvent, newValue: string) => {
    setCurrentTab(newValue);
    navigate(newValue);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Risk Analytics Dashboard
          </Typography>
        </Toolbar>
      </AppBar>
      
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={currentTab} onChange={handleTabChange} centered>
          <Tab label="Dashboard" value="/" />
          <Tab label="Risk Metrics" value="/metrics" />
          <Tab label="Pipeline Control" value="/pipeline" />
          <Tab label="Data Explorer" value="/data" />
          <Tab label="Configuration" value="/config" />
        </Tabs>
      </Box>

      <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
        {children}
      </Container>
    </Box>
  );
};

export default Layout;

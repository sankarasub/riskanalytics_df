import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider, CssBaseline } from '@mui/material'
import Layout from './components/common/Layout'
import Dashboard from './pages/Dashboard'
import RiskMetrics from './pages/RiskMetrics'
import PipelineControl from './pages/PipelineControl'
import DataExplorer from './pages/DataExplorer'
import Configuration from './pages/Configuration'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
})

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <Layout>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/metrics" element={<RiskMetrics />} />
              <Route path="/pipeline" element={<PipelineControl />} />
              <Route path="/data" element={<DataExplorer />} />
              <Route path="/config" element={<Configuration />} />
            </Routes>
          </Layout>
        </BrowserRouter>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

const theme = {
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
}

export default App

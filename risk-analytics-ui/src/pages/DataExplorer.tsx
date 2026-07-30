import { Box, Typography, Grid, Card, CardContent, TextField, MenuItem, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, LinearProgress, Alert, Pagination } from '@mui/material';
import { useState, useEffect } from 'react';
import { dataService, TableInfo, TableData } from '../services/dataService';

const DataExplorer = () => {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);

  const fetchTables = async () => {
    setLoading(true);
    setError(null);
    try {
      const tableList = await dataService.listTables();
      setTables(tableList);
    } catch (err) {
      setError('Failed to fetch tables');
      console.error('Error fetching tables:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchTableData = async (tableName: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await dataService.getTableData(tableName, rowsPerPage, page * rowsPerPage);
      setTableData(data);
    } catch (err) {
      setError('Failed to fetch table data');
      console.error('Error fetching table data:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      fetchTableData(selectedTable);
    }
  }, [selectedTable, page, rowsPerPage]);

  const handleTableChange = (tableName: string) => {
    setSelectedTable(tableName);
    setPage(0);
    setTableData(null);
  };

  const handlePageChange = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleRowsPerPageChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Data Explorer
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Tables
              </Typography>
              
              {loading && !tables.length ? (
                <LinearProgress />
              ) : (
                <TextField
                  fullWidth
                  select
                  label="Select Table"
                  value={selectedTable}
                  onChange={(e) => handleTableChange(e.target.value)}
                  disabled={loading}
                >
                  {tables.map((table) => (
                    <MenuItem key={table.name} value={table.name}>
                      {table.name}
                    </MenuItem>
                  ))}
                </TextField>
              )}
              
              <Button 
                variant="outlined" 
                onClick={fetchTables}
                disabled={loading}
                sx={{ mt: 2 }}
                fullWidth
              >
                Refresh Tables
              </Button>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={8}>
          {tableData && (
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  {selectedTable}
                </Typography>
                
                {loading && <LinearProgress />}
                
                {tableData.schema && tableData.schema.length > 0 && (
                  <Box sx={{ mb: 2 }}>
                    <Typography variant="subtitle2" gutterBottom>
                      Schema:
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                      {tableData.schema.map((column, index) => (
                        <Box 
                          key={index}
                          sx={{ 
                            px: 1, 
                            py: 0.5, 
                            bgcolor: 'primary.main', 
                            color: 'white',
                            borderRadius: 1,
                            fontSize: '0.8rem'
                          }}
                        >
                          {column.name}: {column.type}
                        </Box>
                      ))}
                    </Box>
                  </Box>
                )}
                
                {tableData.data && tableData.data.length > 0 ? (
                  <>
                    <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
                      <Table stickyHeader size="small">
                        <TableHead>
                          <TableRow>
                            {Object.keys(tableData.data[0]).map((key) => (
                              <TableCell key={key}>{key}</TableCell>
                            ))}
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {tableData.data.map((row, rowIndex) => (
                            <TableRow key={rowIndex}>
                              {Object.values(row).map((value, cellIndex) => (
                                <TableCell key={cellIndex}>
                                  {value !== null ? String(value) : 'NULL'}
                                </TableCell>
                              ))}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                    
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
                      <Typography variant="caption">
                        Showing {tableData.data.length} of {tableData.total} records
                      </Typography>
                      <Pagination
                        count={Math.ceil(tableData.total / rowsPerPage)}
                        page={page + 1}
                        onChange={handlePageChange}
                        color="primary"
                      />
                    </Box>
                  </>
                ) : (
                  <Typography variant="body2" color="textSecondary">
                    No data available
                  </Typography>
                )}
              </CardContent>
            </Card>
          )}
          
          {!selectedTable && (
            <Card>
              <CardContent>
                <Typography variant="body2" color="textSecondary">
                  Select a table to view its data
                </Typography>
              </CardContent>
            </Card>
          )}
        </Grid>
      </Grid>
    </Box>
  );
};

export default DataExplorer;

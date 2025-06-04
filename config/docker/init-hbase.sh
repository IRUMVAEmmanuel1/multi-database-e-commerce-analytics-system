#!/bin/bash
# config/docker/init-hbase.sh
# HBase Table Creation for E-commerce Analytics

echo " Initializing HBase for E-commerce Analytics..."
echo "================================================"

# Wait for HBase to be fully ready
echo "⏳ Waiting for HBase to be ready..."
sleep 60

# Function to check HBase status
check_hbase_status() {
    docker exec ecommerce_hbase_master hbase shell -n -e "status" 2>/dev/null
    return $?
}

# Wait for HBase to be responsive
max_attempts=20
attempt=1
while [ $attempt -le $max_attempts ]; do
    echo "Checking HBase status (attempt $attempt/$max_attempts)..."
    if check_hbase_status; then
        echo " HBase is ready!"
        break
    else
        echo "⏳ HBase not ready yet, waiting 15 seconds..."
        sleep 15
        attempt=$((attempt + 1))
    fi
done

if [ $attempt -gt $max_attempts ]; then
    echo " HBase failed to start after $max_attempts attempts"
    exit 1
fi

# Create HBase tables for e-commerce analytics
echo " Creating HBase tables for analytics..."

docker exec ecommerce_hbase_master hbase shell << 'HBASESHELL'

# Disable existing tables if they exist (for fresh setup)
disable_all 'user_.*'
drop_all 'user_.*'
disable_all 'product_.*'
drop_all 'product_.*'

# Create user_sessions table for time-series session data
create 'user_sessions', 
  {NAME => 'session_info', VERSIONS => 1, COMPRESSION => 'SNAPPY', 
   BLOOMFILTER => 'ROW', TTL => 31536000}, # 1 year TTL
  {NAME => 'page_views', VERSIONS => 1, COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW'},
  {NAME => 'device_data', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'geo_data', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'conversion_data', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

# Create product_views table for product interaction tracking
create 'product_views',
  {NAME => 'view_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW'},
  {NAME => 'interaction_data', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'performance_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

# Create user_events table for general event tracking
create 'user_events',
  {NAME => 'event_data', VERSIONS => 1, COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW'},
  {NAME => 'metadata', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'aggregated_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

# Create time-series aggregation tables
create 'daily_metrics',
  {NAME => 'user_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'product_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'revenue_metrics', VERSIONS => 1, COMPRESSION => 'SNAPPY'}

# Pre-split tables for better performance
split 'user_sessions', 'user_005000'
split 'product_views', 'prod_02500'

# Enable tables
enable 'user_sessions'
enable 'product_views' 
enable 'user_events'
enable 'daily_metrics'

# List tables to verify creation
list

# Show table descriptions
describe 'user_sessions'
describe 'product_views'

# Exit HBase shell
exit

HBASESHELL

echo ""
echo " HBase tables created successfully!"
echo ""
echo " Created Tables:"
echo "  • user_sessions    - Time-series user session data"
echo "  • product_views    - Product interaction tracking"
echo "  • user_events      - General event logging"
echo "  • daily_metrics    - Aggregated daily statistics"
echo ""
echo "Access Points:"
echo "  • HBase Master UI: http://localhost:16010"
echo "  • Thrift Server:   localhost:9090"
echo "  • REST API:        http://localhost:8080"
echo ""
echo "Connection Details:"
echo "  • Zookeeper:       localhost:2181"
echo "  • HDFS NameNode:   http://localhost:9870"
echo ""
echo "================================================"
echo " HBase initialization complete!"
import json
import boto3
import psycopg2
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class AuroraMigrationAutomation:
    def __init__(self, event):
        self.event = event
        self.source_config = event.get('source_db', {})
        self.dest_config = event.get('dest_db', {})
        self.migration_config = event.get('migration_config', {})
        self.validation_errors = []
        
        # Initialize AWS clients
        self.rds_client = boto3.client('rds')
        self.dms_client = boto3.client('dms')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.ec2_client = boto3.client('ec2')
        
    def validate_parameters(self):
        """Validate all required parameters are present"""
        required_source_params = ['host', 'port', 'username', 'password', 'database', 'engine']
        required_dest_params = ['cluster_identifier', 'instance_class', 'engine', 'engine_version']
        
        for param in required_source_params:
            if param not in self.source_config:
                self.validation_errors.append(f"Missing source parameter: {param}")
                
        for param in required_dest_params:
            if param not in self.dest_config:
                self.validation_errors.append(f"Missing destination parameter: {param}")
        
        if self.validation_errors:
            return False
        return True
    
    def check_source_connectivity(self):
        """Test connection to source PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                host=self.source_config['host'],
                port=self.source_config['port'],
                user=self.source_config['username'],
                password=self.source_config['password'],
                database=self.source_config['database']
            )
            
            # Get database statistics
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            
            cursor.execute("SELECT pg_database_size(%s);", (self.source_config['database'],))
            db_size = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT count(*) FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            table_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'status': 'success',
                'version': version[0],
                'database_size_bytes': db_size,
                'table_count': table_count
            }
            
        except Exception as e:
            logger.error(f"Source database connection failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def analyze_source_database(self):
        """Analyze source database for migration planning"""
        try:
            conn = psycopg2.connect(
                host=self.source_config['host'],
                port=self.source_config['port'],
                user=self.source_config['username'],
                password=self.source_config['password'],
                database=self.source_config['database']
            )
            
            cursor = conn.cursor()
            
            # Get table sizes and row counts
            cursor.execute("""
                SELECT 
                    table_name,
                    pg_size_pretty(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"')) as size,
                    pg_total_relation_size('"' || table_schema || '"."' || table_name || '"') as size_bytes,
                    (SELECT count(*) FROM ("' || table_schema || '"."' || table_name || '")) as row_count
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY size_bytes DESC
            """)
            
            tables = cursor.fetchall()
            
            # Get extension information
            cursor.execute("SELECT name, installed_version FROM pg_available_extensions WHERE installed_version IS NOT NULL")
            extensions = cursor.fetchall()
            
            # Get database parameters
            cursor.execute("SHOW ALL;")
            parameters = cursor.fetchall()
            
            conn.close()
            
            return {
                'tables': [{'name': t[0], 'size': t[1], 'size_bytes': t[2], 'row_count': t[3]} for t in tables],
                'extensions': [{'name': e[0], 'version': e[1]} for e in extensions],
                'parameters': [{'name': p[0], 'setting': p[1]} for p in parameters]
            }
            
        except Exception as e:
            logger.error(f"Source database analysis failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def check_aurora_compatibility(self, source_analysis):
        """Check compatibility with Aurora PostgreSQL"""
        compatibility_issues = []
        
        # Check extension compatibility
        aurora_supported_extensions = [
            'plpgsql', 'pg_stat_statements', 'pgcrypto', 'plv8', 
            'postgis', 'hstore', 'uuid-ossp', 'pg_prewarm'
        ]
        
        for ext in source_analysis.get('extensions', []):
            if ext['name'] not in aurora_supported_extensions:
                compatibility_issues.append(f"Extension {ext['name']} may not be supported in Aurora")
        
        # Check parameter compatibility
        aurora_unsupported_params = [
            'shared_buffers', 'huge_pages', 'wal_buffers'
        ]
        
        for param in source_analysis.get('parameters', []):
            if param['name'] in aurora_unsupported_params:
                compatibility_issues.append(f"Parameter {param['name']} is managed by Aurora and cannot be modified")
        
        return compatibility_issues
    
    def calculate_aurora_sizing(self, source_analysis):
        """Calculate appropriate Aurora instance size based on source database"""
        total_size_bytes = sum(table['size_bytes'] for table in source_analysis.get('tables', []))
        total_size_gb = total_size_bytes / (1024 ** 3)
        
        # Simple sizing logic - adjust based on your requirements
        if total_size_gb < 100:
            instance_class = 'db.r5.large'
            storage_size = 100
        elif total_size_gb < 500:
            instance_class = 'db.r5.xlarge'
            storage_size = 500
        elif total_size_gb < 1000:
            instance_class = 'db.r5.2xlarge'
            storage_size = 1000
        else:
            instance_class = 'db.r5.4xlarge'
            storage_size = max(1000, total_size_gb * 1.2)  # 20% buffer
        
        return {
            'instance_class': instance_class,
            'allocated_storage': int(storage_size),
            'iops': 1000 if storage_size > 1000 else 0  # Provisioned IOPS for large databases
        }
    
    def create_aurora_cluster(self, sizing_recommendation):
        """Create Aurora PostgreSQL cluster"""
        try:
            cluster_identifier = self.dest_config['cluster_identifier']
            
            # Check if cluster already exists
            try:
                response = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier=cluster_identifier
                )
                logger.info(f"Aurora cluster {cluster_identifier} already exists")
                return {'status': 'exists', 'cluster': response['DBClusters'][0]}
            except ClientError as e:
                if e.response['Error']['Code'] != 'DBClusterNotFoundFault':
                    raise
            
            # Create Aurora cluster
            create_params = {
                'DBClusterIdentifier': cluster_identifier,
                'Engine': self.dest_config.get('engine', 'aurora-postgresql'),
                'EngineVersion': self.dest_config.get('engine_version', '13.7'),
                'MasterUsername': self.dest_config.get('master_username', 'postgres'),
                'MasterUserPassword': self.dest_config.get('master_password'),
                'DatabaseName': self.dest_config.get('database_name', self.source_config['database']),
                'DBClusterInstanceClass': sizing_recommendation['instance_class'],
                'AllocatedStorage': sizing_recommendation['allocated_storage'],
                'StorageEncrypted': True,
                'DeletionProtection': False,
                'BackupRetentionPeriod': 7,
                'Port': 5432,
                'EnableCloudwatchLogsExports': ['postgresql']
            }
            
            if sizing_recommendation.get('iops', 0) > 0:
                create_params['Iops'] = sizing_recommendation['iops']
                create_params['StorageType'] = 'io1'
            
            response = self.rds_client.create_db_cluster(**create_params)
            
            # Create cluster instance
            instance_response = self.rds_client.create_db_instance(
                DBInstanceIdentifier=f"{cluster_identifier}-instance-1",
                DBInstanceClass=sizing_recommendation['instance_class'],
                Engine=self.dest_config.get('engine', 'aurora-postgresql'),
                DBClusterIdentifier=cluster_identifier,
                PubliclyAccessible=False
            )
            
            return {'status': 'creating', 'cluster': response['DBCluster']}
            
        except Exception as e:
            logger.error(f"Aurora cluster creation failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def wait_for_aurora_availability(self, cluster_identifier, timeout=1800):
        """Wait for Aurora cluster to become available"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier=cluster_identifier
                )
                
                status = response['DBClusters'][0]['Status']
                logger.info(f"Aurora cluster status: {status}")
                
                if status == 'available':
                    # Check instance status too
                    instances = self.rds_client.describe_db_instances(
                        Filters=[{'Name': 'db-cluster-id', 'Values': [cluster_identifier]}]
                    )
                    
                    all_available = all(instance['DBInstanceStatus'] == 'available' 
                                      for instance in instances['DBInstances'])
                    
                    if all_available:
                        return {'status': 'available', 'cluster': response['DBClusters'][0]}
                
                elif status in ['creating', 'backing-up', 'modifying']:
                    time.sleep(30)
                    continue
                else:
                    return {'status': 'error', 'message': f"Cluster in unexpected state: {status}"}
                    
            except Exception as e:
                logger.error(f"Error checking cluster status: {str(e)}")
                time.sleep(30)
        
        return {'status': 'timeout', 'message': 'Cluster creation timed out'}
    
    def setup_dms_infrastructure(self):
        """Set up DMS replication instance, endpoints, and task"""
        try:
            # Create DMS replication instance
            replication_instance_id = f"{self.dest_config['cluster_identifier']}-dms-instance"
            
            try:
                instances = self.dms_client.describe_replication_instances(
                    Filters=[{'Name': 'replication-instance-id', 'Values': [replication_instance_id]}]
                )
                replication_instance_arn = instances['ReplicationInstances'][0]['ReplicationInstanceArn']
                logger.info(f"DMS replication instance {replication_instance_id} already exists")
            except (ClientError, IndexError):
                dms_response = self.dms_client.create_replication_instance(
                    ReplicationInstanceIdentifier=replication_instance_id,
                    ReplicationInstanceClass='dms.t3.medium',
                    AllocatedStorage=50,
                    PubliclyAccessible=False,
                    EngineVersion='3.4.6'
                )
                replication_instance_arn = dms_response['ReplicationInstance']['ReplicationInstanceArn']
            
            # Create source endpoint
            source_endpoint_id = f"{self.source_config['database']}-source-endpoint"
            source_endpoint_arn = self.create_dms_endpoint(
                source_endpoint_id, 'source', self.source_config
            )
            
            # Create target endpoint
            target_endpoint_id = f"{self.dest_config['cluster_identifier']}-target-endpoint"
            
            # Get Aurora cluster endpoint
            cluster_info = self.rds_client.describe_db_clusters(
                DBClusterIdentifier=self.dest_config['cluster_identifier']
            )
            cluster_endpoint = cluster_info['DBClusters'][0]['Endpoint']
            
            target_config = {
                'host': cluster_endpoint,
                'port': 5432,
                'username': self.dest_config.get('master_username', 'postgres'),
                'password': self.dest_config.get('master_password'),
                'database': self.dest_config.get('database_name', self.source_config['database'])
            }
            
            target_endpoint_arn = self.create_dms_endpoint(
                target_endpoint_id, 'target', target_config
            )
            
            # Create replication task
            task_id = f"{self.source_config['database']}-to-{self.dest_config['cluster_identifier']}-task"
            task_arn = self.create_dms_task(
                task_id, replication_instance_arn, source_endpoint_arn, target_endpoint_arn
            )
            
            return {
                'replication_instance_arn': replication_instance_arn,
                'source_endpoint_arn': source_endpoint_arn,
                'target_endpoint_arn': target_endpoint_arn,
                'task_arn': task_arn
            }
            
        except Exception as e:
            logger.error(f"DMS setup failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def create_dms_endpoint(self, endpoint_id, endpoint_type, config):
        """Create DMS source or target endpoint"""
        try:
            endpoints = self.dms_client.describe_endpoints(
                Filters=[{'Name': 'endpoint-id', 'Values': [endpoint_id]}]
            )
            
            if endpoints['Endpoints']:
                return endpoints['Endpoints'][0]['EndpointArn']
                
        except ClientError:
            pass
        
        # Create new endpoint
        endpoint_config = {
            'EndpointIdentifier': endpoint_id,
            'EndpointType': endpoint_type,
            'EngineName': 'postgres',
            'Username': config['username'],
            'Password': config['password'],
            'ServerName': config['host'],
            'Port': config['port'],
            'DatabaseName': config['database'],
            'SslMode': 'require'
        }
        
        if endpoint_type == 'source':
            endpoint_config['ExtraConnectionAttributes'] = 'heartbeatFrequency=5;'
        else:
            endpoint_config['ExtraConnectionAttributes'] = 'parallelApplyThreads=4;'
        
        response = self.dms_client.create_endpoint(**endpoint_config)
        return response['Endpoint']['EndpointArn']
    
    def create_dms_task(self, task_id, replication_instance_arn, source_endpoint_arn, target_endpoint_arn):
        """Create DMS replication task"""
        try:
            tasks = self.dms_client.describe_replication_tasks(
                Filters=[{'Name': 'replication-task-id', 'Values': [task_id]}]
            )
            
            if tasks['ReplicationTasks']:
                return tasks['ReplicationTasks'][0]['ReplicationTaskArn']
                
        except ClientError:
            pass
        
        # Create table mappings - include all tables
        table_mappings = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "1",
                    "object-locator": {
                        "schema-name": "%",
                        "table-name": "%"
                    },
                    "rule-action": "include"
                }
            ]
        }
        
        # Create replication task
        response = self.dms_client.create_replication_task(
            ReplicationTaskIdentifier=task_id,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load-and-cdc',
            TableMappings=json.dumps(table_mappings),
            ReplicationTaskSettings=json.dumps({
                "TargetMetadata": {
                    "TargetSchema": "",
                    "SupportLobs": True,
                    "FullLobMode": False,
                    "LobChunkSize": 64,
                    "LimitedSizeLobMode": True,
                    "LobMaxSize": 32
                },
                "FullLoadSettings": {
                    "TargetTablePrepMode": "DO_NOTHING",
                    "CommitRate": 10000
                },
                "Logging": {
                    "EnableLogging": True
                }
            })
        )
        
        return response['ReplicationTask']['ReplicationTaskArn']
    
    def start_dms_migration(self, task_arn):
        """Start DMS migration task"""
        try:
            # Start the replication task
            response = self.dms_client.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType='start-replication'
            )
            
            return {'status': 'started', 'task_status': response['ReplicationTask']['Status']}
            
        except Exception as e:
            logger.error(f"DMS migration start failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def monitor_migration_progress(self, task_arn, timeout=86400):  # 24 hour timeout
        """Monitor DMS migration progress"""
        start_time = time.time()
        last_stats = {}
        
        while time.time() - start_time < timeout:
            try:
                # Get task status
                task_response = self.dms_client.describe_replication_tasks(
                    Filters=[{'Name': 'replication-task-arn', 'Values': [task_arn]}]
                )
                
                task_status = task_response['ReplicationTasks'][0]['Status']
                logger.info(f"DMS task status: {task_status}")
                
                if task_status in ['stopped', 'ready']:
                    # Get final statistics
                    stats_response = self.dms_client.describe_replication_task_statistics(
                        ReplicationTaskArn=task_arn
                    )
                    
                    return {
                        'status': 'completed',
                        'task_status': task_status,
                        'statistics': stats_response['ReplicationTaskStatistics']
                    }
                
                elif task_status == 'running':
                    # Get current statistics
                    stats_response = self.dms_client.describe_replication_task_statistics(
                        ReplicationTaskArn=task_arn
                    )
                    
                    current_stats = stats_response['ReplicationTaskStatistics']
                    
                    # Check if stats have changed (indicating progress)
                    if current_stats != last_stats:
                        last_stats = current_stats
                        logger.info(f"Migration progress: {json.dumps(current_stats, default=str)}")
                    
                    time.sleep(60)  # Check every minute
                    continue
                
                elif task_status in ['failed', 'error']:
                    return {'status': 'error', 'message': f"Migration failed with status: {task_status}"}
                
                else:
                    time.sleep(60)
                    continue
                    
            except Exception as e:
                logger.error(f"Error monitoring migration: {str(e)}")
                time.sleep(60)
        
        return {'status': 'timeout', 'message': 'Migration monitoring timed out'}
    
    def validate_migration(self):
        """Validate data after migration"""
        try:
            # Connect to both source and target databases for validation
            source_conn = psycopg2.connect(
                host=self.source_config['host'],
                port=self.source_config['port'],
                user=self.source_config['username'],
                password=self.source_config['password'],
                database=self.source_config['database']
            )
            
            # Get Aurora cluster endpoint
            cluster_info = self.rds_client.describe_db_clusters(
                DBClusterIdentifier=self.dest_config['cluster_identifier']
            )
            cluster_endpoint = cluster_info['DBClusters'][0]['Endpoint']
            
            target_conn = psycopg2.connect(
                host=cluster_endpoint,
                port=5432,
                user=self.dest_config.get('master_username', 'postgres'),
                password=self.dest_config.get('master_password'),
                database=self.dest_config.get('database_name', self.source_config['database'])
            )
            
            source_cursor = source_conn.cursor()
            target_cursor = target_conn.cursor()
            
            # Compare table counts
            validation_results = {}
            
            # Get list of tables
            source_cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            tables = [row[0] for row in source_cursor.fetchall()]
            
            for table in tables:
                # Compare row counts
                source_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                source_count = source_cursor.fetchone()[0]
                
                target_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                target_count = target_cursor.fetchone()[0]
                
                validation_results[table] = {
                    'source_count': source_count,
                    'target_count': target_count,
                    'match': source_count == target_count
                }
            
            source_conn.close()
            target_conn.close()
            
            # Check if all tables match
            all_match = all(result['match'] for result in validation_results.values())
            
            return {
                'status': 'success' if all_match else 'validation_failed',
                'validation_results': validation_results,
                'all_tables_match': all_match
            }
            
        except Exception as e:
            logger.error(f"Migration validation failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def execute_migration(self):
        """Execute the complete migration process"""
        migration_report = {
            'start_time': datetime.utcnow().isoformat(),
            'steps': {},
            'status': 'started'
        }
        
        try:
            # Step 1: Validate parameters
            if not self.validate_parameters():
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Parameter validation failed'
                migration_report['validation_errors'] = self.validation_errors
                return migration_report
            
            migration_report['steps']['parameter_validation'] = {'status': 'success'}
            
            # Step 2: Check source connectivity
            source_connectivity = self.check_source_connectivity()
            if source_connectivity['status'] != 'success':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Source database connectivity failed'
                migration_report['connectivity_error'] = source_connectivity['message']
                return migration_report
            
            migration_report['steps']['source_connectivity'] = {
                'status': 'success',
                'version': source_connectivity['version'],
                'database_size_bytes': source_connectivity['database_size_bytes'],
                'table_count': source_connectivity['table_count']
            }
            
            # Step 3: Analyze source database
            source_analysis = self.analyze_source_database()
            if 'status' in source_analysis and source_analysis['status'] == 'error':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Source database analysis failed'
                migration_report['analysis_error'] = source_analysis['message']
                return migration_report            
            
            migration_report['steps']['source_analysis'] = source_analysis
            
            # Step 4: Check Aurora compatibility
            compatibility_issues = self.check_aurora_compatibility(source_analysis)
            migration_report['steps']['compatibility_check'] = {
                'issues': compatibility_issues,
                'compatible': len(compatibility_issues) == 0
            }
            
            if compatibility_issues and not self.migration_config.get('force_migration', False):
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Compatibility issues found'
                return migration_report
            
            # Step 5: Calculate Aurora sizing
            sizing_recommendation = self.calculate_aurora_sizing(source_analysis)
            migration_report['steps']['sizing_recommendation'] = sizing_recommendation
            
            # Step 6: Create Aurora cluster
            cluster_creation = self.create_aurora_cluster(sizing_recommendation)
            migration_report['steps']['cluster_creation'] = cluster_creation
            
            if cluster_creation['status'] == 'error':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Aurora cluster creation failed'
                migration_report['creation_error'] = cluster_creation['message']
                return migration_report            
            
            # Step 7: Wait for cluster availability
            if cluster_creation['status'] == 'creating':
                cluster_availability = self.wait_for_aurora_availability(
                    self.dest_config['cluster_identifier']
                )
                migration_report['steps']['cluster_availability'] = cluster_availability
                
                if cluster_availability['status'] != 'available':
                    migration_report['status'] = 'failed'
                    migration_report['error'] = 'Aurora cluster not available'
                    migration_report['availability_error'] = cluster_availability['message']
                    return migration_report
            
            # Step 8: Set up DMS infrastructure
            dms_setup = self.setup_dms_infrastructure()
            migration_report['steps']['dms_setup'] = dms_setup
            
            if 'status' in dms_setup and dms_setup['status'] == 'error':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'DMS setup failed'
                migration_report['dms_error'] = dms_setup['message']
                return migration_report            
            
            # Step 9: Start migration
            migration_start = self.start_dms_migration(dms_setup['task_arn'])
            migration_report['steps']['migration_start'] = migration_start
            
            if migration_start['status'] != 'started':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Migration start failed'
                migration_report['migration_start_error'] = migration_start.get('message', 'Unknown error')
                return migration_report            
            
            # Step 10: Monitor migration
            migration_progress = self.monitor_migration_progress(dms_setup['task_arn'])
            migration_report['steps']['migration_progress'] = migration_progress
            
            if migration_progress['status'] != 'completed':
                migration_report['status'] = 'failed'
                migration_report['error'] = 'Migration failed or timed out'
                migration_report['migration_error'] = migration_progress.get('message', 'Unknown error')
                return migration_report            
            
            # Step 11: Validate migration
            validation = self.validate_migration()
            migration_report['steps']['validation'] = validation
            
            if validation['status'] != 'success':
                migration_report['status'] = 'validation_failed'
                migration_report['error'] = 'Migration validation failed'
                return migration_report
            
            # Migration successful
            migration_report['status'] = 'completed'
            migration_report['end_time'] = datetime.utcnow().isoformat()
            
            return migration_report
            
        except Exception as e:
            logger.error(f"Migration execution failed: {str(e)}")
            migration_report['status'] = 'failed'
            migration_report['error'] = str(e)
            migration_report['end_time'] = datetime.utcnow().isoformat()
            return migration_report

def lambda_handler(event, context):
    """AWS Lambda handler function for PostgreSQL to Aurora migration"""
    try:
        # Initialize migration automation
        migration = AuroraMigrationAutomation(event)
        
        # Execute migration
        result = migration.execute_migration()
        
        # Return results
        return {
            'statusCode': 200 if result['status'] in ['completed', 'validation_failed'] else 500,
            'body': json.dumps(result, default=str)
        }
        
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

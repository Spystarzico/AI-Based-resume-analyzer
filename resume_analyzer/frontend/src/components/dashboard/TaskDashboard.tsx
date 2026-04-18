import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { 
  CheckCircle, 
  Circle, 
  Code, 
  Database, 
  Server, 
  Cloud, 
  Workflow,
  Zap,
  Layers,
  Terminal,
  FileText,
  BarChart3,
  Clock,
  Cpu,
  HardDrive,
  Network,
  GitBranch,
  Box,
  Sparkles,
  TrendingUp,
  Award
} from 'lucide-react';

interface Task {
  id: number;
  title: string;
  description: string;
  status: 'completed' | 'in-progress' | 'pending';
  category: string;
  concepts: string[];
  fileName: string;
}

const tasks: Task[] = [
  // Foundation
  { id: 1, title: 'Linux + File System', description: 'Set up Linux environment, directory structure, permissions, shell scripts', status: 'completed', category: 'Foundation', concepts: ['File Permissions', 'Shell Scripts', 'Directory Structure'], fileName: 'task01_linux_filesystem.py' },
  { id: 2, title: 'Networking', description: 'API simulation, protocol analysis, packet capture, secure data flow', status: 'completed', category: 'Foundation', concepts: ['HTTP/HTTPS', 'SSL/TLS', 'Packet Capture'], fileName: 'task02_networking.py' },
  { id: 3, title: 'Python Basics', description: 'Read CSV files, clean missing values, merge datasets', status: 'completed', category: 'Foundation', concepts: ['CSV Processing', 'Data Cleaning', 'Data Merging'], fileName: 'task03_python_basics.py' },
  { id: 4, title: 'Advanced Python', description: 'Reusable modules for normalization, aggregation, validation', status: 'completed', category: 'Foundation', concepts: ['Decorators', 'Abstract Classes', 'Pipelines'], fileName: 'task04_advanced_python.py' },
  { id: 5, title: 'Pandas + NumPy', description: 'Large-scale data analysis with 1M+ rows, memory optimization', status: 'completed', category: 'Foundation', concepts: ['Memory Optimization', 'Performance Tuning'], fileName: 'task05_pandas_numpy.py' },
  
  // Database & SQL
  { id: 6, title: 'SQL Basics', description: 'Student database design with SELECT, WHERE, GROUP BY, ORDER BY', status: 'completed', category: 'Database & SQL', concepts: ['Database Design', 'Basic Queries', 'Filtering'], fileName: 'task06_sql_basics.py' },
  { id: 7, title: 'Advanced SQL', description: 'Joins, subqueries, window functions for business reporting', status: 'completed', category: 'Database & SQL', concepts: ['JOINs', 'Subqueries', 'Window Functions'], fileName: 'task07_advanced_sql.py' },
  { id: 8, title: 'Database Concepts', description: 'Compare OLTP vs OLAP systems with schema design', status: 'completed', category: 'Database & SQL', concepts: ['OLTP', 'OLAP', 'Normalization'], fileName: 'task08_database_concepts.py' },
  { id: 9, title: 'Data Warehousing', description: 'Star schema design for e-commerce analytics', status: 'completed', category: 'Database & SQL', concepts: ['Star Schema', 'Fact Tables', 'Dimensions'], fileName: 'task09_data_warehousing.py' },
  { id: 10, title: 'ETL vs ELT', description: 'Build and compare ETL and ELT pipeline performance', status: 'completed', category: 'Database & SQL', concepts: ['ETL Pipeline', 'ELT Pipeline'], fileName: 'task10_etl_vs_elt.py' },
  
  // Big Data
  { id: 11, title: 'Data Ingestion', description: 'Batch ingestion pipeline from CSV to database', status: 'completed', category: 'Big Data', concepts: ['Batch Processing', 'Data Validation'], fileName: 'task11_data_ingestion.py' },
  { id: 12, title: 'Hadoop', description: 'Set up HDFS locally and upload structured/unstructured data', status: 'completed', category: 'Big Data', concepts: ['HDFS', 'Namenode', 'Datanode'], fileName: 'task12_hadoop.py' },
  { id: 13, title: 'HDFS Architecture', description: 'Explain Namenode, Datanode and simulate data storage', status: 'completed', category: 'Big Data', concepts: ['HDFS Architecture', 'Replication'], fileName: 'task13_hdfs_architecture.py' },
  { id: 14, title: 'Spark Basics', description: 'Create Spark job to process large dataset', status: 'completed', category: 'Big Data', concepts: ['RDD', 'Transformations', 'Actions'], fileName: 'task14_spark_basics.py' },
  { id: 15, title: 'Spark DataFrames', description: 'Perform transformations and actions on dataset', status: 'completed', category: 'Big Data', concepts: ['DataFrame API', 'select/filter', 'join'], fileName: 'task15_spark_dataframes.py' },
  { id: 16, title: 'Spark SQL', description: 'Query large dataset using Spark SQL', status: 'completed', category: 'Big Data', concepts: ['Spark SQL', 'Temporary Views'], fileName: 'task16_spark_sql.py' },
  { id: 17, title: 'PySpark Advanced', description: 'Optimize Spark jobs using partitioning and caching', status: 'completed', category: 'Big Data', concepts: ['Partitioning', 'Caching', 'Broadcast Join'], fileName: 'task17_pyspark_advanced.py' },
  
  // Streaming
  { id: 18, title: 'Streaming Concepts', description: 'Simulate real-time data processing using streaming APIs', status: 'completed', category: 'Streaming', concepts: ['Stream Processing', 'Windows'], fileName: 'task18_streaming_concepts.py' },
  { id: 19, title: 'Kafka Basics', description: 'Set up Kafka producer-consumer pipeline', status: 'completed', category: 'Streaming', concepts: ['Topics', 'Partitions', 'Producers'], fileName: 'task19_kafka_basics.py' },
  { id: 20, title: 'Kafka Advanced', description: 'Implement partitioning and offset management', status: 'completed', category: 'Streaming', concepts: ['Partitioning', 'Offset Management'], fileName: 'task20_kafka_advanced.py' },
  { id: 21, title: 'Structured Streaming', description: 'Build Spark streaming job for real-time logs', status: 'completed', category: 'Streaming', concepts: ['Structured Streaming', 'Output Modes'], fileName: 'task21_structured_streaming.py' },
  
  // Orchestration
  { id: 22, title: 'Airflow Basics', description: 'Create DAG for ETL pipeline', status: 'completed', category: 'Orchestration', concepts: ['DAGs', 'Tasks', 'Operators'], fileName: 'task22_airflow_basics.py' },
  { id: 23, title: 'Airflow Advanced', description: 'Add scheduling, monitoring, retry mechanisms', status: 'completed', category: 'Orchestration', concepts: ['Scheduling', 'Retries', 'SLAs'], fileName: 'task23_airflow_advanced.py' },
  
  // Cloud
  { id: 24, title: 'Cloud Basics', description: 'Compare AWS, GCP, Azure services', status: 'completed', category: 'Cloud', concepts: ['AWS', 'GCP', 'Azure'], fileName: 'task24_cloud_basics.py' },
  { id: 25, title: 'Cloud Storage', description: 'Upload and retrieve files from S3/GCS', status: 'completed', category: 'Cloud', concepts: ['S3', 'GCS', 'Lifecycle Policies'], fileName: 'task25_cloud_storage.py' },
  { id: 26, title: 'Cloud Compute', description: 'Launch EC2 instance and deploy data pipeline', status: 'completed', category: 'Cloud', concepts: ['EC2', 'Auto Scaling', 'User Data'], fileName: 'task26_cloud_compute.py' },
  { id: 27, title: 'Data Warehouse Cloud', description: 'Use BigQuery/Redshift to analyze large data', status: 'completed', category: 'Cloud', concepts: ['BigQuery', 'Redshift'], fileName: 'task27_data_warehouse_cloud.py' },
  
  // Advanced
  { id: 28, title: 'Lakehouse', description: 'Implement Delta Lake/Iceberg and manage versions', status: 'completed', category: 'Advanced', concepts: ['Delta Lake', 'ACID', 'Time Travel'], fileName: 'task28_lakehouse.py' },
  { id: 29, title: 'Data Quality', description: 'Build validation checks and anomaly detection', status: 'completed', category: 'Advanced', concepts: ['Data Validation', 'Anomaly Detection'], fileName: 'task29_data_quality.py' },
  { id: 30, title: 'Final Project', description: 'End-to-end pipeline: ingestion → processing → storage → orchestration → dashboard', status: 'completed', category: 'Advanced', concepts: ['End-to-End Pipeline', 'Architecture'], fileName: 'task30_final_project.py' },
];

const categoryIcons: Record<string, any> = {
  'Foundation': Terminal,
  'Database & SQL': Database,
  'Big Data': Server,
  'Streaming': Zap,
  'Orchestration': Workflow,
  'Cloud': Cloud,
  'Advanced': Layers
};

export default function TaskDashboard() {
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null);

  const categories = Array.from(new Set(tasks.map(t => t.category)));
  
  const completedTasks = tasks.filter(t => t.status === 'completed').length;
  const totalTasks = tasks.length;
  const completionPercentage = (completedTasks / totalTasks) * 100;

  const getTasksByCategory = (category: string) => tasks.filter(t => t.category === category);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-slate-900">Task Dashboard</h2>
        <p className="text-slate-600 mt-1">
          Track progress across all 30 Data Engineering tasks
        </p>
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Total Tasks</p>
                <p className="text-3xl font-bold text-slate-900">{totalTasks}</p>
              </div>
              <Box className="h-8 w-8 text-blue-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Completed</p>
                <p className="text-3xl font-bold text-green-600">{completedTasks}</p>
              </div>
              <CheckCircle className="h-8 w-8 text-green-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Categories</p>
                <p className="text-3xl font-bold text-indigo-600">{categories.length}</p>
              </div>
              <Layers className="h-8 w-8 text-indigo-600" />
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-slate-600">Progress</p>
                <p className="text-3xl font-bold text-purple-600">{completionPercentage.toFixed(0)}%</p>
              </div>
              <TrendingUp className="h-8 w-8 text-purple-600" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Overall Progress */}
      <Card>
        <CardHeader>
          <CardTitle>Overall Progress</CardTitle>
          <CardDescription>
            {completedTasks} of {totalTasks} tasks completed
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Progress value={completionPercentage} className="h-3" />
          <div className="mt-4 grid grid-cols-7 gap-1">
            {tasks.map((task) => (
              <div
                key={task.id}
                className={`h-8 rounded flex items-center justify-center text-xs font-medium ${
                  task.status === 'completed'
                    ? 'bg-green-500 text-white'
                    : 'bg-slate-200 text-slate-500'
                }`}
                title={`Task ${task.id}: ${task.title}`}
              >
                {task.id}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Category Progress */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {categories.map((category) => {
          const categoryTasks = getTasksByCategory(category);
          const completed = categoryTasks.filter(t => t.status === 'completed').length;
          const total = categoryTasks.length;
          const percentage = (completed / total) * 100;
          const Icon = categoryIcons[category] || Box;
          
          return (
            <Card key={category} className="hover:shadow-md transition-shadow cursor-pointer"
                  onClick={() => setSelectedCategory(selectedCategory === category ? null : category)}>
              <CardContent className="p-6">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className="p-2 bg-blue-100 rounded-lg">
                      <Icon className="h-5 w-5 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-slate-900">{category}</h3>
                      <p className="text-sm text-slate-600">{completed}/{total} tasks</p>
                    </div>
                  </div>
                  <Badge variant={percentage === 100 ? 'default' : 'secondary'}>
                    {percentage.toFixed(0)}%
                  </Badge>
                </div>
                <Progress value={percentage} className="mt-4 h-2" />
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Task List */}
      <Tabs defaultValue="all" className="w-full">
        <TabsList className="grid w-full grid-cols-4 lg:grid-cols-8">
          <TabsTrigger value="all">All</TabsTrigger>
          {categories.map(cat => (
            <TabsTrigger key={cat} value={cat}>{cat}</TabsTrigger>
          ))}
        </TabsList>
        
        <TabsContent value="all" className="mt-6">
          <div className="grid grid-cols-1 gap-4">
            {tasks.map((task) => (
              <TaskCard key={task.id} task={task} />
            ))}
          </div>
        </TabsContent>
        
        {categories.map(cat => (
          <TabsContent key={cat} value={cat} className="mt-6">
            <div className="grid grid-cols-1 gap-4">
              {getTasksByCategory(cat).map((task) => (
                <TaskCard key={task.id} task={task} />
              ))}
            </div>
          </TabsContent>
        ))}
      </Tabs>
    </div>
  );
}

function TaskCard({ task }: { task: Task }) {
  return (
    <Card className="hover:shadow-md transition-shadow">
      <CardContent className="p-4">
        <div className="flex items-start gap-4">
          <div className={`p-2 rounded-lg ${
            task.status === 'completed' ? 'bg-green-100' : 'bg-slate-100'
          }`}>
            {task.status === 'completed' ? (
              <CheckCircle className="h-5 w-5 text-green-600" />
            ) : (
              <Circle className="h-5 w-5 text-slate-400" />
            )}
          </div>
          
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-1">
              <Badge variant="outline" className="text-xs">Task {task.id}</Badge>
              <h3 className="font-semibold text-slate-900">{task.title}</h3>
            </div>
            
            <p className="text-sm text-slate-600 mb-2">{task.description}</p>
            
            <div className="flex flex-wrap gap-1 mb-2">
              {task.concepts.map((concept, idx) => (
                <Badge key={idx} variant="secondary" className="text-xs">
                  {concept}
                </Badge>
              ))}
            </div>
            
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Code className="h-3 w-3" />
              <span>{task.fileName}</span>
            </div>
          </div>
          
          <Badge className={
            task.status === 'completed' 
              ? 'bg-green-100 text-green-700' 
              : 'bg-slate-100 text-slate-600'
          }>
            {task.status === 'completed' ? 'Completed' : 'Pending'}
          </Badge>
        </div>
      </CardContent>
    </Card>
  );
}

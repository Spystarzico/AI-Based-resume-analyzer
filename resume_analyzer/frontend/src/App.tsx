import { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';
import { 
  Database, 
  Server, 
  Cloud, 
  Code, 
  BarChart3, 
  Layers,
  CheckCircle,
  Sparkles,
  Box,
  Menu,
  X,
  Terminal,
  Zap,
  Workflow,
  Search
} from 'lucide-react';
import ResumeAnalyzer from './components/resume/ResumeAnalyzer';
import TaskDashboard from './components/dashboard/TaskDashboard';

function App() {
  const [activeTab, setActiveTab] = useState('analyzer');
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const taskCategories = [
    { id: 'foundation', name: 'Foundation', icon: Terminal, tasks: [1, 2, 3, 4, 5] },
    { id: 'database', name: 'Database & SQL', icon: Database, tasks: [6, 7, 8, 9, 10] },
    { id: 'bigdata', name: 'Big Data', icon: Server, tasks: [11, 12, 13, 14, 15, 16, 17] },
    { id: 'streaming', name: 'Streaming', icon: Zap, tasks: [18, 19, 20, 21] },
    { id: 'orchestration', name: 'Orchestration', icon: Workflow, tasks: [22, 23] },
    { id: 'cloud', name: 'Cloud', icon: Cloud, tasks: [24, 25, 26, 27] },
    { id: 'advanced', name: 'Advanced', icon: Layers, tasks: [28, 29, 30] },
  ];

  const stats = {
    totalTasks: 30,
    completedTasks: 30,
    categories: 7,
    pythonFiles: 30,
    linesOfCode: '15,000+'
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center gap-3">
              <Button 
                variant="ghost" 
                size="icon"
                onClick={() => setSidebarOpen(!sidebarOpen)}
                className="lg:hidden"
              >
                {sidebarOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
              </Button>
              <div className="flex items-center gap-2">
                <div className="bg-gradient-to-r from-blue-600 to-indigo-600 p-2 rounded-lg">
                  <Sparkles className="h-6 w-6 text-white" />
                </div>
                <div>
                  <h1 className="text-xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">
                    AI Resume Analyzer
                  </h1>
                  <p className="text-xs text-slate-500">30 Data Engineering Tasks</p>
                </div>
              </div>
            </div>
            
            <div className="flex items-center gap-4">
              <div className="hidden md:flex items-center gap-4 text-sm text-slate-600">
                <span className="flex items-center gap-1">
                  <CheckCircle className="h-4 w-4 text-green-500" />
                  {stats.completedTasks}/{stats.totalTasks} Tasks
                </span>
                <span className="flex items-center gap-1">
                  <Code className="h-4 w-4 text-blue-500" />
                  {stats.pythonFiles} Python Files
                </span>
              </div>
              <Badge variant="secondary" className="bg-green-100 text-green-700">
                Complete
              </Badge>
            </div>
          </div>
        </div>
      </header>

      <div className="flex max-w-7xl mx-auto">
        {/* Sidebar */}
        {sidebarOpen && (
          <aside className="w-64 bg-white border-r border-slate-200 min-h-[calc(100vh-64px)] hidden lg:block">
            <ScrollArea className="h-[calc(100vh-64px)]">
              <div className="p-4">
                <nav className="space-y-1">
                  <button
                    onClick={() => setActiveTab('analyzer')}
                    className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      activeTab === 'analyzer'
                        ? 'bg-blue-50 text-blue-700'
                        : 'text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <Search className="h-4 w-4" />
                    Resume Analyzer
                  </button>
                  <button
                    onClick={() => setActiveTab('dashboard')}
                    className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      activeTab === 'dashboard'
                        ? 'bg-blue-50 text-blue-700'
                        : 'text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <BarChart3 className="h-4 w-4" />
                    Task Dashboard
                  </button>
                  <button
                    onClick={() => setActiveTab('overview')}
                    className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      activeTab === 'overview'
                        ? 'bg-blue-50 text-blue-700'
                        : 'text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <Box className="h-4 w-4" />
                    Project Overview
                  </button>
                </nav>

                <Separator className="my-4" />

                <div className="space-y-2">
                  <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider px-3">
                    Task Categories
                  </h3>
                  <nav className="space-y-1">
                    {taskCategories.map((category) => (
                      <button
                        key={category.id}
                        onClick={() => setActiveTab(category.id)}
                        className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                          activeTab === category.id
                            ? 'bg-blue-50 text-blue-700'
                            : 'text-slate-600 hover:bg-slate-50'
                        }`}
                      >
                        <category.icon className="h-4 w-4" />
                        <span className="flex-1 text-left">{category.name}</span>
                        <Badge variant="secondary" className="text-xs">
                          {category.tasks.length}
                        </Badge>
                      </button>
                    ))}
                  </nav>
                </div>
              </div>
            </ScrollArea>
          </aside>
        )}

        {/* Main Content */}
        <main className="flex-1 p-4 sm:p-6 lg:p-8">
          {activeTab === 'analyzer' && <ResumeAnalyzer />}
          {activeTab === 'dashboard' && <TaskDashboard />}
          {activeTab === 'overview' && <ProjectOverview />}
          {taskCategories.map((cat) => (
            activeTab === cat.id && <CategoryView key={cat.id} category={cat} />
          ))}
        </main>
      </div>
    </div>
  );
}

function ProjectOverview() {
  const features = [
    {
      title: 'Resume Analysis',
      description: 'Extract skills, experience, and match resumes to job descriptions',
      icon: Search,
      color: 'blue'
    },
    {
      title: '30 Data Engineering Tasks',
      description: 'Complete implementation of Linux, SQL, Spark, Kafka, Airflow, Cloud',
      icon: Code,
      color: 'indigo'
    },
    {
      title: 'End-to-End Pipeline',
      description: 'Ingestion → Processing → Storage → Orchestration → Dashboard',
      icon: Workflow,
      color: 'green'
    },
    {
      title: 'Cloud Integration',
      description: 'AWS, GCP, Azure services comparison and implementation',
      icon: Cloud,
      color: 'orange'
    }
  ];

  const techStack = [
    { name: 'Python', category: 'Language', icon: Code },
    { name: 'Pandas/NumPy', category: 'Data Processing', icon: Database },
    { name: 'Apache Spark', category: 'Big Data', icon: Zap },
    { name: 'Apache Kafka', category: 'Streaming', icon: Server },
    { name: 'Apache Airflow', category: 'Orchestration', icon: Workflow },
    { name: 'SQL/SQLite', category: 'Database', icon: Database },
    { name: 'AWS/GCP/Azure', category: 'Cloud', icon: Cloud },
    { name: 'Delta Lake', category: 'Storage', icon: Layers },
  ];

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-slate-900">Project Overview</h2>
        <p className="text-slate-600 mt-1">
          AI-based Resume Analyzer with 30 comprehensive Data Engineering tasks
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {features.map((feature, idx) => (
          <Card key={idx} className="hover:shadow-md transition-shadow">
            <CardContent className="p-6">
              <div className="flex items-start gap-4">
                <div className={`p-3 rounded-lg bg-${feature.color}-100`}>
                  <feature.icon className={`h-6 w-6 text-${feature.color}-600`} />
                </div>
                <div>
                  <h3 className="font-semibold text-slate-900">{feature.title}</h3>
                  <p className="text-sm text-slate-600 mt-1">{feature.description}</p>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Technology Stack</CardTitle>
          <CardDescription>Tools and technologies used across all 30 tasks</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {techStack.map((tech, idx) => (
              <div key={idx} className="flex items-center gap-3 p-3 rounded-lg bg-slate-50">
                <tech.icon className="h-5 w-5 text-slate-500" />
                <div>
                  <p className="font-medium text-sm text-slate-900">{tech.name}</p>
                  <p className="text-xs text-slate-500">{tech.category}</p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Implementation Statistics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div className="text-center">
              <p className="text-3xl font-bold text-blue-600">30</p>
              <p className="text-sm text-slate-600">Python Files</p>
            </div>
            <div className="text-center">
              <p className="text-3xl font-bold text-indigo-600">7</p>
              <p className="text-sm text-slate-600">Categories</p>
            </div>
            <div className="text-center">
              <p className="text-3xl font-bold text-green-600">15K+</p>
              <p className="text-sm text-slate-600">Lines of Code</p>
            </div>
            <div className="text-center">
              <p className="text-3xl font-bold text-orange-600">100%</p>
              <p className="text-sm text-slate-600">Complete</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function CategoryView({ category }: { category: any }) {
  const taskDetails: Record<number, { title: string; description: string; concepts: string[] }> = {
    1: { title: 'Linux + File System', description: 'Set up Linux environment, directory structure, permissions, shell scripts', concepts: ['File Permissions', 'Shell Scripts', 'Directory Structure', 'Logging'] },
    2: { title: 'Networking', description: 'API simulation, protocol analysis, packet capture, secure data flow', concepts: ['HTTP/HTTPS', 'FTP/FTPS', 'SSL/TLS', 'Packet Capture'] },
    3: { title: 'Python Basics', description: 'Read CSV files, clean missing values, merge datasets', concepts: ['CSV Processing', 'Data Cleaning', 'Data Merging', 'Error Handling'] },
    4: { title: 'Advanced Python', description: 'Reusable modules for normalization, aggregation, validation', concepts: ['Decorators', 'Abstract Classes', 'Data Validation', 'Pipelines'] },
    5: { title: 'Pandas + NumPy', description: 'Large-scale data analysis with 1M+ rows, memory optimization', concepts: ['Memory Optimization', 'Performance Tuning', 'Chunk Processing', 'Benchmarking'] },
    6: { title: 'SQL Basics', description: 'Student database design with SELECT, WHERE, GROUP BY, ORDER BY', concepts: ['Database Design', 'Basic Queries', 'Filtering', 'Sorting'] },
    7: { title: 'Advanced SQL', description: 'Joins, subqueries, window functions for business reporting', concepts: ['JOINs', 'Subqueries', 'Window Functions', 'CTEs'] },
    8: { title: 'Database Concepts', description: 'Compare OLTP vs OLAP systems with schema design', concepts: ['OLTP', 'OLAP', 'Normalization', 'Schema Design'] },
    9: { title: 'Data Warehousing', description: 'Star schema design for e-commerce analytics', concepts: ['Star Schema', 'Fact Tables', 'Dimension Tables', 'Aggregations'] },
    10: { title: 'ETL vs ELT', description: 'Build and compare ETL and ELT pipeline performance', concepts: ['ETL Pipeline', 'ELT Pipeline', 'Performance Comparison'] },
    11: { title: 'Data Ingestion', description: 'Batch ingestion pipeline from CSV to database', concepts: ['Batch Processing', 'Data Validation', 'Audit Logging', 'Error Handling'] },
    12: { title: 'Hadoop', description: 'Set up HDFS locally and upload structured/unstructured data', concepts: ['HDFS', 'Namenode', 'Datanode', 'Block Storage'] },
    13: { title: 'HDFS Architecture', description: 'Explain Namenode, Datanode and simulate data storage', concepts: ['HDFS Architecture', 'Replication', 'Rack Awareness'] },
    14: { title: 'Spark Basics', description: 'Create Spark job to process large dataset', concepts: ['RDD', 'Transformations', 'Actions', 'Lazy Evaluation'] },
    15: { title: 'Spark DataFrames', description: 'Perform transformations and actions on dataset', concepts: ['DataFrame API', 'select/filter', 'groupBy/agg', 'join'] },
    16: { title: 'Spark SQL', description: 'Query large dataset using Spark SQL', concepts: ['Spark SQL', 'SQL Queries', 'Temporary Views', 'Catalyst Optimizer'] },
    17: { title: 'PySpark Advanced', description: 'Optimize Spark jobs using partitioning and caching', concepts: ['Partitioning', 'Caching', 'Broadcast Join', 'Shuffle Optimization'] },
    18: { title: 'Streaming Concepts', description: 'Simulate real-time data processing using streaming APIs', concepts: ['Stream Processing', 'Windows', 'Watermarking', 'State Management'] },
    19: { title: 'Kafka Basics', description: 'Set up Kafka producer-consumer pipeline', concepts: ['Topics', 'Partitions', 'Producers', 'Consumers'] },
    20: { title: 'Kafka Advanced', description: 'Implement partitioning and offset management', concepts: ['Partitioning Strategies', 'Offset Management', 'Consumer Groups'] },
    21: { title: 'Structured Streaming', description: 'Build Spark streaming job for real-time logs', concepts: ['Structured Streaming', 'Output Modes', 'Windowed Aggregations'] },
    22: { title: 'Airflow Basics', description: 'Create DAG for ETL pipeline', concepts: ['DAGs', 'Tasks', 'Operators', 'Dependencies'] },
    23: { title: 'Airflow Advanced', description: 'Add scheduling, monitoring, retry mechanisms', concepts: ['Scheduling', 'Retries', 'SLAs', 'Monitoring'] },
    24: { title: 'Cloud Basics', description: 'Compare AWS, GCP, Azure services', concepts: ['AWS', 'GCP', 'Azure', 'Service Comparison'] },
    25: { title: 'Cloud Storage', description: 'Upload and retrieve files from S3/GCS', concepts: ['S3', 'GCS', 'Lifecycle Policies', 'Presigned URLs'] },
    26: { title: 'Cloud Compute', description: 'Launch EC2 instance and deploy data pipeline', concepts: ['EC2', 'Security Groups', 'Auto Scaling', 'User Data'] },
    27: { title: 'Data Warehouse Cloud', description: 'Use BigQuery/Redshift to analyze large data', concepts: ['BigQuery', 'Redshift', 'Serverless', 'Columnar Storage'] },
    28: { title: 'Lakehouse', description: 'Implement Delta Lake/Iceberg and manage versions', concepts: ['Delta Lake', 'ACID Transactions', 'Time Travel', 'Schema Evolution'] },
    29: { title: 'Data Quality', description: 'Build validation checks and anomaly detection', concepts: ['Data Validation', 'Anomaly Detection', 'Quality Rules', 'Monitoring'] },
    30: { title: 'Final Project', description: 'Design end-to-end pipeline: ingestion → processing → storage → orchestration → dashboard', concepts: ['End-to-End Pipeline', 'Architecture', 'Integration'] },
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <category.icon className="h-8 w-8 text-blue-600" />
        <div>
          <h2 className="text-2xl font-bold text-slate-900">{category.name}</h2>
          <p className="text-slate-600">Tasks {category.tasks[0]} - {category.tasks[category.tasks.length - 1]}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4">
        {category.tasks.map((taskNum: number) => {
          const task = taskDetails[taskNum];
          if (!task) return null;
          
          return (
            <Card key={taskNum} className="hover:shadow-md transition-shadow">
              <CardContent className="p-6">
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <Badge variant="secondary">Task {taskNum}</Badge>
                      <h3 className="font-semibold text-lg text-slate-900">{task.title}</h3>
                    </div>
                    <p className="text-slate-600 mb-3">{task.description}</p>
                    <div className="flex flex-wrap gap-2">
                      {task.concepts.map((concept, idx) => (
                        <Badge key={idx} variant="outline" className="text-xs">
                          {concept}
                        </Badge>
                      ))}
                    </div>
                  </div>
                  <CheckCircle className="h-6 w-6 text-green-500 flex-shrink-0 ml-4" />
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

export default App;

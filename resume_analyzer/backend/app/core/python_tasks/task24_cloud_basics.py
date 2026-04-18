#!/usr/bin/env python3
"""
Task 24: Cloud Basics
Compare AWS, GCP, Azure services.
"""

import json
import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CloudComparison:
    """Compare major cloud providers."""
    
    def __init__(self):
        self.providers = {
            'aws': 'Amazon Web Services',
            'gcp': 'Google Cloud Platform',
            'azure': 'Microsoft Azure'
        }
    
    def get_compute_comparison(self) -> Dict:
        """Compare compute services."""
        return {
            'virtual_machines': {
                'aws': {
                    'service': 'Amazon EC2',
                    'features': [
                        '200+ instance types',
                        'Auto Scaling Groups',
                        'Spot Instances (up to 90% savings)',
                        'Dedicated Hosts'
                    ],
                    'pricing': 'Per second billing, Reserved Instances for savings'
                },
                'gcp': {
                    'service': 'Compute Engine',
                    'features': [
                        'Custom machine types',
                        'Preemptible VMs (up to 80% savings)',
                        'Live migration',
                        'Sustained use discounts'
                    ],
                    'pricing': 'Per second billing, Sustained use discounts'
                },
                'azure': {
                    'service': 'Azure Virtual Machines',
                    'features': [
                        'Hybrid benefit for Windows Server',
                        'Scale Sets',
                        'Spot VMs (up to 90% savings)',
                        'Dedicated Hosts'
                    ],
                    'pricing': 'Per minute billing, Reserved VM Instances'
                }
            },
            'containers': {
                'aws': {
                    'service': 'Amazon ECS/EKS',
                    'features': ['Fargate (serverless)', 'EKS (Kubernetes)', 'ECR (registry)']
                },
                'gcp': {
                    'service': 'Google Kubernetes Engine',
                    'features': ['Autopilot mode', 'Auto-scaling', 'Auto-repair']
                },
                'azure': {
                    'service': 'Azure Kubernetes Service',
                    'features': ['Virtual nodes', 'Dev Spaces', 'Azure Container Registry']
                }
            },
            'serverless': {
                'aws': {
                    'service': 'AWS Lambda',
                    'features': ['15 minute timeout', '1M free requests/month', 'Step Functions']
                },
                'gcp': {
                    'service': 'Cloud Functions',
                    'features': ['9 minute timeout', '2M free invocations/month', 'Cloud Run']
                },
                'azure': {
                    'service': 'Azure Functions',
                    'features': ['10 minute timeout', '1M free executions/month', 'Durable Functions']
                }
            }
        }
    
    def get_storage_comparison(self) -> Dict:
        """Compare storage services."""
        return {
            'object_storage': {
                'aws': {
                    'service': 'Amazon S3',
                    'tiers': ['Standard', 'IA, Glacier, Deep Archive'],
                    'features': ['11 9s durability', 'Cross-region replication', 'Event notifications']
                },
                'gcp': {
                    'service': 'Cloud Storage',
                    'classes': ['Standard', 'Nearline', 'Coldline', 'Archive'],
                    'features': ['11 9s durability', 'Multi-regional', 'Object versioning']
                },
                'azure': {
                    'service': 'Azure Blob Storage',
                    'tiers': ['Hot', 'Cool', 'Archive'],
                    'features': ['12 9s durability', 'Geo-redundancy', 'Lifecycle management']
                }
            },
            'block_storage': {
                'aws': 'Amazon EBS (SSD, HDD options)',
                'gcp': 'Persistent Disk (SSD, Standard)',
                'azure': 'Azure Disk Storage (Premium SSD, Standard SSD, HDD)'
            },
            'file_storage': {
                'aws': 'Amazon EFS (fully managed NFS)',
                'gcp': 'Filestore (managed NFS)',
                'azure': 'Azure Files (managed SMB/NFS)'
            }
        }
    
    def get_database_comparison(self) -> Dict:
        """Compare database services."""
        return {
            'relational': {
                'aws': {
                    'service': 'Amazon RDS/Aurora',
                    'engines': ['MySQL', 'PostgreSQL', 'MariaDB', 'Oracle', 'SQL Server'],
                    'features': ['Aurora (5x performance)', 'Multi-AZ', 'Read replicas']
                },
                'gcp': {
                    'service': 'Cloud SQL/Spanner',
                    'engines': ['MySQL', 'PostgreSQL', 'SQL Server'],
                    'features': ['Cloud Spanner (globally distributed)', 'Auto-scaling']
                },
                'azure': {
                    'service': 'Azure SQL Database',
                    'engines': ['SQL Server', 'MySQL', 'PostgreSQL', 'MariaDB'],
                    'features': ['Hyperscale', 'Elastic pools', 'Managed Instance']
                }
            },
            'nosql': {
                'aws': {
                    'document': 'Amazon DocumentDB (MongoDB-compatible)',
                    'key_value': 'Amazon DynamoDB',
                    'wide_column': 'Amazon Keyspaces (Cassandra)',
                    'graph': 'Amazon Neptune'
                },
                'gcp': {
                    'document': 'Firestore',
                    'key_value': 'Cloud Bigtable',
                    'wide_column': 'Cloud Bigtable',
                    'graph': 'JanusGraph on GKE'
                },
                'azure': {
                    'document': 'Azure Cosmos DB (MongoDB API)',
                    'key_value': 'Azure Cosmos DB (Table API)',
                    'wide_column': 'Azure Cosmos DB (Cassandra API)',
                    'graph': 'Azure Cosmos DB (Gremlin API)'
                }
            },
            'data_warehouse': {
                'aws': 'Amazon Redshift (columnar, petabyte-scale)',
                'gcp': 'BigQuery (serverless, ML integration)',
                'azure': 'Azure Synapse Analytics (unified analytics)'
            }
        }
    
    def get_big_data_comparison(self) -> Dict:
        """Compare big data services."""
        return {
            'hadoop_spark': {
                'aws': 'Amazon EMR (managed Hadoop/Spark)',
                'gcp': 'Dataproc (managed Spark/Hadoop)',
                'azure': 'Azure HDInsight (managed Hadoop/Spark)'
            },
            'stream_processing': {
                'aws': 'Amazon Kinesis (Data Streams, Analytics)',
                'gcp': 'Dataflow (Apache Beam)',
                'azure': 'Azure Stream Analytics'
            },
            'etl_orchestration': {
                'aws': 'AWS Glue (serverless ETL)',
                'gcp': 'Cloud Data Fusion / Cloud Composer (Airflow)',
                'azure': 'Azure Data Factory'
            },
            'analytics_query': {
                'aws': 'Amazon Athena (serverless SQL)',
                'gcp': 'BigQuery (serverless data warehouse)',
                'azure': 'Azure Synapse Serverless SQL'
            }
        }
    
    def get_ml_ai_comparison(self) -> Dict:
        """Compare ML/AI services."""
        return {
            'ml_platform': {
                'aws': 'Amazon SageMaker (build, train, deploy)',
                'gcp': 'Vertex AI (unified ML platform)',
                'azure': 'Azure Machine Learning'
            },
            'auto_ml': {
                'aws': 'SageMaker Autopilot',
                'gcp': 'AutoML (Tables, Vision, NLP)',
                'azure': 'Azure Automated ML'
            },
            'prebuilt_apis': {
                'aws': ['Rekognition (vision)', 'Polly (text-to-speech)', 'Lex (chatbots)'],
                'gcp': ['Vision AI', 'Speech-to-Text', 'Dialogflow (chatbots)'],
                'azure': ['Computer Vision', 'Speech Services', 'Bot Service']
            }
        }
    
    def get_networking_comparison(self) -> Dict:
        """Compare networking services."""
        return {
            'vpc': {
                'aws': 'Amazon VPC (Virtual Private Cloud)',
                'gcp': 'Virtual Private Cloud (VPC)',
                'azure': 'Azure Virtual Network (VNet)'
            },
            'cdn': {
                'aws': 'Amazon CloudFront',
                'gcp': 'Cloud CDN',
                'azure': 'Azure CDN'
            },
            'load_balancer': {
                'aws': 'Elastic Load Balancer (ALB, NLB, CLB)',
                'gcp': 'Cloud Load Balancing',
                'azure': 'Azure Load Balancer / Application Gateway'
            },
            'dns': {
                'aws': 'Amazon Route 53',
                'gcp': 'Cloud DNS',
                'azure': 'Azure DNS'
            },
            'vpn': {
                'aws': 'AWS VPN / AWS Direct Connect',
                'gcp': 'Cloud VPN / Cloud Interconnect',
                'azure': 'Azure VPN Gateway / ExpressRoute'
            }
        }
    
    def get_pricing_comparison(self) -> Dict:
        """Compare pricing models."""
        return {
            'pricing_models': {
                'aws': {
                    'on_demand': 'Pay per use, no commitment',
                    'reserved': '1-3 year commitment, up to 72% savings',
                    'spot': 'Up to 90% discount, interruptible',
                    'savings_plans': 'Flexible commitment based on usage'
                },
                'gcp': {
                    'on_demand': 'Pay per use, per second billing',
                    'committed': '1-3 year commitment, up to 57% savings',
                    'preemptible': 'Up to 80% discount, interruptible',
                    'sustained_use': 'Automatic discounts for sustained usage'
                },
                'azure': {
                    'pay_as_you_go': 'Pay per use, no commitment',
                    'reserved': '1-3 year commitment, up to 72% savings',
                    'spot': 'Up to 90% discount, interruptible',
                    'hybrid_benefit': 'Use existing licenses for savings'
                }
            },
            'free_tier': {
                'aws': '12 months free tier + always free services',
                'gcp': '$300 credit for 90 days + always free tier',
                'azure': '12 months free services + $200 credit + always free'
            }
        }
    
    def get_strengths_weaknesses(self) -> Dict:
        """Get strengths and weaknesses of each provider."""
        return {
            'aws': {
                'strengths': [
                    'Largest service catalog (200+ services)',
                    'Most mature platform',
                    'Largest market share',
                    'Extensive global infrastructure',
                    'Strong enterprise support'
                ],
                'weaknesses': [
                    'Complex pricing',
                    'Steep learning curve',
                    'Support costs extra'
                ],
                'best_for': ['Enterprise', 'Startups', 'Complex workloads']
            },
            'gcp': {
                'strengths': [
                    'Best-in-class AI/ML and data analytics',
                    'Kubernetes leadership (created K8s)',
                    'Competitive pricing',
                    'Strong open-source support',
                    'Superior network performance'
                ],
                'weaknesses': [
                    'Smaller market share',
                    'Fewer services than AWS',
                    'Smaller partner ecosystem'
                ],
                'best_for': ['Data analytics', 'AI/ML', 'Kubernetes workloads']
            },
            'azure': {
                'strengths': [
                    'Best Microsoft integration',
                    'Strong hybrid cloud capabilities',
                    'Enterprise-friendly',
                    'Good government cloud offerings',
                    'Seamless Windows integration'
                ],
                'weaknesses': [
                    'Less mature than AWS',
                    'Management tools can be complex',
                    'Some services lag behind AWS/GCP'
                ],
                'best_for': ['Microsoft shops', 'Hybrid cloud', 'Enterprise']
            }
        }
    
    def run_comparison(self) -> Dict:
        """Run complete cloud comparison."""
        return {
            'compute': self.get_compute_comparison(),
            'storage': self.get_storage_comparison(),
            'database': self.get_database_comparison(),
            'big_data': self.get_big_data_comparison(),
            'ml_ai': self.get_ml_ai_comparison(),
            'networking': self.get_networking_comparison(),
            'pricing': self.get_pricing_comparison(),
            'strengths_weaknesses': self.get_strengths_weaknesses()
        }


def main():
    """Main execution for Task 24."""
    comparison = CloudComparison()
    results = comparison.run_comparison()
    
    print("\n=== Cloud Provider Comparison ===")
    
    print("\n1. Compute Services:")
    for service_type, providers in results['compute'].items():
        print(f"\n  {service_type.replace('_', ' ').title()}:")
        for provider, info in providers.items():
            print(f"    {provider.upper()}: {info.get('service', info)}")
    
    print("\n2. Data Warehouse:")
    for provider, service in results['big_data']['analytics_query'].items():
        print(f"  {provider.upper()}: {service}")
    
    print("\n3. Strengths:")
    for provider, info in results['strengths_weaknesses'].items():
        print(f"\n  {provider.upper()}:")
        for strength in info['strengths'][:3]:
            print(f"    + {strength}")
    
    return results


if __name__ == "__main__":
    main()

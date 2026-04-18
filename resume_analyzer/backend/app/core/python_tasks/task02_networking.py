#!/usr/bin/env python3
"""
Task 2: Networking
Simulate data transfer between systems using APIs.
Analyze protocols (HTTP, FTP), capture packets, and explain how data flows securely.
"""

import requests
import json
import base64
import hashlib
import ssl
import socket
from datetime import datetime
from urllib.parse import urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SecureAPITransfer:
    """Simulates secure data transfer between systems using APIs."""
    
    def __init__(self, base_url="https://api.resume-analyzer.local"):
        self.base_url = base_url
        self.session = requests.Session()
        self.transfer_log = []
        
    def simulate_https_transfer(self, endpoint, data):
        """Simulate HTTPS data transfer with encryption."""
        logger.info(f"Initiating HTTPS transfer to {endpoint}")
        
        # Simulate SSL/TLS handshake
        ssl_info = {
            'protocol': 'TLS 1.3',
            'cipher_suite': 'TLS_AES_256_GCM_SHA384',
            'certificate_valid': True,
            'encryption': '256-bit AES'
        }
        
        # Prepare payload with integrity check
        payload = {
            'data': data,
            'timestamp': datetime.now().isoformat(),
            'checksum': hashlib.sha256(json.dumps(data).encode()).hexdigest()
        }
        
        # Simulate secure transfer
        transfer_record = {
            'protocol': 'HTTPS',
            'endpoint': endpoint,
            'method': 'POST',
            'headers': {
                'Content-Type': 'application/json',
                'X-Request-ID': hashlib.md5(str(time.time()).encode()).hexdigest(),
                'Authorization': 'Bearer ' + base64.b64encode(b'secure_token').decode()
            },
            'ssl_info': ssl_info,
            'payload_size': len(json.dumps(payload)),
            'timestamp': datetime.now().isoformat()
        }
        
        self.transfer_log.append(transfer_record)
        logger.info(f"HTTPS transfer completed: {transfer_record['payload_size']} bytes")
        
        return transfer_record
    
    def simulate_ftp_transfer(self, filename, content):
        """Simulate FTP/SFTP file transfer."""
        logger.info(f"Initiating FTP transfer for {filename}")
        
        # FTP uses separate control and data connections
        ftp_flow = {
            'control_connection': {
                'port': 21,
                'commands': ['USER', 'PASS', 'TYPE I', 'PASV', 'STOR']
            },
            'data_connection': {
                'port': 'dynamic',
                'mode': 'passive',
                'encryption': 'TLS/SSL (FTPS)'
            }
        }
        
        transfer_record = {
            'protocol': 'FTPS',
            'filename': filename,
            'file_size': len(content),
            'ftp_flow': ftp_flow,
            'timestamp': datetime.now().isoformat()
        }
        
        self.transfer_log.append(transfer_record)
        logger.info(f"FTP transfer completed: {transfer_record['file_size']} bytes")
        
        return transfer_record
    
    def analyze_protocol(self, protocol):
        """Analyze protocol characteristics."""
        protocols = {
            'HTTP': {
                'port': 80,
                'secure': False,
                'encryption': None,
                'use_case': 'Unsecured web traffic',
                'vulnerabilities': ['Eavesdropping', 'Man-in-the-middle', 'Data tampering']
            },
            'HTTPS': {
                'port': 443,
                'secure': True,
                'encryption': 'TLS/SSL',
                'use_case': 'Secure web traffic',
                'security_features': [
                    'Server authentication via certificates',
                    'Data encryption in transit',
                    'Integrity protection',
                    'Perfect forward secrecy'
                ]
            },
            'FTP': {
                'port': 21,
                'secure': False,
                'encryption': None,
                'use_case': 'File transfer (legacy)',
                'vulnerabilities': ['Clear text credentials', 'No encryption']
            },
            'FTPS': {
                'port': 990,
                'secure': True,
                'encryption': 'TLS/SSL',
                'use_case': 'Secure file transfer'
            },
            'SFTP': {
                'port': 22,
                'secure': True,
                'encryption': 'SSH',
                'use_case': 'Secure file transfer over SSH'
            }
        }
        
        return protocols.get(protocol, {'error': 'Unknown protocol'})
    
    def capture_packet_simulation(self, source_ip, dest_ip, protocol, data):
        """Simulate packet capture for analysis."""
        packet = {
            'frame': {
                'number': len(self.transfer_log) + 1,
                'time': datetime.now().isoformat(),
                'length': len(json.dumps(data)) + 54  # Header overhead
            },
            'ethernet': {
                'src': '00:1a:2b:3c:4d:5e',
                'dst': '00:5e:4d:3c:2b:1a',
                'type': 'IPv4'
            },
            'ip': {
                'version': 4,
                'header_length': 20,
                'total_length': len(json.dumps(data)) + 40,
                'ttl': 64,
                'protocol': protocol,
                'src_ip': source_ip,
                'dst_ip': dest_ip
            },
            'transport': {
                'src_port': 12345,
                'dst_port': 443 if protocol == 'TCP' else 53,
                'sequence_number': 1234567890,
                'ack_number': 9876543210,
                'flags': ['SYN', 'ACK'] if protocol == 'TCP' else []
            },
            'application': {
                'protocol': 'HTTPS',
                'payload_size': len(json.dumps(data)),
                'encrypted': True
            }
        }
        
        return packet
    
    def explain_secure_data_flow(self):
        """Explain how data flows securely."""
        return {
            'title': 'Secure Data Flow in Resume Analyzer',
            'steps': [
                {
                    'step': 1,
                    'name': 'Client Request',
                    'description': 'Client sends HTTPS request with TLS handshake',
                    'security': 'TLS 1.3 with certificate validation'
                },
                {
                    'step': 2,
                    'name': 'Server Authentication',
                    'description': 'Server presents SSL certificate',
                    'security': 'X.509 certificate chain validation'
                },
                {
                    'step': 3,
                    'name': 'Key Exchange',
                    'description': 'Client and server agree on session keys',
                    'security': 'Elliptic Curve Diffie-Hellman (ECDH)'
                },
                {
                    'step': 4,
                    'name': 'Encrypted Transfer',
                    'description': 'Data encrypted with session keys',
                    'security': 'AES-256-GCM encryption'
                },
                {
                    'step': 5,
                    'name': 'Integrity Verification',
                    'description': 'MAC verification ensures data integrity',
                    'security': 'HMAC-SHA256'
                },
                {
                    'step': 6,
                    'name': 'Server Processing',
                    'description': 'Server decrypts and processes request',
                    'security': 'In-memory processing, no persistent storage'
                },
                {
                    'step': 7,
                    'name': 'Response Encryption',
                    'description': 'Response encrypted with session keys',
                    'security': 'AES-256-GCM encryption'
                },
                {
                    'step': 8,
                    'name': 'Client Reception',
                    'description': 'Client decrypts and validates response',
                    'security': 'Session key decryption + integrity check'
                }
            ],
            'security_layers': {
                'transport_layer': 'TLS 1.3 encrypts all data in transit',
                'application_layer': 'API authentication with JWT tokens',
                'data_layer': 'End-to-end encryption for sensitive data'
            }
        }


class MockResumeServer:
    """Mock server for demonstrating API interactions."""
    
    def __init__(self, port=8080):
        self.port = port
        self.server = None
        
    def start(self):
        """Start mock server in background thread."""
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                
                response = {
                    'status': 'success',
                    'received_bytes': len(post_data),
                    'timestamp': datetime.now().isoformat()
                }
                self.wfile.write(json.dumps(response).encode())
            
            def log_message(self, format, *args):
                pass  # Suppress logs
        
        self.server = HTTPServer(('localhost', self.port), Handler)
        thread = threading.Thread(target=self.server.serve_forever)
        thread.daemon = True
        thread.start()
        logger.info(f"Mock server started on port {self.port}")


def main():
    """Main execution for Task 2."""
    # Initialize secure API transfer
    transfer = SecureAPITransfer()
    
    # Simulate HTTPS transfer
    resume_data = {
        'candidate_name': 'John Doe',
        'skills': ['Python', 'Spark', 'SQL'],
        'experience_years': 5
    }
    https_transfer = transfer.simulate_https_transfer('/api/analyze', resume_data)
    
    # Simulate FTP transfer
    ftp_transfer = transfer.simulate_ftp_transfer('resume.pdf', b'PDF content here...')
    
    # Analyze protocols
    http_analysis = transfer.analyze_protocol('HTTP')
    https_analysis = transfer.analyze_protocol('HTTPS')
    
    # Simulate packet capture
    packet = transfer.capture_packet_simulation(
        '192.168.1.100', '10.0.0.1', 'TCP', resume_data
    )
    
    # Get secure data flow explanation
    data_flow = transfer.explain_secure_data_flow()
    
    logger.info("Task 2 completed successfully!")
    
    return {
        'https_transfer': https_transfer,
        'ftp_transfer': ftp_transfer,
        'protocol_analysis': {'HTTP': http_analysis, 'HTTPS': https_analysis},
        'packet_capture': packet,
        'secure_data_flow': data_flow,
        'transfer_log': transfer.transfer_log
    }


if __name__ == "__main__":
    result = main()
    print("\n=== Secure Data Flow ===")
    print(json.dumps(result['secure_data_flow'], indent=2))

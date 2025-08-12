import sys
import time
import threading
import socket
import json
import re
import csv
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import queue
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MasterNode:
    def __init__(self, base_url, scraping_time_minutes, num_nodes, port=8000):
        self.base_url = base_url
        self.scraping_time_seconds = scraping_time_minutes * 60
        self.num_nodes = num_nodes
        self.port = port
        self.start_time = None

        # Shared data structures with thread-safe access
        self.url_queue = queue.Queue()
        self.scraped_urls = set()
        self.email_data = []
        self.statistics = {
            'url': base_url,
            'pages_scraped': 0,
            'emails_found': 0,
            'start_time': None,
            'end_time': None
        }

        # Thread synchronization
        self.data_lock = threading.Lock()
        self.url_lock = threading.Lock()

        # Worker nodes management
        self.worker_nodes = []
        self.node_connections = {}

        # Email regex pattern
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

        # Server socket for coordination
        self.server_socket = None

    def start_coordination_server(self):
        """Start the coordination server for worker nodes"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(self.num_nodes)

            logger.info(f"Coordination server started on port {self.port}")

            # Accept worker connections
            threading.Thread(target=self.accept_worker_connections, daemon=True).start()

        except Exception as e:
            logger.error(f"Failed to start coordination server: {e}")
            raise

    def accept_worker_connections(self):
        """Accept connections from worker nodes"""
        while len(self.worker_nodes) < self.num_nodes:
            try:
                client_socket, address = self.server_socket.accept()
                worker_id = f"worker_{len(self.worker_nodes) + 1}"

                self.worker_nodes.append(worker_id)
                self.node_connections[worker_id] = client_socket

                logger.info(f"Worker {worker_id} connected from {address}")

                # Start message handler for this worker
                threading.Thread(
                    target=self.handle_worker_messages,
                    args=(worker_id, client_socket),
                    daemon=True
                ).start()

            except Exception as e:
                logger.error(f"Error accepting worker connection: {e}")

    def handle_worker_messages(self, worker_id, client_socket):
        """Handle messages from a specific worker node"""
        try:
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break

                message = json.loads(data.decode())
                self.process_worker_message(worker_id, message)

        except Exception as e:
            logger.error(f"Error handling messages from {worker_id}: {e}")
        finally:
            if worker_id in self.node_connections:
                del self.node_connections[worker_id]
            client_socket.close()

    def process_worker_message(self, worker_id, message):
        """Process messages received from worker nodes"""
        msg_type = message.get('type')

        if msg_type == 'url_request':
            # Worker requesting URLs to scrape
            urls = self.get_urls_for_worker(message.get('batch_size', 5))
            response = {
                'type': 'url_assignment',
                'urls': urls,
                'time_remaining': self.get_time_remaining()
            }
            self.send_message_to_worker(worker_id, response)

        elif msg_type == 'scraping_results':
            # Worker sending back scraping results
            self.process_scraping_results(worker_id, message.get('results', {}))

        elif msg_type == 'new_urls':
            # Worker found new URLs to add to queue
            self.add_urls_to_queue(message.get('urls', []))

    def get_urls_for_worker(self, batch_size):
        """Get a batch of URLs for a worker to process"""
        urls = []
        with self.url_lock:
            for _ in range(batch_size):
                if not self.url_queue.empty():
                    url = self.url_queue.get()
                    if url not in self.scraped_urls:
                        urls.append(url)
                        self.scraped_urls.add(url)
                else:
                    break
        return urls

    def add_urls_to_queue(self, urls):
        """Add new URLs to the processing queue"""
        with self.url_lock:
            for url in urls:
                if url not in self.scraped_urls:
                    self.url_queue.put(url)

    def process_scraping_results(self, worker_id, results):
        """Process results received from worker nodes"""
        with self.data_lock:
            # Update statistics
            self.statistics['pages_scraped'] += results.get('pages_processed', 0)

            # Add email data
            emails = results.get('emails', [])
            self.email_data.extend(emails)
            self.statistics['emails_found'] = len(self.email_data)

            logger.info(f"Received {len(emails)} emails from {worker_id}")

    def send_message_to_worker(self, worker_id, message):
        """Send message to a specific worker node"""
        try:
            if worker_id in self.node_connections:
                socket_conn = self.node_connections[worker_id]
                message_str = json.dumps(message)
                socket_conn.send(message_str.encode())
        except Exception as e:
            logger.error(f"Error sending message to {worker_id}: {e}")

    def broadcast_message(self, message):
        """Broadcast message to all worker nodes"""
        for worker_id in self.worker_nodes:
            self.send_message_to_worker(worker_id, message)

    def get_time_remaining(self):
        """Get remaining scraping time"""
        if self.start_time is None:
            return self.scraping_time_seconds

        elapsed = time.time() - self.start_time
        return max(0, self.scraping_time_seconds - elapsed)

    def initialize_url_queue(self):
        """Initialize the URL queue with the base URL"""
        self.url_queue.put(self.base_url)
        logger.info(f"Initialized URL queue with base URL: {self.base_url}")

    def start_distributed_scraping(self):
        """Start the distributed scraping process"""
        logger.info("Starting distributed scraping coordination...")

        # Initialize
        self.start_time = time.time()
        self.statistics['start_time'] = datetime.now().isoformat()
        self.initialize_url_queue()

        # Start coordination server
        self.start_coordination_server()

        # Wait for all worker nodes to connect
        logger.info(f"Waiting for {self.num_nodes} worker nodes to connect...")
        while len(self.worker_nodes) < self.num_nodes:
            time.sleep(1)

        logger.info("All worker nodes connected. Starting scraping...")

        # Send start signal to all workers
        start_message = {
            'type': 'start_scraping',
            'base_url': self.base_url,
            'scraping_time': self.scraping_time_seconds
        }
        self.broadcast_message(start_message)

        # Monitor scraping progress
        self.monitor_scraping_progress()

    def monitor_scraping_progress(self):
        """Monitor the scraping progress and coordination"""
        while self.get_time_remaining() > 0:
            time.sleep(5)  # Check every 5 seconds

            # Log progress
            with self.data_lock:
                logger.info(f"Progress - Pages: {self.statistics['pages_scraped']}, "
                            f"Emails: {self.statistics['emails_found']}, "
                            f"Time remaining: {self.get_time_remaining():.0f}s")

        # Time's up - signal workers to stop
        logger.info("Scraping time completed. Signaling workers to stop...")
        stop_message = {'type': 'stop_scraping'}
        self.broadcast_message(stop_message)

        # Wait a bit for final results
        time.sleep(5)

        # Finalize results
        self.finalize_results()

    def finalize_results(self):
        """Finalize and save scraping results"""
        self.statistics['end_time'] = datetime.now().isoformat()

        logger.info("Finalizing results...")

        # Save email data to CSV
        self.save_email_data()

        # Save statistics
        self.save_statistics()

        # Close connections
        self.cleanup()

        logger.info("Distributed scraping completed successfully!")

    def save_email_data(self):
        """Save email data to CSV file"""
        filename = f"emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['email', 'name', 'office', 'department', 'unit', 'url_found']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for email_entry in self.email_data:
                writer.writerow(email_entry)

        logger.info(f"Email data saved to {filename}")

    def save_statistics(self):
        """Save scraping statistics to file"""
        filename = f"statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        with open(filename, 'w', encoding='utf-8') as f:
            f.write("Distributed Email Scraping Statistics\n")
            f.write("=" * 40 + "\n")
            f.write(f"Website URL: {self.statistics['url']}\n")
            f.write(f"Number of pages scraped: {self.statistics['pages_scraped']}\n")
            f.write(f"Number of email addresses found: {self.statistics['emails_found']}\n")
            f.write(f"Number of worker nodes used: {self.num_nodes}\n")
            f.write(f"Start time: {self.statistics['start_time']}\n")
            f.write(f"End time: {self.statistics['end_time']}\n")

        logger.info(f"Statistics saved to {filename}")

    def cleanup(self):
        """Clean up resources"""
        try:
            if self.server_socket:
                self.server_socket.close()

            for worker_id, connection in self.node_connections.items():
                connection.close()

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    parser = argparse.ArgumentParser(description='Distributed Email Web Scraper - Master Node')
    parser.add_argument('url', help='URL of the website to be scraped')
    parser.add_argument('time', type=int, help='Scraping time in minutes')
    parser.add_argument('nodes', type=int, help='Number of worker nodes to use')
    parser.add_argument('--port', type=int, default=8000, help='Coordination server port')

    args = parser.parse_args()

    # Validate arguments
    if not args.url.startswith(('http://', 'https://')):
        logger.error("URL must start with http:// or https://")
        sys.exit(1)

    if args.time <= 0:
        logger.error("Scraping time must be positive")
        sys.exit(1)

    if args.nodes <= 0:
        logger.error("Number of nodes must be positive")
        sys.exit(1)

    try:
        # Create and start coordinator
        coordinator = MasterNode(
            base_url=args.url,
            scraping_time_minutes=args.time,
            num_nodes=args.nodes,
            port=args.port
        )

        coordinator.start_distributed_scraping()

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

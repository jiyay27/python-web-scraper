import sys
import time
import threading
import socket
import json
import re
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Comment
import html
import urllib.parse
import logging
from concurrent.futures import ThreadPoolExecutor
import argparse
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkerNode:
    def __init__(self, master_host='localhost', master_port=8000, worker_id=None):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = worker_id or f"worker_{int(time.time())}"
        self.master_socket = None
        self.is_running = False
        self.scraping_active = False
        self.base_url = None
        self.scraping_time = None
        self.start_time = None
        self.local_emails = []
        self.local_pages_processed = 0
        self.local_urls_found = []

        self.email_patterns = [
            re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'),
            re.compile(r'([A-Za-z0-9._%+-]+)\s*\[?\(?at\)?\]?\s*([A-Za-z0-9.-]+)\s*\[?\(?dot\)?\]?\s*([A-Za-z]{2,})', re.IGNORECASE)
        ]

        self.name_patterns = [
            re.compile(r'(?:Dr\.|Prof\.|Mr\.|Ms\.|Mrs\.)\s*([A-Za-z\s]+)', re.IGNORECASE),
            re.compile(r'<h[1-6][^>]*>([^<]*(?:Dr\.|Prof\.|Mr\.|Ms\.|Mrs\.)[^<]*)</h[1-6]>', re.IGNORECASE),
            re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b')
        ]

        self.dept_keywords = ['department', 'faculty', 'college', 'school', 'division', 'institute', 'center']
        self.office_keywords = ['office', 'room', 'building', 'hall']

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.session.verify = True
        self.ssl_fallback = False

    def connect_to_master(self):
        try:
            self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.master_socket.connect((self.master_host, self.master_port))
            logger.info(f"Worker {self.worker_id} connected to master at {self.master_host}:{self.master_port}")
            self.is_running = True
            threading.Thread(target=self.handle_master_messages, daemon=True).start()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to master: {e}")
            return False

    def handle_master_messages(self):
        try:
            while self.is_running:
                data = self.master_socket.recv(4096)
                if not data:
                    break
                message = json.loads(data.decode())
                self.process_master_message(message)
        except Exception as e:
            logger.error(f"Error handling master messages: {e}")
        finally:
            self.cleanup_connection()

    def process_master_message(self, message):
        msg_type = message.get('type')
        if msg_type == 'start_scraping':
            logger.info("Received start scraping signal from master")
            self.base_url = message.get('base_url')
            self.scraping_time = message.get('scraping_time')
            self.start_time = time.time()
            self.scraping_active = True
            threading.Thread(target=self.start_scraping_process, daemon=True).start()
        elif msg_type == 'url_assignment':
            urls = message.get('urls', [])
            time_remaining = message.get('time_remaining', 0)
            if urls and time_remaining > 0:
                logger.info(f"Received {len(urls)} URLs to process")
                threading.Thread(target=self.process_url_batch, args=(urls,), daemon=True).start()
        elif msg_type == 'stop_scraping':
            logger.info("Received stop signal from master")
            self.scraping_active = False
            self.send_final_results()

    def start_scraping_process(self):
        logger.info("Starting scraping process...")
        while self.scraping_active and self.get_time_remaining() > 0:
            self.request_urls_from_master()
            time.sleep(2)
        logger.info("Scraping process completed")

    def request_urls_from_master(self):
        request_message = {
            'type': 'url_request',
            'batch_size': 5,
            'worker_id': self.worker_id
        }
        self.send_message_to_master(request_message)

    def process_url_batch(self, urls):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(self.scrape_single_url, url) for url in urls if self.scraping_active and self.get_time_remaining() > 0]
            for future in futures:
                try:
                    future.result(timeout=30)
                except Exception as e:
                    logger.error(f"Error in scraping task: {e}")

    def scrape_single_url(self, url):
        try:
            logger.info(f"Scraping URL: {url}")
            try:
                response = self.session.get(url, timeout=15, verify=True)
                response.raise_for_status()
            except requests.exceptions.SSLError:
                logger.warning(f"SSL verification failed for {url}, attempting without verification")
                response = self.session.get(url, timeout=15, verify=False)
                response.raise_for_status()
                self.ssl_fallback = True

            soup = BeautifulSoup(response.text, 'html.parser')
            page_emails = self.extract_emails_from_content(soup, url)
            new_urls = self.extract_links_from_page(soup, url)

            self.local_pages_processed += 1
            self.local_emails.extend(page_emails)
            self.local_urls_found.extend(new_urls)

            if new_urls:
                self.send_new_urls_to_master(new_urls)

            if page_emails:
                self.send_intermediate_results()

            logger.info(f"Scraped {url}: found {len(page_emails)} emails, {len(new_urls)} new URLs")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")

    def extract_emails_from_content(self, soup, source_url):
        emails_found = set()
        email_matches = []

        def decode(text):
            return urllib.parse.unquote(html.unescape(str(text)))

        # 1. Visible text
        page_text = decode(soup.get_text())
        for pattern in self.email_patterns:
            email_matches.extend(pattern.findall(page_text))

        # 2. Tag attributes
        for tag in soup.find_all(True):
            for attr, value in tag.attrs.items():
                if isinstance(value, list):
                    value = ' '.join(value)
                value_decoded = decode(value)
                for pattern in self.email_patterns:
                    email_matches.extend(pattern.findall(value_decoded))

        # 3. HTML comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment_decoded = decode(comment)
            for pattern in self.email_patterns:
                email_matches.extend(pattern.findall(comment_decoded))

        # 4. Mailto links
        for a in soup.find_all('a', href=True):
            if a['href'].lower().startswith('mailto:'):
                email = decode(a['href'][7:].split('?')[0])
                emails_found.add(email)

        # 5. Normalize matches
        for match in email_matches:
            if isinstance(match, tuple):
                emails_found.add(f"{match[0]}@{match[1]}.{match[2]}")
            else:
                emails_found.add(match)

        return [self.extract_email_context(soup, email, source_url) for email in emails_found]

    def extract_email_context(self, soup, email, source_url):
        email_info = {
            'email': email,
            'name': '',
            'office': '',
            'department': '',
            'unit': '',
            'url_found': source_url
        }
        try:
            email_elements = soup.find_all(string=re.compile(re.escape(email)))
            for element in email_elements:
                parent = element.parent
                for _ in range(3):
                    if parent:
                        parent_text = parent.get_text().strip()
                        for pattern in self.name_patterns:
                            name_match = pattern.search(parent_text)
                            if name_match:
                                email_info['name'] = name_match.group(1).strip()
                                break
                        text_lower = parent_text.lower()
                        for keyword in self.dept_keywords:
                            if keyword in text_lower:
                                for line in parent_text.split('\n'):
                                    if keyword in line.lower():
                                        email_info['department'] = line.strip()
                                        break
                        for keyword in self.office_keywords:
                            if keyword in text_lower:
                                for line in parent_text.split('\n'):
                                    if keyword in line.lower():
                                        email_info['office'] = line.strip()
                                        break
                        parent = parent.parent
                    else:
                        break
        except Exception as e:
            logger.debug(f"Error extracting context for {email}: {e}")
        return email_info

    def extract_links_from_page(self, soup, current_url):
        new_urls = []
        try:
            base_domain = urlparse(self.base_url).netloc
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                full_url = urljoin(current_url, href)
                if urlparse(full_url).netloc.endswith(base_domain) and self.is_valid_url(full_url):
                    new_urls.append(full_url)
            new_urls = list(set(new_urls))[:30]
        except Exception as e:
            logger.error(f"Error extracting links from {current_url}: {e}")
        return new_urls

    def is_valid_url(self, url):
        skip_extensions = ['.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.zip', '.doc', '.docx']
        skip_keywords = ['javascript:', 'mailto:', '#', 'tel:']
        url_lower = url.lower()
        if any(url_lower.endswith(ext) for ext in skip_extensions):
            return False
        if any(keyword in url_lower for keyword in skip_keywords):
            return False
        return True

    def send_new_urls_to_master(self, urls):
        if urls:
            message = {'type': 'new_urls', 'urls': urls, 'worker_id': self.worker_id}
            self.send_message_to_master(message)

    def send_intermediate_results(self):
        if self.local_emails or self.local_pages_processed > 0:
            results = {'emails': self.local_emails.copy(), 'pages_processed': self.local_pages_processed}
            message = {'type': 'scraping_results', 'results': results, 'worker_id': self.worker_id}
            self.send_message_to_master(message)
            self.local_emails.clear()
            self.local_pages_processed = 0

    def send_final_results(self):
        results = {'emails': self.local_emails.copy(), 'pages_processed': self.local_pages_processed}
        message = {'type': 'scraping_results', 'results': results, 'worker_id': self.worker_id, 'final': True}
        self.send_message_to_master(message)
        logger.info(f"Sent final results: {len(self.local_emails)} emails, {self.local_pages_processed} pages")

    def send_message_to_master(self, message):
        try:
            if self.master_socket:
                message_str = json.dumps(message)
                self.master_socket.send(message_str.encode())
            else:
                logger.warning("Master socket not connected, skipping send")
        except Exception as e:
            logger.error(f"Error sending message to master: {e}")
            self.master_socket = None

    def get_time_remaining(self):
        if self.start_time is None or self.scraping_time is None:
            return float('inf')
        elapsed = time.time() - self.start_time
        return max(0, self.scraping_time - elapsed)

    def cleanup_connection(self):
        try:
            if self.master_socket:
                self.master_socket.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def run(self):
        logger.info(f"Starting worker {self.worker_id}")
        if not self.connect_to_master():
            logger.error("Failed to connect to master. Exiting.")
            return False
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        except Exception as e:
            logger.error(f"Worker error: {e}")
        finally:
            self.cleanup_connection()
        logger.info(f"Worker {self.worker_id} shutting down")
        return True


def main():
    parser = argparse.ArgumentParser(description='Distributed Email Web Scraper - Worker Node')
    parser.add_argument('--master-host', default='localhost', help='Master node hostname/IP')
    parser.add_argument('--master-port', type=int, default=8000, help='Master node port')
    parser.add_argument('--worker-id', help='Worker ID (auto-generated if not provided)')
    args = parser.parse_args()
    try:
        worker = WorkerNode(master_host=args.master_host, master_port=args.master_port, worker_id=args.worker_id)
        success = worker.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Worker error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
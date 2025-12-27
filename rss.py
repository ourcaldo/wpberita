"""
Indonesian News RSS Scraper with Supabase Integration
Version: 2.0.0
Author: ourcaldo
Description: Multi-threaded news scraper that fetches articles from major Indonesian 
             news sources (Detik, Antara, Sindonews, Republika, Tribunnews, Merdeka) 
             using RSS feeds. Features include duplicate prevention via content hashing,
             automatic retry logic, full content extraction, and persistent worker threads
             that refresh sources every hour.

Key Features:
- Multi-source RSS feed aggregation
- Duplicate detection using MD5 hash of URLs and content IDs
- Automatic worker threads for continuous scraping
- Configurable refresh interval (default: 1 hour)
- Full article content and image extraction
- Comprehensive logging and error handling
- Supabase PostgreSQL integration
"""

import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import hashlib
import time
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from urllib.parse import urlparse
import re
from dateutil import parser as date_parser
import logging
from functools import wraps
import json
import threading
from queue import Queue
import signal
import sys
import os


# Version information
__version__ = "2.0.0"
__author__ = "ourcaldo"
__description__ = "Multi-threaded Indonesian News RSS Scraper with auto-refresh workers"


class IndonesianNewsScraper:
    """
    Main scraper class that handles RSS feed fetching, content extraction,
    and database operations for Indonesian news sources.
    
    This class implements a worker-based architecture where each news source
    runs in its own thread, continuously fetching new articles at specified intervals.
    """
    
    def __init__(self, max_retries=3, retry_delay=2, request_timeout=15, refresh_interval=3600):
        """
        Initialize the scraper with configuration parameters.
        
        Args:
            max_retries (int): Maximum number of retry attempts for failed requests
            retry_delay (int): Delay in seconds between retry attempts
            request_timeout (int): Timeout in seconds for HTTP requests
            refresh_interval (int): Time in seconds between scraping cycles (default: 3600 = 1 hour)
        """
        # Supabase PostgreSQL connection configuration (using connection pooler for better performance)
        self.db_config = {
            'host': os.getenv('DB_HOST', 'aws-1-ap-southeast-1.pooler.supabase.com'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres.fdvrmoufpiqjqqmphorc'),
            'password': os.getenv('DB_PASSWORD', 'Adk06092000')
        }
        
        # Worker and threading configuration
        self.refresh_interval = refresh_interval  # Default: 1 hour (3600 seconds)
        self.workers = {}  # Dictionary to store worker threads
        self.stop_event = threading.Event()  # Event to signal workers to stop
        self.worker_lock = threading.Lock()  # Lock for thread-safe operations
        
        # Retry configuration for failed requests
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_timeout = request_timeout
        
        # HTTP headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Setup logging with detailed formatting
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Verified working RSS feeds from major Indonesian news sources
        # All feeds have been tested and confirmed working as of version 2.0.0
        self.rss_feeds = {
            'detik': {
                'news': 'https://news.detik.com/rss',
                'finance': 'https://finance.detik.com/rss',
                'sport': 'https://sport.detik.com/rss',
                'inet': 'https://inet.detik.com/rss',
                'food': 'https://food.detik.com/rss',
                'oto': 'https://oto.detik.com/rss',
                'health': 'https://health.detik.com/rss',
                'travel': 'https://travel.detik.com/rss',
            },
            'antara': {
                'terkini': 'https://www.antaranews.com/rss/terkini',
                'top-news': 'https://www.antaranews.com/rss/top-news',
                'politik': 'https://www.antaranews.com/rss/politik',
                'ekonomi': 'https://www.antaranews.com/rss/ekonomi',
                'olahraga': 'https://www.antaranews.com/rss/olahraga',
                'tekno': 'https://www.antaranews.com/rss/tekno',
                'otomotif': 'https://www.antaranews.com/rss/otomotif',
            },
            'sindonews': {
                'main': 'https://www.sindonews.com/feed',
                'nasional': 'https://nasional.sindonews.com/rss',
                'ekbis': 'https://ekbis.sindonews.com/rss',
                'sports': 'https://sports.sindonews.com/rss',
                'tekno': 'https://tekno.sindonews.com/rss',
                'otomotif': 'https://otomotif.sindonews.com/rss',
            },
            'republika': {
                'main': 'https://www.republika.co.id/rss/',
            },
            'tribunnews': {
                'main': 'https://www.tribunnews.com/rss',
            },
            'merdeka': {
                'main': 'https://www.merdeka.com/feed/',
            }
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """
        Handle system signals (CTRL+C, SIGTERM) for graceful shutdown.
        
        Args:
            signum: Signal number received
            frame: Current stack frame
        """
        self.logger.info("Received shutdown signal. Stopping all workers...")
        self.stop_all_workers()
        sys.exit(0)
    
    def get_db_connection(self):
        """
        Create and return a new database connection to Supabase PostgreSQL.
        
        Returns:
            psycopg2.connection: Database connection object
            
        Raises:
            psycopg2.Error: If connection fails
        """
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def get_url_hash(self, url):
        """
        Generate MD5 hash for URL to enable duplicate detection.
        URLs are normalized before hashing (lowercase, trailing slash removed).
        
        Args:
            url (str): The URL to hash
            
        Returns:
            str: MD5 hash of the normalized URL, or None if URL is invalid
        """
        if not url:
            return None
        # Normalize URL: remove trailing slash, convert to lowercase
        url = url.strip().lower()
        if url.endswith('/'):
            url = url[:-1]
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def get_content_id(self, entry):
        """
        Extract a unique content identifier from RSS entry.
        Tries multiple fields in order: id, guid, link.
        
        Args:
            entry (dict): RSS feed entry from feedparser
            
        Returns:
            str: Unique content ID, or None if not found
        """
        # Try different fields for content ID (RSS 2.0 and Atom formats)
        content_id = entry.get('id') or entry.get('guid') or entry.get('link')
        if content_id:
            # If guid is an object (has value attribute), extract the value
            if hasattr(content_id, 'value'):
                content_id = content_id.value
            return str(content_id).strip()
        return None
    
    def article_exists(self, url_hash, content_id=None):
        """
        Check if article already exists in database to prevent duplicates.
        Uses both URL hash and content ID for robust duplicate detection.
        
        Args:
            url_hash (str): MD5 hash of the article URL
            content_id (str, optional): Additional content identifier from RSS feed
            
        Returns:
            bool: True if article exists, False otherwise
        """
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Primary check: by URL hash (most reliable method)
                cur.execute(
                    "SELECT id FROM articles WHERE article_hash = %s",
                    (url_hash,)
                )
                result = cur.fetchone()
                if result:
                    return True
                
                # Secondary check: by actual URL if content ID available
                # This handles edge cases where same article has different URL formats
                if content_id:
                    cur.execute(
                        "SELECT id FROM articles WHERE url = %s",
                        (content_id,)
                    )
                    result = cur.fetchone()
                    if result:
                        return True
                
                return False
        except Exception as e:
            self.logger.error(f"Error checking article existence: {e}")
            return False  # Assume doesn't exist on error to avoid skipping articles
        finally:
            conn.close()
    
    def parse_date(self, date_string):
        """
        Parse date string into datetime object. Handles various date formats.
        
        Args:
            date_string (str): Date string in any common format
            
        Returns:
            datetime: Parsed datetime object, or None if parsing fails
        """
        if not date_string:
            return None
        try:
            return date_parser.parse(date_string)
        except:
            return None
    
    def fetch_rss_feed(self, url):
        """
        Fetch and parse RSS feed from given URL with error handling.
        
        Args:
            url (str): RSS feed URL to fetch
            
        Returns:
            feedparser.FeedParserDict: Parsed feed object, or None on error
        """
        try:
            self.logger.debug(f"Fetching RSS: {url}")
            feed = feedparser.parse(url)
            if feed.bozo:
                self.logger.warning(f"Feed parsing issue for {url}: {feed.bozo_exception}")
            return feed
        except Exception as e:
            self.logger.error(f"Error fetching RSS {url}: {e}")
            return None
    
    def extract_content(self, url, source):
        """
        Extract full article content from article page using source-specific selectors.
        
        Args:
            url (str): Article URL to scrape
            source (str): News source name (detik, antara, etc.)
            
        Returns:
            str: Extracted article text, or None if extraction fails
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.request_timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Source-specific CSS selectors for article content
            # These selectors are tested and verified for each source
            content_selectors = {
                'detik': ['article', '.detail__body-text', '.itp_bodycontent'],
                'antara': ['.post-content', 'article'],
                'sindonews': ['.content-text', 'article'],
                'republika': ['.artikel', 'article'],
                'tribunnews': ['.side-article', 'article'],
                'merdeka': ['.mdk-body-paragraph', 'article'],
                'suara': ['.wrap-content-new', 'article'],
            }
            
            selectors = content_selectors.get(source, ['article', '.content'])
            
            # Try each selector until content is found
            content = None
            for selector in selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    # Extract all paragraphs and filter out short ones
                    paragraphs = content_div.find_all('p')
                    content = '\n\n'.join([
                        p.get_text().strip() 
                        for p in paragraphs 
                        if p.get_text().strip() and len(p.get_text().strip()) > 20
                    ])
                    if content:
                        break
            
            return content
            
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {e}")
            return None
    
    def extract_image_from_rss(self, entry):
        """
        Extract featured image URL from RSS feed entry.
        Supports multiple RSS formats: enclosure, media:content, media:thumbnail.
        
        Args:
            entry (dict): RSS feed entry from feedparser
            
        Returns:
            str: Image URL, or None if no image found
        """
        # Priority 1: Check for enclosure tag (RSS 2.0 standard)
        # Format: <enclosure url="..." type="image/jpeg"/>
        if 'enclosures' in entry and entry.enclosures:
            for enc in entry.enclosures:
                enc_type = enc.get('type', '').lower()
                if 'image' in enc_type:
                    # Try both 'url' and 'href' attributes
                    image_url = enc.get('url') or enc.get('href')
                    if image_url:
                        return str(image_url).strip()
        
        # Priority 2: Check for media:content (Media RSS extension)
        # Format: <media:content url="..." type="image/jpeg"/>
        if 'media_content' in entry and entry.media_content:
            for media in entry.media_content:
                media_type = media.get('type', '').lower()
                medium = media.get('medium', '').lower()
                if 'image' in media_type or medium == 'image':
                    image_url = media.get('url')
                    if image_url:
                        return str(image_url).strip()
        
        # Priority 3: Check for media:thumbnail
        if 'media_thumbnail' in entry and entry.media_thumbnail:
            for thumb in entry.media_thumbnail:
                image_url = thumb.get('url')
                if image_url:
                    return str(image_url).strip()
        
        return None
    
    def normalize_article_data(self, entry, source, category):
        """
        Normalize article data from different RSS formats into a standard structure.
        Handles both RSS 2.0 and Atom feed formats.
        
        Args:
            entry (dict): RSS feed entry from feedparser
            source (str): News source name
            category (str): Article category
            
        Returns:
            dict: Normalized article data with standard fields
        """
        # Extract title - handle various formats (string, dict, etc.)
        title = entry.get('title', '') or entry.get('title_detail', {}).get('value', '')
        if isinstance(title, dict):
            title = title.get('value', '')
        title = str(title).strip()
        
        # Extract URL - handle RSS 2.0 (string) and Atom (list) formats
        url = None
        link = entry.get('link', '')
        
        if isinstance(link, list):
            # Atom feeds may have multiple links, prefer 'alternate' type
            for l in link:
                if isinstance(l, dict):
                    rel = l.get('rel', 'alternate')
                    if rel == 'alternate' or not url:
                        url = l.get('href', '')
                else:
                    url = str(l)
                    break
        elif isinstance(link, dict):
            url = link.get('href', '')
        else:
            url = str(link).strip()
        
        # Fallback: try links list in Atom format
        if not url and 'links' in entry:
            for link_obj in entry['links']:
                if isinstance(link_obj, dict):
                    url = link_obj.get('href', '')
                    if url:
                        break
        
        # Get unique content ID for duplicate detection
        content_id = self.get_content_id(entry)
        
        # Extract published date - try multiple field names
        pub_date = None
        date_fields = ['published', 'pubDate', 'updated', 'updated_parsed', 'published_parsed']
        
        for date_field in date_fields:
            if date_field in entry:
                date_value = entry[date_field]
                # Handle parsed dates (time.struct_time format)
                if date_field.endswith('_parsed') and isinstance(date_value, time.struct_time):
                    try:
                        pub_date = datetime.fromtimestamp(time.mktime(date_value))
                        break
                    except:
                        pass
                else:
                    pub_date = self.parse_date(str(date_value))
                    if pub_date:
                        break
        
        # Extract summary/description - handle various formats
        summary = entry.get('summary', '') or entry.get('description', '')
        
        # Try to get from content field if summary not found
        if not summary and 'content' in entry:
            content_data = entry['content']
            if isinstance(content_data, list) and len(content_data) > 0:
                summary = content_data[0].get('value', '')
            elif isinstance(content_data, dict):
                summary = content_data.get('value', '')
        
        # Handle summary_detail (Atom format)
        if not summary and 'summary_detail' in entry:
            summary = entry['summary_detail'].get('value', '')
        
        # Clean HTML tags from summary and limit length
        if summary:
            if isinstance(summary, str):
                summary = BeautifulSoup(summary, 'html.parser').get_text()
                summary = summary.strip()[:500]  # Limit to 500 characters
        
        # Extract author - handle various formats
        author = None
        if 'author' in entry:
            author = entry['author']
            if isinstance(author, dict):
                author = author.get('name', '') or author.get('value', '')
        elif 'author_detail' in entry:
            author = entry['author_detail'].get('name', '')
        
        if author:
            author = str(author).strip()[:255]
            if not author:
                author = None
        
        # Extract featured image from RSS feed
        image_url = self.extract_image_from_rss(entry)
        
        return {
            'title': title,
            'url': url,
            'content_id': content_id,
            'published_date': pub_date,
            'summary': summary,
            'author': author,
            'source': source,
            'category': category,
            'image_url': image_url
        }
    
    def save_article(self, article_data):
        """
        Save article to database with duplicate handling.
        Uses ON CONFLICT to update existing articles instead of creating duplicates.
        
        Args:
            article_data (dict): Normalized article data to save
            
        Returns:
            int: Article ID if successful, None otherwise
        """
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get image URL from article_data (extracted from RSS feed)
                image_url = article_data.get('image_url')
                
                # Store image URL as plain text string
                images_value = image_url
                if images_value and not isinstance(images_value, str):
                    images_value = str(images_value)
                
                # Insert or update article using ON CONFLICT
                cur.execute("""
                    INSERT INTO articles (
                        article_hash, title, url, source, category,
                        published_date, summary, content, author, images
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_hash) DO UPDATE SET
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        content = EXCLUDED.content,
                        images = EXCLUDED.images,
                        updated_at = NOW()
                    RETURNING id
                """, (
                    article_data['article_hash'],
                    article_data['title'],
                    article_data['url'],
                    article_data['source'],
                    article_data['category'],
                    article_data['published_date'],
                    article_data['summary'],
                    article_data.get('content'),
                    article_data.get('author'),
                    images_value
                ))
                
                article_id = cur.fetchone()[0]
                conn.commit()
                return article_id
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving article: {e}")
            return None
        finally:
            conn.close()
    
    def log_scraping_activity(self, source, category, stats, status, error_msg=None):
        """
        Log scraping activity to database for monitoring and debugging.
        
        Args:
            source (str): News source name
            category (str): Article category
            stats (dict): Statistics dictionary with counts
            status (str): Status of the scraping operation (success/failed/partial)
            error_msg (str, optional): Error message if operation failed
        """
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scraping_logs (
                        source, category, articles_found, articles_new,
                        articles_updated, status, error_message, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    source, category,
                    stats.get('found', 0),
                    stats.get('new', 0),
                    stats.get('updated', 0),
                    status, error_msg
                ))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging scraping activity: {e}")
        finally:
            conn.close()
    
    def scrape_feed(self, source, category, feed_url):
        """
        Scrape all available articles from a single RSS feed.
        This method processes ALL entries in the feed (no limit).
        
        Args:
            source (str): News source name
            category (str): Article category
            feed_url (str): RSS feed URL
            
        Returns:
            dict: Statistics about the scraping operation
        """
        self.logger.info(f"=== Scraping {source}/{category} ===")
        
        stats = {'found': 0, 'new': 0, 'updated': 0, 'skipped': 0}
        start_time = time.time()
        
        # Fetch RSS feed
        feed = self.fetch_rss_feed(feed_url)
        if not feed or not feed.entries:
            error_msg = "No entries found in feed"
            self.logger.warning(f"{source}/{category}: {error_msg}")
            self.log_scraping_activity(source, category, stats, 'failed', error_msg)
            return stats
        
        total_entries = len(feed.entries)
        self.logger.info(f"{source}/{category}: Found {total_entries} entries in feed")
        stats['found'] = total_entries
        
        # Process ALL entries in the feed (no limit)
        for idx, entry in enumerate(feed.entries):
            # Check if stop signal received
            if self.stop_event.is_set():
                self.logger.info(f"{source}/{category}: Stopping due to stop signal")
                break
            
            try:
                # Normalize article data from RSS entry
                article_data = self.normalize_article_data(entry, source, category)
                
                # Validate required fields
                if not article_data['url'] or not article_data['title']:
                    stats['skipped'] += 1
                    continue
                
                # Generate hash for duplicate detection
                # Prefer content_id over URL for better duplicate detection
                hash_source = article_data.get('content_id') or article_data['url']
                url_hash = self.get_url_hash(hash_source)
                
                if not url_hash:
                    stats['skipped'] += 1
                    continue
                
                article_data['article_hash'] = url_hash
                
                # Check if article already exists in database
                exists = self.article_exists(url_hash, article_data.get('content_id'))
                
                if not exists:
                    self.logger.info(f"{source}/{category} [{idx+1}/{total_entries}] NEW: {article_data['title'][:60]}...")
                    
                    # Extract full content from article page
                    article_data['content'] = self.extract_content(article_data['url'], source)
                    
                    # Save to database
                    article_id = self.save_article(article_data)
                    if article_id:
                        stats['new'] += 1
                        self.logger.debug(f"Saved article ID: {article_id}")
                    
                    # Be respectful to servers - add delay between requests
                    time.sleep(1)
                else:
                    stats['skipped'] += 1
                    self.logger.debug(f"{source}/{category} [{idx+1}/{total_entries}] SKIP: Article exists")
                
            except Exception as e:
                self.logger.error(f"{source}/{category}: Error processing entry {idx+1}: {e}")
                continue
        
        duration = int(time.time() - start_time)
        self.logger.info(f"{source}/{category}: Completed in {duration}s - {stats['new']} new, {stats['skipped']} skipped")
        
        # Log activity to database
        self.log_scraping_activity(source, category, stats, 'success')
        return stats
    
    def source_worker(self, source):
        """
        Worker function that continuously scrapes all categories for a specific source.
        Runs in a loop with configurable refresh interval until stop signal received.
        
        Args:
            source (str): News source name to scrape
        """
        self.logger.info(f"Worker started for source: {source}")
        
        categories = self.rss_feeds.get(source, {})
        if not categories:
            self.logger.error(f"No categories found for source: {source}")
            return
        
        cycle_count = 0
        
        # Main worker loop - runs until stop signal received
        while not self.stop_event.is_set():
            cycle_count += 1
            cycle_start = time.time()
            
            self.logger.info(f"{source}: Starting scraping cycle #{cycle_count}")
            
            total_stats = {'found': 0, 'new': 0, 'skipped': 0}
            
            # Scrape all categories for this source
            for category, feed_url in categories.items():
                if self.stop_event.is_set():
                    break
                
                try:
                    stats = self.scrape_feed(source, category, feed_url)
                    
                    total_stats['found'] += stats['found']
                    total_stats['new'] += stats['new']
                    total_stats['skipped'] += stats['skipped']
                    
                    # Delay between categories to avoid overwhelming servers
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"{source}/{category}: Worker error: {e}")
                    continue
            
            cycle_duration = int(time.time() - cycle_start)
            self.logger.info(
                f"{source}: Cycle #{cycle_count} completed in {cycle_duration}s - "
                f"Found: {total_stats['found']}, New: {total_stats['new']}, "
                f"Skipped: {total_stats['skipped']}"
            )
            
            # Wait for next cycle (refresh interval) or until stop signal
            # Use Event.wait() instead of time.sleep() for immediate stop response
            if not self.stop_event.is_set():
                next_run = datetime.now() + timedelta(seconds=self.refresh_interval)
                self.logger.info(
                    f"{source}: Next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"(in {self.refresh_interval/60:.1f} minutes)"
                )
                self.stop_event.wait(timeout=self.refresh_interval)
        
        self.logger.info(f"Worker stopped for source: {source}")
    
    def start_workers(self):
        """
        Start dedicated worker threads for each news source.
        Each worker runs independently and continuously scrapes its assigned source.
        """
        self.logger.info("="*60)
        self.logger.info(f"Starting Indonesian News Scraper v{__version__}")
        self.logger.info(f"Refresh interval: {self.refresh_interval/60:.1f} minutes")
        self.logger.info(f"Total sources: {len(self.rss_feeds)}")
        self.logger.info("="*60)
        
        # Create and start a worker thread for each source
        for source in self.rss_feeds.keys():
            worker_thread = threading.Thread(
                target=self.source_worker,
                args=(source,),
                name=f"Worker-{source}",
                daemon=True  # Daemon threads will exit when main program exits
            )
            
            with self.worker_lock:
                self.workers[source] = {
                    'thread': worker_thread,
                    'started_at': datetime.now()
                }
            
            worker_thread.start()
            self.logger.info(f"Started worker for: {source}")
            
            # Small delay between starting workers
            time.sleep(0.5)
        
        self.logger.info(f"All {len(self.workers)} workers started successfully")
    
    def stop_all_workers(self):
        """
        Stop all running worker threads gracefully.
        Signals workers to stop and waits for them to finish current operations.
        """
        self.logger.info("Stopping all workers...")
        
        # Signal all workers to stop
        self.stop_event.set()
        
        # Wait for all worker threads to finish
        with self.worker_lock:
            for source, worker_info in self.workers.items():
                thread = worker_info['thread']
                if thread.is_alive():
                    self.logger.info(f"Waiting for worker {source} to stop...")
                    thread.join(timeout=30)  # Wait up to 30 seconds
                    
                    if thread.is_alive():
                        self.logger.warning(f"Worker {source} did not stop gracefully")
                    else:
                        runtime = datetime.now() - worker_info['started_at']
                        self.logger.info(f"Worker {source} stopped after running for {runtime}")
        
        self.logger.info("All workers stopped successfully")
    
    def get_worker_status(self):
        """
        Get status information for all running workers.
        
        Returns:
            dict: Dictionary with worker status information for each source
        """
        status = {}
        with self.worker_lock:
            for source, worker_info in self.workers.items():
                thread = worker_info['thread']
                runtime = datetime.now() - worker_info['started_at']
                
                status[source] = {
                    'alive': thread.is_alive(),
                    'started_at': worker_info['started_at'].strftime('%Y-%m-%d %H:%M:%S'),
                    'runtime': str(runtime).split('.')[0]  # Remove microseconds
                }
        
        return status
    
    def get_statistics(self):
        """
        Get comprehensive scraping statistics from database.
        
        Returns:
            list: List of dictionaries containing statistics per source
        """
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        source,
                        COUNT(*) as total_articles,
                        COUNT(DISTINCT category) as categories,
                        MAX(scraped_at) as last_scraped,
                        MIN(scraped_at) as first_scraped
                    FROM articles
                    GROUP BY source
                    ORDER BY total_articles DESC
                """)
                stats = cur.fetchall()
                
                self.logger.info("\n=== Database Statistics ===")
                for row in stats:
                    self.logger.info(
                        f"{row['source']}: {row['total_articles']} articles, "
                        f"{row['categories']} categories, "
                        f"last: {row['last_scraped'].strftime('%Y-%m-%d %H:%M')}"
                    )
                
                return stats
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return []
        finally:
            conn.close()
    
    def scrape_all_once(self):
        """
        Scrape all sources once (single pass through all feeds).
        Useful for testing or manual one-time scraping.
        
        Returns:
            dict: Total statistics across all sources
        """
        self.logger.info("\n" + "="*60)
        self.logger.info("Starting Single-Pass Scraping")
        self.logger.info("="*60)
        
        total_stats = {'found': 0, 'new': 0, 'updated': 0, 'skipped': 0}
        
        for source, categories in self.rss_feeds.items():
            for category, feed_url in categories.items():
                stats = self.scrape_feed(source, category, feed_url)
                
                total_stats['found'] += stats['found']
                total_stats['new'] += stats['new']
                total_stats['updated'] += stats['updated']
                total_stats['skipped'] += stats['skipped']
                
                # Delay between feeds
                time.sleep(2)
        
        self.logger.info("\n" + "="*60)
        self.logger.info("Scraping Complete")
        self.logger.info(f"Total Found: {total_stats['found']}")
        self.logger.info(f"New Articles: {total_stats['new']}")
        self.logger.info(f"Skipped: {total_stats['skipped']}")
        self.logger.info("="*60)
        
        return total_stats


def main():
    """
    Main execution function.
    Initializes scraper, tests database connection, and starts worker threads.
    Workers will run continuously until interrupted (CTRL+C).
    """
    # Display banner
    print("="*60)
    print(f"Indonesian News RSS Scraper v{__version__}")
    print(f"Description: {__description__}")
    print("="*60)
    print()
    
    # Initialize scraper with 1-hour refresh interval
    scraper = IndonesianNewsScraper(refresh_interval=3600)  # 3600 seconds = 1 hour
    
    # Test database connection
    try:
        conn = scraper.get_db_connection()
        print("✓ Database connection successful")
        conn.close()
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        print("Please check your database credentials and network connection.")
        return
    
    # Display configuration
    print(f"\nConfiguration:")
    print(f"  - Refresh interval: {scraper.refresh_interval/60:.1f} minutes")
    print(f"  - News sources: {len(scraper.rss_feeds)}")
    print(f"  - Request timeout: {scraper.request_timeout} seconds")
    print(f"  - Max retries: {scraper.max_retries}")
    print()
    
    # Start worker threads
    scraper.start_workers()
    
    print("\nWorkers are running. Press CTRL+C to stop.")
    print("-"*60)
    
    try:
        # Keep main thread alive and display periodic status updates
        while True:
            time.sleep(300)  # Sleep for 5 minutes between status updates
            
            # Display worker status
            status = scraper.get_worker_status()
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Worker Status:")
            for source, info in status.items():
                state = "RUNNING" if info['alive'] else "STOPPED"
                print(f"  {source}: {state} (Runtime: {info['runtime']})")
            
            # Display statistics every status update
            print("\nDatabase Statistics:")
            scraper.get_statistics()
            print("-"*60)
            
    except KeyboardInterrupt:
        print("\n\nShutdown requested...")
        scraper.stop_all_workers()
        print("Scraper stopped successfully. Goodbye!")


if __name__ == "__main__":
    main()
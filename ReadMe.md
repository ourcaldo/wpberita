# Indonesian News RSS Scraper

A comprehensive Python scraper for Indonesian news websites using RSS feeds with Supabase PostgreSQL database storage.

## Features

✅ **Verified RSS Feeds** - All feeds tested and working
✅ **Duplicate Detection** - Uses URL hashing to prevent duplicate articles
✅ **Full Content Extraction** - Scrapes complete article content and images
✅ **Universal Format** - Normalizes data from different sources
✅ **Supabase Integration** - Stores data in PostgreSQL database
✅ **Logging & Statistics** - Tracks scraping activities and provides insights

## Verified News Sources

### Detik (8 categories)
- News, Finance, Sport, Technology, Food, Automotive, Health, Travel

### Antara (7 categories)
- Latest, Top News, Politics, Economy, Sports, Technology, Automotive

### Sindonews (7 categories)  
- Main, National, Metro, Economy & Business, Sports, Technology, Automotive

### Others
- Republika, Tribunnews, Merdeka, Suara

## Installation

### 1. Install Required Packages

```bash
pip install feedparser requests beautifulsoup4 psycopg2-binary python-dateutil
```

**requirements.txt:**
```
feedparser==6.0.10
requests==2.31.0
beautifulsoup4==4.12.2
psycopg2-binary==2.9.9
python-dateutil==2.8.2
```

### 2. Setup Supabase Database

1. Run the SQL schema script to create tables:
   - Open Supabase SQL Editor
   - Copy and paste the entire SQL schema from `supabase_news_schema.sql`
   - Execute the script

This will create:
- `articles` table - Main article storage
- `article_images` table - Article images
- `rss_feeds` table - RSS feed tracking
- `scraping_logs` table - Activity logs
- Indexes for performance
- Views for statistics

### 3. Configure Database Connection

The scraper is pre-configured with your Supabase credentials:
```python
'host': 'db.fdvrmoufpiqjqqmphorc.supabase.co'
'database': 'postgres'
'user': 'postgres'
'password': 'Adk06092000'
```

## Usage

### Basic Scraping

```python
from indonesia_news_scraper import IndonesianNewsScraper

scraper = IndonesianNewsScraper()

# Scrape all feeds (10 articles per feed)
scraper.scrape_all(limit_per_feed=10)

# Get statistics
scraper.get_statistics()
```

### Scrape Specific Feed

```python
# Scrape only Detik news
stats = scraper.scrape_feed('detik', 'news', 
    'https://news.detik.com/rss', limit=20)
```

### Run the Complete Script

```bash
python indonesia_news_scraper.py
```

## Database Schema

### articles
- `article_hash` - MD5 hash of URL (prevents duplicates)
- `title` - Article title
- `url` - Article URL
- `source` - News source (detik, antara, etc.)
- `category` - Category (news, sports, etc.)
- `published_date` - Publication date
- `summary` - Article summary/excerpt
- `content` - Full article content
- `author` - Article author

### article_images
- `article_id` - Foreign key to articles
- `image_url` - Image URL
- `alt_text` - Image alt text
- `image_order` - Image order in article

## Key Features Explained

### 1. Duplicate Detection
Uses MD5 hash of article URL:
```python
article_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
```
Database constraint ensures no duplicates:
```sql
CONSTRAINT unique_article_hash UNIQUE (article_hash)
```

### 2. Universal Format
Normalizes data from different RSS formats:
- Handles various date formats
- Cleans HTML from summaries
- Extracts author information
- Limits text lengths appropriately

### 3. Content Extraction
Source-specific selectors for accurate content extraction:
```python
content_selectors = {
    'detik': ['article', '.detail__body-text'],
    'antara': ['.post-content', 'article'],
    # ... etc
}
```

### 4. Image Extraction
- Extracts up to 5 images per article
- Filters out icons, logos, ads
- Stores with alt text and order

## Querying the Database

### Get Recent Articles
```sql
SELECT * FROM articles 
ORDER BY scraped_at DESC 
LIMIT 10;
```

### Get Articles by Source
```sql
SELECT * FROM articles 
WHERE source = 'detik' 
ORDER BY published_date DESC;
```

### Get Articles with Images
```sql
SELECT a.*, COUNT(ai.id) as image_count
FROM articles a
LEFT JOIN article_images ai ON a.id = ai.id
GROUP BY a.id
ORDER BY image_count DESC;
```

### View Statistics
```sql
SELECT * FROM vw_scraping_stats;
```

## Monitoring & Logs

Check scraping logs:
```sql
SELECT * FROM scraping_logs 
ORDER BY started_at DESC 
LIMIT 10;
```

View feed status:
```sql
SELECT * FROM rss_feeds 
WHERE is_active = true
ORDER BY last_successful_scrape DESC;
```

## Error Handling

The scraper includes comprehensive error handling:
- Failed RSS fetches are logged
- Content extraction errors don't stop the process
- Database errors are caught and logged
- All activities recorded in `scraping_logs`

## Best Practices

1. **Rate Limiting**: Built-in delays (1s between articles, 2s between feeds)
2. **Respectful Scraping**: Uses proper User-Agent headers
3. **Incremental Updates**: Only scrapes new articles
4. **Data Quality**: Validates and cleans data before storage

## Maintenance

### Update RSS Feeds
Add new feeds via database:
```sql
INSERT INTO rss_feeds (source, category, feed_url, is_active)
VALUES ('kompas', 'news', 'https://example.com/rss', true);
```

### Deactivate Broken Feeds
```sql
UPDATE rss_feeds 
SET is_active = false 
WHERE source = 'example' AND category = 'broken';
```

## Troubleshooting

### Connection Issues
- Verify Supabase credentials
- Check network connectivity
- Ensure PostgreSQL port 5432 is accessible

### No Articles Scraped
- Check RSS feed URLs are still valid
- Verify `is_active` status in `rss_feeds` table
- Check `scraping_logs` for errors

### Duplicate Articles
- Hash-based detection should prevent this
- If occurs, check for URL variations
- Normalize URLs before hashing if needed

## Performance Optimization

Current optimizations:
- Database indexes on key columns
- Batch image inserts
- Connection pooling ready
- Efficient duplicate checking

For high-volume scraping:
- Consider async scraping with `asyncio`
- Use connection pooling
- Implement caching layer
- Schedule regular cleanups

## License

MIT License - Feel free to modify and use

## Support

For issues or questions:
1. Check `scraping_logs` table
2. Verify RSS feeds are still active
3. Test database connection
4. Review error messages in logs
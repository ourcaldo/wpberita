-- Create articles table
CREATE TABLE IF NOT EXISTS articles (
    id BIGSERIAL PRIMARY KEY,
    article_hash VARCHAR(64) UNIQUE NOT NULL,  -- MD5 hash of URL for duplicate detection
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    source VARCHAR(50) NOT NULL,  -- e.g., 'detik', 'sindonews'
    category VARCHAR(50),  -- e.g., 'news', 'sports', 'tech'
    published_date TIMESTAMP WITH TIME ZONE,
    summary TEXT,
    content TEXT,
    author VARCHAR(255),
    images TEXT,  -- Store single featured image URL from RSS feed (enclosure/media_content)
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes for performance
    CONSTRAINT unique_article_hash UNIQUE (article_hash)
);

-- Create RSS feeds tracking table
CREATE TABLE IF NOT EXISTS rss_feeds (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    feed_url TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    last_checked TIMESTAMP WITH TIME ZONE,
    last_successful_scrape TIMESTAMP WITH TIME ZONE,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT unique_feed UNIQUE (source, category)
);

-- Create scraping logs table
CREATE TABLE IF NOT EXISTS scraping_logs (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    articles_found INTEGER DEFAULT 0,
    articles_new INTEGER DEFAULT 0,
    articles_updated INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL,  -- 'success', 'partial', 'failed'
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_date DESC);
CREATE INDEX IF NOT EXISTS idx_articles_scraped ON articles(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(article_hash);
CREATE INDEX IF NOT EXISTS idx_articles_images ON articles(images) WHERE images IS NOT NULL;  -- B-tree index for TEXT
CREATE INDEX IF NOT EXISTS idx_rss_feeds_active ON rss_feeds(is_active);
CREATE INDEX IF NOT EXISTS idx_scraping_logs_status ON scraping_logs(status);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for articles table
DROP TRIGGER IF EXISTS update_articles_updated_at ON articles;
CREATE TRIGGER update_articles_updated_at
    BEFORE UPDATE ON articles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Insert initial RSS feeds (verified working feeds)
INSERT INTO rss_feeds (source, category, feed_url, is_active) VALUES
    -- Detik
    ('detik', 'news', 'https://news.detik.com/rss', true),
    ('detik', 'finance', 'https://finance.detik.com/rss', true),
    ('detik', 'sport', 'https://sport.detik.com/rss', true),
    ('detik', 'inet', 'https://inet.detik.com/rss', true),
    ('detik', 'food', 'https://food.detik.com/rss', true),
    ('detik', 'oto', 'https://oto.detik.com/rss', true),
    ('detik', 'health', 'https://health.detik.com/rss', true),
    ('detik', 'travel', 'https://travel.detik.com/rss', true),
    
    -- Antara
    ('antara', 'terkini', 'https://www.antaranews.com/rss/terkini', true),
    ('antara', 'top-news', 'https://www.antaranews.com/rss/top-news', true),
    ('antara', 'politik', 'https://www.antaranews.com/rss/politik', true),
    ('antara', 'ekonomi', 'https://www.antaranews.com/rss/ekonomi', true),
    ('antara', 'olahraga', 'https://www.antaranews.com/rss/olahraga', true),
    ('antara', 'tekno', 'https://www.antaranews.com/rss/tekno', true),
    ('antara', 'otomotif', 'https://www.antaranews.com/rss/otomotif', true),
    
    -- Sindonews
    ('sindonews', 'main', 'https://www.sindonews.com/feed', true),
    ('sindonews', 'nasional', 'https://nasional.sindonews.com/rss', true),
    ('sindonews', 'metro', 'https://metro.sindonews.com/rss', true),
    ('sindonews', 'ekbis', 'https://ekbis.sindonews.com/rss', true),
    ('sindonews', 'sports', 'https://sports.sindonews.com/rss', true),
    ('sindonews', 'tekno', 'https://tekno.sindonews.com/rss', true),
    ('sindonews', 'otomotif', 'https://otomotif.sindonews.com/rss', true),
    
    -- Republika
    ('republika', 'main', 'https://www.republika.co.id/rss/', true),
    
    -- Tribunnews
    ('tribunnews', 'main', 'https://www.tribunnews.com/rss', true),
    
    -- Merdeka
    ('merdeka', 'main', 'https://www.merdeka.com/feed/', true),
    
    -- Suara
    ('suara', 'main', 'https://www.suara.com/rss', true)
ON CONFLICT (source, category) DO NOTHING;

-- Create view for recent articles with image count
CREATE OR REPLACE VIEW vw_recent_articles AS
SELECT 
    a.id,
    a.article_hash,
    a.title,
    a.url,
    a.source,
    a.category,
    a.published_date,
    a.summary,
    a.author,
    a.scraped_at,
    a.images,
    CASE WHEN a.images IS NOT NULL AND a.images != '' THEN 1 ELSE 0 END as image_count
FROM articles a
ORDER BY a.scraped_at DESC;

-- Create view for scraping statistics
CREATE OR REPLACE VIEW vw_scraping_stats AS
SELECT 
    source,
    COUNT(*) as total_articles,
    COUNT(DISTINCT category) as categories_count,
    MAX(scraped_at) as last_scraped,
    MIN(published_date) as oldest_article,
    MAX(published_date) as newest_article
FROM articles
GROUP BY source
ORDER BY total_articles DESC;

COMMENT ON TABLE articles IS 'Stores scraped news articles from various Indonesian news sources';
COMMENT ON TABLE rss_feeds IS 'Tracks RSS feed sources and their status';
COMMENT ON TABLE scraping_logs IS 'Logs scraping activities for monitoring';
COMMENT ON COLUMN articles.article_hash IS 'MD5 hash of URL to prevent duplicate articles';
COMMENT ON COLUMN articles.images IS 'Single featured image URL from RSS feed (enclosure or media_content)';
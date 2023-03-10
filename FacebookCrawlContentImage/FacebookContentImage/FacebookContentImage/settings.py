# Scrapy settings for FacebookContentImage project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'FacebookContentImage'

SPIDER_MODULES = ['FacebookContentImage.spiders']
NEWSPIDER_MODULE = 'FacebookContentImage.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'FacebookContentImage (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 32

# LOG LEVEL
LOG_LEVEL = 'INFO'

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'FacebookContentImage.middlewares.FacebookcontentimageSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # 'FacebookContentImage.middlewares.RandomProxyDownloaderMiddleware': 543,
    'FacebookContentImage.middlewares.TqdmDownloaderMiddleware': 544
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'FacebookContentImage.pipelines.FacebookcontentimagePipeline': 300,
   #  'FacebookContentImage.pipelines.FacebookcontentimagePipeline': 302,
    # 'FacebookContentImage.pipelines.FacebookMongoDB': 301,
   'FacebookContentImage.pipelines.VideoDownload': 303,
    # 'FacebookContentImage.pipelines.FacebookPhotoMongoDB': 304
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

DOWNLOAD_MAXSIZE = 1073741824
DOWNLOAD_WARNSIZE = 52428801
DOWNLOAD_TIMEOUT = 1800

# MONGODB SETTING
MONGO_HOST = "192.168.3.50"
MONGO_PORT = 27017
MONGO_USERNAME = "developer01"
MONGO_PASSWORD = "&lz3s3hf1#"
MONGO_SET_2 = "facebook_image_new_1"
MONGO_SET_OLD = "facebook_image"

# google storage bucket
IMAGES_STORE = 'gs://koolerbucket.adtechinno.com/kooler/'

FILES_STORE = 'gs://koolerbucket.adtechinno.com/kooler/'



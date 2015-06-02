# -*- coding: utf-8 -*-

# Scrapy settings for autohome_bbs_spider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'autohome_bbs_spider'

SPIDER_MODULES = ['autohome_bbs_spider.spiders']
NEWSPIDER_MODULE = 'autohome_bbs_spider.spiders'

ITEM_PIPELINES = {
    'autohome_bbs_spider.pipelines.AutohomeBbsSpiderPipeline': 1,
    'autohome_bbs_spider.pipelines.MySqlStorePipeline': 2,
}

# The maximum number of concurrent items (per response) to process in parallel in the Item Processor. Default: 100
CONCURRENT_ITEMS = 100

# The maximum number of concurrent requests that will be performed by the Scrapy downloader. Default: 16
CONCURRENT_REQUESTS = 16

# The maximun depth that will be allowed to crawl for any site. If zero, no limit will be imposed.
DEPTH_LIMIT = 0

# The amount of time (in secs) that the downloader should wait before downloading consecutive pages from the save website.
# This can be used to throttle the crawling speed to avoid hitting servers too hard. Decimal number are supported.
DOWNLOAD_DELAY = 0.05

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'autohome_bbs_spider (+http://www.yourdomain.com)'

#MYSQL_HOST = 'kennethhao.mysql.rds.aliyuncs.com'
#MYSQL_DBNAME = 'scrapy'
#MYSQL_USER = 'scrapy'
#MYSQL_PASSWD = 'scrapy'

MYSQL_HOST = '192.168.0.210'
MYSQL_DBNAME = 'haoyuewen'
MYSQL_USER = 'haoyuewen'
MYSQL_PASSWD = 'JUFL7Kl5NsPX'

# DELTA
DELTA_DAYS = 7

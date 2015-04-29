# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AutohomeBbsSpiderItem(scrapy.Item):

    title = scrapy.Field()
    content = scrapy.Field()
    pub_time = scrapy.Field()

    author = scrapy.Field()
    author_url = scrapy.Field()
    reg_time = scrapy.Field()
    addr = scrapy.Field()
    attent_vehicle = scrapy.Field()

    cdate = scrapy.Field()
    from_url = scrapy.Field()

    floor = scrapy.Field()
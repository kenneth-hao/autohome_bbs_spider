# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
from datetime import datetime
from twisted.enterprise import adbapi
from scrapy.conf import settings

from pymongo import MongoClient

import logging
logger = logging.getLogger()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class AutohomeBbsSpiderPipeline(object):

    keywords_maps = {
        '1': ['别克', 'Buick', 'buick', 'BUICK'],
        '2': ['GL8', '凯越', '英朗', '君威', '君越', '昂科威', '昂科拉'],
        '3': ['1.4T', '1.5T', '1.5L', '1.6T', '1.6L', '2.0T', '2.0L', '2.4L', '3.6L', '15N', '18T', '20T', '28T',
              '舒适型', '旗舰型', '领先型', '精英型', '豪华型', '进取型', '运动型', '尊享型', '行政版', '豪雅版', '经典版',
              '心动', '纠结', '想买', '入手', '订车', '颜色', '哪个好', '喜欢', '哪款', '优惠', '团购', '购车'],
        '4': ['怎么样', '对比', '便宜', '多少', '报价', '降价', 'gs', 'GS'],
    }

    def process_item(self, item, spider):

        for (level, keywords) in AutohomeBbsSpiderPipeline.keywords_maps.items():
            for keyword in keywords:
                if item['floor'] == '楼主' and keyword in item['title']:
                    item['key_level'] = int(level)
                    item['keyword'] = keyword

                    return item

                if keyword in item['content']:
                    item['key_level'] = int(level)
                    item['keyword'] = keyword

                    return item

        return None


class MongoDBPipeline(object):

    def __init__(self):
        uri = 'mongodb://%s:%s' % (settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        connection = MongoClient(uri)
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
        if item is not None:
            self.collection.insert(dict(item))
            logger.debug('Post added to MongoDB database!')

            return item

class MySqlStorePipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host = settings['MYSQL_HOST'],
            db = settings['MYSQL_DBNAME'],
            user = settings['MYSQL_USER'],
            passwd = settings['MYSQL_PASSWD'],
            charset = 'utf8',
            use_unicode = True
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)

        return cls(dbpool)

    def process_item(self, item, spider):
        d = self.dbpool.runInteraction(self._do_upsert, item, spider)
        d.addErrback(self._handler_error, item, spider)
        d.addBoth(lambda _: item)
        return d


    def _do_upsert(self, conn, item, spider):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if item is not None:
            conn.execute('''
                INSERT INTO autohome_bbs_content_0601 (title, content, pub_time, author, author_url, reg_time, addr, attent_vehicle, from_url, floor, target_url, cdate, key_level, keyword)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (item['title'],
                  item['content'],
                  item['pub_time'],
                  item['author'],
                  item['author_url'],
                  item['reg_time'],
                  item['addr'],
                  item['attent_vehicle'],
                  item['from_url'],
                  item['floor'],
                  item['target_url'],
                  now,
                  item['key_level'],
                  item['keyword']
            ))

    def _handler_error(self, failure, item, spider):
        logger.error(failure)


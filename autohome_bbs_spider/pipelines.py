# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
from datetime import datetime
from twisted.enterprise import adbapi

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class AutohomeBbsSpiderPipeline(object):

    keywords_maps = {
        1: ['君威', '君越', '昂科威', '昂科拉'],
        2: ['想买', '准备入手', '准备订车', '折', '什么颜色', '哪个好', '很喜欢', '选哪款', '对比'],
        3: ['还是', '怎么样'],
        4: ['GL8时尚型优惠45800合适吗', '全新英朗大家觉得多少优惠合适', '本人四十有余,选什么颜色合适', '1.5T昂科威两驱精英与2.4L两驱全新达智能的选择']
    }

    def process_item(self, item, spider):

        if '北京' not in item['addr']:
            return

        for (level, keywords) in AutohomeBbsSpiderPipeline.keywords_maps:
            for keyword in keywords:
                if keyword in item['content']:
                    item['key_level'] = level
                    item['keyword'] = keyword

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
        log.err(failure)


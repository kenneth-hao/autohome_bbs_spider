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

    words_to_filter_one = ['想买', '准备入手', '准备订车', '折', '什么颜色', '哪个好', '很喜欢', '选哪款', '对比']

    words_to_filter_two = ['还是', '怎么样']

    words_to_filter_three = ['ATSL舒适优惠45800合适吗', '广西科帕奇大家觉得多少优惠合适', '本人四十有余,选什么颜色合适', '1.5T昂科威两驱精英与2.4L两驱全新达智能的选择']

    def process_item(self, item, spider):

        if item['pub_time'] is not None and item['pub_time'] != '':
            pub_time_t = datetime.strptime(item['pub_time'], '%Y-%m-%d %H:%M:%S')
            now = datetime.now()
            if (now - pub_time_t).days > 7:
                item['week_diff'] = 2
            else:
                item['week_diff'] = 1
        else:
            item['week_diff'] = 0

        for word in self.words_to_filter_one:
            if word in item['content']:
                print item['content']
                item['key_level'] = 1
                item['keyword'] = word

                return item

        for word in self.words_to_filter_two:
            if word in item['content']:
                print item['content']
                item['key_level'] = 2
                item['keyword'] = word

                return item

        for word in self.words_to_filter_three:
            if word in item['content']:
                print item['content']
                item['key_level'] = 3
                item['keyword'] = word

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
                INSERT INTO autohome_bbs_content (title, content, pub_time, author, author_url, reg_time, addr, attent_vehicle, from_url, floor, cdate)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                  now))

    def _do_clear(self, conn):
        conn.execute('''
            DELETE FROM autohome_bbs_content
        ''')

    def _handler_error(self, failure, item, spider):
        log.err(failure)


# -*- coding: utf-8 -*-
from scrapy import Request
from scrapy import Spider
from scrapy import log
from scrapy.conf import settings

from autohome_bbs_spider.items import AutohomeBbsSpiderItem

import re
from datetime import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class WhatSpider(Spider):
    name = "what"
    allowed_domains = ["club.autohome.com.cn"]
    start_urls = (
        # 凯越
        'http://club.autohome.com.cn/bbs/forum-c-875-1.html',
        # 英朗
        'http://club.autohome.com.cn/bbs/forum-c-982-1.html',
        # GL8
        'http://club.autohome.com.cn/bbs/forum-c-166-1.html',
        # 昂科威
        'http://club.autohome.com.cn/bbs/forum-c-3554-1.html',
        # 昂科拉
        'http://club.autohome.com.cn/bbs/forum-c-2896-1.html',
        # 君越
        'http://club.autohome.com.cn/bbs/forum-c-834-1.html',
        # 君威
        'http://club.autohome.com.cn/bbs/forum-c-164-1.html',
        # 北京地区论坛
        #'http://club.autohome.com.cn/bbs/forum-a-100002-1.html',
        # 上海地区论坛
        #'http://club.autohome.com.cn/bbs/forum-a-100024-1.html',
        # 广州地区论坛
        #'http://club.autohome.com.cn/bbs/forum-a-100006-1.html',
        # 天津地区论坛
        #'http://club.autohome.com.cn/bbs/forum-a-100026-1.html',
    )

    # 正则, 用于替换页码为 {{page}}
    P = re.compile('(-)\d+(.html$)')

    # 帖子计算工具
    post_prop_utils = {}

    # 版块计算工具
    board_prop_utils = {}

    # 请求地址的域名
    DOMAIN = 'http://club.autohome.com.cn'

    # 希望获取最近 (N) 天的帖子内容中的 N
    DELTA_DAYS = settings["DELTA_DAYS"]-1

    # main 入口, 爬取第一页的数据, 并作为计算时间差内(如一周内)帖子列表的入口
    def parse(self, response):
        urls = []
        # 爬取当前页面中官方帖子的链接
        offic_urls = self.proc_offic_post_list(response)
        urls.extend(offic_urls)
        # 爬取当前页面中普通帖子的链接
        comm_urls = self.proc_comm_post_list(response)
        urls.extend(comm_urls)

        for u in urls:
            yield Request(u, callback=self.parse_post)

        # 获取总页数附近的字符串 { DOM: 共xxx页 }
        total_page_text = response.xpath('//div[@class="pagearea"]/span[@class="fr"]/text()').extract()[0]
        # 截取首尾字符串, 得到总页数
        total_page = int(total_page_text[1:][:-1])

        prop_util = CalcPropUtil(total_page=total_page, last_lt_page=1, last_mt_page=total_page)
        # 将本版块帖子列表的第一页, 作为 key,
        self.board_prop_utils[response.url] = prop_util

        if total_page > 1:
            next_url = self._get_repl_url(response.url, total_page/2+1)
            yield Request(next_url, callback=self.proc_comm_post_list_in_other_pages_and_date_range)


    def proc_offic_post_list(self, response):
        """爬取官方的帖子列表

        :param response: 响应页面
        :return: 下次请求
        """
        urls = []

        offic_post_list = response.xpath('//div[@class="area"]/dl[not(@style)][@class!="last_dl"]')
        for r in offic_post_list:
            lastest_reply_time = r.xpath('dd[last()]/span[@class="ttime"]/text()').extract()[0]
            delta_days = self._calc_delta_days(datetime.strptime(lastest_reply_time, "%Y-%m-%d %H:%M"))
            if delta_days <= WhatSpider.DELTA_DAYS:
                u = r.xpath('dt/a[1]/@href').extract()[0]
                urls.append(WhatSpider.DOMAIN + u)

        return urls

    def cb_proc_comm_post_list(self, response):
        """请求帖子列表页时的回调函数

        :param response: 响应页面
        :return: 下次请求
        """
        urls = self.proc_comm_post_list(response)
        for u in urls:
            yield Request(u, callback=self.parse_post)


    def proc_comm_post_list(self, response):
        """爬取置顶和普通的帖子列表

        :param response: 响应页面
        :return: 下次请求
        """
        urls = []
        comm_post_list = response.xpath('//div[@id="subcontent"]/dl[not(@class="list_dl bluebg")]')
        for r in comm_post_list:
            lastest_reply_time = r.xpath('dd[last()]/span[@class="ttime"]/text()').extract()[0]
            delta_days = self._calc_delta_days(datetime.strptime(lastest_reply_time, "%Y-%m-%d %H:%M"))
            if delta_days <= WhatSpider.DELTA_DAYS:
                u = r.xpath('dt/a[1]/@href').extract()[0]
                urls.append(WhatSpider.DOMAIN + u)
        return urls

    def proc_comm_post_list_in_other_pages_and_date_range(self, response):
        """获取时间区间范围内的其他分页的页面, 如果分页在时间范围内, 同时爬取页面内的帖子地址, 用于下次请求

        :param response: 响应页面
        :return: 下次请求
        """
        pagination = self._get_pagination(response.url)
        s_lastest_reply_time = response.xpath('//div[@id="subcontent"]/dl[2]/dd[last()]/span[@class="ttime"]/text()').extract()[0]
        delta_days = self._calc_delta_days(datetime.strptime(s_lastest_reply_time, "%Y-%m-%d %H:%M"))

        key = self._get_repl_url(response.url, 1)
        prop_util = self.board_prop_utils[key]

        if delta_days > WhatSpider.DELTA_DAYS:
            is_finish = pagination-prop_util.last_lt_page <= 1
            prop_util.last_mt_page = pagination
            if not is_finish:
                mid = ((pagination-prop_util.last_lt_page)/2) + prop_util.last_lt_page
            else:
                max_page = prop_util.last_lt_page
        else:
            urls = self.proc_comm_post_list(response)
            for u in urls:
                yield Request(u, callback=self.parse_post)

            is_finish = prop_util.last_mt_page-pagination <= 1
            prop_util.last_lt_page = pagination
            if not is_finish:
                mid = ((prop_util.last_mt_page-pagination)/2) + pagination
            else:
                max_page = prop_util.last_lt_page

        if is_finish:
            log.msg('[%s] 查询到的最大页码: [%d]' % (key, max_page), log.DEBUG)
            for p in range(2, max_page):
                next_url = self._get_repl_url(response.url, p)
                yield Request(next_url, callback=self.cb_proc_comm_post_list)
        else:
            next_url = self._get_repl_url(response.url, mid)
            yield Request(next_url, callback=self.proc_comm_post_list_in_other_pages_and_date_range)


    def parse_post(self, response):
        """爬取帖子

        :param response:
        :return:
        """
        items = self.proc_post_content(response)
        for i in items:
            yield i

        # 总页码
        s_total_page = response.xpath('//div[@class="pagearea"]//span[@class="fs"]/text()').extract()[0][3:][:-2]
        post_prop_util = CalcPropUtil(total_page=int(s_total_page), last_lt_page=1, last_mt_page=int(s_total_page))
        WhatSpider.post_prop_utils[response.url] = post_prop_util

        mid = post_prop_util.total_page/2+1
        yield Request(self._get_repl_url(response.url, mid), callback=self.proc_reply_in_other_pages_and_date_range)

    def proc_reply_in_other_pages_and_date_range(self, response):
        """获取时间区间范围内的其他分页的回复内容

        :param response:
        :return:
        """
        pagination = self._get_pagination(response.url)
        s_lastest_reply_time = response.xpath('//div[@class="rconten"][last()]//span[@xname="date"]/text()').extract()[0]
        delta_days = self._calc_delta_days(datetime.strptime(s_lastest_reply_time, "%Y-%m-%d %H:%M:%S"))

        key = self._get_repl_url(response.url, 1)
        prop_util = self.post_prop_utils[key]

        if delta_days > WhatSpider.DELTA_DAYS:
            is_finish = pagination-prop_util.last_lt_page <= 1
            prop_util.last_lt_page = pagination
            if not is_finish:
                mid = ((prop_util.last_mt_page-pagination)/2) + pagination
            else:
                min_page = prop_util.last_mt_page
        else:
            items = self.proc_post_content(response)
            for i in items:
                yield i

            is_finish = prop_util.last_mt_page-pagination <= 1
            prop_util.last_mt_page = pagination
            if not is_finish:
                mid = ((pagination-prop_util.last_lt_page)/2) + prop_util.last_lt_page
            else:
                min_page = prop_util.last_mt_page

        if is_finish:
            log.msg('[%s] 查询到的最小页码: [%d]' % (key, min_page), log.DEBUG)
            for p in range(min_page, prop_util.total_page+1):
                next_url = self._get_repl_url(response.url, p)
                yield Request(next_url, callback=self.cb_proc_post_content)
        else:
            next_url = self._get_repl_url(response.url, mid)
            yield Request(next_url, callback=self.proc_reply_in_other_pages_and_date_range)

    def cb_proc_post_content(self, response):
        """处理帖子内容的回调函数

        :param response:
        :return:
        """
        items = self.proc_post_content(response)
        for i in items:
            yield i

    def proc_post_content(self, response):
        """处理帖子内容

        :param response:
        :return: 爬取结果的集合
        """
        items = []

        p = self._get_pagination(response.url)

        # 帖子标题
        title = response.xpath('//div[@id="consnav"]/span[last()]/text()').extract()[0]
        # 如果是第一页, 爬取楼主发表的内容
        if p == 1:

            f0_box = response.xpath('//div[@id="F0"]')
            s_pub_time = f0_box.xpath('.//div[contains(@class, "rtopcon")]/span[@xname="date"]/text()').extract()[0]

            if self._is_in_time_range(datetime.strptime(s_pub_time, "%Y-%m-%d %H:%M:%S")):
                l_box = f0_box.xpath('.//div[contains(@class, "conleft")]')
                r_box = f0_box.xpath('.//div[contains(@class, "conright")]')

                tag_author = l_box.xpath('./ul[@class="maxw"]/li/a')
                # 名称
                author = tag_author.xpath('text()').extract()[0]
                # 个人主页
                author_home_page = tag_author.xpath('@href').extract()[0]

                tn_reg_time = l_box.xpath('./ul[@class="leftlist"]/li[5]/text()').extract()[0]
                # 注册时间
                s_reg_time = tn_reg_time[3:]
                # 所在地
                from_addr = l_box.xpath('./ul[@class="leftlist"]/li[6]/a/text()').extract()[0]
                # 关注车型
                nd_vehicle = l_box.xpath('./ul[@class="leftlist"]/li[7]/a/text()')
                vehicle = nd_vehicle.extract()[0] if nd_vehicle else ''

                tn_contents = r_box.xpath('.//div[@xname="content"]//text()').extract()

                content = ''
                for c in tn_contents:
                    if c.strip():
                        content = content + c + '\n'

                reply_item = AutohomeBbsSpiderItem()
                reply_item['title'] = title
                reply_item['content'] = content
                reply_item['pub_time'] = datetime.strptime(s_pub_time, "%Y-%m-%d %H:%M:%S")
                reply_item['author'] = author
                reply_item['author_url'] = author_home_page
                reply_item['reg_time'] = s_reg_time
                reply_item['addr'] = from_addr
                reply_item['attent_vehicle'] = vehicle
                reply_item['from_url'] = response.url
                reply_item['floor'] = '楼主'
                reply_item['target_url'] = response.url + '#0'

                items.append(reply_item)

        # 爬取回复内容
        reply_boxes = response.xpath('//div[@id="maxwrap-reply"]/div[re:test(@id, "F\d+")]')

        for reply_box in reply_boxes:
            r_box = reply_box.xpath('./div[contains(@class, "conright")]')
            s_pub_time = r_box.xpath('.//div[contains(@class, "rtopconnext")]/span[@xname="date"]/text()').extract()[0]

            if self._is_in_time_range(datetime.strptime(s_pub_time, "%Y-%m-%d %H:%M:%S")):
                l_box = reply_box.xpath('./div[contains(@class, "conleft")]')

                nd_label = l_box.xpath('.//div[@class="brand-left-info"]/text()')
                # 过滤官方认证账号的回复内容
                if nd_label and nd_label.extract()[0].strip() == u'官方认证账号':
                    continue

                tag_author = l_box.xpath('./ul[@class="maxw"]/li/a')
                # 名称
                author = tag_author.xpath('text()').extract()[0].strip()
                # 个人主页
                author_home_page = tag_author.xpath('@href').extract()[0]

                t = l_box.xpath('./ul[@class="leftlist"]/li[5]/text()').extract()
                tn_reg_time = l_box.xpath('./ul[@class="leftlist"]/li[5]/text()').extract()[0]
                # 注册时间
                s_reg_time = tn_reg_time[3:]
                # 所在地
                from_addr = l_box.xpath('./ul[@class="leftlist"]/li[6]/a/text()').extract()[0]
                # 关注车型
                nd_vehicle  = l_box.xpath('./ul[@class="leftlist"]/li[7]/a/text()')
                vehicle = nd_vehicle.extract()[0] if nd_vehicle else ''

                # 回复所在的楼层
                floor = r_box.xpath('.//div[@class="fr"]/a/text()').extract()[0]

                tn_contents = r_box.xpath('.//div[@xname="content"]//div[@class="yy_reply_cont"]//text()').extract()
                if len(tn_contents) == 0:
                    tn_contents = r_box.xpath('.//div[@xname="content"]//text()').extract()

                content = ''
                for c in tn_contents:
                    if c.strip():
                        content = content + c + '\n'

                reply_item = AutohomeBbsSpiderItem()
                reply_item['title'] = title
                reply_item['content'] = content
                reply_item['pub_time'] = datetime.strptime(s_pub_time, "%Y-%m-%d %H:%M:%S")
                reply_item['author'] = author
                reply_item['author_url'] = author_home_page
                reply_item['reg_time'] = s_reg_time
                reply_item['addr'] = from_addr
                reply_item['attent_vehicle'] = vehicle
                reply_item['from_url'] = response.url
                reply_item['floor'] = floor

                if floor == '沙发':
                    floor_num = 1
                elif floor == '板凳':
                    floor_num = 2
                elif floor == '地板':
                    floor_num = 3
                else:
                    floor_num = int(floor[:-1])
                reply_item['target_url'] = response.url + '#' + str(floor_num)

                items.append(reply_item)

        return items

    def _calc_delta_days(self, date):
        """计算之前的某一天到现在的间隔天数

        :param date: 之前某一天的日期
        :return: 间隔天数
        """
        now = datetime.now()
        delta = now-date
        return delta.days

    def _is_in_time_range(self, date):
        delta_days = self._calc_delta_days(date)
        return delta_days <= WhatSpider.DELTA_DAYS

    def _get_repl_url(self, url, pagination):
        """获取替换页码后的新 URL 地址

        :param url: 准备进行页码替换的 url 地址
        :param pagination: 页码
        :return: 替换页码后的 URL
        """
        repl_url = re.sub(WhatSpider.P, '\\1{{page}}\\2', url)
        return repl_url.replace('{{page}}', str(pagination))


    def _get_pagination(self, url):
        """根据 URL 获取当前页码

        :param url: 当前 URL 地址
        :return: 当前页码
        """
        p = re.compile('-(\d+).html$')
        s_curr_page = p.search(url).groups()[0]

        return int(s_curr_page)

class CalcPropUtil(object):

    def __init__(self, *args, **kw):
        self.total_page = kw['total_page']
        # 上一次爬取的较大的页码, 用于计算符合时间差的页码
        self.last_mt_page = kw['last_mt_page']
        # 上一次爬取的较小的页码, 用于计算符合时间差的页码
        self.last_lt_page = kw['last_lt_page']

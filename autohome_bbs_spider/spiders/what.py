# -*- coding: utf-8 -*-
from scrapy import Request
from scrapy import Spider
from scrapy import log
from scrapy.utils.project import get_project_settings

from autohome_bbs_spider.items import AutohomeBbsSpiderItem

import re
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class WhatSpider(Spider):
    name = "what"
    allowed_domains = ["club.autohome.com.cn"]
    start_urls = (
        # 昂科威
        # 'http://club.autohome.com.cn/bbs/forum-c-3554-1.html',
        # 昂科拉
        #'http://club.autohome.com.cn/bbs/forum-c-2896-1.html',
        # 君越
        #'http://club.autohome.com.cn/bbs/forum-c-834-1.html',
        # 君威
        #'http://club.autohome.com.cn/bbs/forum-c-164-1.html',
        # 北京地区论坛
        'http://club.autohome.com.cn/bbs/forum-a-100002-1.html',
    )
    # 上一次爬取的较大的页码, 用于计算符合时间差的页码
    last_mt_page = -1
    # 上一次爬取的较小的页码, 用于计算符合时间差的页码
    last_lt_page = -1
    # 正则, 用于替换页码为 {{page}}
    p = re.compile('(-)\d+(.html$)')
    # 总页数
    total_page = -1

    post_prop_utils = {}

    # 希望获取最近 (N) 天的帖子内容中的 N
    settings = get_project_settings()
    DELTA_DAYS = settings.get("DELTA_DAYS")

    # main 入口, 爬取第一页的数据, 并作为计算时间差内(如一周内)帖子列表的入口
    def parse(self, response):
        log.msg('>>> 爬取符合时间规定内的文章列表')
        # 获取总页数 { DOM: 共xxx页 }
        total_page_text = response.xpath('//div[@class="pagearea"]/span[@class="fr"]/text()').extract()[0]
        # 截取首尾字符串, 得到总页数
        s_total_page = total_page_text[1:][:-1]
        WhatSpider.total_page = int(s_total_page)

        log.msg('>>> 爬取到的文章总页数为: %s' % WhatSpider.total_page)

        # 当前页面的 URL
        curr_url = response.url

        # replace str to >> -{{page}}.html
        repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', curr_url)
        next_url = repl_url.replace('{{page}}', str(WhatSpider.total_page/2+1))

        WhatSpider.last_mt_page = WhatSpider.total_page
        WhatSpider.last_lt_page = 1
        yield Request(next_url, callback=self.parse_is_in_time_range)
        # 爬取本页内的文章列表
        self.parse_paged_list(response)

    # 计算时间差内(如一周内)帖子列表
    def parse_is_in_time_range(self, response):
        curr_url = response.url

        p = re.compile('-(\d+).html$')
        s_curr_page = p.search(curr_url).groups()[0]
        # 获取当前页码, 通过折半算法, 计算下次爬取的页码
        curr_page = int(s_curr_page)

        log.msg('>>> 当前爬取的 URL: %s, 页码: %s' % (curr_url, curr_page))
        # 获取本页帖子中, 最新一条回复的帖子的回复时间
        # 如果不是第一页
        if curr_page != 1:
            s_last_reply_time = response.xpath('//div[@class="carea"]//dl[@class="list_dl bluebg"][1]/following::dl[1]//span[@class="ttime"]/text()').extract()[0]
        # 如果是第一页
        else:
            s_last_reply_time = response.xpath('//div[@class="carea"]//dl[@class="list_dl bluebg"][2]/following::dl[1]//span[@class="ttime"]/text()').extract()[0]
        # 最新一条回复的帖子的回复时间
        last_reply_time = datetime.datetime.strptime(s_last_reply_time, '%Y-%m-%d %H:%M')
        now = datetime.datetime.now()
        # 时间差
        delta = now - last_reply_time
        log.msg('时间差: %s' % delta.days)



        # 用于替换页码的地址
        repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', curr_url)

        if delta.days > WhatSpider.DELTA_DAYS:
            WhatSpider.last_mt_page = curr_page

            mid = ((curr_page - WhatSpider.last_lt_page)/2)+WhatSpider.last_lt_page
            s_mid = str(mid)

            next_url = repl_url.replace('{{page}}', s_mid)

            yield Request(next_url, callback=self.parse_is_in_time_range)
        else:
            WhatSpider.last_lt_page = curr_page

            mid = ((WhatSpider.last_mt_page-curr_page)/2)+curr_page
            s_mid=str(mid)

            next_url = repl_url.replace('{{page}}', s_mid)

            yield Request(next_url, callback=self.parse_is_in_time_range)
            # 在时间区间内, 爬取本页内的文章列表
            self.parse_paged_list(response)

        max_request_page = -1
        if (WhatSpider.last_mt_page-WhatSpider.last_lt_page) == 1:
            max_request_page = WhatSpider.last_lt_page
        elif (WhatSpider.last_mt_page-WhatSpider.last_lt_page) == 0:
            max_request_page = WhatSpider.last_mt_page

        if max_request_page != -1:
            repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', curr_url)
            next_urls = [repl_url.replace('{{page}}', str(i)) for i in range(2, max_request_page+1)]

            for next_url in next_urls:
                yield Request(next_url, callback=self.parse_paged_list)


    def parse_paged_list(self, response):
        log.msg('>>> 爬取 [%s] 下所有的文章的标题和超链接' % response.url)

        resp_a = response.xpath('//div[@id="subcontent"]/dl[@lang]/dt/a')
        texts = resp_a.xpath('text()').extract()
        hrefs = resp_a.xpath('@href').extract()

        if len(texts) != len(hrefs):
            raise Exception('标题 和 对应的链接不匹配, 请排查文档结构是否有变动')

        for i in range(0, len(texts)):
            # like '/bbs/thread-c-166-5968706-1.html'
            href_sub = hrefs[i]
            hrefs[i] = 'http://club.autohome.com.cn' + href_sub
            print '>>> ', texts[i].strip(), ' <<< ', hrefs[i]

        for next_url in hrefs:
            yield Request(next_url, callback=self.parse_post_url_pages)

    def parse_post_url_pages(self, response):
        log.msg('>>> 开始爬取帖子 [%s] 下所有分页的链接地址' % response.url)
        # 总页码
        s_total_page = response.xpath('//div[@class="pagearea"]//span[@class="fs"]/text()').extract()[0][3:][:-2]
        post_prop_util = CalcPropUtil(total_page=int(s_total_page), last_lt_page=1, last_mt_page=int(s_total_page))
        WhatSpider.post_prop_utils[response.url] = post_prop_util

        repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', response.url)
        next_url = repl_url.replace('{{page}}', str(post_prop_util.total_page/2+1))
        yield Request(next_url, callback=self.parse_post_is_in_time_range)
        # 爬取本页的帖子内容

     # 计算时间差内(如一周内)帖子列表
    def parse_post_is_in_time_range(self, response):
        curr_url = response.url

        p = re.compile('-(\d+).html$')
        s_curr_page = p.search(curr_url).groups()[0]
        # 获取当前页码, 通过折半算法, 计算下次爬取的页码
        curr_page = int(s_curr_page)

        log.msg('>>> 当前爬取的 URL: %s, 页码: %s' % (curr_url, curr_page))
        # 获取本页帖子中, 最新一条回复的帖子的回复时间
        # 如果不是第一页
        if curr_page != 1:
            s_last_reply_time = response.xpath('//div[@class="plr26 rtopconnext"][1]//span[@xname="date"]/text()').extract()[0]
        # 如果是第一页
        else:
            s_last_reply_time = response.xpath('//div[@class="conright fr"]//span[@xname="date"]/text()').extract()[0]
        # 最新一条回复的帖子的回复时间
        last_reply_time = datetime.datetime.strptime(s_last_reply_time, '%Y-%m-%d %H:%M:%S')
        now = datetime.datetime.now()
        # 时间差
        delta = now - last_reply_time
        log.msg('时间差: %s' % delta.days)

        # 用于替换页码的地址
        repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', curr_url)
        post_prop_util_key = repl_url.replace("{{page}}", '1')

        post_prop_util = WhatSpider.post_prop_utils[post_prop_util_key]

        if delta.days > WhatSpider.DELTA_DAYS:
            post_prop_util.last_mt_page = curr_page

            mid = ((curr_page - post_prop_util.last_lt_page)/2)+post_prop_util.last_lt_page
            s_mid = str(mid)

            next_url = repl_url.replace('{{page}}', s_mid)

            yield Request(next_url, callback=self.parse_post_is_in_time_range)
        else:
            post_prop_util.last_lt_page = curr_page

            mid = ((post_prop_util.last_mt_page-curr_page)/2)+curr_page
            s_mid=str(mid)

            next_url = repl_url.replace('{{page}}', s_mid)

            yield Request(next_url, callback=self.parse_post_is_in_time_range)
            # 在时间区间内, 爬取本页内的帖子内容
            self.parse_post_content(response)

        max_request_page = -1
        if (post_prop_util.last_mt_page-post_prop_util.last_lt_page) == 1:
            max_request_page = post_prop_util.last_lt_page
        elif (post_prop_util.last_mt_page-post_prop_util.last_lt_page) == 0:
            max_request_page = post_prop_util.last_mt_page

        if max_request_page != -1:
            repl_url = re.sub(WhatSpider.p, '\\1{{page}}\\2', curr_url)
            next_urls = [repl_url.replace('{{page}}', str(i)) for i in range(2, max_request_page+1)]

            for next_url in next_urls:
                yield Request(next_url, callback=self.parse_post_content)

    def parse_post_content(self, response):
        log.msg('>>> 爬取帖子 [%s] 的内容' % response.url)

        # 如果当前爬取的页面是第一页, 则爬取楼主发表的内容
        if response.url.find('-1.html') == -1:
            pass
        else:
            # 采用 css 选择器的规则
            # 论坛主题内容 DOM 父元素
            maintopic_dom = response.css('div#cont_main div#maxwrap-maintopic')

            # 论坛文章的标题
            title_arr = maintopic_dom.css('div#consnav span:last-child::text').extract()
            title = title_arr[0] if title_arr else ''
            # 文章内容 DOM 父元素
            contstxt_dom = maintopic_dom.css('div.contstxt')

            # 文章发表时间
            pubtime_arr = contstxt_dom.css('div.conright div.rtopcon span[xname=date]::text').extract()
            s_pubtime = pubtime_arr[0] if pubtime_arr else ''

            if s_pubtime:
                deltaDays = self.calcDeltaDays(datetime.datetime.strptime(s_pubtime, '%Y-%m-%d %H:%M:%S'))
                if (deltaDays > WhatSpider.DELTA_DAYS):
                    return

            # 论坛文章的内容 (HTML 代码), , 采用 css 选择器的规则
            contents = contstxt_dom.css('div.conright div.rconten div.conttxt div.w740 *::text').extract()

            # 文章作者 和 个人主页
            author_a_dom = maintopic_dom.css('div.conleft ul.maxw li.txtcenter a.c01439a')
            author_arr = author_a_dom.css('::text').extract()
            author = author_arr[0] if author_arr else ''
            author_url_arr = author_a_dom.css('::attr(href)').extract()
            author_url = author_url_arr[0] if author_url_arr else ''

            # 作者注册时间
            reg_time_arr = maintopic_dom.css('div.conleft ul.leftlist li:nth-child(5)::text').extract()
            reg_time = reg_time_arr[0] if reg_time_arr else ''
            reg_time = reg_time[3:] if reg_time else ''
            # 作者所在地
            addr_arr = maintopic_dom.css('div.conleft ul.leftlist li:nth-child(6) a.c01439a::text').extract()
            addr = addr_arr[0] if addr_arr else ''
            # 作者关注车型
            attent_vehicle_arr = maintopic_dom.css('div.conleft ul.leftlist li:nth-child(7) a.c01439a::text').extract()
            attent_vehicle = attent_vehicle_arr[0] if attent_vehicle_arr else ''

            content = ''
            for c in contents:
                if c.strip():
                    content = content + c + '\n'

            topic_item = AutohomeBbsSpiderItem()
            topic_item['title'] = title
            topic_item['content'] = content
            topic_item['pub_time'] = s_pubtime
            topic_item['author'] = author
            topic_item['author_url'] = author_url
            topic_item['reg_time'] = reg_time
            topic_item['addr'] = addr
            topic_item['attent_vehicle'] = attent_vehicle
            topic_item['from_url'] = response.url
            topic_item['floor'] = '楼主'
            topic_item['target_url'] = response.url + '#0'

            yield topic_item

        ## 论坛文章回复的内容 ##
        reply_doms = response.css('div#cont_main div#maxwrap-reply div.contstxt')

        for reply_dom in reply_doms:

            reply_author_a_dom = reply_dom.css('div.conleft ul.maxw li.txtcenter a.c01439a')
            s_reply_pub_time = reply_dom.css('div.conright div.rtopconnext span[xname=date]::text').extract()[0]
            if s_reply_pub_time:
                deltaDays = self.calcDeltaDays(datetime.datetime.strptime(s_reply_pub_time, '%Y-%m-%d %H:%M:%S'))
                if (deltaDays > WhatSpider.DELTA_DAYS):
                    return

            reply_author = reply_author_a_dom.css('::text').extract()[0]

            reply_author_url = reply_author_a_dom.css('::attr(href)').extract()[0]

            reply_floor = reply_dom.css('div.conright div.rconten div.rtopconnext div.fr a.rightbutlz::text').extract()[0]

            # 作者注册时间
            reply_reg_time = reply_dom.css('div.conleft ul.leftlist li:nth-child(5)::text').extract()[0]
            reply_reg_time = reply_reg_time[3:] if reply_reg_time else ''

            # 作者所在地
            reply_addr = reply_dom.css('div.conleft ul.leftlist li:nth-child(6) a.c01439a::text').extract()[0]

            # 作者关注车型
            reply_attent_vehicle_arr = reply_dom.css('div.conleft ul.leftlist li:nth-child(7) a.c01439a::text').extract()
            reply_attent_vehicle = reply_attent_vehicle_arr[0] if reply_attent_vehicle_arr else ''

            reply_contents_dom = reply_dom.css('div.conright div.rconten div.x-reply div.w740')
            reply_contents = []
            if (reply_contents_dom.css('div.yy_reply_cont')):
                reply_contents = reply_contents_dom.css('div.yy_reply_cont *::text').extract()
            else:
                reply_contents = reply_contents_dom.css('*::text').extract()

            reply_content = ''
            for c in reply_contents:
                if c.strip():
                    reply_content = reply_content + c + '\n'

            reply_item = AutohomeBbsSpiderItem()
            reply_item['title'] = ''
            reply_item['content'] = reply_content
            reply_item['pub_time'] = s_reply_pub_time
            reply_item['author'] = reply_author
            reply_item['author_url'] = reply_author_url
            reply_item['reg_time'] = reply_reg_time
            reply_item['addr'] = reply_addr
            reply_item['attent_vehicle'] = reply_attent_vehicle
            reply_item['from_url'] = response.url
            reply_item['floor'] = reply_floor

            if reply_floor == '沙发':
                floor_num = 1
            elif reply_floor == '板凳':
                floor_num = 2
            elif reply_floor == '地板':
                floor_num = 3
            else:
                floor_num = int(reply_floor[:-1])
            reply_item['target_url'] = response.url + '#' + str(floor_num)

            yield reply_item

    def calcDeltaDays(self, date):
        now = datetime.datetime.now()
        dalta = now-date
        return dalta.days

class CalcPropUtil(object):

    def __init__(self, *args, **kw):
        self.total_page = kw['total_page']
        self.last_mt_page = kw['last_mt_page']
        self.last_lt_page = kw['last_lt_page']

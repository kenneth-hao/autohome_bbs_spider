# -*- coding: utf-8 -*-
from scrapy import Request
from scrapy import Spider
from autohome_bbs_spider.items import AutohomeBbsSpiderItem

import re
import time

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class WhatSpider(Spider):
    name = "what"
    allowed_domains = ["club.autohome.com.cn"]
    start_urls = (
        # 别克 GL8 论坛
        # 'http://club.autohome.com.cn/bbs/forum-c-166-1.html',
        # 北京地区论坛
        'http://club.autohome.com.cn/bbs/forum-a-100002-1.html',
    )

    def parse(self, response):
        print '>>> 开始爬取论坛下所有的文章列表'
        self._wait()
        # 获取总页数 { DOM: 共xxx页 }
        total_page_text = response.xpath('//div[@class="pagearea"]/span[@class="fr"]/text()').extract()[0]
        # 截取首尾字符串, 得到总页数
        total_page = total_page_text[1:][:-1]

        # 当前页面的 URL
        curr_url = response.url

        # replace str to >> -{{page}}.html
        p = re.compile('(-)\d+(.html$)')
        repl_url = re.sub(p, '\\1{{page}}\\2', curr_url)

        # 接下来要爬取的URLs
        next_urls = [repl_url.replace('{{page}}', str(i)) for i in range(2, int(total_page)+1)]

        for next_url in next_urls:
            print '>>> ', next_url
            # time.sleep(0.3)
            yield Request(next_url, callback=self.parse_paged_list)
        # 爬取本页内的
        self.parse_paged_list(response)


    def parse_paged_list(self, response):
        print '>>> 开始爬取论坛下所有的文章的标题和超链接'
        self._wait()

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
            # time.sleep(0.3)

        # next_urls = response.xpath('//div[@id="subcontent"]/dl[@lang]/dt/a/@href').extract()
        for next_url in hrefs:
            yield Request(next_url, callback=self.parse_post_url_pages)

    def parse_post_url_pages(self, response):
        print '>>> 开始爬取帖子下所有分页的链接地址'
        self._wait()

        # 总页码
        total_page = response.xpath('//div[@class="pagearea"]//span[@class="fs"]/text()').extract()[0][3:][:-2]

        print '>>> [post] Total Page <<< ', total_page

        # 当前页面的 URL
        curr_url = response.url

        # replace str to >> -{{page}}.html
        p = re.compile('(-)\d+(.html$)')
        repl_url = re.sub(p, '\\1{{page}}\\2', curr_url)

        # 接下来要爬取的URLs
        next_urls = [repl_url.replace('{{page}}', str(i)) for i in range(2, int(total_page)+1)]

        for next_url in next_urls:
            print '>>> [post] ', next_url
            # time.sleep(0.3)
            yield Request(next_url, callback=self.parse_post_url_pages)

        print '>>> 开始爬取帖子内容 <<< ', response.url
        self._wait()
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
            pubtime = pubtime_arr[0] if pubtime_arr else ''

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


            print '=============================== Gold Line ================================'
            print '文章标题 ==> ', title
            print '发表时间 ==>', pubtime
            print '作者 ==> ', author
            print '个人主页 ==> ', author_url
            print '注册时间 ==> ', reg_time
            print '所在地 ==> ', addr
            print '关注车型 ==> ', attent_vehicle
            print '文章主题内容 ==>'


            content = ''
            for c in contents:
                if c.strip():
                    print c
                    content = content + c + '\n'

            topic_item = AutohomeBbsSpiderItem()
            topic_item['title'] = title
            topic_item['content'] = content
            topic_item['pub_time'] = pubtime
            topic_item['author'] = author
            topic_item['author_url'] = author_url
            topic_item['reg_time'] = reg_time
            topic_item['addr'] = addr
            topic_item['attent_vehicle'] = attent_vehicle
            topic_item['from_url'] = response.url
            topic_item['floor'] = '楼主'

            yield topic_item

        ## 论坛文章回复的内容 ##
        reply_doms = response.css('div#cont_main div#maxwrap-reply div.contstxt')

        for reply_dom in reply_doms:


            reply_author_a_dom = reply_dom.css('div.conleft ul.maxw li.txtcenter a.c01439a')
            reply_pub_time = reply_dom.css('div.conright div.rtopconnext span[xname=date]::text').extract()[0]

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

            print '回复人 ==> ', reply_author
            print '发表时间 ==>', reply_pub_time
            print '回复人主页 ==> ', reply_author_url
            print '回复人注册时间 ==> ', reply_reg_time
            print '回复人所在地 ==> ', reply_addr
            print '回复人关注车型 ==> ', reply_attent_vehicle
            print '楼层 ==>', reply_floor
            print '回复内容 ==>'

            reply_content = ''
            for c in reply_contents:
                if c.strip():
                    print c
                    reply_content = reply_content + c + '\n'


            reply_item = AutohomeBbsSpiderItem()
            reply_item['title'] = ''
            reply_item['content'] = reply_content
            reply_item['pub_time'] = reply_pub_time
            reply_item['author'] = reply_author
            reply_item['author_url'] = reply_author_url
            reply_item['reg_time'] = reply_reg_time
            reply_item['addr'] = reply_addr
            reply_item['attent_vehicle'] = reply_attent_vehicle
            reply_item['from_url'] = response.url
            reply_item['floor'] = reply_floor

            yield reply_item


    def _wait(self):
        for i in range(0, 3):
            print '.' * (i%3+1)
            time.sleep(0.01)



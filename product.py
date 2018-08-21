#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/14/2018 7:05 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : crawler.py
# @Software: PyCharm Community Edition

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from urllib.parse import quote
from pyquery import PyQuery as pq
import logbook
from logbook import FileHandler, Logger, TimedRotatingFileHandler
import os
from pymongo import MongoClient
import redis
import threading
import uuid
import time



'''配置好logbook'''
def log_type(record,handler):
    log = "[{date}] [{level}] [{filename}] [{func_name}] [{lineno}] {msg}".format(
        date = record.time,                              # 日志时间
        level = record.level_name,                       # 日志等级
        filename = os.path.split(record.filename)[-1],   # 文件名
        func_name = record.func_name,                    # 函数名
        lineno = record.lineno,                          # 行号
        msg = record.message                             # 日志内容
    )
    return log
LOG_DIR = os.path.join("Log")
logbook.set_datetime_format("local")
#handler = FileHandler('app.log')
handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, '%s.log' % 'log'), date_format='%Y-%m-%d', bubble=True, encoding='utf-8')
handler.formatter = log_type
handler.push_application()
log = Logger('taobao')



'''配置好selenium 链接上Chrome'''
options = webdriver.ChromeOptions()
options.add_argument('--headless')
browser = webdriver.Chrome(executable_path=r'C:\Temp\jdk1.8.0\bin\chromedriver.exe', chrome_options=options)
wait = WebDriverWait(browser, 10)



class redisDB():
    '''
    waiting集合中是待读取的个人user-token
    pools列表是免费IP代理池
    topics列表中是待读取个人主题的user-token
    topic_success集合是成功读取个人主题的user-token
    '''

    def __init__(self):
        self.rdb = redis.StrictRedis(host='localhost', port=6379, db=0)

    def getRedis(self):
        return self.rdb


class mongoDB():
    def __init__(self):
        self.client = MongoClient(maxPoolSize=50, waitQueueMultiple=10, waitQueueTimeoutMS=100)
        self.db = self.client['Taobao']

    def getCollection(self, keyward):
        name = 'Taobao_{}'.format(keyward)
        collection = self.db[name]
        return collection


class findProducts(threading.Thread):
    def __init__(self, keyward):
        threading.Thread.__init__(self,)
        #self.collection = mongoDB().getCollection(keyward)
        self.rdb = redisDB().getRedis()
        self.keyward = keyward

    def index_page_old(self, page):
        '''page为页码'''
        url = 'https://s.taobao.com/search?q=' + quote(self.keyward)
        log.info('正在爬取 {} 页'.format(page))
        try:
            browser.get(url)
            if page > 1:
                input = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#mainsrp-pager div.form > input')))
                submit = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '#mainsrp-pager div.form > span.btn.J_Submit')))
                time.sleep(5)
                input.clear()
                time.sleep(5)
                input.send_keys(page)
                submit.click()
            wait.until(
                EC.text_to_be_present_in_element((By.CSS_SELECTOR, '#mainsrp-pager .items li.item.active > span'), str(page)))
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#mainsrp-itemlist .m-itemlist .items .item')))
            time.sleep(5)
            self.getProducts()
            time.sleep(5)
        except Exception as e:
            log.warn('Index_Page {0}-----{1}'.format(page, str(e)))

    def index_page(self, page):
        '''page为页码'''
        url = 'https://s.taobao.com/search?q={0}&bcoffset=3&ntoffset=3&p4ppushleft=1%2C48&s={1}'.format(quote(self.keyward), page*44)
        log.info('正在爬取 {} 页'.format(page + 1))
        print('正在爬取 {} 页'.format(page + 1))
        try:
            browser.get(url)
            #wait.until(
                #EC.text_to_be_present_in_element((By.CSS_SELECTOR, '#mainsrp-pager .items li.item.active > span'), str(page)))
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#mainsrp-itemlist .m-itemlist .items .item')))
            time.sleep(5)
            self.getProducts()
        except Exception as e:
            log.warn('Index_Page {0}-----{1}'.format(page + 1, str(e)))

    def getProducts(self):
        log.info('获取本页商品信息')
        html = browser.page_source
        doc = pq(html)
        items = doc('#mainsrp-itemlist .m-itemlist .items .item').items()
        for item in items:
            product = {
                'img': item.find('.pic .img').attr('data-src'),
                'cnt': item.find('.deal-cnt').text(),
                'title': item.find('.title').text(),
                'location': item.find('.location').text(),
                'dealer': item.find('.shop').text(),
                'price': item.find('.price').text(),
            }
            if product['cnt'] == '':
                product['cnt'] = '0人付款'
            self.insert2Redis(product)
            #print(product)

    def insert2Redis(self, dict):
        '''通过uuid1()确定生成一个唯一的key'''
        '''name在Redis中会建立一个同名的List，然后在List中存放的Hash中的Keys'''
        '''name参数在类对象创建时会初始化，如果不存在的话'''
        '''uuid.uuid1()的方法会根据时间值来生成一个唯一的数值'''
        name = self.keyward
        i = uuid.uuid1()
        key = '{0}_{1}'.format(name, i)
        '''将数据插入进hash表中'''
        try:
            if self.rdb.hmset(key, dict) == 1:
                log.info('商品数据插入进Redis--Hash成功')
            else:
                log.info('商品数据插入进Redis--Hash失败')
        except Exception as e:
            log.warn('findProducts-----insert2Redis-----insertHash{}'.format(str(e)))
        '''将hash表的key存入列表中'''
        try:
            if self.rdb.lpush(name, key) > 0:
                log.info('商品数据插入进Redis--List成功')
            else:
                log.info('商品数据插入进Redis--List失败')
        except Exception as e:
            log.warn('findProducts-----insert2Redis-----insertList{}'.format(str(e)))

    def startPage(self):
        for i in range(0, 109):
            self.index_page(i)

    def run(self):
        self.startPage()


class insert2DB(threading.Thread):
    def __init__(self, keyward):
        threading.Thread.__init__(self,)
        self.collection = mongoDB().getCollection(keyward)
        self.rdb = redisDB().getRedis()
        self.keyward = keyward
        self.exit = False

    def getFromList(self):
        name = ''
        count = 0
        self.exit = False
        while True:
            if self.rdb.llen(self.keyward) == 0:
                time.sleep(5)
                log.warn('{} List 在redis中数量为0'.format(self.keyward))
                count += 1
                if count == 10:
                    log.info('网页数据为空')
                    print('网页数据为空')
                    self.exit = True
                    return None
            else:
                break
        key = self.rdb.rpop(self.keyward)
        key = str(key, encoding='utf-8')  # 将字节类型的数据转化成字符串
        '''通过Key在Hash表中找到对应的数值'''
        item = self.rdb.hgetall(key)
        '''在找到key对应的数值之后将这个hash表的记录删除'''
        if self.rdb.hdel(key, 'price', 'cnt', 'dealer', 'title', 'img', 'location') > 0:
            log.info('{} 删除成功'.format(name))
        return item

    def transForm(self, dicts):
        keys = []
        values = []
        for key, value in dicts.items():
            key = str(key, encoding='utf-8')
            value = str(value, encoding='utf-8')
            keys.append(key)
            values.append(value)
        items =dict(zip(keys, values))
        return items

    def insert2MongoDB(self, dict):
        dict = self.transForm(dict)
        dealer = dict['dealer']
        title = dict['title']
        list = self.checkMongoDB(dealer, title)

        try:
            if list.count() == 0:
                self.collection.insert_one(dict)
                log.info('insert2DB-----insert2MongoDB-----成功')
            else:
                self.collection.update(
                    {'dealer': dealer, 'title': title}, {'$set': {'price': dict['price'], 'cnt': dict['cnt']}}, upsert=False, manipulate=True)
                log.info('{0}, {1}更新后价格为{2}, 数量为{3}'.format(title, dealer, dict['price'], dict['cnt']))
        except Exception as e:
            log.warn('数据插入mongoDB失败, {}'.format(str(e)))

    def checkMongoDB(self, key1, key2):
        list = self.collection.find({'dealer': key1, 'title': key2})
        return list

    def startDB(self):
        while True:
            if self.exit == False:
                item = self.getFromList()
                if item is not None:
                    self.insert2MongoDB(item)
            else:
                break

    def run(self):
        self.startDB()



if __name__ == '__main__':
    KEYWORD = 'iPhone'
    threads = []

    indexPagesThread = findProducts(KEYWORD)
    insert2DBThread = insert2DB(KEYWORD)


    indexPagesThread.setName('indesPages')
    insert2DBThread.setName('insert2DB')


    indexPagesThread.start()
    insert2DBThread.start()


    threads.append(indexPagesThread)
    threads.append(insert2DBThread)

    for t in threads:
        t.join()

    #browser.quit()
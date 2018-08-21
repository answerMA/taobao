#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/17/2018 7:14 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : comments.py
# @Software: PyCharm Community Edition

import requests
import logbook
from logbook import FileHandler, Logger, TimedRotatingFileHandler
import os
import re
import json
from pymongo import MongoClient
import redis
import threading
import uuid
import time
from elasticsearch import Elasticsearch



headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #"Proxy-Authorization": 'Basic ' + appKey,
    #'Authorization': 'oauth c3cef7c66a1843f8b3a9e6a1e3160e20',
    #'_xsrf':'xsrf',
    'referer': 'https://detail.tmall.com/item.htm?spm=a230r.1.14.1.4d404d1fAoHW8p&id=562099309982&cm_id=140105335569ed55e27b&abbucket=6&sku_properties=10004:709990523;5919063:6536025',

}


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
log = Logger('comments')

'''ElasticSearch 初始化'''
'''
def ESearch():
    mapping= {
        'properties': {
           'type': {
               'type': 'text',
               'analyzer': 'ik_max_word',
               'search_analyzer': 'ik_max_word',
               'fielddata': True
           },
            'content': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                'search_analyzer': 'ik_max_word',
                'fielddata': True
            },
            'append_content': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                'search_analyzer': 'ik_max_word',
                'fielddata': True
            },
        }
    }
    es = Elasticsearch()
    es.indices.create(index='taobao', ignore=[400, 401])
    es.indices.put_mapping(index='taobao', doc_type='iPhone', body=mapping)
    return es
'''

urls = [
    'https://rate.tmall.com/list_detail_rate.htm?itemId=562099309982&spuId=893336129&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvqQvbvnpvUvCkvvvvvjiPPsFZQjtnPLz9AjthPmPyQjYPP2FO1jEbnLSh0j18RphvCvvvphm5vpvhvvCCBvhCvvOv9hCvvvvEvpCW9cEgJCz%2BWLeA%2Bm%2BpFO7t%2B3vXw6ehefIz%2BLIiBde9R34zD2rUz4ZT%2BLpZbQfrFfmt%2B3CQo%2BexRdItn0vEYE7reCuKN6qvtEAfjX31o4yCvv9vvhhyjXES8OyCvvOCvhE20RmtvpvIvvCvpvvvvvvvvhZLvvvCKQvvBBWvvUhvvvCHhQvvv7QvvhZLvvvCfvGCvvpvvPMMRphvCvvvphmjvpvhvUCvpUhCvvswMmnNWnMwznQvpxI%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=558760911386&spuId=877095771&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvdvvbvnQvUpCkvvvvvjiPPsFZtj1ERLswsjrCPmPpAj1UPLz91ji8RLqZzjlHRIhCvCLwMvlNhnMwznsYzDS5IYQMz8144vhCvvXvppvvvvmtvpvIphvvvvvvphCvpvFDvvCmnZCvHHyvvhn2phvZ7pvvpiivpCBXvvCmeIyCvvXmp99hV1AivpvUphvhD4OohiyEvpCWp2nyv8WaJhDgLd2XrqpyCjCbFO7t%2BeCoJ9kx6fItb9gDNrBl5dUfb360kU0TKoybfvDrl8TJEctl88AUn1H%2BmB%2BdaNoAdcwuNZmxfpGCvvLMMQvvRphvChCvvvm5vpvhphvhHUwCvvBvppvvRphvChCvvvv%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=558550356564&spuId=878124235&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hv7pvPvp%2BvjQCkvvvvvjiPPsFZtj18Psz9ljthPmP9tjn2P2chtj3WPF5h6j0CvpvZ7DKyv8bw7Di4eBs5Mvr4pxd9z6krvpvEvU2AYGpvvC%2Fz3QhvCvmvphvjvpvEcHdNzY1MqwNbiQhvCvvvpZptvpvhvvCvpvGCvvLMMQvvvphvC9mvphvvv8yCvv9vvhhynXLjRUyCvvOCvhE20RmtvpvIvvCvpvvvvvvvvhZLvvvCKQvvBBWvvUhvvvCHhQvvv7QvvhZLvvvCfvyCvhQmjdkgjc7x%2Bu6Xwk%2FQiXTOjErgxneYr2UpVj%2BO3w0AhCyOJ9kx6fItn1vDN%2BCl53h%2BReERiNoAdBkKN6qv6EQXjX31B%2FLUHFKzrmphRphvCvvvphmrvpvEvU25gq9vvE9zRphvCvvvphv%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=545246502913&spuId=218950390&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvRpvpvp%2BvUvCkvvvvvjiPPsFZtj1bRLSvtjthPmPyQj1nRLLU6jrURLMvAjrURphvCvvvphmCvpvW7DSHvHsw7Di4%2BDLNdphvmpmv5tuAvvvIavwCvvDw7rMNzV2S1G%2BtvpvhvvCvpvyCvhAv1d0YjO97%2Bu6Xwos6D40OVTtKjwV1r2IZh7Eb%2BE7reCKKNB3rAEkKHsyDZtcEKfUZDVQEfwkOdiTAVAdOaBwlrqpKCIkUDC4AdcHjuphvmvvvpopl521ZkphvC9hvpyPOgbyCvm9vvhCvvvvvvvvvBBWvvvVqvvCHhQvv9pvvvhZLvvvCfvvvBBWvvvH%2BvphvC9mvphvvvvGCvvpvvPMMiQhvCvvvpZojvpvhvUCvp2yCvvpvvhCvCQhvmx14zYMwthMjAsyCvvpvvhCv&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=522157359219&spuId=382573494&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvFvvbvnQvUvCkvvvvvjiPPsFZtj1HRLsZtjD2PmPwsjnhn2cw0j1HP2FO1jEh9phvHHifBNYczHi47exjtMQb7Sx40rYU3QhvChCCvvvtvpvhphvvvvyCvh1v2AQvI19aUmx%2F1WmKD7EPOyTxfXkOjovDN%2BLvd3wspA1H64vBh7DHEcqhz8TJEct1pj7gRfU0ZsECD4mZHkx%2FwZHlYb8rwZOaWTeYkphvCyEmmvpfj8yCvv3vpvoxi3Z%2BdbyCvm3vpvvvvvCvphCvmRZvvhUlphvZ7pvvp6nvpCBXvvCmeyCvHHyvvh84vphvCyCCvvvvvvGCvvpvvPMMiQhvChCvCCptvpvhphvvvv%3D%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=567423250312&spuId=877095771&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvv9vbvnpvUvCkvvvvvjiPPsFZtj1HnLcvzjthPmPUzjibn2cUAjnbRFSptj3n9phv2Hif1NMzzHi47e5zzTwCvvpvvhHhvphvC9mvphvvvUyCvvOCvhE20RmivpvUvvCCE27%2BDP8tvpvIvvCvpvvvvvvvvhZLvvvCKQvvBBWvvUhvvvCHhQvvv7QvvhZLvvvCfvyCvhQCpbV1j70wdi7xfvc66CAOHF%2BSBiVvVE01%2B2n79WoOjLeAnhjEKBm65dUf8jcQ%2Bu6wdegmDfesRFoNw6LplC0OeE3sBb2XrqpAhjCb2QhvCvvvMMGtvpvhvvCvpUwCvvpv9hCvRphvCvvvphv%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=560745175443&spuId=878124235&sellerId=2616970884&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvwQvbvnQvUpCkvvvvvjiPPsFZtjn2nLcZAjrCPmPvgjt8RLqUQj38R25W6jEmnUhCvCLwMCnNVnMwznAwjDS5IYQRzvr44UwCvvpv9hCv2QhvCPMMvvvCvpvVvUCvpvvvKphv8vvvphvvvvvvvvCmRQvvvf%2BvvhZLvvmCvvvvBBWvvvH%2BvvCHhQvvv7AivpvUvvCCE27Q7omEvpvVvpCmpYF9mphvLvbaSdgag8TJ%2BulgE4AU%2BdWBHkx%2Fsj7J%2Bu6wjLPnQL46R3Bkp8oQ%2Bu0Od56OfwAKHseAnDPEsfUZDCODNKCl5d8reC6suf06n1pBOy%2BtvpvhvvCvp8wCvvpvvhHhRphvCvvvphv%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=558509313522&spuId=877095771&sellerId=1917047079&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvrQvWvRhvUvCkvvvvvjiPPsFZtjnmRFSvQjrCPmPZljl8RFLh1jt8n2FwAjnjRphvCvvvvvmjvpvhvvpvv2yCvvpvvvvvKphv8vvvvvCvpvvvvvmm86Cvmn6vvUUdphvWvvvv9krvpv3Fvvmm86CvmVRivpvUvvmv%2B969teJEvpvVmvvC9jXRmphvLvBpj9vjcnClYh6TRoDj5fkXAfJ0v0KHAp0YWdEI27zOa4p7%2B3%2B%2BaNoxfXKK4Qtr6j7QrEtsBrLZ%2BnezrmphQRAn3feAOHNIAXcBKFyCvpvVvvpvvhCv2QhvCvvvMM%2F5vpvhvvmv9u6Cvvyvvh0CCvpvzUoCvpvW7DrbYQbw7Di4DvfN&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=558420556696&spuId=878124235&sellerId=1917047079&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvSvvWvRyvUvCkvvvvvjiPPsFZtjnCR2Mp0jYHPmP9AjYjP25OAjYWPszw6jrRRphvCvvvvvmjvpvhvvpvv8wCvvpvvUmmRphvCvvvvvvPvpvhvv2MMQhCvvOvCvvvphvEvpCWCCSUvvay3w0x9EkwJ9kx6acEn1vDN%2BBld8Q7rjlUzWsy%2B2Kz8Z0vQRAn%2BbyDCwFvTWeARFxjKOmxfBuK46mQiNLUei1scYeYiXhpVbyCvm9vvvvvphvvvvvvvaYvpvFavvmm86Cv2vvvvUUdphvUOQvv9krvpv3Fuphvmvvv9bUnUtWekphvC99vvOC0Lu6CvvyvC2AC7DIvbh%2FCvpvZ7DS8Yg2w7Di4XuP5MvC4jDd4z69%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=560257961625&spuId=893336129&sellerId=1917047079&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvfvvWvnQvUvCkvvvvvjiPPsFZtjnvP2dwQj1VPmPWsjrEPsdhtjn8PFq90jinCQhvCli4zYMwNU9Pvpvhvv2MMQhCvvOvCvvvphvEvpCWhhPDvvw1TWex6fItb9TDYExre4tMoR2v1RkOej3v6b2XSfpAOH2%2BFOcn%2B3C1BKFEDaVTRogRD7zOaXgAb6sxrX7tR3wFZX7H6X7knuyCvv9vvUmA2oESZIyCvvOUvvVvayTtvpvIvvvvvhCvvvvvvUUdphvUzpvv9krvpvQvvvmm86CvmVWvvUUdphvUOsyCvvpvvvvviQhvCvvv9U8jvpvhvvpvv2yCvvpvvvvvdphvmpvhfpmYSpChup%3D%3D&needFold=0&_ksTS={1}&callback={2}',
    'https://rate.tmall.com/list_detail_rate.htm?itemId=538282868333&spuId=699374051&sellerId=1917047079&order=3&{0}&append=0&content=1&tagId=&posi=&picture=&ua=098%23E1hvlvvRvPQvUvCkvvvvvjiPPsFZtjnvnLLp0jYHPmPp6j3bn2qp6jiPPFSytjiURphvCvvvvvmCvpvZ7DK4Ygcw7Di4a4S5MvC4TDd4z69tvpvhvvvvv8wCvvpvvUmmRphvCvvvvvvPvpvhvv2MMQhCvvOvCvvvphmtvpvIvvvvvhCvvvvvvUUdphvUzpvv9krvpvQvvvmm86CvmVWvvUUdphvUOTyCvv9vvUmA2LQECUyCvvOUvvVvaygEvpCWvR2rvvak6acEKBm6NB3r1W1ljdUf8%2BBlKbVAnAaLINspgEQfJySvQRoQRqwiLO2vqU0QKoZH1WLIAfUTnZJt9ExreC%2BaUExr1nkKDOwCvvpvCvvvCQhvCli4zYMwzp%2FrvpvEvvBWvrd4vVvq&needFold=0&_ksTS={1}&callback={2}',

]


class redisDB():
    '''
    hash表中存放 uuid 和 评论的内容
    comments 列表中放入的是等待写入 mongoDB的评论内容的 uuid
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



class comments(threading.Thread):
    def __init__(self, keyward):
        threading.Thread.__init__(self)
        self.page = 0
        self.userAU = 0
        self.keyward = keyward
        self.rdb = redisDB().getRedis()
        #self.es = ESearch()

    def get_comment_page(self, current_url):
        self.page = 0
        max_page = 50
        self.userAU = 0
        while True:
            if self.page < max_page:

                times = str(time.time())
                timestamp = '{0}_{1}'.format(times[0], times[1])
                jsp = 'jsonp{0}'.format(str(int(times[1]) + 1))
                currentPage = 'currentPage={}'.format(str(self.page + 1))
                url = current_url.format(currentPage, timestamp, jsp)

                try:
                    status = requests.get(url=url, headers=headers, verify=False, allow_redirects=True, timeout=2)
                    if status.status_code == 401:
                        self.userAU = 1
                        log.error('评论列表进入401状态')
                        break
                    elif status.status_code != 200 and status.status_code != 401:
                        log.warn('网页返回码是：' + str(status.status_code) + '----------评论列表')
                        log.warn(status.content)
                        time.sleep(2)
                        continue
                    print(status.status_code)
                except Exception as e:
                    log.warn(str(e))
                    continue

                '''通过正则表达式 找出我们需要的 JSON 格式的数据'''
                response = re.findall(r'{.*}', status.text)[0]
                '''通过正则之后的数据，需要被 JSON 化，因为他不能使用 response.json() 函数来操作，所以需要额外的 json.loads() 来执行'''
                response = json.loads(response)
                '''获取rateDetail, 和 rateList'''
                rateDetail = response['rateDetail']
                rateList = rateDetail['rateList']
                '''从 rateDetail 里面获取 最后一页的页数'''
                pageInit = rateDetail['paginator']
                lastPage = pageInit['lastPage']

                for i in rateList:
                    rate = {
                        'type': i['auctionSku'],
                        'date': i['rateDate'],
                        'content': i['rateContent'],
                        'reply': i['reply'],
                        'sellerId': i['sellerId'],
                        'goldUser': i['goldUser'],
                        'postion': i['position'],
                        'userVIP': i['userVipLevel'],
                        'useful': i['useful'],
                    }
                    '''确认是否有追加评论，如果有，则加入追加评论内容'''
                    if i['appendComment'] is not None:
                        rate['append_content'] = i['appendComment']['content'],
                        rate['append_days'] = i['appendComment']['days'],
                        rate['append_reply'] = i['appendComment']['reply'],
                    else:
                        rate['append_content'] = None,
                        rate['append_days'] = None,
                        rate['append_reply'] = None,

                    if rate['reply'] == '':
                        rate['reply'] = None

                    self.insert2Redis(rate)
                    #print(rate)
                    #self.es.index('taobao', doc_type='iPhone', body=rate, ignore=400)
                #print(lastPage)

                self.page += 1
                max_page = lastPage
            else:
                print('当前商品的所有评论已被全部读取')
                break

    def insert2Redis(self, dict):
        '''通过uuid1()确定生成一个唯一的key'''
        '''name在Redis中会建立一个同名的List，然后在List中存放的Hash中的Keys'''
        '''name参数在类对象创建时会初始化，如果不存在的话'''
        '''uuid.uuid1()的方法会根据时间值来生成一个唯一的数值'''
        name = 'comments'
        i = uuid.uuid1()
        key = '{0}_{1}'.format(name, i)
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

    def get_pages(self):
        for i in urls:
            self.get_comment_page(i)
            if self.userAU == 1:
                log.error('评论列表401')
                break

    def run(self):
        self.get_pages()







if __name__ == '__main__':
    comment = comments()
    comment.get_comment_page()
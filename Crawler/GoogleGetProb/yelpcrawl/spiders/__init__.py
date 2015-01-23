# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import re
import hashlib
import json
import ast
import sys
import traceback
from scrapy.item import Item, Field
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from yelpcrawl.items import YelpcrawlItem
import logging
from scrapy.log import ScrapyFileLogObserver
from scrapy.xlib.pydispatch import dispatcher
from scrapy.signalmanager import SignalManager
import time
from scrapy import signals
import urllib2
from threading import Thread
from scrapy.exceptions import CloseSpider
import atexit
from inspect import Traceback

RESTAURANTS = ['sixteen-chicago']

class Yelp2aSpider(Spider):
    name = "zoogle"
    allowed_domains = ["yelp.com","google.com"]
    GOOGLE_URL="https://www.google.com/search?q="
    linksLeft={}
    usersAdded={}
    def genHash(self,a):               
        return a[0:2]
    
    def storeInHash(self,dict,a):
        hashkey=self.genHash(a)
        if hashkey in dict:
            dict[hashkey].append(a)
        else:
            dict[hashkey]=[a]
         
    def checkInHash(self,dict,a):
        hashkey=self.genHash(a)
        if hashkey in dict:
            if a in dict[hashkey]:
                return True
            else:
                return False
        else:
            return False 
        
    def storeInHashData(self,dict,key,val):
        
        self.linksLeft[key] = val
         
    def deleteFromHashData(self,dict,key,val):
        
        if key in dict:
            del dict[key]
           
             
    def __init__(self, *args, **kwargs):
      
      logfile = open('testlog.log', 'a')
      log_observer = ScrapyFileLogObserver(logfile, level=logging.DEBUG)
      log_observer.start()
      SignalManager(dispatcher.Any).connect(
            self.spider_closed, signal=signals.spider_closed)
      atexit.register(self.spider_closed,self)
   
    def start_requests(self):
      #If userPage is called,then register parse_userpage
      pages=[]
      try:
          lines = [line.strip() for line in open('not_recommended_reviewer_data.txt')]
          for review in lines:
              data = ast.literal_eval(review)
              self.start_urls= [self.GOOGLE_URL+data['UserName']+" "+data['UserLocation']+" "+"yelp user_details"+" "+data['friendCount']+" friends"+" "+data['reviewCount']+" reviews"]
              obj= Request(self.start_urls[0],callback=self.parse_google)
              links_left_hash = str(data['restaurantId'])+str(data['reviewerImageId'])
              self.storeInHashData(self.linksLeft, links_left_hash,data)
              obj.meta['data']=data
              pages.append(obj)
      except:
          print traceback.print_exc()


      return pages 
    
    '''
    Checks in google for possible links
    '''
    def parse_google(self, response):
        sel = Selector(response)
        data=response.meta['data']
        key = str(data['restaurantId'])+str(data['reviewerImageId'])
        self.deleteFromHashData(self.linksLeft, key, data)
        links=sel.xpath('//li[@class="g"]')
      
        for obj in links:
          #title=obj.xpath('.//h3[@class="r"]/a/text()').extract()
          div=obj.xpath('.//div[@class="s"]')
         # textLink=div.xpath('.//b/text()').extract()
          textLink=" ".join(div.xpath('.//b/text()|.//em/text()').extract())
          link=div.xpath('.//cite/text()').extract()
          try:
              if 'yelp' in textLink and 'user_details' in textLink:
                name=data['UserName']
                firstName=name.split(' ')[0]
                if firstName in textLink:
                  '''
                  Add this yelp link to queue
                  '''
                  partOfLink=link[2]
                  reviewerId=partOfLink.rsplit('=',1)[1]
                  if(len(reviewerId)!=22):
                    print 'Length not appropirate:-'+reviewerId
                    continue
                  data['reviewerId']=reviewerId
                  data['yelpLink']='http://www.yelp.com/user_details?userid='+reviewerId
                  userReview={'UserName': data['UserName'], 'yelpLink': data['yelpLink'],'UserLocation':data['UserLocation'],'restaurantId':data['restaurantId'],'reviewerId':str(data['reviewerId']),'friendCount':data['friendCount'], 'rating':data['rating'], 'reviewerImageId': data['reviewerImageId'] , 'reviewDate': data['reviewDate'], 'reviewText' :data['reviewText'], 'reviewCount': data['reviewCount']}
                  
                        #write crawled review to file
                  file_error=open("not_recommended_google_filtered.txt","a");
                  file_error.write(str(userReview)+"\n");
                  file_error.close()
          except:
              pass        
          #details=obj.xpath('.//span[@class="st"]/em/text()').extract()
          
       
    
         
       
    def spider_closed(self, spider):
        print 'End of days'
        f = open('not_recommended_reviewer_data.txt', 'w')
        for u in self.linksLeft:
          x = self.linksLeft[u]
          f.write(str(x)+str('\n'))
        f.close()
                #print spider.linksLeft
    
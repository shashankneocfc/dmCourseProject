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
    name = "toughie"
    allowed_domains = ["yelp.com","google.com"]
    GOOGLE_URL="https://www.google.com/search?q="
    linksLeft={}
    linksLeft_google_filtered={}
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
        
        dict[key] = val
         
    def deleteFromHashData(self,dict,key,val):
        
        if key in dict:
            del dict[key]
                
    def parse_listing(self, request):
      if 'google' in request.url:
           self.download_delay = 15
      else:
          self.download_delay = 0.25
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
          lines = [line.strip() for line in open('not_recommended_google_filtered.txt')]
          for review in lines:
              data = ast.literal_eval(review)
              self.start_urls= [str(data['yelpLink'])]
              obj= Request(self.start_urls[0],callback=self.parseYelpUserProfile)
              
              links_left_hash = str(data['restaurantId'])+str(data['reviewerId'])
              self.storeInHashData(self.linksLeft_google_filtered, links_left_hash,data)  
              obj.meta['data']=data
              pages.append(obj)
      except:
          print traceback.print_exc()


      return pages 

    def parseYelpUserProfile(self, response):
        sel = Selector(response)
        data=response.meta['data']
        tempreviewerImageId= sel.xpath('//div[@id="user-main-photo"]//img[@class="photo-box-img"]/@src').extract()[0]
        userStats= sel.xpath('//ul[@id="user_stats"]//li//a/text()').extract()
        friends=0
        reviewCount=0
        
        key = str(data['restaurantId'])+str(data['reviewerId'])
        self.deleteFromHashData(self.linksLeft_google_filtered, key, data)
       
        imageIds =  str(tempreviewerImageId)
        for stats in userStats:
            if 'Friend' in stats:
                friends=int(re.findall(r'\d+', str(stats))[0])
            if 'Review' in stats:
                reviewCount=int(re.findall(r'\d+', str(stats))[0])
        actualReviewerImageId=str(data['reviewerImageId'])
          
              
        possibleReviewerImageId=imageIds.rsplit('/',2)[1]
        possibleReviewerImageId = str(possibleReviewerImageId)
        try:          
           # print"poss="+possibleReviewerImageId+" actual="+actualReviewerImageId+" len poss="+str(len(possibleReviewerImageId))+"act len poss="+str(len(actualReviewerImageId))
          
            if possibleReviewerImageId != actualReviewerImageId:
                pass      
            else:
                if friends!=0:
                    if friends!=int(data['friendCount']):
                        return
                if reviewCount!=0:
                    if reviewCount!=int(data['reviewCount']):
                        return 
                if(self.checkInHash(self.usersAdded,data['reviewerId']+data['restaurantId'])==True):
                        print 'Duplicate printing for user='+data['UserName']+" restaurant="+data['restaurantId']
                        return     
                userReview={'UserName': data['UserName'],'UserLocation':data['UserLocation'],'restaurantId':data['restaurantId'],'reviewerId':str(data['reviewerId']),'friendCount':data['friendCount'], 'rating':data['rating'], 'reviewerImageId': data['reviewerImageId'] , 'reviewDate': data['reviewDate'], 'reviewText' :data['reviewText'], 'reviewCount': data['reviewCount']}
                
                user_url = "http://www.yelp.com/user_details?userid="+str(data['reviewerId'])
                self.linksLeft[user_url] = "not_recom_user"        
                        #write crawled review to file
                file_error=open("not_recommended_review_filtered.txt","a");
                file_error.write(str(userReview)+"\n");
                file_error.close()
                self.storeInHash(self.usersAdded,data['reviewerId']+data['restaurantId'])       
            
        except:
            print traceback.print_exc()    
         
       
    def spider_closed(self, spider):
        print 'End of days'
        f = open('not_recommended_google_filtered.txt', 'w')
        for u in self.linksLeft_google_filtered:
          x = self.linksLeft_google_filtered[u]
          f.write(str(x)+str('\n'))
        f.close()
        
        with open('not_recom_user_links_left.json', 'a') as outfile:
            json.dump(self.linksLeft, outfile, sort_keys = True, indent = 4, ensure_ascii=False)
        #print spider.linksLeft
    
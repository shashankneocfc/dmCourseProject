# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import re
import hashlib
import json
import ast
import sys
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
import traceback
RESTAURANTS = ['sixteen-chicago']

class Yelp2aSpider(Spider):
    name = "yelp"
    allowed_domains = ["yelp.com"]
    crawledUsers={}
    crawledRestaurants={}
    linksLeft={}
    seedRestaurants=[]
    countRecReviews=0
    countNotRecReviews=0
    countLinksVisited=0
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
    def __init__(self, *args, **kwargs):
     
      logfile = open('testlog.log', 'a')
      log_observer = ScrapyFileLogObserver(logfile, level=logging.DEBUG)
      log_observer.start()
      SignalManager(dispatcher.Any).connect(
            self.spider_closed, signal=signals.spider_closed)
      atexit.register(self.spider_closed,self)
   
    def start_requests(self):
      pages=[]               
                #links left
      with open("links_left.json") as json_file:
        try:
          print 'In links left'  
          json_data = str(json.load(json_file))
          if json_data=='':
            raise Exception("Links left empty")
          json_data = str(json_data)
          self.linksLeft = ast.literal_eval(json_data)
          for link in self.linksLeft:
            if self.linksLeft[link] == 'user':
              obj=Request(link,callback=self.parseUserMainPage)
              userId = str(link).rsplit('=',1)
              userId = userId[1]
              print 'Links left Added user page='+userId
              obj.meta['userId']=userId
            elif self.linksLeft[link] == 'user_review':
              obj=Request(link,callback=self.parseuserpage)
            elif self.linksLeft[link] == 'restaurant':
              obj=Request(link,callback=self.parseRestaurantMainPage)
            elif self.linksLeft[link] == 'recom_review':
              obj=Request(link,callback=self.parse_review)
              restId = str(link).rsplit('/',1)
              restId = restId[1]
              restId = str(restId).rsplit('?',1)
              obj.meta['restId']=restId[0]
              print 'Links left Added rest page='+restId[0]
            elif self.linksLeft[link] == 'not_recom_review':
              obj=Request(link,callback=self.parse_NotRecommendedReview)
              restId = str(link).rsplit('/',1)
              restId = restId[1]
              restId = str(restId).rsplit('?',1)
              obj.meta['restId']=restId[0]
              print 'Links left Added rest page='+restId[0]
            pages.append(obj)
           #read crawled users
          try:
              print 'Initializing crawled users'
              lines = [line.strip() for line in open('crawled_users.txt')]
              for userid in lines:
                  self.storeInHash(self.crawledUsers, userid)
          except:
              print 'Error initializing crawled users'
              traceback.print_exc()
          #read crawled rests
          try:
              print 'Initializing crawled restaurants'
              lines = [line.strip() for line in open('crawled_restaurants.txt')]
              for restid in lines:
                  self.storeInHash(self.crawledRestaurants, restid)
          except:
              print 'Error initializing crawled restaurants'
              traceback.print_exc() 
          #read not-recommended user links from not_recommended_user_links
          with open("not_recommended_user_links.json") as json_file:
            try:
              print 'In not-recommended user links' 
              json_data = str(json.load(json_file))
              json_data = str(json_data)
              not_recommended_users_links = ast.literal_eval(json_data)
              for link in not_recommended_users_links:
                if not_recommended_users_links[link] == 'not_recom_user':
                  obj=Request(link,callback=self.parseUserMainPage)
                  userId = str(link).rsplit('=',1)
                  userId = userId[1][:-2]
                  if userId in self.crawledUsers:
                    continue
                  else:
                    print 'UserId already crawled by scrapy:'+userId
                  obj.meta['userId']=userId
                  pages.append(obj)
                  self.linksLeft[link]='user'
                  print 'Not recommended user link added for userId='+userId
            except:
              print 'Error initializing not-recommended user links' 
              traceback.print_exc()
        except:
               print 'Picking seed restaurants'
               with open("restaurants.json") as json_file:
                  try:
                     json_data = str(json.load(json_file))
                     r = ast.literal_eval(json_data)
                     for item in r:
                        self.seedRestaurants.append(item['id'])
                     for s in self.seedRestaurants:
                        link= 'http://www.yelp.com/biz/'+s
                        obj=Request(link,callback=self.parseRestaurantMainPage)
                        self.linksLeft[link] = 'restaurant'
                        pages.append(obj)
                  except ValueError:
                     print 'Error during initial seeding of Restaurants'
                     traceback.print_exc()
        return pages                              
      
      
       
    def createRestaurantRecommendedPageLinks(self, response, restId):
      reviewsPerPage = 40
      pages=[]
      sel = Selector(response)
      '''
         Get total number of recommended reviews on this page
     '''
      totalRecommendedReviews = int(sel.xpath('//div[@class="rating-info clearfix"]//span[@itemprop="reviewCount"]/text()').extract()[0].strip().split(' ')[0])
      '''
     Generates the various review pages from start=? of various restaurants
     '''
      print 'totalRecommendedReviews for'+restId+'= '+str(totalRecommendedReviews)  
      for n in range(totalRecommendedReviews/reviewsPerPage):
        obj=Request(url=response.url + '?start=' + str(reviewsPerPage*(n+1)), callback=self.parse_review)
        obj.meta['restId']=restId
        pages.append(obj)
        self.linksLeft[obj.url]="recom_review"
        return pages    
  
    def createRestaurantNotRecommendedPageLinks(self, response, restId):
      reviewsPerPage = 10
      sel = Selector(response)
      pageUrl=response.url
      productName=pageUrl.rsplit('/',1)
      notRecommendedUrl='http://www.yelp.com/not_recommended_reviews/'+productName[1]
      '''
         Get total number of not recommended reviews on this page
     '''
      anchor= sel.xpath('//div[@class="not-recommended ysection"]/a/text()').extract()
      totalNotRecommendedReviews=int(re.findall(r'\d+', str(anchor))[0])

      
      '''
     Generates the various not recommended review pages from start=? of various restaurants
     '''
      print 'totalRecommendedReviews for'+restId+'= '+str(totalNotRecommendedReviews) 
      pages = []
      for n in range(totalNotRecommendedReviews/reviewsPerPage):
        obj=Request(url=notRecommendedUrl + '?not_recommended_start=' + str(reviewsPerPage*(n)), callback=self.parse_NotRecommendedReview)
        obj.meta['restId']=restId    
        self.linksLeft[obj.url]="not_recom_review"
        pages.append(obj)
      return pages
    '''
    Generates different reviews pages from user profile, so that it can be further parsed to generate restaurants
    '''
    def createReviewerPageLinks(self,response):
      pages=[]
      sel = Selector(response)
      div=sel.xpath('//div[@id="about_user_column"]')
      totalReviewsByUser=0
      reviewsPerPage = 10
      if div:
        totalReviewsByUser=int(div.xpath('.//a[contains(@href, "user_details_reviews_self")]/text()').extract()[0].strip().split(' ')[0])
      userId=response.url.rsplit('=',1)
      print 'total reviews by User'+userId[1]+'= '+str(totalReviewsByUser)
      for n in range(totalReviewsByUser/reviewsPerPage):
        
        userReviewUrl='http://www.yelp.com/user_details_reviews_self?userid='+userId[1]+'&review_sort=time&review_filter=category&category_filter=restaurants&rec_pagestart='+str(reviewsPerPage*(n))
        self.linksLeft[userReviewUrl]='user_review'
        pages.append(Request(url=userReviewUrl, callback=self.parse_userpage)) 
      return pages
    
    '''
    Function for generating all further pages from main restaurant
    
    '''
    def parseRestaurantMainPage(self, response):
        '''
        Parse the Restaurant main page reviews first, then head to generating other recommended/not
        recommended reviews
        '''
        #Extracting restaurant id       
        id = str(response.url).rsplit('/',1)
        response.meta['restId']=id[1]
        pages = []
        try:
          print 'parseRestaurantMainPage: '+str(id[1])  
          pages=self.parse_review(response)
         # print 'parseRestaurantMainPage After parsereview: '+str(id[1])
          pages.extend(self.createRestaurantRecommendedPageLinks(response,id[1]))
         # print 'parseRestaurantMainPage After createRestaurantRecommendedPageLinks: '+str(id[1])
          pages.extend(self.createRestaurantNotRecommendedPageLinks(response,id[1]))
         # print 'parseRestaurantMainPage After createRestaurantNotRecommendedPageLinks: '+str(id[1])
        except :
          print 'Error in parseRestaurantMainPage'+str(id[1]) 
          print traceback.print_exc() 
          pass
        self.storeInHash(self.crawledRestaurants,id[1])
        return pages
    '''
      Return different pages in users for finding restaurants name
    '''
    def  parseUserMainPage(self, response):
      pages=[]
      userId=response.meta['userId']
      pages.extend(self.createReviewerPageLinks(response))
      try:
          del self.linksLeft[str(response.url)]
          self.storeInHash(self.crawledUsers,userId)
      except:
          pass
      return pages
    '''
    Along with parsing of various reviews, this function also generates page of user profile page
    for further scraping
    '''
    def parse_review(self, response, ):
        sel = Selector(response)
        reviews = sel.xpath('//div[@class="review review--with-sidebar"]')
        listReviewerId=[]
        userWebPages=[]
        restId=response.meta['restId']
        print 'In parse review: '+str(restId)
       # print str(reviews)
        for review in reviews:
            
            restaurantId = restId;
            venueName = str(sel.xpath('//meta[@property="og:title"]/@content').extract()[0])
            reviewer = str(review.xpath('.//li[@class="user-name"]/a/text()').extract()[0])
            reviewerLoc = str(review.xpath('.//li[@class="user-location"]/b/text()').extract()[0])
            rating = str(review.xpath('.//meta[@itemprop="ratingValue"]/@content').extract()[0])
            reviewDate =str( review.xpath('.//meta[@itemprop="datePublished"]/@content').extract()[0])
            reviewsText = review.xpath('.//p[@itemprop="description"]/text()').extract()[0].encode('utf-8')
            reviewerUrl= str(review.xpath('.//li[@class="user-name"]//a/@href').extract()[0])
            '''
            Write regex for generating userId from reviewerUrl
            '''
            #Parsing user-id
            tempreviewerid = str(reviewerUrl).rsplit('=',1)
            reviewerId = tempreviewerid[1]
            
            
            #cleaning review text
            reviewText=''
            for i in str(reviewsText):
                if  i in 'qwertyuiopasdfghjklzxcvbnmABCDEFGHIJKLMNOPQRSTUVWXYZ. ':
                    reviewText=reviewText+i
                
            reviewText = str(reviewText).replace("u'","'")
            reviewText = reviewText.replace("u\"","\"")
            reviewText = reviewText[1:]
            userReview={'restaurantId': restaurantId,'venueName':venueName,'reviewer':reviewer,'reviewerLoc':reviewerLoc,'rating':rating, 'reviewDate':reviewDate,'reviewerId': reviewerId, 'reviewText': reviewText }
           # userReview = str(userReview).replace("u","")
            
            userReview = str(userReview).replace("[","")
            userReview = userReview.replace("]","")
            
            #write crawled review to file
            file_error=open("reviewer_data.txt","a");
            file_error.write(str(userReview)+"\n");
            file_error.close()
            self.countRecReviews=self.countRecReviews+1
            print "Written review to file for restaurant="+restaurantId+" reviewer="+reviewer
            #Only add this usermainpage for further crawling if it hasn't already been crawled, and add their links to links left 
            if(self.checkInHash(self.crawledUsers,reviewerId)==False):
                listReviewerId.append(reviewerId)
                reviewerUrl = str(reviewerUrl).replace("[u'", "")
                reviewerUrl = str(reviewerUrl).replace("']", "")
                str_reviewerUrl = 'http://yelp.com'+str(reviewerUrl)
                self.linksLeft[str_reviewerUrl]='user'
            else:
              print 'User= '+reviewer+" already crawled by scrapy"
        


        for userId in listReviewerId:        
          obj=Request('http://www.yelp.com/user_details?userid='+userId,callback=self.parseUserMainPage)
          obj.meta['userId'] = userId
          userWebPages.append(obj)
          
        #Add to crawled User
        for ids in listReviewerId:
          self.storeInHash(self.crawledUsers,ids)
        
        #Remove response.url from links-left file
        try:
            del self.linksLeft[str(response.url)]
        except:
            traceback.print_exc()
               
        return userWebPages
    
    def parse_NotRecommendedReview(self, response):
        sel = Selector(response)
        reviews = sel.xpath('//div[@class="review review--with-sidebar"]')
        restaurantId=response.meta['restId']
        for review in reviews:
            userName = str(review.xpath('.//li/span[@class="user-display-name"]/text()').extract()[0])
            userLocation = str(review.xpath('.//li[@class="user-location"]/b/text()').extract()[0])
            reviewDate = str(review.xpath('.//span[@class="rating-qualifier"]/text()').extract()[0])
            friendCount = str(review.xpath('.//li[@class="friend-count"]/span/b/text()').extract()[0])
            reviewCount = str(review.xpath('.//li[@class="review-count"]/span/b/text()').extract()[0])
            rating = str(review.xpath('.//div[@class="rating-very-large"]/i/@title').extract()[0])
            tempReviewText = review.xpath('.//div[@class="review-content"]/p/text()').extract()[0].encode('utf-8')
            tempreviewerImageId= str(review.xpath('.//img[@class="photo-box-img"]/@src').extract()[0])
            reviewerImageId=tempreviewerImageId.rsplit('/',2)[1]
            
            finalRating=int(re.findall(r'\d+', str(rating))[0])
            reviewText=''
            for i in str(tempReviewText):
                if  i in 'qwertyuiopasdfghjklzxcvbnmABCDEFGHIJKLMNOPQRSTUVWXYZ. ':
                    reviewText=reviewText+i
            
           # reviewDate_filtered = str(reviewDate).replace("u'\\n        ","")
           # reviewDate_filtered = reviewDate_filtered.replace("\\n    '","")
           # reviewDate_filtered = reviewDate_filtered.split("/")
           # reviewDate = reviewDate_filtered[2]+"-"+reviewDate_filtered[0]+"-"+reviewDate_filtered[1]
            reviewDate=reviewDate.strip()
            if reviewText != 'This review has been removed for violating or Content Gidelines or Terms of Services':       
                userReview={'restaurantId': restaurantId,'UserName':userName,'UserLocation':userLocation,'reviewDate':reviewDate,'rating':finalRating, 'friendCount':friendCount, 'reviewCount': reviewCount , 'reviewerId': '','reviewerImageId':reviewerImageId,'reviewText':reviewText}
               # userReview = str(userReview).replace("u","")
                userReview = str(userReview).replace("[","")
                userReview = userReview.replace("]","")
                #write crawled review to file
                file_error=open("not_recommended_reviewer_data.txt","a");
                file_error.write(str(userReview)+"\n");
                file_error.close()
                self.countNotRecReviews=self.countNotRecReviews+1
                print "Written review to file for restaurant="+restaurantId+" reviewer="+userName
            
            
        #return userWebPages
    def parse_userpage(self,response):
        sel = Selector(response)
        reviews = sel.xpath('//div[@class="review"]')
        hotelWebPages=[]
        for review in reviews:
          hotel=review.xpath('.//a[@class="biz-name"]/@href').extract()
          temp=hotel[0].split('?')
          restaurantId=temp[0].rsplit('/',1)
          #Only add this page for further crawling if it haven't already been crawled 
          unicodeRest=restaurantId[1].encode('ascii','ignore')
          if(self.checkInHash(self.crawledRestaurants,unicodeRest)==False):
               url='http://www.yelp.com/biz/'+unicodeRest
               print 'Restaurant added for further crawl='+unicodeRest
               hotelWebPages.append(Request(url,callback=self.parseRestaurantMainPage))
               self.linksLeft[url]="restaurant"
          else:
            print 'Restrant already crawled.Dont add='+unicodeRest
        try:
          del self.linksLeft[str(response.url)]
        except:
          traceback.print_exc()
        
        return hotelWebPages
    def spider_closed(self, spider):
        print 'End of days'
        with open('links_left.json', 'w') as outfile:
          json.dump(self.linksLeft, outfile, sort_keys = True, indent = 4, ensure_ascii=False)
        f = open('crawled_users.txt', 'w')
        for u in self.crawledUsers:
          x = self.crawledUsers[u]
          for i in x:
             f.write(str(i)+str('\n'))
        f.close()
        f = open('crawled_restaurants.txt', 'w')
        for u in self.crawledRestaurants:
          x = self.crawledRestaurants[u]
          for i in x:
             f.write(str(i)+str('\n'))
        f.close()
        print "Recommended reviews="+str(self.countRecReviews) +"Not Recommended reviews="+str(self.countNotRecReviews)
        #print spider.linksLeft
    
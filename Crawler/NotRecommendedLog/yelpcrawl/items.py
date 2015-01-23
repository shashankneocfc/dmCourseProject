# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class YelpcrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
   venueName = scrapy.Field()
   reviewer = scrapy.Field()
   reviewerLoc = scrapy.Field()
   rating = scrapy.Field()
   reviewDate = scrapy.Field()
   reviewDate = scrapy.Field()
   reviewText = scrapy.Field()
   reviewerId = scrapy.Field()
   restaurantId = scrapy.Field()
   pass

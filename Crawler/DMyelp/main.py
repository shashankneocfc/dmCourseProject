import scrapy.cmdline

def main():
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'yelp',])

if  __name__ =='__main__':
    main()
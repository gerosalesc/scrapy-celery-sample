# http://stackoverflow.com/questions/24232744/scrapy-spider-not-following-links-when-using-celery

from celery.app import shared_task
from celery.app.base import Celery
from scrapy.crawler import Crawler
from scrapy.conf import settings
from scrapy import log, project, signals
from twisted.internet import reactor
from billiard import Process
from scrapy.utils.project import get_project_settings
from craigslist_sample.spiders.test2 import MySpider

from celery.utils.log import get_task_logger

app = Celery('tasks', broker='amqp://guest@localhost//')
app.config_from_object('celeryconfig')

logger = get_task_logger(__name__)

class UrlCrawlerScript(Process):
        def __init__(self, spider):
            Process.__init__(self)
            settings = get_project_settings()
            self.crawler = Crawler(settings)
            self.crawler.configure()
            # self.crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
            self.spider = spider

        def run(self):
            self.crawler.crawl(self.spider)
            self.crawler.start()
            # reactor.run()

def run_spider(url):
    spider = MySpider(url)
    crawler = UrlCrawlerScript(spider)
    crawler.start()
    crawler.join()

@app.task
def crawl(domain):
    return run_spider(domain)
import logging
import multiprocessing
import re
import sys
import time
from ssl import SSLZeroReturnError
from threading import Thread
from typing import Dict
from urllib.parse import urlparse

from aiohttp import ClientConnectorError, ClientConnectorCertificateError, ClientResponseError
from bs4 import BeautifulSoup
import confuse
from multiprocessing import Pool
import validators
import aiohttp
import asyncio
import logging.handlers

from neo4j import GraphDatabase, Neo4jDriver, Transaction, Result, Session
from neo4j.exceptions import ServiceUnavailable

SHARED_DICT_COUNTER = 'counter'

NEO4J_CONNECTION_RETRY_INTERVAL = 5

CONFIG_PATH = '/configs/config.yaml'

EMAIL_REGEX = r'[\w.+-]+@[\w-]+\.[\w.-]+'


def get_neo4j_driver(neo4j_config: Dict[str, str], logger: logging.Logger) -> Neo4jDriver:
    """
    Create a neo4j driver from config
    :param neo4j_config: config cotnaining ip, port, user, password of neo4j
    :param logger: logger
    :return: driver
    """
    logger.debug(f'connecting to neo4j,  address:{neo4j_config["ip"]} port: {neo4j_config["port"]}')
    return GraphDatabase.driver(f'bolt://{neo4j_config["ip"]}:{neo4j_config["port"]}',
                                auth=(neo4j_config["user"], neo4j_config["password"]))


def get_logger(logger_config: Dict[str, str]) -> logging.Logger:
    """
    Create a logger from config, using stdout and timed rotating file handler
    :param logger_config: configt containing format, level adn file path
    :return: logger
    """
    log_format = logger_config['format']
    current = multiprocessing.current_process()
    logger = logging.getLogger(f"{current.name}_logger")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter(log_format))
    log_level = logging.getLevelName(logger_config['level'])
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(logger_config['file'], when='midnight')
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    return logger


class Worker:
    def __init__(self, queue, shared_dict, config):
        self.__queue = queue
        self.__shared_dict = shared_dict
        self.__config = config
        self._logger = get_logger(config['logger'])
        self.__neo4j_driver = get_neo4j_driver(config['neo4j'], self._logger)
        self.__loop_max_size = config['loop_max_size']
        self.__task_sleep = config['task_sleep']

    async def _crawling_loop(self):
        while True:
            if q.empty():
                await asyncio.sleep(1)
            url = q.get()
            loop_size = len(asyncio.all_tasks())
            if loop_size >= self.__loop_max_size:  # This without this we might overfill the event loop, instead release computation time
                self._logger.debug(
                    f'{multiprocessing.current_process().name} number of tasks in loop is > {self.__loop_max_size}, releaseing time')
                await asyncio.sleep(self.__task_sleep)
            asyncio.create_task(self._do_crawl(url))
            await asyncio.sleep(self.__task_sleep)

    def _create_email_relationship(self, tx: Transaction, url: str, email: str) -> Result:
        """
        Create a link from a url to an email address.
        Will only create a single link if one doesn't exist.
        Will create a node only if one doesn't exist.
        :param tx: Transaction in which this function runs
        :param url: pointing url
        :param email: pointed email
        :return: result
        """
        self._logger.debug(f'{multiprocessing.current_process().name} found mail {email} for url {url}')
        result = tx.run("MERGE (n1:HREF {url: $url}) "
                        "MERGE (n2:EMAIL {email: $email}) "
                        "MERGE (n1)-[:references_email ]->(n2)", url=url, email=email)
        return result

    def run(self):
        """
        Run in async loop
        :return:
        """
        asyncio.run(self._crawling_loop())

    async def _do_crawl(self, url: str) -> None:
        """
        scrape the specific url and extract links and email adresses
        :param url: url tho scrape
        :return:
        """
        if url in self.__shared_dict.keys():
            return
        d[
            url] = 1  # this is a memory leak, we do not clear this dict, for a more robust solution use lru cache here as well.
        with self.__neo4j_driver.session() as neo4j_session:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url) as resp:
                        content = await resp.content.read()
                        soup = BeautifulSoup(content, features='html5lib')
                        text = soup.get_text()
                        emails = re.findall(EMAIL_REGEX, str(text))  # the regex may have false positives
                        filtered_emails = filter(validators.email, emails)
                        list(map(lambda email: neo4j_session.write_transaction(self._create_email_relationship, url,
                                                                               email),
                                 filtered_emails))
                        links = map(lambda a: a['href'], soup.find_all('a', href=True))
                        validated_links = filter(validators.url, links)
                        await asyncio.gather(
                            *[self._process_link(neo4j_session, url, href) for href in validated_links])
                except ClientConnectorError:
                    self._logger.error(f'connection error - failed scraping url {url}')
                except SSLZeroReturnError:
                    self._logger.error(f'ssl error - failed scraping url {url}')
                except ClientConnectorCertificateError:
                    self._logger.error(f'client certifcate error - failed scraping url {url}')
                except ClientResponseError:
                    self._logger.error(f'client response error - failed scraping url {url}')
                except Exception as e:
                    self._logger.error(f'failed scraping url {url} with exception{e}')

    def _create_relationship(self, tx: Transaction, url1: str, url2: str, href_domain: str) -> Result:
        """
        Create a releationship between two urls, where url1 points to url2.
        If relationship or node exist, don't create new.
        :param tx: transaction
        :param url1: origin url
        :param url2: pointed url
        :param href_domain: the domain of the new link
        :return: result
        """
        self._logger.debug(f'{multiprocessing.current_process().name} found link {url2} in url {url1}')
        result = tx.run("MERGE (n1:HREF {url: $url1}) "
                        "MERGE (n2:HREF {url: $url2, domain: $href_domain}) "
                        "MERGE (n1)-[:references_url ]->(n2)", url1=url1, url2=url2, href_domain=href_domain)
        return result

    async def _process_link(self, neo4j_session: Session, url: str, href: str) -> None:
        """
        Process new link - href, add relationship to neo4j and add new scraping task to the queue
        :param neo4j_session:
        :param url: origin url
        :param href: new url
        :return:
        """
        href_domain = urlparse(href).netloc
        neo4j_session.write_transaction(self._create_relationship, url, href, href_domain)
        if not self.__queue.full():
            if href not in self.__shared_dict.keys():
                self.__queue.put(href)
                self.__shared_dict['counter'] += 1
            else:
                self._logger.debug(f'worker {multiprocessing.current_process().name} url {url} already in keys')


def performance_monitor(shared_dict: Dict[str, int], logger: logging.Logger, metric_log_interval):
    """
    Heleper deamon which logs the performance of the system.
    Currently it outputs the throughput
    :param shared_dict: shared dict
    :param logger: logger
    :return:
    """
    logger.info('starting monitoring daemon')
    last_collection = time.time()
    sleep_interval = metric_log_interval
    counter = shared_dict[SHARED_DICT_COUNTER]
    time.sleep(metric_log_interval)
    while True:
        current_time = time.time()
        qsize = shared_dict[SHARED_DICT_COUNTER]
        new_links_per_second = (qsize - counter) / (current_time - last_collection)
        logger.info(f'throughput:  {new_links_per_second} new links per second')
        last_collection = current_time
        counter = qsize
        time.sleep(sleep_interval)


def run_worker(kwargs):
    queue = kwargs['queue']
    shared_dict = kwargs['shared_dict']
    config = kwargs['config']
    worker = Worker(queue, shared_dict, config)
    worker.run()


if __name__ == '__main__':
    config = confuse.load_yaml(CONFIG_PATH)
    neo4j_config = config['neo4j']
    max_queue_size = config['max_queue_size']
    workers = config['workers']
    metric_log_interval = config['metric_log_interval']
    seeds = config['seeds']
    logger = get_logger(config['logger'])
    while True:
        try:
            driver = get_neo4j_driver(neo4j_config, logger)

            res = driver.verify_connectivity()
            if res:
                logger.info('verified connectivity with neo4j')
                break
        except ServiceUnavailable as e:
            logger.error('failed connecting to neo4j sleeping and retrying')
            time.sleep(NEO4J_CONNECTION_RETRY_INTERVAL)
    m = multiprocessing.Manager()
    q = m.Queue(maxsize=max_queue_size)
    d = m.dict()
    d[SHARED_DICT_COUNTER] = 0
    [q.put(f'{i}') for i in seeds]  # populate the queue with the initial seeds
    Thread(target=performance_monitor,
           kwargs={'shared_dict': d, 'logger': logger, 'metric_log_interval': metric_log_interval}).start()
    logger.info(f'starting pool with {workers} processes')
    with Pool(workers) as p:
        args = [{'queue': q, 'shared_dict': d, 'config': config} for i in range(workers)]
        p.map(run_worker, args)

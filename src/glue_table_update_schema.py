import argparse
from time import sleep
from base.cfg import BaseConfig
from base.logger import get_logger

logger = get_logger(__name__)

class GlueTableSchemaUpdater:
    CRAWLER_NAME_TEMPLATE = "temp_{}_{}_crawler"
    GLUE_ROLE_NAME_TEMPLATE = "sds-{}-common-store-glue-crawler-role"

    def __init__(self, cfg: BaseConfig, database_name:str, table_name: str):
        self.cfg = cfg
        self.session = cfg.session
        self.glue = self.session.client('glue')
        self.database_name = database_name
        self.table_name = table_name

    def get_crawlers(self) -> list[str]:
        crawlers = []
        response = self.glue.get_crawlers()
        crawlers.extend(response['Crawlers'])

        while 'NextToken' in response:
            response = self.glue.get_crawlers(NextToken=response['NextToken'])
            crawlers.extend(response['Crawlers'])

        return [crawler['Name'] for crawler in crawlers]

    def get_table_info(self) -> dict[str, str]:
        response = self.glue.get_table(
            DatabaseName=self.database_name,
            Name=self.table_name
        )
        logger.debug("Response from get_table_info: {}".format(response))

        return {'Location': response['Table']['StorageDescriptor']['Location']}

    def create_crawler(self, name: str):
        table_info = self.get_table_info()
        logger.debug(f"Got table information: {table_info}")

        crawler_params = {
            "DatabaseName": self.database_name,
            "Name": name,
            "Role": GlueTableSchemaUpdater.GLUE_ROLE_NAME_TEMPLATE.format(self.cfg.env),
            "Targets": {
                "CatalogTargets": [
                    {
                        'DatabaseName': self.database_name,
                        'Tables': [
                            self.table_name,
                        ]
                    }
                ]
            },
            "SchemaChangePolicy": {
                "DeleteBehavior": "LOG",
                "UpdateBehavior": "UPDATE_IN_DATABASE"
            }
        }

        self.glue.create_crawler(**crawler_params)
        logger.info(f"Crawler {name} created")

    def delete_crawler(self, name: str):
        self.glue.delete_crawler(Name=name)
        logger.info(f"Crawler {name} deleted")

    def list_crawls(self, name):
        crawls = []
        response = self.glue.list_crawls(CrawlerName=name)
        if response['Crawls']:
            crawls.extend(response['Crawls'])

        while 'NextToken' in response:
            response = self.glue.list_crawls(CrawlerName=name, NextToken=response['NextToken'])
            if response['Crawls']:
                crawls.extend(response['Crawls'])

        return crawls

    def get_crawler_state(self, name: str) -> str:
        response = self.glue.get_crawler(Name=name)
        return response['Crawler']['State']

    def start_and_wait_crawler(self, name: str):
        self.glue.start_crawler(Name=name)
        logger.info(f"Started crawler {name}")

        while True:
            sleep(10)
            crawler_state = self.get_crawler_state(name)
            if crawler_state == 'READY':
                logger.info(f"Crawler {name} completed")
                break
            else:
                logger.info(f"Crawler {name} state is {crawler_state}, waiting")

    def execute(self):
        crawlers = self.get_crawlers()
        logger.debug(f"Got crawlers: {crawlers}")

        crawler_name = self.CRAWLER_NAME_TEMPLATE.format(self.database_name, self.table_name)
        if crawler_name in crawlers:
            logger.error(f"Crawler {crawler_name} already exists, please, check")
        else:
            self.create_crawler(crawler_name)
            self.start_and_wait_crawler(crawler_name)
            self.delete_crawler(crawler_name)

    def __call__(self, *args, **kwargs):
        self.execute()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('database_name')
    parser.add_argument('table_name')

    args = parser.parse_args()
    logger.info(f'Starting updating schema with args: {args}')

    config = BaseConfig(args.env)
    GlueTableSchemaUpdater(config, args.database_name.format(args.env), args.table_name)()

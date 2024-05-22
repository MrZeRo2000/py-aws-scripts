import boto3
import pandas as pd
import numpy as np

session = boto3.session.Session(profile_name='prod', region_name='eu-west-1')
glue = session.client('glue')


def list_crawlers():
    """Retrieve the list of all Glue crawlers."""
    crawlers = []
    response = glue.get_crawlers()
    crawlers.extend(response['Crawlers'])

    while 'NextToken' in response:
        response = glue.get_crawlers(NextToken=response['NextToken'])
        crawlers.extend(response['Crawlers'])

    return [crawler['Name'] for crawler in crawlers]


def parse_crawl_response(response):
    return [
        (r['StartTime'].day, int((r['EndTime'] - r['StartTime']).total_seconds()))
        for r in response['Crawls']
        if r['StartTime'].month == 5 and r['StartTime'].year == 2024 and r['State'] == 'COMPLETED']


def list_crawls(crawler_name):
    """Retrieve all crawl runs for a given crawler."""
    crawls = []
    response = glue.list_crawls(CrawlerName=crawler_name)
    if response['Crawls']:
        crawls.extend(parse_crawl_response(response))

    while 'NextToken' in response:
        response = glue.list_crawls(CrawlerName=crawler_name, NextToken=response['NextToken'])
        crawls.extend(parse_crawl_response(response))

    return crawls


if __name__ == "__main__":
    # crawler_list = [c for c in list_crawlers() if c in ['sds-dev-store-operations-shop-crawler', 'sds-dev-store-payment-shop-crawler-1to1']]
    crawler_list = list_crawlers()
    dfr = None
    for c in crawler_list:
        print(f"Crawler Name: {c}")
        crawler_crawls = list_crawls(c)
        if crawler_crawls:
            records = [{'crawler': c, 'day': crcr[0], 'duration': crcr[1]} for crcr in crawler_crawls]
            df = pd.DataFrame(records)
            dfa = df.groupby(['crawler', 'day']).agg(duration=('duration', np.max)).reset_index()

            if dfr is None:
                dfr = dfa
            else:
                dfr = pd.concat([dfr, dfa] ,axis=0)

    pdfr = pd.pivot(data=dfr, values=['duration'], index=['crawler'], columns=['day'])
    pdfr.columns = [c[1] for c in pdfr.columns]
    pdfr = pdfr.reset_index()
    pdfr.to_csv("../data/pdfr.csv", index=False)
    pass

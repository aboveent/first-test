import pandas as pd
import numpy as np
import datetime
import time
import calendar
import sys
from dateutil import relativedelta
from googleads import adwords
from datetime import datetime
from uuid import uuid4
from googleads import adwords
from googleads import errors

five_hundred_lines = []
PAGE_SIZE = 500

def CreateFeedItemAddOperation(client, feed):
    """Creates a FeedItemOperation.
    The generated FeedItemOperation will create a FeedItem with the specified
    values when sent to FeedItemService.mutate.
    Args:
    name: the value for the name attribute of the FeedItem.
    price: the value for the price attribute of the FeedItem.
    date: the value for the date attribute of the FeedItem.
    ad_customizer_feed: the AdCustomizerFeed we're associating the FeedItems
        with.
    Returns:
    A new FeedItemOperation for adding a FeedItem.
    """
    global five_hundred_lines
    o = 0
    for item in feed:
        line = "{ 'operator': 'REMOVE', 'operand': " + str(feed[o]) + "}"
        five_hundred_lines.append(eval(line))
        #print(line)
        o += 1

    operation = five_hundred_lines

    #print(five_hundred_lines)

    # Get the FeedItemService
    feed_item_service = client.GetService('FeedItemService', 'v201806')
    #response = feed_item_service.mutate(operation)
    #print(response)
    #time.sleep(3)

    try:
        response = feed_item_service.mutate(operation)
        print("pushed 500")
    except:
        try:
            response = feed_item_service.mutate(operation)
        except:
            print("skipped")
    five_hundred_lines = []


def main(client):
    
    ad_customizer_feed_service = client.GetService('FeedItemService', 'v201806')

    offset = 0
    selector = {
      'fields': ['FeedId', 'FeedItemId', 'Status'],
      'predicates': [
          {
              'field': 'FeedId',
              'operator': 'EQUALS',
              'values': 80391037
          },
          {
              'field': 'Status',
              'operator': 'EQUALS',
              'values': 'ENABLED'
          }
      ],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
    }

    more_pages = True
    while more_pages:
        page = ad_customizer_feed_service.get(selector)

        #offset += PAGE_SIZE
        #selector['paging']['startIndex'] = str(offset)
        more_pages = PAGE_SIZE < int(page['totalNumEntries'])
        print("Num of items left " + str(page['totalNumEntries']))
        print("offset " + str(offset))
        feed = page['entries']
        #feed_item_ids = [mydict[x] for x in mykeys]
        #print(feed)
        CreateFeedItemAddOperation(client, feed)
        

if __name__ == '__main__':
    adwords_client = adwords.AdWordsClient.LoadFromStorage("E:\\Cron-Jobs-E-DO-NOT-DELETE\\BWW-Ad-Customizers\\googleads.yaml")
    main(adwords_client)

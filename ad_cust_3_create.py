import pandas as pd
import numpy as np
import datetime
import time
import calendar
import sys
from dateutil import relativedelta
from googleads import adwords
from datetime import datetime
from datetime import date
from uuid import uuid4
from googleads import adwords
from googleads import errors

my_date = date.today()
today_date = date.today() #datetime.now() 
today_str = today_date.strftime("%Y-%m-%d")
day_of_week = calendar.day_name[my_date.weekday()]

week_5_end = date(2018,10,8)
week_6_end = date(2018,10,15)
week_7_end = date(2018,10,22)
week_8_end = date(2018,10,29)
week_9_end = date(2018,11,5)
week_10_end = date(2018,11,12)
week_11_end = date(2018,11,19)
week_12_end = date(2018,11,26)
week_13_end = date(2018,12,3)
week_14_end = date(2018,12,10)
week_15_end = date(2018,12,17)
week_16_end = date(2018,12,24)

# print(week_5_end)
# print(today_date)
# print(week_6_end)

if week_5_end <= today_date <= week_6_end:
    week = str(6)
elif week_6_end <= today_date <= week_7_end:
    week = str(7)
elif week_7_end <= today_date <= week_8_end:
    week = str(8)
elif week_8_end <= today_date <= week_9_end:
    week = str(9)
elif week_9_end <= today_date <= week_10_end:
    week = str(10)
elif week_10_end <= today_date <= week_11_end:
    week = str(11)
elif week_11_end <= today_date <= week_12_end:
    week = str(12)
elif week_12_end <= today_date <= week_13_end:
    week = str(13)
elif week_13_end <= today_date <= week_14_end:
    week = str(14)
elif week_14_end <= today_date <= week_15_end:
    week = str(15)
elif week_15_end <= today_date <= week_16_end:
    week = str(16)

print(week)

file = pd.read_excel(r'E:\Cron-Jobs-E-DO-NOT-DELETE\BWW-Ad-Customizers\NFL Ad Customizers\Week ' + week + r'\Customizers 3\NFL Ad Customizers3 - Week ' + week + ' - Through ' + day_of_week + '.xlsx')

file['Game Time EST (text)'] = file['Game Time EST (text)'].apply(lambda x: x.strftime('%I:%M%p') if not isinstance(x, float) else "")
file['Game Time Actual (text)'] = file['Game Time Actual (text)'].apply(lambda x: x.strftime('%I:%M%p') if not isinstance(x, float) else "")

locIds = pd.read_csv('E:\\Cron-Jobs-E-DO-NOT-DELETE\\BWW-Ad-Customizers\\AdWords Zip Criteria.csv')
combine_with_geo_ids = pd.merge(file, locIds)
combine_with_geo_ids_drop_na = combine_with_geo_ids.fillna("")
final = combine_with_geo_ids_drop_na.drop(columns=['Status', 'Target Type', 'Parent ID', 'Country Code', 'Canonical Name'])
final.to_string(columns=['Criteria ID', 'Game Time EST (text)', 'Game Time Actual (text)'])
print(final.shape)
print(final.head())

five_hundred_lines = []
five_hundred_locations_lines = []
location_ids = final['Criteria ID'].tolist()

def GetCustomizerFeed(client):
    ad_customizer_feed_service = client.GetService('AdCustomizerFeedService',
                                                 'v201806')

    selector = {
      'fields': ['FeedId', 'FeedName', 'FeedAttributes'],
      'predicates': [
          {
              'field': 'FeedId',
              'operator': 'EQUALS',
              'values': 80391037
          }
      ],
    }

    response = ad_customizer_feed_service.get(selector)

    feed = response['entries'][0]
    feed_data = {
        'feedId': feed['feedId'],
        'nameId': feed['feedAttributes'][0]['id'],
        'priceId': feed['feedAttributes'][1]['id'],
        'dateId': feed['feedAttributes'][2]['id']
    }
    print (feed['feedName'], feed['feedId'])
    return feed


def GetLines(ad_customizer_feed, five_hundred_rows):
    for index, row in five_hundred_rows.iterrows():
        if row['Criteria ID'] is None:
            print("skipping")
            continue
        else:
            line = "CreateFeedItemAddOperation( '" + str(row['Time Zone (text)']) +  "', '" + str(row['Home Team (text)']) + "', '" + str(row['Opponent (text)']) + "', '" + str(row['Game Time EST (text)']) + "', '" + str(row['Game Day (text)']) + "', '" + str(row['Game Time Actual (text)']) + "', '" + str(row['Week (text)']) + "', ad_customizer_feed)"
            five_hundred_lines.append(eval(line))
            
    return five_hundred_lines


def RestrictFeedItemToAdGroup(client, res_values, five_hundred_locations):
    """Restricts the feed item to an ad group.
    Args:
    client: an AdWordsClient instance.
    feed_item: The feed item.
    adgroup_id: The ad group ID.
    """
    global five_hundred_locations_lines
    location_operations = []

    # Get the FeedItemTargetService
    feed_item_target_service = client.GetService(
      'FeedItemTargetService', 'v201806')

    # Optional: Restrict the first feed item to only serve with ads for the
    # specified ad group ID.

    j = 0
    for locat in res_values:
        location_target = "{ 'xsi_type': 'FeedItemCriterionTarget', 'feedId':" + str(res_values[j]['feedId']) + ", 'feedItemId':" + str(res_values[j]['feedItemId']) + ", 'criterion':  { 'xsi_type': 'Location', 'id':" +  str(five_hundred_locations[j]) + ", },}"
        five_hundred_locations_lines.append(eval(location_target))
        j += 1

    k = 0
    for line in five_hundred_locations_lines:
        operation = "{'operator': 'ADD', 'operand':" + str(five_hundred_locations_lines[k]) + "}"
        location_operations.append(eval(operation))
        k += 1
        
    try: 
        response = feed_item_target_service.mutate(location_operations)
        print(response)
##        new_location_target = response['value'][0]
##        print(new_location_target['feedId'], new_location_target['feedItemId'])
    except:
        response = feed_item_target_service.mutate(location_operations)
        print(response)
##        new_location_target = response['value'][0]
##        print(new_location_target['feedId'], new_location_target['feedItemId'])
    five_hundred_locations_lines = []
    location_operations = []

def CreateCustomizerFeedItems(client, five_hundred_locations, five_hundred_rows, ad_customizer_feed):
    """Creates FeedItems for the specified AdGroups.
    These FeedItems contain values to use in ad customizations for the AdGroups.
    Args:
    client: an AdWordsClient instance.
    adgroup_ids: a list containing two AdGroup Ids.
    ad_customizer_feed: the AdCustomizerFeed we're associating the FeedItems
        with.
    Raises:
    GoogleAdsError: if no FeedItems were added.
    """
    # Get the FeedItemService
    feed_item_service = client.GetService('FeedItemService', 'v201806')

    feed_item_operations = five_hundred_rows #all_rows

    response = feed_item_service.mutate(feed_item_operations)
    i = 1
    if 'value' in response:
        for feed_item in response['value']:
          print('Added FeedItem with ID %d.' % feed_item['feedItemId'] + " Number: " + str(i))
          i += 1
    else:
        raise errors.GoogleAdsError('No FeedItems were added.')
    res_values = response['value']
    print(len(response['value']))
    print(len(five_hundred_locations))
    print(response['value'])
    time.sleep(15)
    RestrictFeedItemToAdGroup(client, res_values, five_hundred_locations)
##    for feed_item, location_id in zip(response['value'], five_hundred_locations):
##        RestrictFeedItemToAdGroup(client, feed_item, location_id)


def CreateFeedItemAddOperation(TimeZone, HomeTeam, Opponent, GameTimeEST, GameDay, GameTimeActual, Week, ad_customizer_feed):
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
    feed_item = {
      'feedId': ad_customizer_feed['feedId'],
      'attributeValues': [
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][0]['id'],
              'stringValue': TimeZone
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][1]['id'],
              'stringValue': HomeTeam
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][2]['id'],
              'stringValue': Opponent
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][3]['id'],
              'stringValue': GameTimeEST
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][4]['id'],
              'stringValue': GameDay
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][5]['id'],
              'stringValue': GameTimeActual
          },
          {
              'feedAttributeId': ad_customizer_feed['feedAttributes'][6]['id'],
              'stringValue': Week
          }
      ]
    }

    operation = {
      'operator': 'ADD',
      'operand': feed_item
    }

    return operation

def main(client, five_hundred_locations, five_hundred_lines):
    # Create a customizer feed. One feed per account can be used for all ads.
    ad_customizer_feed = GetCustomizerFeed(client)
    time.sleep(2)
    GetLines(ad_customizer_feed, five_hundred_rows)
    time.sleep(2)
    CreateCustomizerFeedItems(client, five_hundred_locations, five_hundred_lines, ad_customizer_feed)

if __name__ == '__main__':
    print("hi")
    start = 0
    end = 750
    while start < len(location_ids):
        print(start)
        print(location_ids)
        five_hundred_rows = final.loc[start:end]
        five_hundred_locations = five_hundred_rows['Criteria ID'].tolist()
        print(five_hundred_rows.tail())
        
        # Initialize client object.
        adwords_client = adwords.AdWordsClient.LoadFromStorage("E:\\Cron-Jobs-E-DO-NOT-DELETE\\BWW-Ad-Customizers\\googleads.yaml")
        main(adwords_client, five_hundred_locations, five_hundred_lines)
        
        time.sleep(3)
        five_hundred_lines = []
        start += 750
        end += 750

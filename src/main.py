import base64
import os
import json
import requests
from pprint import pprint as pp
from time import sleep
from datetime import datetime, timedelta
import urllib.parse as urlparse
from urllib.parse import parse_qs
from google.cloud import bigquery
from google.cloud import pubsub_v1

######################################################################
######################################################################
####   Не меняйте функцю в панели GCP. Загрузите из Гита          ####
####   и редактируйте ее локально, а потом опубликуйте            ####
######################################################################
######################################################################

def get_vk_stat_one_day(event, context):
    global client,dataset,access_token
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    props = json.loads(pubsub_message)
    pp(props)
    client = bigquery.Client(props.get('gbq_project'))
    dataset = props.get('gbq_dataset')
    access_token = props.get('access_token')
    
    load_cost_data(props['account_id'], props['client_id'],
                   props.get('from', ''), props.get('to', ''))


added_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def api_request(method, params={}, count=0):
    sleep(1)
    params['access_token'] = access_token
    params['v'] = '5.130'
    api_url = f'https://api.vk.com/method/{method}'
    resp = requests.post(url=api_url, data=params)
    data = resp.json()
    if 'error' in data:
        pp(data)
        code = data['error']['error_code'] 
        if code == 6 or code == 9:
            sleep(1)
            if count < 3:
                return api_request(method, params, count+1)
            else:
                return None
    if 'response' in data:
        return data['response']
    return data


def map_list_as_dict(keyFunction, values):
    return dict((keyFunction(v), v) for v in values)


def load_clients(account_id):
    clients = api_request('ads.getClients', {'account_id': account_id})
    return clients


def load_client_campaigns(account_id, client_id):
    campaigns = api_request('ads.getCampaigns', {'account_id': account_id,
                                                 'client_id': client_id})
    pp(f'Campaigns: {len(campaigns)}')
    return map_list_as_dict(lambda a:str( a["id"]), campaigns)


def load_client_ads(account_id, client_id):
    ads = api_request('ads.getAds', {'account_id': account_id,
                                     'client_id': client_id})
    if ads:
        pp(f'Ads: {len(ads)}')
    else:
        pp("No ads found")
    return map_list_as_dict(lambda a: str(a["id"]), ads)
    


def load_client_ad_layouts(account_id, client_id):
    layouts = api_request('ads.getAdsLayout', {'account_id': account_id,
                                               'client_id': client_id})
    if not layouts:
        pp(f"No layouts for account {account_id} client {client_id}")
    posts = []
    ids = {}
    for layout in layouts:
        if layout['ad_format'] == 9:
            post_id = layout['link_url'].split('wall')[1]
            posts.append(post_id)
            ids[post_id] = str(layout['id'])

    mapped_layouts = map_list_as_dict(lambda a: str(a["id"]), layouts)

    wall_posts = api_request('wall.getById', {'posts': ','.join(posts)})

    for p in wall_posts:
        if isinstance(p, list):
            d = p[0]
        else:
            d = p
        if 'attachments' not in d:
            pp(d)
        if 'link' not in d['attachments'][0]:
            pp(d['attachments'][0])
            continue
        url = d['attachments'][0]['link']['button']['action']['url']

        post_id = str(d['from_id'])+"_"+str(d['id'])
        layout = mapped_layouts[ids[post_id]]
        url = url.replace('{campaign_id}', str(layout['campaign_id'])).replace(
            '{ad_id}', str(layout['id']))
        parsed = urlparse.urlparse(url)

        layout['link_url'] = url
        props = parse_qs(parsed.query)
        for utm in ['utm_campaign', 'utm_content', 'utm_medium', 'utm_source']:
            layout[utm] = props.get(utm, [''])[0]

    return mapped_layouts


def load_client_ad_stats(account_id, client_id, ad_ids, date_from, date_to):
    ids = map(lambda x: str(x), ad_ids)
    stats = api_request('ads.getStatistics', {'account_id': account_id,
                                              'ids_type': 'ad',
                                              'ids': ','.join(ids),
                                              'period': 'day',
                                              'date_from': date_from,
                                              'date_to': date_to})
    stats = [stat for stat in stats if len(stat['stats']) > 0]
    return map_list_as_dict(lambda a: a["id"], stats)


def make_bq_object(ad):
    days = []
    ad_for_json = ad.copy()
    ad_for_json.pop('stats')

    for day in ad['stats']:
        ad_for_json['stat'] = day
        ad_json = json.dumps(ad_for_json)
        medium = 'cpc' if ad['layout']['cost_type'] == 0 else 'lead'
        o = {
            'source': ad['layout']['utm_source'] or 'vk',  # . join owox
            'medium': ad['layout']['utm_medium'] or medium,  # . join owox
            # . join owox
            'campaign': ad['layout']['utm_campaign'] or None,
            'campaignId': ad['campaign']['id'] or None,
            'campaignName': ad['campaign']['name'] or None,
            'keyword': None,  # . join owox
            # . join owox
            'adContent': ad['layout']['utm_content'] or None,
            'adCost': round(float(day.get('spent', 0.0))/1.2, 4),
            'adClicks': day.get('clicks', 0),
            'impressions': day.get('impressions', 0),
            'vat': '20',
            'date': day['day'],
            'currency': 'RUB',
            'originalAdCost': day.get('spent', 0),
            'originalCurrency': 'RUB',
            'adAccount': ad['client_id'],
            'id': ad['id'],
            'json': ad_json,
            'added': added_time
        }
        days.append(o)
    return days


def map_layout(ad):
    data = {**ad['layout'], **ad['settings']}
    r = {
        'created': added_time,
        'id': data['id'],
        'adaccount': ad['client_id'],
        'json': json.dumps(data)
    }
    return r


def get_stats_data(ads):
    t = list(map(make_bq_object, ads))
    stats = [item for sublist in t for item in sublist]
    return stats


def upload_to_bq(client_id, ads):

    ad_layout_data = list(map(map_layout, ads))
    if ad_layout_data:
        pp(f'Streaming layouts')
        r = client.insert_rows_json(
            dataset+'.ad_layouts', ad_layout_data)
        if(len(r)):
            pp(r)
    else:
        print('No layouts')

    ad_stats_data = get_stats_data(ads)
    if len(ad_stats_data):
        print(f'Streaming stats {len(ad_stats_data)}')
        r = client.insert_rows_json(
            dataset+'.vk_stats', ad_stats_data)
        if(len(r)):
            pp(r)
    else:
        print('No stats found')


def load_cost_data(account_id, client_id, date_from="", date_to=""):
    yesterday = datetime.strftime((datetime.now() - timedelta(1)), '%Y-%m-%d')
    if not date_from:
        date_from = yesterday
    if not date_to:
        date_to = yesterday
    if client_id:
        ads = []
        print(f'Loading data for {client_id}')
        campaigns = load_client_campaigns(account_id, client_id)
        layouts = load_client_ad_layouts(account_id, client_id)
        ads_settings = load_client_ads(account_id, client_id)

        ad_ids = layouts.keys()
        if len(ad_ids) == 0:
            return
        stats = load_client_ad_stats(
            account_id, client_id, ad_ids, date_from, date_to)
        for id, ad in stats.items():
            ad['layout'] = layouts[str(id)]
            ad['settings'] = ads_settings[str(id)]
            ad['campaign'] = campaigns[str(layouts[str(id)]['campaign_id'])]
            ad['client_id'] = client_id
            ads.append(ad)

        upload_to_bq(client_id, ads)



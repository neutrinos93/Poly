import pandas as pd
from tqdm import tqdm

from utils.market_util import MarketUtil

trades_data = pd.read_csv('trades_data.csv')

MARKET_FEATURE_FIELDS = [
    "conditionId",
    'id',
    "startDate",
    "startDateIso",
    "endDate",
    "endDateIso",
    "closedTime",
    "active",
    "closed",
    "resolved",
    "category",
    "marketType",
    "formatType",
    "ammType",
    "enableOrderBook",
    "fpmmLive",
    "negRisk",
    "enableNegRisk",
    "volume",
    "volumeNum",
    "liquidity",
    "liquidityNum",
    "volume24hr",
    "volume1wk",
    "volume1mo",
    "volume1yr",
    "outcomes",
    "outcomePrices",
    "lastTradePrice",
    "bestBid",
    "bestAsk",
    "fee",
    "makerBaseFee",
    "takerBaseFee",
]

# Map each conditionId to market and event metadata
unique_conditionIDs = trades_data['conditionId'].unique()

with MarketUtil() as mu:
  map_conditionIDs_market_data = {}

  for cid in tqdm(unique_conditionIDs):
    market_obj = mu.get_market_by_conditionId(cid)

    if isinstance(market_obj, list):
      market_obj = market_obj[0] if market_obj else None
    if not market_obj:
      map_conditionIDs_market_data[cid] = {'conditionId': cid, "_missing": True}
      continue

    map_conditionIDs_market_data[cid] = {f: market_obj.get(f) for f in MARKET_FEATURE_FIELDS}

    events = market_obj.get('events') or []
    eventId = events[0].get('id') if events and isinstance(events[0], dict) else None
    #print(eventId)
    event_obj = None

    if eventId:
      event_obj = mu.get_event_by_eventId(eventId)
      if isinstance(event_obj, list):
        event_obj = event_obj[0] if event_obj else None

    if event_obj:
      map_conditionIDs_market_data[cid]['event_category'] = event_obj.get('category')
      map_conditionIDs_market_data[cid]['event_subcategory'] = event_obj.get('subcategory')
      #print(event_obj.get('category'))
    else:
      map_conditionIDs_market_data[cid]['event_category'] = None
      map_conditionIDs_market_data[cid]['event_subcategory'] = None
      print('invalid')

    # Event category is mostly empty for recent events, so take first 5 tags
    tags = mu.get_event_tags(event_obj, first_n_tags=6)
    for i, tag in enumerate(tags):
      if not tag:
        break
      # print(tag)
      header = 'tag_' + str(i+1)
      map_conditionIDs_market_data[cid][header] = tag

    
markets_df = pd.DataFrame.from_dict(map_conditionIDs_market_data, orient="index").reset_index(drop=True)
markets_df.to_csv('market_info.csv', index=False)
    
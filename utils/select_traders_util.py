import httpx
from urllib.parse import urljoin

import utils.urls_endpoints as ep

class GetTraders:
  def __init__(self):
    self.client = httpx.Client(http2=True, timeout=30.0)

  def _build_url(self, base_url: str, endpoint: str) -> str:
    return urljoin(base_url, endpoint)
  
  def get_top_traders_by_category(self, max_number = 1000, category: str|None = 'OVERALL', time_period : str|None = "ALL", order_by: str|None = "PNL", limit: int|None = 50):
    if category not in ['OVERALL', 'POLITICS', 'SPORTS', 'CRYPTO', 'CULTURE', 'MENTIONS', 'WEATHER', 'ECONOMICS', 'TECH', 'FINANCE']:
      return []
    if time_period not in ['DAY', 'WEEK', 'MONTH', 'ALL']:
      return []
    if order_by not in ['PNL', 'VOL']:
      return []
    
    params = {}
    seen = set()
    rows = []
    offset = 0

    if category:
      params['category'] = category
    if time_period:
      params['timePeriod'] = time_period
    if order_by:
      params['orderBy'] = order_by
    if limit:
      params['limit'] = limit

    while True:
      if len(rows) >= max_number:
        break
      params['offset'] = offset
      response = self.client.get(self._build_url(ep.DATA_LEADER_URL,ep.LEADERBORD_ENDPOINT), params=params)
      response.raise_for_status()
      page = response.json()
      if not page:
        break
      for x in page:
        key = x.get('proxyWallet')
        if key in seen:
          continue
        seen.add(key)
        rows.append(x)
      offset += limit

    return rows
  
  @staticmethod
  def get_trader_basic_info(trader_obj: dict|list[dict] = None) -> list[dict]:
    if trader_obj is None:
      return []
    
    if isinstance(trader_obj, dict):
      traders = [trader_obj]
    elif isinstance(trader_obj, list):
      traders = trader_obj
    else:
      raise TypeError("Input must be a dict or list[dict].")
    
    return [{'rank': trader['rank'], 'proxyWallet': trader['proxyWallet'], 'pnl': trader['pnl']} for trader in traders]
  
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()
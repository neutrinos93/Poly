import httpx
from urllib.parse import urljoin
import pandas as pd

import urls_endpoints as ep
import utils.mytools as mytools

class TradersUtil:
  def __init__(self):
    self.client = httpx.Client(http2=True, timeout=30.0)

  def _build_url(self, base_url: str, endpoint: str) -> str:
    return urljoin(base_url, endpoint)
  
  def get_user_pnl(self, proxyWallet, period='all', frequency = '1d'):
    params = {
      "user_address": proxyWallet,
      "interval": period, # ["all", "1m", "1w", "1d"] 
      "fidelity": frequency, # ["1h", "3h", "12h", "1d"]
      }
    response = self.client.get(ep.USERPNL_URL, params=params)
    response.raise_for_status()
    df =  pd.DataFrame(response.json())
    df = df.sort_values('t')
    df["datetime_utc"] = pd.to_datetime(df["t"], unit="s", utc=True)
    return df

  def get_total_markets_traded(self, proxyWallet: str|None = None):
    response = self.client.get(self._build_url(ep.DATA_URL,ep.TOT_MARKETS_ENDPOINT), params={})
    response.raise_for_status()
    return response.json().get("traded")
  
  def get_user_trades(self, proxyWallet: str|None = None, cutoff_year=2024):
    # Assumes descending ordering, which is verified for now, but it's not robust against changes

    if not proxyWallet:
      raise ValueError("proxyWallet is required")
    
    offset = 0
    limit = 500
    seen = set()
    rows = []
    CUTOFF_TS = mytools.year_start_timestamp_utc(cutoff_year)

    while True:
      if offset>10000:
        break
      #print('\tGetting user\'s trades... offset =', offset)
      params = {
        'user': proxyWallet,
        'limit': limit,
        'offset': offset,
        'takerOnly': False,
      }
      response = self.client.get(self._build_url(ep.DATA_URL, ep.TRADES_ENDPOINT), params = params)
      response.raise_for_status()
      page = response.json()
      if not page:
        break

      timestamps = [x['timestamp'] for x in page]
      stop_after = min(timestamps) < CUTOFF_TS

      for x in page:
        if x['timestamp'] < CUTOFF_TS:
          continue
        
        key = x['transactionHash']
        if key in seen:
          break # Debatable that this is smart
        seen.add(key)
        rows.append(x)

      if stop_after:
        break

      offset += limit

    df = pd.DataFrame(rows)
    if df.empty:
      return df
    
    df.drop(['icon', 'name', 'pseudonym', 'bio', 'profileImage', 'profileImageOptimized'], axis=1, inplace=True)
    df = df.sort_values('timestamp')
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    return df

  
  def get_closed_positions_pnl_timeseries(self, proxyWallet: str|None = None, cutoff_year=2024):
    offset = 0
    limit = 50
    seen = set()
    rows = []
    CUTOFF_TS = mytools.year_start_timestamp_utc(cutoff_year)

    while True:
      if offset>100000:
        break
      params = {
        'user': proxyWallet,
        'limit': limit,
        'offset': offset,
        'sortBy': 'TIMESTAMP',
        'sortDirection': 'DESC'
      }
      response = self.client.get(self._build_url(ep.DATA_URL, ep.CLOSED_POSITIONS_ENDPOINT), params = params)
      response.raise_for_status()
      page = response.json()
      if not page:
        break

      timestamps = [x['timestamp'] for x in page]
      stop_after = min(timestamps) < CUTOFF_TS

      for x in page:
        if x['timestamp'] < CUTOFF_TS:
          continue
        
        key = (x['conditionId'], x['asset'], x['outcomeIndex'], x['timestamp'], x['realizedPnl'])
        if key in seen:
          continue
        seen.add(key)
        rows.append(x)

      if stop_after:
        break

      offset += limit

    df = pd.DataFrame(rows)
    if df.empty:
      return df
    
    df = df.sort_values('timestamp')
    df['realizedPnl'] = pd.to_numeric(df['realizedPnl'])
    df['cum_realized_pnl'] = df['realizedPnl'].cumsum()
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    return df
  
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()
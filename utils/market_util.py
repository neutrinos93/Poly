import httpx
from urllib.parse import urljoin
import pandas as pd

import urls_endpoints as ep
import utils.mytools as mytools

class MarketUtil:
  def __init__(self):
    self.client = httpx.Client(http2=True, timeout=30.0)

  def _build_url(self, base_url: str, endpoint: str) -> str:
    return urljoin(base_url, endpoint)
  
  def get_market_by_conditionId(self, conditionId:str) -> dict:
    if not conditionId:
      return {}
    params = {
      "condition_ids": conditionId,
    }
    response = self.client.get(self._build_url(ep.GAMMA_URL,ep.MARKETS_ENDPOINT), params=params)
    response.raise_for_status()
    return response.json()
  
  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def get_event_by_eventId(self, eventId:str) -> dict:
    if not eventId:
      return {}
    params = {}
    response = self.client.get(self._build_url(ep.GAMMA_URL,ep.EVENTS_ENDPOINT+f"/{eventId}"), params=params)
    response.raise_for_status()
    return response.json()
  
  def get_event_tags(self, eventObj:dict, first_n_tags=5) -> list:
    if not eventObj or not isinstance(eventObj, dict):
      return []
    tags = []
    tags_obj = eventObj.get("tags")
    if not tags_obj:
      return []
    for i, tag_obj in enumerate(tags_obj):
      if i >= first_n_tags:
        break
      tags.append(tag_obj.get("label"))
    return tags
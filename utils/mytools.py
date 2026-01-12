from datetime import datetime, timezone

def dt_unix_to_utc(unix_ts: int):
  if unix_ts is not None:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)
  
def year_start_timestamp_utc(year: int) -> int:
  """
  Return Unix timestamp (seconds) for Jan 1st of `year` at 00:00:00 UTC.
  """
  if year < 1970:
      raise ValueError("Year must be >= 1970 for Unix timestamps.")

  dt = datetime(year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
  return int(dt.timestamp())

# def resolve_pagination(client, url, params, timestamp_cutoff_year=None):
#   seen = set()
#   rows = []
#   offset = 0

#   if timestamp_cutoff_year:
#     CUTOFF_TS = year_start_timestamp_utc(timestamp_cutoff_year)
  
#   while True:
#     params['offset'] = offset
#     response = client.get(url, params = params)
#     response.raise_for_status()
#     page = response.json()
#     if not page:
#       break

#     if timestamp_cutoff_year:
#       timestamps = [x['timestamp'] for x in page]
#       stop_after = min(timestamps) < CUTOFF_TS
#   pass
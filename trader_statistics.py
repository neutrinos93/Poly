import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

td = pd.read_csv('trades_data.csv')
md = pd.read_csv('market_info.csv')

td["datetime_utc"] = pd.to_datetime(td["datetime_utc"], utc=True, errors="coerce")
td = td.dropna(subset=["proxyWallet", "datetime_utc", "conditionId"])
td["date_utc"] = td["datetime_utc"].dt.floor("D")
td = td.sort_values(["proxyWallet", "datetime_utc"])

g = td.groupby("proxyWallet", sort=False)

activity = g.agg(
  n_trades=("proxyWallet", "size"),
  n_markets=("conditionId", "nunique"),
  active_days=("date_utc", "nunique"),
  first_trade_dt=("datetime_utc", "min"),
  last_trade_dt=("datetime_utc", "max"),
)

activity["trades_per_active_day"] = activity["n_trades"] / activity["active_days"].replace(0, np.nan)

# Median inter-trade time (seconds)
td["dt_prev"] = g["datetime_utc"].shift(1)
td["intertrade_s"] = (td["datetime_utc"] - td["dt_prev"]).dt.total_seconds()
median_intertrade = td.groupby("proxyWallet")["intertrade_s"].median()
activity["median_intertrade_s"] = median_intertrade

# % days active since first trade
# (active_days) / (number of calendar days between first and last, inclusive)
span_days = (activity["last_trade_dt"].dt.floor("D") - activity["first_trade_dt"].dt.floor("D")).dt.days + 1
activity["pct_days_active"] = activity["active_days"] / span_days.replace(0, np.nan)

# Average trades per market
activity["trades_per_market"] = activity["n_trades"] / activity["n_markets"].replace(0, np.nan)

activity.select_dtypes(include="number").hist(bins=50, figsize=(12, 10))
plt.tight_layout()
plt.show()

######################### Tags analysis
TAG_COLS = [c for c in md.columns if c.startswith("tag_")]
md_tags = md[["conditionId"] + TAG_COLS]
md_tags_long = md_tags.melt(id_vars='conditionId', value_vars=TAG_COLS, value_name="tag")
md_tags_long = md_tags_long.dropna(subset=["tag"])
md_tags_long = md_tags_long[md_tags_long["tag"].astype(str).str.len() > 0]
md_tags_long = md_tags_long[["conditionId", "tag"]].drop_duplicates()

td["size"] = pd.to_numeric(td["size"], errors="coerce").fillna(0.0)

td_tags = td.merge(md_tags_long, on="conditionId", how="left") # trader | market | tag | trade
td_tags = td_tags.dropna(subset=["tag"])

trader_tag_size = td_tags.groupby(["proxyWallet", "tag"], as_index=False).agg(tag_size=("size", "sum")) # trader | tag | size
total_size = trader_tag_size.groupby("proxyWallet").agg(total_size=("tag_size", "sum"))

trader_tag_size = trader_tag_size.merge(total_size, on="proxyWallet", how="left")
trader_tag_size["tag_frac"] = trader_tag_size["tag_size"] / trader_tag_size["total_size"]

n_tags = trader_tag_size.groupby("proxyWallet")["tag"].nunique().rename("n_tags")
favourite_tag = trader_tag_size.sort_values(["proxyWallet", "total_size"]).groupby("proxyWallet").first()["tag"].rename("favourite_tag")

def entropy(p):
  p = p[p>0]
  return -(p * np.log(p)).sum()
tag_entropy = trader_tag_size.groupby("proxyWallet")["tag_frac"].apply(entropy).rename("tag_entropy")
tag_hhi = trader_tag_size.groupby("proxyWallet")["tag_frac"].apply(lambda x: (x**2).sum()).rename("tag_hhi")

tag_features = pd.concat([n_tags, favourite_tag, tag_entropy, tag_hhi], axis=1,).reset_index()

tag_features.select_dtypes(include="number").hist(bins=50, figsize=(12, 10))
plt.tight_layout()
plt.show()
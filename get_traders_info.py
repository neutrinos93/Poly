import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

from utils.select_traders_util import GetTraders
from utils.trader_util import TradersUtil

# Get top traders overall
with GetTraders() as gt:
  top_traders = gt.get_top_traders_by_category(max_number = 100)
  print(len(top_traders))

pnl_frames = []
trades_frames = []

# For each trop trader, get pnl info and list of trades
with TradersUtil() as tu:
  for trader in tqdm(top_traders, desc="Fetching trades"):
    address = trader.get("proxyWallet")
    rank = trader.get("rank")
    pnl = trader.get("pnl")
    vol = trader.get("vol")

    # closed_pnl_series = tp.get_closed_positions_pnl_timeseries(proxyWallet=address, cutoff_year=2024)
    
    user_pnl = tu.get_user_pnl(proxyWallet=address, period='all', frequency='1d')
    if user_pnl is not None and not user_pnl.empty:
      user_pnl = user_pnl.copy()
      user_pnl['trader'] = address
      user_pnl['rank'] = rank
      user_pnl['pnl'] = pnl
      user_pnl['vol'] = vol
      pnl_frames.append(user_pnl)

    user_trades = tu.get_user_trades(proxyWallet=address, cutoff_year=2022)
    if user_trades is not None and not user_trades.empty:
      user_trades = user_trades.copy()
      user_trades['trader'] = address
      user_trades['rank'] = rank
      user_trades['pnl'] = pnl
      user_trades['vol'] = vol
      trades_frames.append(user_trades)

df_user_pnl = pd.concat(pnl_frames, ignore_index=True) if pnl_frames else pd.DataFrame()
df_trades = pd.concat(trades_frames, ignore_index=True) if trades_frames else pd.DataFrame()

df_user_pnl.to_csv('pnl_data.csv', index=False)
df_trades.to_csv('trades_data.csv', index=False)
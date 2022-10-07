from functions import *
import sys

if len(sys.argv) == 2 and (sys.argv[1] == 'minute' or sys.argv[1] =='hour'):
    CSV_FILE = f'/home/gustavo/btc_futures_data/{sys.argv[1]}_funding_rate.csv'
else:
    sys.exit("Program argument must be 'minute' or 'hour', aborting...")


while True:
    try:
        df_usdt = get_df_from_url('fr_usdt', round_5min=False)
        df_token = get_df_from_url('fr_token', round_5min=False)
        df_oi = get_df_from_url('oi', round_5min=False)
    except Exception as e:
        continue

    if df_usdt.empty or df_token.empty or df_oi.empty:
        print('One of the DFs is empty! Retrying...')
        continue
    else:
        df_usdt = get_weighted_mean_funding_rate(df_usdt, df_oi)
        df_token = get_weighted_mean_funding_rate(df_token, df_oi)

        df_usdt['type'] = 'USDT'
        df_token['type'] = 'TOKEN'

        df = pd.concat([df_usdt.tail(1), df_token.tail(1)])
        df.to_csv(CSV_FILE, mode='a', index=True, header=False)

    break

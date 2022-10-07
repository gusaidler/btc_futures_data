import requests
import pandas as pd
import paramiko

# direct link to market data json (from https://www.coinglass.com/funding/BTC -> Inspect -> Network)
URLS= {
    'fr_usdt': "https://fapi.coinglass.com/api/fundingRate/v2/history/chart?symbol=BTC&type=U&interval=h8",
    'fr_token': "https://fapi.coinglass.com/api/fundingRate/v2/history/chart?symbol=BTC&type=C&interval=h8",
    'oi': 'https://fapi.coinglass.com/api/openInterest/v3/chart?symbol=BTC&timeType=0&exchangeName=&currency=USD&type=0'
}


def get_df_from_url(data_type, round_5min=True):
    # data_type can be 'fr_usdt', 'fr_token' or 'oi'
    
    r = requests.get(URLS[data_type])
    data = r.json()
    
    df = pd.DataFrame(data['data']['dataMap'])
    
    if round_5min:
        se_dates = pd.Series(pd.to_datetime(data['data']['dateList'], unit='ms')).dt.round("5min")
    else:
        se_dates = pd.Series(pd.to_datetime(data['data']['dateList'], unit='ms')).dt.round("min")
        
    df['date'] = se_dates.values
    df.set_index('date', inplace=True)
    
    if data_type in ['fr_usdt', 'fr_token']:
        df['mean_funding_rate'] = df.mean(axis=1)
        
        # Add predicted funding rate
        df_pfr = pd.DataFrame(data['data']['frDataMap'])
        df_pfr['mean_predicted_funding_rate'] = df_pfr.mean(axis=1)
        df['mean_predicted_funding_rate'] = df_pfr['mean_predicted_funding_rate'].values
        
    elif data_type == 'oi':
        df['total_open_interest'] = round(df.sum(axis=1))
        
    se_prices = pd.Series(data['data']['priceList'])
    df['price'] = se_prices.values
    
    return df 


def get_weighted_mean_funding_rate(df_fr, df_oi):

    df = (df_fr
    .reset_index()
    .melt(
        id_vars=["date","mean_funding_rate", "mean_predicted_funding_rate", "price"],
        var_name="Exchange", 
        value_name="Funding Rate")
    .sort_values(by='date')
    )

    # Melt exchange columns into single "exchange" column with exchanges as rows
    df_oi_pct = (df_oi
            .reset_index()
            .melt(id_vars=["date", "price"], 
                    var_name="Exchange", 
                    value_name="Open interest")
            .sort_values(by='date')
                )

    # Make sure exchanges are matching in both DFs
    df_oi_pct = df_oi_pct[df_oi_pct['Exchange'].isin(df['Exchange'])]
    df = df[df['Exchange'].isin(df_oi_pct['Exchange'])]

    # Calculate total open interest
    df_oi_pct['total_open_interest'] = df_oi_pct.groupby(['date'])['Open interest'].transform('sum')

    # Get the weight of each exchange based on its open interest
    df_oi_pct['pct_oi'] = (df_oi_pct['Open interest'] / df_oi_pct['total_open_interest'])

    # Get weight of each Exchange based on open interest
    df_exchange_weight = (df_oi_pct
        .tail(df_oi_pct['Exchange']
                .nunique())
        .sort_values(by='pct_oi', ascending=False)[['Exchange', 'pct_oi']]
    ).reset_index(drop=True)

    # Join FR dataframe with OI dataframe 
    df_merge = pd.merge(df, df_exchange_weight, how='left', on='Exchange')

    # Calculate weighted funding rate and mean
    df_merge['weighted_funding_rate'] = df_merge['Funding Rate'] * df_merge['pct_oi']
    df_merge['weighted_mean_funding_rate'] = df_merge.groupby(['date'])['weighted_funding_rate'].transform('sum')

    # Unmelt the dataframe (pivot)
    df_merge = df_merge.pivot(index=['date', 'mean_funding_rate','mean_predicted_funding_rate', 'price','weighted_mean_funding_rate'], columns='Exchange', values='Funding Rate')
    df_merge = df_merge.reset_index()
    df_merge.columns.name = None
    df_merge = df_merge.set_index('date')

    return df_merge
     

def read_csv_sftp(hostname: str, username: str, remotepath: str, *args, **kwargs) -> pd.DataFrame:
    """
    Read a file from a remote host using SFTP over SSH.
    Args:
        hostname: the remote host to read the file from
        username: the username to login to the remote host with
        remotepath: the path of the remote file to read
        *args: positional arguments to pass to pd.read_csv
        **kwargs: keyword arguments to pass to pd.read_csv
    Returns:
        a pandas DataFrame with data loaded from the remote host
    """
    # open an SSH connection
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, username=username, key_filename="C:\\Users\\gusta\\.ssh\\gcp")
    # read the file using SFTP
    sftp = client.open_sftp()
    remote_file = sftp.open(remotepath)
    remote_file.prefetch()
    dataframe = pd.read_csv(remote_file, *args, **kwargs)
    remote_file.close()
    # close the connections
    sftp.close()
    client.close()
    return dataframe
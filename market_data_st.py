from functions import *
import streamlit as st
import datetime
from numerize import numerize
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import warnings
warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="BTC Futures Market Data",
    page_icon="📈",
    layout="wide"
    )

api_key = get_api_key()

@st.cache()
def get_data():

    df_usdt = get_df_from_url('fr_usdt', api_key)
    df_token = get_df_from_url('fr_token', api_key)
    df_oi = get_df_from_url('oi', api_key)

    df_usdt = get_weighted_mean_funding_rate(df_usdt, df_oi)
    df_token = get_weighted_mean_funding_rate(df_token, df_oi)

    df = read_csv_sftp('134.209.242.180', 'gusbot', '/home/gusbot/btc_futures_data/minute_funding_rate.csv')
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert('Europe/Budapest')
    df.set_index('date', inplace=True)

    return df_usdt, df_token, df_oi, df

df_usdt, df_token, df_oi, df = get_data()


if st.button('Refresh'):
    st.runtime.legacy_caching.clear_cache()
    st.experimental_rerun()

st.title('BTC Futures Market Data')


##################################################################
####################### Funding Rate #############################
##################################################################

st.subheader('Funding rate data')

mask_usdt = df['type'] == 'USDT'
mask_token = df['type'] == 'TOKEN'

se_usdt_weighted_mean_funding_rate = df[mask_usdt]['weighted_mean_funding_rate']
se_token_weighted_mean_funding_rate = df[mask_token]['weighted_mean_funding_rate']

se_weighted_mean_funding_rate = round((se_token_weighted_mean_funding_rate + se_usdt_weighted_mean_funding_rate) / 2, 4)
se_weighted_mean_funding_rate.dropna(inplace=True)

# Get Simple Moving Average
sma = 55
se_weighted_mean_funding_rate_sma = se_weighted_mean_funding_rate.rolling(sma).mean()


# Chart config
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    # Resample df
    resample_filter = st.radio(
        "Resample filter:",
        ('5 min', '15 min', '1 hour', '8 hours', '1 day'))

    if resample_filter == '5 min':
        resample_t = '5T'
    elif resample_filter == '15 min':
        resample_t = '15T'
    elif resample_filter == '1 hour':
        resample_t = '1H'
    elif resample_filter == '8 hours':
        resample_t = '8H'
    else:
        resample_t = '1D'

with col2:
    # date filter
    d = st.number_input('Days filter', min_value=1, max_value=300, value=1, step=1)
    date_filter = (datetime.datetime.now() - datetime.timedelta(days = d)).strftime('%Y-%m-%d-%H')


# Instantiate fig with 2 y axis
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add price plot
fig.add_trace(go.Scatter(x=df[mask_usdt][date_filter:].resample(resample_t).first().index, 
                         y=df[mask_usdt][date_filter:]['price'].resample(resample_t).first(),
                         name="Price",
                         mode='lines+markers',
                         ),
                         secondary_y=True,
                         
             )

# Add funding rate plot
fig.add_trace(go.Bar(x=df[mask_usdt][date_filter:].resample(resample_t).first().index, 
                         y=se_weighted_mean_funding_rate[date_filter:].resample(resample_t).first(),
                         name="Weighted Average Funding Rate",
                         marker_color='yellowgreen'            
                        ),
                        secondary_y=False
                        
             )

# Add funding rate SMA plot
fig.add_trace(go.Scatter(x=df[mask_usdt][date_filter:].resample(resample_t).first().index, 
                         y=se_weighted_mean_funding_rate_sma[date_filter:].resample(resample_t).first(),
                         name=f"Weighted Average Funding Rate {sma} SMA",
                         mode='lines',
                         marker_color='red'
                        ),
                        secondary_y=False
                        
             )

# Change axis ranges
#fig.update_yaxes(range=[df[date_filter]['price'].min() - 500 , df['price'][date_filter].max() + 500], secondary_y=True)
#fig.update_yaxes(range=[-0.015, 0.015], secondary_y=False)
#fig.update_yaxes(range=[df[date_filter]['weighted_mean_funding_rate'].min() - 0.01 , df[date_filter]['weighted_mean_funding_rate'].max() + 0.01], secondary_y=False)
#fig.update_yaxes(range=[-0.01, 0.01], secondary_y=False)

# Add horizontal line
fig.add_hline(y=0.01, line_dash="dot", opacity=0.3, annotation_text='Baseline')

# Change color of primary y axis
fig.update_traces(marker_color='black', secondary_y=True)

# Add annotation to current funding rate
fig.add_annotation(x=df[mask_usdt][date_filter:].resample(resample_t).first().index[-1], 
                   y=se_weighted_mean_funding_rate[date_filter:].resample(resample_t).first()[-1],
                   text='<b>{}%</b>'.format(se_weighted_mean_funding_rate.resample(resample_t).first()[-1]),
                   showarrow=False,
                   xanchor='left',
                   yanchor='bottom',
                   )


# Add Title and height
fig.update_layout(legend=dict(y=0.5, font_size=12), 
                  height=800, 
                  width=1200,
                  title='<B>{} Funding Rate Data (last {} days)</B>'.format(resample_filter, d),
                  title_x=0.5,
                  )

# Get current fear and greed
#Image(url= "https://alternative.me/crypto/fear-and-greed-index.png", width=400, height=400)


st.plotly_chart(fig, use_container_width=True)


##################################################################
############################ Stats ###############################
##################################################################
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    # date filter
    d = st.number_input('Days filter', min_value=1, max_value=300, value=90, step=1)
    date_filter = (datetime.datetime.now() - datetime.timedelta(days = d)).strftime('%Y-%m-%d-%H')

st.subheader(f'8h Metrics')

weighted_mean_funding_rate = round((df_usdt['weighted_mean_funding_rate'] + df_token['weighted_mean_funding_rate']) / 2, 4)
mean_predicted_funding_rate = round((df_usdt['mean_predicted_funding_rate'] + df_token['mean_predicted_funding_rate']) / 2, 4)
total_oi = df_oi['total_open_interest']

col1, col2, col3 = st.columns(3)

col1.metric("Weighted Mean Funding Rate", weighted_mean_funding_rate[-1], round(weighted_mean_funding_rate[-1] - weighted_mean_funding_rate[-2], 4))
col1.bar_chart(weighted_mean_funding_rate[date_filter:])

col2.metric("Mean Predicted Funding Rate", mean_predicted_funding_rate[-1], round(mean_predicted_funding_rate[-1] - mean_predicted_funding_rate[-2], 4))
col2.bar_chart(mean_predicted_funding_rate[date_filter:])

col3.metric("Total Open Interest", numerize.numerize(total_oi[-1]), numerize.numerize(round(total_oi[-1] - total_oi[-2], 4)))
col3.bar_chart(df_oi['total_open_interest'][date_filter:])


#!/usr/bin/env python
# coding: utf-8

# In[74]:


import streamlit as st
import pandas as pd
import numpy as np
from shroomdk import ShroomDK
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.ticker as ticker
import numpy as np
import altair as alt
sdk = ShroomDK("7bfe27b2-e726-4d8d-b519-03abc6447728")


# In[75]:


st.title('The Art of District')


# In[76]:


st.markdown('One of the features that is attracting more users on the Near network is NFTs. It seems that the NEAR ecosystem for NFTs is very promising at the moment. But we will get to that later in the results section.')
st.markdown('While the recent downturn has probably dampened price action, the sheer amount of development happening around NFTs in the NEAR ecosystem is pretty amazing. In fact, this 2022 seems to be where the most activity is being concentrated and new NFT marketplaces are emerging. Now we will look at that next.')


# In[77]:


st.markdown('This section has been created with the intention to analyze the NFT scene on NEAR. The idea is to provide an overview of the main NFT sales activity as well as some characteristics about the Near NFT projects and marketplaces, as well as a user analysis.')
st.markdown('Some user behavior metrics, and NFT marketplace and NFT collection metrics are:')
st.write('- Average number of NFT sold per user')
st.write('- Daily number of sales over time')
st.write('- Daily number of sellers over time')
st.write('- Daily NEAR volume of sales over time')
st.write('- Daily average NFT price over time')


# In[78]:


sql = f"""
WITH
  sales as (SELECT
distinct tx_signer as sellers,
count(distinct tx_hash) as n_sales
from near.core.ez_nft_mints
  where block_timestamp>=CURRENT_DATE-INTERVAL '1 MONTH'
group by 1 order by 1 asc 
)
SELECT
round(avg(n_sales),2) as avg_nft_mints_per_user
from sales
"""


# In[79]:


sql_bis = f"""
WITH
  sales as (SELECT
distinct tx_signer as sellers,
count(distinct tx_hash) as n_sales
from near.core.ez_nft_mints
  where block_timestamp between CURRENT_DATE-INTERVAL '2 MONTHS' and CURRENT_DATE-INTERVAL '1 MONTH'
group by 1 order by 1 asc 
)
SELECT
round(avg(n_sales),2) as avg_nft_mints_per_user
from sales
"""


# In[80]:


sql_numbers="""
SELECT
count(distinct tx_receiver) as active_marketplaces,
count(distinct project_name) as active_collections
from near.core.ez_nft_mints
  where block_timestamp>=CURRENT_DATE-INTERVAL '1 MONTH'

"""


# In[81]:


sql2 = f"""
SELECT
trunc(block_timestamp,'day') as date,
tx_receiver as marketplace,
count(distinct tx_signer) as active_users,
count(distinct tx_hash) as sales
from near.core.ez_nft_mints
  where block_timestamp>=CURRENT_DATE-INTERVAL '1 MONTH'
group by 1,2 
  having sales>1 and active_users>1
order by 1 asc 
"""


# In[82]:

st.experimental_memo(ttl=21600)
@st.cache
def compute(a):
    data=sdk.query(a)
    return data

results = compute(sql)
df = pd.DataFrame(results.records)
df.info()
st.subheader('Main NFT activity metrics over the past month')
st.markdown('In this part, it can be seen the average metrics of NFT activity during the past month as well as the current active marketplaces and collections')


# In[83]:

results_bis = compute(sql_bis)
df_bis = pd.DataFrame(results_bis.records)
df_bis.info()


# In[84]:

results2 = compute(sql2)
df2 = pd.DataFrame(results2.records)
df2.info()


# In[85]:

results_num = compute(sql_numbers)
df_num = pd.DataFrame(results_num.records)
df_num.info()


# In[86]:


col1,col2,col3=st.columns(3)
with col1:
    st.metric('Number of mints per user', df['avg_nft_mints_per_user'][0], int(df['avg_nft_mints_per_user'][0]-df_bis['avg_nft_mints_per_user'][0]))
col2.metric('Monthly active marketplaces',df_num.iloc[0][0])
col3.metric('Monthly active collections',df_num.iloc[0][1])


# In[87]:


st.subheader('NFT activity by marketplaces')
st.markdown('In this last part, it can be seen the daily NFT metrics by marketplaces. The active sellers and the number of sales are the main variables taken into account.')


# In[88]:


st.altair_chart(alt.Chart(df2, height=500, width=500)
    .mark_bar()
    .encode(x='sum(sales)', y=alt.Y('marketplace',sort='-x'),color=alt.Color('marketplace', scale=alt.Scale(scheme='dark2')))
    .properties(title='Number of monthly NFT sales by marketplace'))


# In[89]:


st.altair_chart(alt.Chart(df2, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='sales:Q',color=alt.Color('marketplace', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily NFT sales by marketplace'))


# In[90]:


st.altair_chart(alt.Chart(df2, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='active_users:Q',color=alt.Color('marketplace', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily active sellers by marketplace'))


# In[98]:


sql3 = f"""
SELECT
trunc(block_timestamp,'day') as date,
project_name as collection,
count(distinct tx_signer) as active_users,
count(distinct tx_hash) as sales
from near.core.ez_nft_mints
  where block_timestamp>=CURRENT_DATE-INTERVAL '1 MONTH' and project_name is not null
group by 1,2 
  having sales>1 and active_users>1
order by 1 asc 
"""


# In[99]:

results3 = compute(sql3)
df3 = pd.DataFrame(results3.records)
df3.info()


# In[100]:


st.subheader('NFT activity by collection')
st.markdown('In this last part, it can be seen the daily NFT metrics by collection. The active sellers and the number of sales are the main variables taken into account.')


# In[101]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='sum(sales)', y=alt.Y('collection',sort='-x'),color=alt.Color('collection', scale=alt.Scale(range=["#1f77b4", "#ff7f0e", "#c7c7c7"])))
    .properties(title='Number of monthly NFT sales by collection'))


# In[102]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='sales:Q',color=alt.Color('collection', scale=alt.Scale(range=["#1f77b4", "#ff7f0e", "#c7c7c7"])))
    .properties(title='Daily NFT sales by collection'))


# In[103]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='active_users:Q',color=alt.Color('collection', scale=alt.Scale(range=["#1f77b4", "#ff7f0e", "#c7c7c7"])))
    .properties(title='Daily active sellers by collection'))


# In[ ]:





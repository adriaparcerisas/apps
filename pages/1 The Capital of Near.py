#!/usr/bin/env python
# coding: utf-8

# In[183]:


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


# In[184]:


st.title('The Capital of Near')


# In[185]:


st.markdown('The **activity** of a blockchain network is one of the most important things in crypto. Not only because of it is a measure to track the viability of a project but also because of it provides several relevant metrics about the progress of the network and its usage.')


# In[209]:


st.markdown('In this section, we are gonna track the basic metrics registered on NEAR Protocol so far such as:') 
st.write('- Number of transactions') 
st.write('- Number of users')
st.write('- Transaction fees') 
st.write('- Percentage differences over time')
st.write('- Number of smart contracts')
st.write('- Top 10 contract addresses by smart contract interactions')


# In[187]:


sql = f"""
SELECT
COUNT(DISTINCT TX_HASH) as number_transactions,
COUNT(DISTINCT TX_SIGNER) as unique_users
FROM near.core.fact_transactions
WHERE block_timestamp::date < CURRENT_DATE and block_timestamp::date>=CURRENT_DATE-INTERVAL '1 MONTH'
"""


# In[192]:

st.experimental_memo(ttl=21600)
def compute(a):
    data=sdk.query(a)
    return data

results = compute(sql)
df = pd.DataFrame(results.records)
df.info()
st.subheader('Near activity metrics over the past month')
st.markdown('In this first part, we can take a look at the Capital metrics on Near, where can see how the number of transactions done across the protocol, the evolution of active users, as well as the transaction fees paid in NEAR which usually follows a similar strucutre then the number of transactions.')


# In[193]:


sql_bis = f"""
SELECT
COUNT(DISTINCT TX_HASH) as number_transactions,
COUNT(DISTINCT TX_SIGNER) as unique_users
FROM near.core.fact_transactions
WHERE block_timestamp::date < CURRENT_DATE-INTERVAL '1 MONTH' and block_timestamp::date>=CURRENT_DATE-INTERVAL '2 MONTHS'
"""


# In[194]:

results_bis = compute(sql_bis)
df_bis = pd.DataFrame(results_bis.records)
df_bis.info()


# In[195]:


col1,col2 =st.columns(2)
with col1:
    st.metric('Number of transactions over the past month', df['number_transactions'][0], int(df['number_transactions'][0]-df_bis['number_transactions'][0]))
col2.metric('Number of unique users over the past month', df['unique_users'][0], int(df['unique_users'][0]-df_bis['unique_users'][0]))


# In[196]:


sql2 = f"""
WITH swaps AS (
  SELECT 
  block_timestamp,
  logs[0] AS log, 
  substring(log, 1, CHARINDEX(' wrap.near for', log)) AS first_part, 
  regexp_replace(first_part, '[^0-9]', '')/pow(10, 24) AS near_amount,    
  substring(log, CHARINDEX('for', log), 100) AS second_part,
  substring(second_part, 1, CHARINDEX('dac', second_part)-2) AS second_part_amount,
  regexp_replace(second_part_amount, '[^0-9]', '')/pow(10,6) AS usdt_amount
  FROM near.core.fact_receipts
  WHERE logs[0] like 'Swapped % wrap.near for % dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
  and block_timestamp::date>=CURRENT_DATE-INTERVAL '1 MONTH'
)
SELECT
*,
ROUND((active_users - active_users_prev)/active_users_prev * 100,2) AS pct_diff_active,
ROUND((number_transactions - number_transactions_prev)/number_transactions_prev * 100,2) AS pct_diff_transactions,
ROUND((txn_fees_usd - txn_fees_prev)/txn_fees_prev * 100,2) AS pct_diff_txn_fees
FROM
(
SELECT
*,
LAG(active_users,1) OVER (ORDER BY date) active_users_prev,
LAG(number_transactions,1) OVER (ORDER BY date) number_transactions_prev,
LAG(txn_fees_usd) OVER (ORDER BY date) txn_fees_prev
FROM
(
SELECT
  tr.*,
  txn_fees*np.price AS txn_fees_usd
  FROM
    (
    SELECT
    DATE_TRUNC('day',block_timestamp::date) AS date,
    DATE_TRUNC('day', block_timestamp::date - 1) AS date_prev,
    COUNT(DISTINCT TX_SIGNER) AS active_users,
    COUNT(DISTINCT TX_HASH) AS number_transactions,
    SUM(TRANSACTION_FEE/POW(10,24)) AS txn_fees
    FROM near.core.fact_transactions AS tr
    WHERE date < CURRENT_DATE
    GROUP BY date, date_prev
    ) AS tr
  INNER JOIN (
  SELECT
  DATE_TRUNC('day',block_timestamp) AS date, 
  avg(usdt_amount)/avg(near_amount) AS price
  FROM swaps
  GROUP BY date
  ) AS np
  ON tr.date=np.date
)
)
ORDER BY date DESC
"""


# In[197]:

results = compute(sql2)
df2 = pd.DataFrame(results.records)
df2.info()
#st.markdown('Total number of unique users on Near so far')
#st.dataframe(df2)


# In[198]:


base=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkblue').encode(y=alt.Y('pct_diff_active:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='#0068c9',opacity=0.5).encode(y='active_users:Q')
st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily Near active users',width=600))


# In[199]:


base=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkgreen').encode(y=alt.Y('pct_diff_transactions:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='green',opacity=0.5).encode(y='number_transactions:Q')

st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily Near transactions executed',width=600))


# In[200]:


base=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkred').encode(y=alt.Y('pct_diff_txn_fees:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='red',opacity=0.5).encode(y='txn_fees:Q')

st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily fees paid in NEAR',width=600))


# In[201]:


st.subheader("Near deployed contracts")
st.markdown('To close this section, here it can be seen the number of smart contracts created over time, both successful and failed creations.')
sql3='''
SELECT
    date_trunc('day', call.block_timestamp) as date,
  case when split(split(rc.status_value,':')[0],'{')[1] ilike '%Failure%' then 'Fail execution'
  else 'Successful execution' end as type,
    COUNT(DISTINCT tr.TX_RECEIVER) as smart_contracts,
  sum(smart_contracts) over (partition by type order by date) as cum_smart_contracts
FROM near.core.fact_actions_events_function_call call
INNER JOIN near.core.fact_transactions tr
ON call.TX_HASH = tr.TX_HASH
INNER JOIN near.core.fact_receipts as rc
ON tr.TX_HASH=rc.TX_HASH
    WHERE ACTION_NAME = 'FunctionCall'
    AND METHOD_NAME <> 'new'
    AND date >=CURRENT_DATE-INTERVAL '1 MONTH'
group by 1,2 order by 1 asc,2 desc
'''


# In[202]:

results = compute(sql3)
df3 = pd.DataFrame(results.records)


# In[203]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='date:N', y='smart_contracts:Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily smart contracts used'))


# In[204]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='date:N', y='cum_smart_contracts:Q',color=alt.Color('type', scale=alt.Scale(scheme='dark2')))
    .properties(title='Total smart contracts used'))


# In[205]:


sql4='''
with contracts as (select 
events.block_timestamp as date,
receiver_id as receiver,
row_number() over (partition by receiver_id order by events.block_timestamp asc) as nums
from near.core.fact_actions_events events
join near.core.fact_receipts rec
on rec.tx_hash = events.tx_hash
where action_name ilike '%DEPLOYCONTRACT%'
  AND events.block_timestamp < CURRENT_DATE and events.block_timestamp>=CURRENT_DATE-interval '1 MONTH'
group by receiver, date
qualify nums = 1)

select
count(distinct tx_hash) as transactions,
tx_receiver as contract_address
  from near.core.fact_transactions tr
join (select date, receiver, nums
     from contracts) as contract
     on contract.receiver = tr.tx_receiver
where tr.block_timestamp < CURRENT_DATE and tr.block_timestamp>= CURRENT_DATE-interval '1 MONTH'
group by tx_receiver
order by transactions desc
limit 10
'''


# In[206]:

results = compute(sql4)
df4 = pd.DataFrame(results.records)


# In[207]:


st.altair_chart(alt.Chart(df4, height=500, width=500)
    .mark_bar()
    .encode(x='transactions', y=alt.Y('contract_address',sort='-x'),color=alt.Color('transactions', scale=alt.Scale(scheme='dark2')))
    .properties(title='Top 10 most used contracts'))


# In[ ]:





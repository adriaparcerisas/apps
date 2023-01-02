#!/usr/bin/env python
# coding: utf-8

# In[32]:


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


# In[33]:


st.title('The Financial District')


# In[34]:


st.markdown('In this world, every traveller needs adequate funds to fully enjoy the city. It is not enough to know which places to visit, but you also need to take a good look at how to manage your savings so that you can enjoy your trip with peace of mind. For this reason, another main pillar of Near is **the Financial District**.')


# In[35]:


st.markdown('In this section, we are gonna take a deep look on the financial metrics around Near Protocol. The main metrics to be analyzed are:') 
st.write('- Differences between small fishes users staking metrics and whales staking') 
st.write('- Number of transactions')
st.write('- Volume staked') 
st.write('- Top 25 whales on Near')
st.write('- Top 10 tokens most used')
st.write('- Top 10 tokens most traded')


# In[36]:


sql = f"""
with multi_snd  as (
select  
  tx_hash,
  logs,
  split_part(value,'Sending ',2) a,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount,
  split_part(a,'@',2)::string as receiver 
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL
  and block_timestamp::date < CURRENT_DATE
  )
  , multi_send as (
  SELECT
  a.tx_hash,
  tx_signer as sender,
  receiver,
  amount
 FROM multi_snd a join near.core.fact_transactions b USING(tx_hash)
  
)  
,multi_receive as (
  select 
  tx_hash,
  split_part(value,'Sending ',2) a,  
  split_part(a,'@',2) as receiver,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
  
)         
, trans_receive as ( 
select   
   TX_HASH,
   TX_RECEIVER as receiver,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status = true
  
)
  , trans_send as ( 
select   
   TX_HASH,
   TX_SIGNER as sender,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status=true
 
)
,receive as ( 
select receiver,amount from trans_receive
  UNION ALL 
select receiver,amount from multi_receive
  
)
,send as ( 
select sender , amount from trans_send
  UNION ALL 
select sender , amount from multi_send  
)
  ,send_vol as (
  select 
    sender,
    sum(amount) as send_amount
  from send
  group by 1
  
)
,receive_vol as (
  select 
    receiver,
    sum(amount) as receive_amount
  from receive
  group by 1
  
)
  , top1000_Fishes as (
  select top 1000
  receiver,
  (receive_amount - send_amount) as balance
from receive_vol a
  join send_vol b on  a.receiver = b.sender
  HAVING balance > 1000
order by 2 ASC
  )

--------------bridge_erc20_to_near
--------------bridge_erc20_to_near

, bridge_erc20_to_near as (
  select el.block_timestamp::date as date,
    event_inputs:accountId as receiver
  from ethereum.core.fact_event_logs el
  left join ethereum.core.dim_contracts_extended ec on el.event_inputs:token=ec.contract_address  
  where ORIGIN_TO_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f' -- Near: Rainbow bridge
  and CONTRACT_NAME = 'ERC20Locker'
  and EVENT_NAME = 'Locked'
  and ORIGIN_FUNCTION_SIGNATURE = '0x0889bfe7'
  and symbol is not null
  and el.block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
  -- and receiver like '%.near'

) 

-------------bridge_from_aurora_to_near
-------------bridge_from_aurora_to_near

, bridge_from_aurora_to_near as (
  select 
    BLOCK_TIMESTAMP::date as date,
    tx:receipt[1]:outcome:executor_id as contract_in_near,
    tx:receipt[1]:outcome:logs[0] as log,  
    substring(log, CHARINDEX('from aurora to ', log)+15, len(log)) as receivers
  from near.core.fact_transactions n_t  
  where tx_signer = 'relay.aurora'
  and tx_receiver = 'aurora'
  and (contract_in_near like '%.factory.bridge.near' -- ERC20-Assets
        or contract_in_near like 'aurora' -- ETH
        or contract_in_near like 'wrap.near' -- NEAR  
       )
  and tx:receipt[1]:outcome:logs[0] like 'Transfer % from aurora %'
    and receivers ilike '%.near'
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date   
)
, eth_bridgers as (
    select
  date as bridge_date,
  'Ethereum => Near' as bridge,
  a.receiver
from bridge_erc20_to_near a join top1000_Fishes b on a.receiver = b.receiver  -- join to Top 1000 Fishes
  group by 1,2,3
  )
  , aurora_bridg as (
 select 
  date as bridg_date,
  'Aurora => Near' as bridg,
   a.receivers
from bridge_from_aurora_to_near a join top1000_Fishes b on a.receivers = b.receiver  -- join to Top 1000 Fishes
 group by 1,2,3
)  
  , first_tx as (
  SELECT
  BLOCK_TIMESTAMP::date as tx_date,
  TX_HASH,
  TX_SIGNER
  from near.core.fact_transactions a join eth_bridgers b on a.TX_SIGNER = b.receiver
  WHERE tx_date >= bridge_date   
  
  UNION
  
  SELECT
  BLOCK_TIMESTAMP::date as tx_date,
  TX_HASH,
  TX_SIGNER
  from near.core.fact_transactions a join aurora_bridg b on a.TX_SIGNER = b.receivers
  WHERE tx_date >= bridg_date 
  )
------------------------------*** Staking *** ---------------------------
------------------------------*** Staking *** ---------------------------

 , stake_tx AS (
   SELECT tx_hash
FROM near.core.fact_actions_events_function_call
  WHERE method_name IN ('deposit_and_stake', 'unstake', 'unstake_all') 
   )
   , prs_log as (   
    SELECT 
  block_timestamp,
  tx_hash,
  tx_receiver, 
  tx_signer,
  tx:receipt[0]:outcome:logs as logs,
  SUBSTRING(logs, CHARINDEX('Total number of shares ', logs), LEN(logs)) as Validator_shares,
  TRIM(REGEXP_REPLACE(Validator_shares, '[a-z/-/A-z/./#/*"]', '')) as raw_staked,
  try_to_numeric(raw_staked) as amount
FROM near.core.fact_transactions
  WHERE tx_hash IN (SELECT tx_hash from stake_tx) 
  and tx_hash IN (SELECT TX_hash from first_tx) 
  and tx_signer IN (SELECT TX_SIGNER from first_tx) 
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date)
SELECT
    block_timestamp::date as date,
    count(tx_hash) as tx_count,
    count(tx_signer) as staker_count,
    sum(amount/1e27) as staked_vol, 
    sum(tx_count) over (order by date) as cum_tx_count,
    sum(staked_vol) over (order by date) as cum_staked_vol
FROM prs_log
  WHERE tx_receiver like '%pool%'
  and amount is not null 
group by 1
  order by 1
"""


# In[37]:

st.experimental_memo(ttl=21600)
def compute(a):
    data=sdk.query(a)
    return data

results = compute(sql)
df = pd.DataFrame(results.records)
df.info()
st.subheader('Main Financial metrics over the past month')
st.markdown('In this first part, we can take a look at the Financial metrics on Near, where it can be seen how the number of transactions and the Near staaked done across the protocol. As well, it cna be seen a comparison of these metrics against each type of users: small fishes vs whales.')


# In[38]:


sql2="""
with multi_snd  as (
select  
  tx_hash,
  logs,
  split_part(value,'Sending ',2) a,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount,
  split_part(a,'@',2)::string as receiver 
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
  )
  , multi_send as (
  SELECT
  a.tx_hash,
  tx_signer as sender,
  receiver,
  amount
 FROM multi_snd a join near.core.fact_transactions b USING(tx_hash)
  where b.block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
  
)  
,multi_receive as (
  select 
  tx_hash,
  split_part(value,'Sending ',2) a,  
  split_part(a,'@',2) as receiver,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL
  
)         
, trans_receive as ( 
select   
   TX_HASH,
   TX_RECEIVER as receiver,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status = true
  
)
  , trans_send as ( 
select   
   TX_HASH,
   TX_SIGNER as sender,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status=true
 
)
,receive as ( 
select receiver,amount from trans_receive
  UNION ALL 
select receiver,amount from multi_receive
  
)
,send as ( 
select sender , amount from trans_send
  UNION ALL 
select sender , amount from multi_send  
)
  ,send_vol as (
  select 
    sender,
    sum(amount) as send_amount
  from send
  group by 1
  
)
,receive_vol as (
  select 
    receiver,
    sum(amount) as receive_amount
  from receive
  group by 1
  
)
  , top100_whale as (
  select top 1000
  receiver,
  (receive_amount - send_amount) as balance
from receive_vol a
  join send_vol b on  a.receiver = b.sender
  -- HAVING balance > 100000
order by 2 DESC
  )

--------------bridge_erc20_to_near
--------------bridge_erc20_to_near

, bridge_erc20_to_near as (
  select el.block_timestamp::date as date,
    event_inputs:accountId as receiver
  from ethereum.core.fact_event_logs el
  left join ethereum.core.dim_contracts_extended ec on el.event_inputs:token=ec.contract_address  
  where ORIGIN_TO_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f' -- Near: Rainbow bridge
  and CONTRACT_NAME = 'ERC20Locker'
  and EVENT_NAME = 'Locked'
  and ORIGIN_FUNCTION_SIGNATURE = '0x0889bfe7'
  and symbol is not null
  and el.block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
  -- and receiver like '%.near'

) 

-------------bridge_from_aurora_to_near
-------------bridge_from_aurora_to_near

, bridge_from_aurora_to_near as (
  select 
    BLOCK_TIMESTAMP::date as date,
    tx:receipt[1]:outcome:executor_id as contract_in_near,
    tx:receipt[1]:outcome:logs[0] as log,  
    substring(log, CHARINDEX('from aurora to ', log)+15, len(log)) as receivers
  from near.core.fact_transactions n_t  
  where tx_signer = 'relay.aurora'
  and tx_receiver = 'aurora'
  and (contract_in_near like '%.factory.bridge.near' -- ERC20-Assets
        or contract_in_near like 'aurora' -- ETH
        or contract_in_near like 'wrap.near' -- NEAR  
       )
  and tx:receipt[1]:outcome:logs[0] like 'Transfer % from aurora %'
    and receivers ilike '%.near'
   
)
, eth_bridgers as (
    select
  date as bridge_date,
  'Ethereum => Near' as bridge,
  a.receiver
from bridge_erc20_to_near a join top100_whale b on a.receiver = b.receiver  -- join to Top 1000 Whales
  group by 1,2,3
  )
  , aurora_bridg as (
 select 
  date as bridg_date,
  'Aurora => Near' as bridg,
   a.receivers
from bridge_from_aurora_to_near a join top100_whale b on a.receivers = b.receiver  -- join to Top 1000 Whales
 group by 1,2,3
)  
  , first_tx as (
  SELECT
  BLOCK_TIMESTAMP::date as tx_date,
  TX_HASH,
  TX_SIGNER
  from near.core.fact_transactions a join eth_bridgers b on a.TX_SIGNER = b.receiver
  WHERE tx_date >= bridge_date   
  
  UNION
  
  SELECT
  BLOCK_TIMESTAMP::date as tx_date,
  TX_HASH,
  TX_SIGNER
  from near.core.fact_transactions a join aurora_bridg b on a.TX_SIGNER = b.receivers
  WHERE tx_date >= bridg_date 
  )
------------------------------*** Staking *** ---------------------------
------------------------------*** Staking *** ---------------------------

 , stake_tx AS (
   SELECT tx_hash
FROM near.core.fact_actions_events_function_call
  WHERE method_name IN ('deposit_and_stake', 'unstake', 'unstake_all') 
   )
   , prs_log as (   
    SELECT 
  block_timestamp,
  tx_hash,
  tx_receiver, 
  tx_signer,
  tx:receipt[0]:outcome:logs as logs,
  SUBSTRING(logs, CHARINDEX('Total number of shares ', logs), LEN(logs)) as Validator_shares,
  TRIM(REGEXP_REPLACE(Validator_shares, '[a-z/-/A-z/./#/*"]', '')) as raw_staked,
  try_to_numeric(raw_staked) as amount
FROM near.core.fact_transactions
  WHERE tx_hash IN (SELECT tx_hash from stake_tx) 
  and tx_hash IN (SELECT TX_hash from first_tx) 
  and tx_signer IN (SELECT TX_SIGNER from first_tx) 
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
)
SELECT
    block_timestamp::date as date,
    count(tx_hash) as tx_count,
    count(tx_signer) as staker_count,
    sum(amount/1e27) as staked_vol, 
    sum(tx_count) over (order by date) as cum_tx_count,
    sum(staked_vol) over (order by date) as cum_staked_vol
FROM prs_log
  WHERE tx_receiver like '%pool%'
  and amount is not null 
group by 1
  order by 1
"""


# In[39]:

results2 = compute(sql2)
df2 = pd.DataFrame(results2.records)
df2.info()


# In[40]:


base=alt.Chart(df).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line=base.mark_line(color='darkblue').encode(y=alt.Y('cum_tx_count:Q', axis=alt.Axis(grid=True)))
bar=base.mark_bar(color='#0068c9',opacity=0.5).encode(y='tx_count:Q')


# In[41]:


base2=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line2=base2.mark_line(color='darkblue').encode(y=alt.Y('cum_tx_count:Q', axis=alt.Axis(grid=True)))
bar2=base2.mark_bar(color='#0068c9',opacity=0.5).encode(y='tx_count:Q')


# In[42]:


col1,col2=st.columns(2)
with col1:
    st.altair_chart((bar + line).resolve_scale(y='independent').properties(title='Daily small fishes transactions',width=300))
col2.altair_chart((bar2 + line2).resolve_scale(y='independent').properties(title='Daily whales transactions',width=300))


# In[43]:


base3=alt.Chart(df).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line3=base3.mark_line(color='darkgreen').encode(y=alt.Y('cum_staked_vol:Q', axis=alt.Axis(grid=True)))
bar3=base3.mark_bar(color='green',opacity=0.5).encode(y='staked_vol:Q')



# In[44]:


base4=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line4=base4.mark_line(color='darkgreen').encode(y=alt.Y('cum_staked_vol:Q', axis=alt.Axis(grid=True)))
bar4=base4.mark_bar(color='green',opacity=0.5).encode(y='staked_vol:Q')


# In[45]:


col3,col4=st.columns(2)

with col3:
    st.altair_chart((bar3 + line3).resolve_scale(y='independent').properties(title='Daily small fishes volume staked (NEAR)',width=300))
col4.altair_chart((bar4 + line4).resolve_scale(y='independent').properties(title='Daily small fishes volume staked (NEAR)',width=300))


# In[46]:


sql3="""
with multi_snd  as (
select  
  tx_hash,
  logs,
  split_part(value,'Sending ',2) a,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount,
  split_part(a,'@',2)::string as receiver 
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL  
  )
  , multi_send as (
  SELECT
  a.tx_hash,
  tx_signer as sender,
  receiver,
  amount
 FROM multi_snd a join near.core.fact_transactions b USING(tx_hash)
  
)  
,multi_receive as (
  select 
  tx_hash,
  split_part(value,'Sending ',2) a,  
  split_part(a,'@',2) as receiver,
  try_to_numeric(split_part(a,' yNEAR',1))/1e24 as amount
  from near.core.fact_receipts, lateral flatten(input => logs) b
  where RECEIVER_ID = 'multisender.app.near'
  and STATUS_VALUE ilike '%SuccessValue%'
  and logs[0] is not NULL
  
)         
, trans_receive as ( 
select   
   TX_HASH,
   TX_RECEIVER as receiver,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status = true
  
)
  , trans_send as ( 
select   
   TX_HASH,
   TX_SIGNER as sender,
   DEPOSIT/1e24 as amount
   from near.core.fact_transfers
where status=true
 
)
,receive as ( 
select receiver,amount from trans_receive
  UNION ALL 
select receiver,amount from multi_receive
  
)
,send as ( 
select sender , amount from trans_send
  UNION ALL 
select sender , amount from multi_send  
)
  ,send_vol as (
  select 
    sender,
    sum(amount) as send_amount
  from send
  group by 1
  
)
,receive_vol as (
  select 
    receiver,
    sum(amount) as receive_amount
  from receive
  group by 1
  
)
  
select top 25
  receiver,
  (receive_amount - send_amount) as balance
from receive_vol a
  join send_vol b on  a.receiver = b.sender
order by 2 DESC
"""


# In[47]:

results3 = compute(sql3)
df3 = pd.DataFrame(results3.records)


# In[48]:


st.altair_chart(alt.Chart(df3, height=500, width=500)
    .mark_bar()
    .encode(x='balance', y=alt.Y('receiver',sort='-x'),color=alt.Color('balance', scale=alt.Scale(scheme='dark2')))
    .properties(title='Top 25 Near whales'))


# In[56]:


st.subheader("Most popular tokens on Near")
st.markdown('To close this section, here it can be seen the top tokens most used by Near users and the top most swapped.')

sql4='''
WITH near_token_address AS (
   select 'wrap.near' as contract_address, 'NEAR' as symbol, 24 as decimal union
  select 'meta-pool.near' as contract_address, 'stNEAR' as symbol, 24 as decimal union
  select 'usn' as contract_address, 'USN' as symbol, 18 as decimal union
  select 'sol.token.a11bd.near' as contract_address, 'SOL' as symbol, 18 as decimal union
  select 'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near' as contract_address, 'AURORA' as symbol, 18 as decimal union
  select 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' as contract_address, 'USDT' as symbol, 6 as decimal union -- stable
  select 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' as contract_address, 'USDC' as symbol, 6 as decimal union -- stable
  select '6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near' as contract_address, 'DAI' as symbol, 18 as decimal union -- stable
  select '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near' as contract_address, 'WBTC' as symbol, 8 as decimal union 
  select 'c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.factory.bridge.near' as contract_address, 'WETH' as symbol, 18 as decimal union
  select 'e99de844ef3ef72806cf006224ef3b813e82662f.factory.bridge.near' as contract_address, 'UMINT' as symbol, 18 as decimal union
  select 'aurora' as contract_address, 'WETH' as symbol, 18 as decimal union
  select 'token.paras.near' as contract_address, 'Paras' as symbol, 18 as decimal union
  select 'token.jumbo_exchange.near' as contract_address, 'JUMBO' as symbol, 18 as decimal union
  select 'token.pembrock.near' as contract_address, 'PEM' as symbol, 18 as decimal union
  select 'token.v2.ref-finance.near' as contract_address, 'REF' as symbol, 18 as decimal union
  select 'xtoken.ref-finance.near' as contract_address, 'xREF' as symbol, 18 as decimal union
  select 'linear-protocol.near' as contract_address, 'LINEAR' as symbol, 24 as decimal union
  select 'abr.a11bd.near' as contract_address, 'ABR' as symbol, 24 as decimal union
  select 'token.burrow.near' as contract_address, 'BRRR' as symbol, 18 as decimal union
  select 'berryclub.ek.near' as contract_address, 'BANANA' as symbol, 18 as decimal union
  select 'farm.berryclub.ek.near' as contract_address, 'CUCUMBER' as symbol, 18 as decimal union
  select 'token.skyward.near' as contract_address, 'Skyward' as symbol, 18 as decimal union
  select 'token.shrm.near' as contract_address, 'SHRM' as symbol, 18 as decimal union
  select 'token.cheddar.near' as contract_address, 'Cheddar' as symbol, 24 as decimal union
  select 'v3.oin_finance.near' as contract_address, 'nUSDO' as symbol, 8 as decimal union -- stable
  select 'cusd.token.a11bd.near' as contract_address, 'cUSD' as symbol, 24 as decimal union -- stable
  select 'celo.token.a11bd.near' as contract_address, 'CELO' as symbol, 24 as decimal union -- stable
  select 'marmaj.tkn.near' as contract_address, 'marma' as symbol, 18 as decimal union 
  select 'hak.tkn.near' as contract_address, 'HAK' as symbol, 18 as decimal union 
  select 'pixeltoken.near' as contract_address, 'PXT' as symbol, 6 as decimal union 
  select 'utopia.secretskelliessociety.near' as contract_address, 'UTO' as symbol, 8 as decimal union 
  select 'meta-token.near' as contract_address, 'META' as symbol, 18 as decimal union
  select 'socialmeet.tkn.near' as contract_address, 'SOCIALMEET' as symbol, 18 as decimal union
  select 'meritocracy.tkn.near' as contract_address, 'MERITOCRACY' as symbol, 18 as decimal union
  select 'rekt.tkn.near' as contract_address, 'REKT' as symbol, 18 as decimal union
  select 'deip-token.near' as contract_address, 'DEIP' as symbol, 18 as decimal union
  select 'dbio.near' as contract_address, 'DBIO' as symbol, 18 as decimal union
  select 'myriadcore.near' as contract_address, 'MYRIA' as symbol, 18 as decimal union
  select 'a4ef4b0b23c1fc81d3f9ecf93510e64f58a4a016.factory.bridge.near' as contract_address, '1MIL' as symbol, 18 as decimal union
  select '111111111117dc0aa78b770fa6a738034120c302.factory.bridge.near' as contract_address, '1INCH' as symbol, 18 as decimal union
  select '3ea8ea4237344c9931214796d9417af1a1180770.factory.bridge.near' as contract_address, 'FLX' as symbol, 18 as decimal union
  select '9aeb50f542050172359a0e1a25a9933bc8c01259.factory.bridge.near' as contract_address, 'OIN' as symbol, 8 as decimal union
  select '514910771af9ca656af840dff83e8264ecf986ca.factory.bridge.near' as contract_address, 'LINK' as symbol, 18 as decimal union
  select 'd9c2d319cd7e6177336b0a9c93c21cb48d84fb54.factory.bridge.near' as contract_address, 'HAPI' as symbol, 18 as decimal union
  select '52a047ee205701895ee06a375492490ec9c597ce.factory.bridge.near' as contract_address, 'PULSE' as symbol, 18 as decimal union
  select '4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near' as contract_address, 'WOO' as symbol, 18 as decimal union
  select 'de30da39c46104798bb5aa3fe8b9e0e1f348163f.factory.bridge.near' as contract_address, 'Gitcoin' as symbol, 18 as decimal union
  select 'f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near' as contract_address, 'Octopus Network' as symbol, 18 as decimal 
),
transaction_logs AS (
  SELECT
  block_timestamp,
  tx_hash,
  receiver_id,
  regexp_substr(status_value, 'Success') as reg_success, 
  logs as logs1
FROM near.core.fact_receipts
  WHERE reg_success IS NOT NULL 
  AND block_timestamp::date < CURRENT_DATE
  -- AND receiver_id = 'v2.ref-finance.near' 
),
swaps_unlabelled AS (
  SELECT
  block_timestamp,
  tx_hash,
  receiver_id,
  value,
  split(value, ' ') as split,
  split[0]::string as swap,
  split[1] as amount_in_raw,
  replace(split[2], ',') as token_in_raw,
  split[4] as amount_out_raw,
  replace(split[5], ',') as token_out_raw
FROM transaction_logs,
  table(flatten(input => logs1))
),
swaps AS (
  SELECT
  block_timestamp,
  date(block_timestamp) as date_swap,
  tx_hash,
  receiver_id,
  tx_signer,
  try_to_numeric(amount_in_raw::string) as numeric_check0,
  try_to_numeric(amount_out_raw::string) as numeric_check1,
--  token and amount in 
  nvl(a.symbol, token_in_raw) as token_in,
  div0((try_to_numeric(amount_in_raw::string)),pow(10, a.decimal)) as amount_in,
-- token and amount out 
  nvl(b.symbol, token_out_raw) as token_out,
  div0((try_to_numeric(amount_out_raw::string)),pow(10,b.decimal)) as amount_out
FROM swaps_unlabelled
  LEFT JOIN near_token_address a ON a.contract_address = token_in_raw
  LEFT JOIN near_token_address b ON b.contract_address = token_out_raw 
  LEFT JOIN (SELECT tx_signer, tx_hash as tx_hash1 FROM near.core.fact_transactions) ON tx_hash1 = tx_hash -- attach tx_signer for swaps 
WHERE swap = 'Swapped'
),
ORACLE_PRICE AS ( -- Since NEAR oracle is limited we need to estimate prices from stablecoin swaps 
--  use swap volume to estimate 
  SELECT 
  date(block_timestamp) as date,
  token_in as oracle_symbol,
  div0(sum(amount_out),sum(amount_in)) as price_usd1
FROM swaps 
  WHERE token_out IN ('USDT', 'USN', 'USDT', 'DAI') AND amount_out > 10 
GROUP BY 1,2
UNION
-- Prices from Ethereum oracle due to the absence of USD-WBTC/WETH pairs
SELECT 
  date(hour) as date, 
  symbol as oracle_symbol,
  avg(price) as price_usd1 
FROM ethereum.core.fact_hourly_token_prices 
  WHERE hour > '2021-08-01' 
  AND symbol IN ('WETH', 'WBTC', 'USDC', 'USDT', 'DAI')
  GROUP BY 1,2
-- Use prices updated NEAR oracle prices 
UNION 
  SELECT 
  date(timestamp) as date_price,
  upper(symbol),
  avg(price_usd) as usd_price
FROM near.core.fact_prices
  GROUP BY 1,2
),
ORACLE_PRICES AS (
  SELECT 
  date,
  oracle_symbol,
  avg(price_usd1) as price_usd
FROM ORACLE_PRICE
  GROUP BY 1,2
),
swaps_usd AS (
  SELECT 
  date_swap, 
  tx_hash, 
  receiver_id, 
  tx_signer,
  token_in, 
  amount_in,
  a.price_usd*amount_in as amount_in_usd, 
  token_out, 
  amount_out,
  b.price_usd*amount_out as amount_out_usd,
  case when token_in IN ('USDT', 'USDC', 'DAI', 'USN') then amount_in end as stable_in_vol,
  case when token_out IN ('USDT', 'USDC', 'DAI', 'USN') then amount_out end as stable_out_vol,
  COALESCE(amount_in_usd, amount_out_usd, stable_in_vol, stable_out_vol) as swap_volume_usd -- if there's oracle price take price > if no, estiminate from stable volumes (assume stable = 1)
FROM swaps 
  LEFT JOIN ORACLE_PRICES a ON date_swap = a.date AND a.oracle_symbol = upper(token_in)
  LEFT JOIN ORACLE_PRICES b ON date_swap = b.date AND b.oracle_symbol = upper(token_out)
),
swaps_final AS (
  SELECT 
  date_swap as date,
  tx_hash,
  tx_signer as trader,
  'In' as direction,
  token_In as token,
  amount_in as amount,
  nvl(amount_in_usd, swap_volume_usd) as amount_usd
FROM swaps_usd
UNION  
SELECT 
  date_swap as date,
  tx_hash,
  tx_signer as trader,
  'Out' as direction,
  token_out as token,
  amount_out as amount,
  nvl(amount_out_usd, swap_volume_usd) as amount_usd
FROM swaps_usd
),
ALL_TIME AS (
  SELECT
  token,
  count(distinct case when direction = 'In' then trader end) as sellers,
  count(distinct case when direction = 'Out' then trader end) as buyers,
  count(distinct trader) as traders,
  round(sum( case when direction = 'In' then amount end),1) as selling_volume,
  round(sum( case when direction = 'Out' then amount end),1)  as buying_volume,
  round(sum( case when direction = 'In' then amount_usd end),1) as selling_volume_usd,
  round(sum( case when direction = 'Out' then amount_usd end),1) as buying_volume_usd,
  round(div0(buying_volume,selling_volume),3) as Buy_Sell_Ratio,
  round(sum(amount),1) as volume,
  round(sum(amount_Usd),1) as volume_usd,
  count(tx_hash) as swaps
  FROM swaps_final 
GROUP BY token 
)
  SELECT *
FROM ALL_TIME a
  WHERE a.token IS NOT NULL 
ORDER BY swaps DESC 
limit 25
'''


# In[57]:

results4 = compute(sql4)
df4 = pd.DataFrame(results4.records)


# In[58]:


st.altair_chart(alt.Chart(df4, height=500, width=500)
                    .mark_bar()
                    .encode(x='swaps', y=alt.Y('token',sort='-x'),color=alt.Color('swaps', scale=alt.Scale(scheme='dark2')))
                    .properties(title='Top most swapped tokens'))

st.altair_chart(alt.Chart(df4, height=500, width=500)
    .mark_bar()
    .encode(x='traders', y=alt.Y('token',sort='-x'),color=alt.Color('traders', scale=alt.Scale(scheme='dark2')))
    .properties(title='Top tokens most used by traders'))


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[3]:


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


# In[4]:


st.title('Rainbow Bridge')


# In[5]:


st.markdown('Rainbow Bridge is a platform that acts as an ETH ↔ NEAR bridge for assets to flow freely between the NEAR and Ethereum blockchains, while allowing users to bridge any ERC-20 token they wish.')
st.markdown('Transfers between NEAR and Aurora require a single transaction, cost a few cents and occur instantly. Transfers to Ethereum, on the other hand, involve two transactions: starting in Aurora or NEAR, and ending in Ethereum.')


# In[6]:


st.markdown('To be able to show the bridge conditions, here we are gonna analyze some interesting metrics about the platform on NEAR. The evaluated metrics are:')
st.write('- Volume bridged by directions')
st.write('- Bridge transactions by directions')
st.write('- Post bridger actions by volume tier')


# In[19]:


sql = f"""
with aur_to_near as (
  with bridge_from_aurora_to_near as (
    select 
    	BLOCK_TIMESTAMP::date as date,
      tx_hash,
    	tx:receipt[1]:outcome:executor_id as contract_in_near,
    	case 
    		when contract_in_near = 'wrap.near' then 'NEAR' 
    		when contract_in_near = 'aurora' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
    		else concat('0x', substring(contract_in_near, 1, CHARINDEX('.', contract_in_near)-1)) 
    	end as token_address,
      tx:receipt[1]:outcome:logs[0] as log, 
      substring(log, 1, CHARINDEX(' from aurora to ', log)) as first_part, 
      regexp_replace(first_part, '[^0-9]', '') as asset_amount,
    
      substring(log, CHARINDEX('from aurora to ', log)+15, len(log)) as receiver
    from near.core.fact_transactions n_t
    -- 
    where tx_signer = 'relay.aurora'
    and tx_receiver = 'aurora' and block_timestamp >= current_date-INTERVAL '1 MONTH'
    and (contract_in_near like '%.factory.bridge.near' -- ERC20-Assets
    	or contract_in_near like 'aurora' -- ETH
    	or contract_in_near like 'wrap.near' -- NEAR
    )
    and tx:receipt[1]:outcome:logs[0] like 'Transfer % from aurora %'
    -- and receiver = 'mohammadhs.near'
  ), erc20_prices as (
    select hour::date as date, symbol, token_address, avg(price) as price
    from ethereum.core.fact_hourly_token_prices
    where token_address in (select distinct token_address from bridge_from_aurora_to_near)
    and hour >= current_date-INTERVAL '1 MONTH'
    group by 1,2,3
  ), near_prices as (
    with swaps as (
      select 
        block_timestamp::date as date,
        logs[0] as log, 
        substring(log, 1, CHARINDEX(' wrap.near for', log)) as first_part, 
        regexp_replace(first_part, '[^0-9]', '')/pow(10, 24) as near_amount,
      
        substring(log, CHARINDEX('for', log), 100) as second_part,
        substring(second_part, 1, CHARINDEX('dac', second_part)-2) as second_part_amount,
        regexp_replace(second_part_amount, '[^0-9]', '')/pow(10,6) as usdt_amount
      from near.core.fact_receipts
      where logs[0] like 'Swapped % wrap.near for % dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
      and block_timestamp >= current_date-INTERVAL '1 MONTH'
    )
    
    select date, 'NEAR' as symbol, 'NEAR' as token_address, avg(usdt_amount)/avg(near_amount) as price
    from swaps
    group by 1
    
  ), all_prices as (
    select * from erc20_prices
    union all
    select * from near_prices
  ), attach_symbols as (
    select 
    	a_r.date,
    	tx_hash, 
    	receiver, 
    	contract_in_near,
    	CASE when contract_in_near = 'aurora' then 'ETH'
    	when contract_in_near = 'wrap.near' then 'NEAR'
    	when a_r.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 'MODA'
    	else e_c.symbol END as token_name,
  
    	CASE when contract_in_near = 'aurora' then 18
    	when contract_in_near = 'wrap.near' then 24
    	when a_r.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 18
    	else decimals END as token_decimals,
    
    	price as token_price,
      asset_amount/pow(10, token_decimals) as amount,
    	amount*token_price as amount_usd
    
    from bridge_from_aurora_to_near a_r
    left join ethereum.core.dim_contracts_extended e_c on a_r.token_address=e_c.contract_address
    left join all_prices p on (a_r.token_address=p.token_address and a_r.date=p.date)
  )
  
  
  select trunc(date, 'day') as date, 

  	'Aurora->NEAR' as way,
    sum(amount) as total_amount, sum(amount_usd) as total_amount_usd,
    count(distinct tx_hash) as bridge_txs, count(distinct receiver) as unique_receivers,
    sum(total_amount_usd) over (partition by way order by trunc(date, 'day')) as cum_total_amount_usd
  from attach_symbols
  group by 1,2
  order by 1
), near_to_aurora as ( -- ==================================================================================
  with bridges_from_near_to_aurora as (
    select *
    from near.core.fact_receipts
    where status_value:SuccessReceiptId is not null
    and block_timestamp >= current_date-INTERVAL '1 MONTH'
  ), flatten_logs as ( -- // Flatten log values
    select
    block_timestamp::date as date,
    tx_hash,
    logs.value as log
    from
      bridges_from_near_to_aurora bridge,
      lateral flatten(input => bridge.logs) logs
    where log like 'Transfer % from %.near to aurora'
  ), attach_asset as (
    select date, tx_hash, log, TX_RECEIVER, tx_signer
    from flatten_logs f left join near.core.fact_transactions t using(tx_hash)
  ), explode_log as (
    select 
    	date,
      tx_hash,
    	log,
    	TX_RECEIVER as contract_in_near,
      TX_SIGNER as sender_near_address,
    	case 
    		when contract_in_near = 'wrap.near' then 'NEAR' 
    		when contract_in_near = 'aurora' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
    		else concat('0x', substring(contract_in_near, 1, CHARINDEX('.', contract_in_near)-1)) 
    	end as token_address,
      substring(log, 1, CHARINDEX(' from ', log)) as first_part, 
      regexp_replace(first_part, '[^0-9]', '') as asset_amount
    from attach_asset
    where (contract_in_near like '%.factory.bridge.near' -- ERC20-Assets
    	or contract_in_near like 'aurora' -- ETH
    	or contract_in_near like 'wrap.near' -- NEAR
    )
  ), erc20_prices as (
    select hour::date as date, symbol, token_address, avg(price) as price
    from ethereum.core.fact_hourly_token_prices
    where token_address in (select distinct token_address from explode_log)
    and hour >= current_date-INTERVAL '1 MONTH'
    group by 1,2,3
  ), near_prices as (
    with swaps as (
      select 
        block_timestamp::date as date,
        logs[0] as log, 
        substring(log, 1, CHARINDEX(' wrap.near for', log)) as first_part, 
        regexp_replace(first_part, '[^0-9]', '')/pow(10, 24) as near_amount,
      
        substring(log, CHARINDEX('for', log), 100) as second_part,
        substring(second_part, 1, CHARINDEX('dac', second_part)-2) as second_part_amount,
        regexp_replace(second_part_amount, '[^0-9]', '')/pow(10,6) as usdt_amount
      from near.core.fact_receipts
      where logs[0] like 'Swapped % wrap.near for % dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
      and block_timestamp >= current_date-INTERVAL '1 MONTH'
    )
    
    select date, 'NEAR' as symbol, 'NEAR' as token_address, avg(usdt_amount)/avg(near_amount) as price
    from swaps
    group by 1
    
  ), all_prices as (
    select * from erc20_prices
    union all
    select * from near_prices
  ), attach_symbols as (
    select 
    	explode.date,
    	tx_hash, 
    	sender_near_address, 
    	contract_in_near,
    	CASE when contract_in_near = 'aurora' then 'ETH'
    	when contract_in_near = 'wrap.near' then 'NEAR'
    	when explode.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 'MODA'
    	else e_c.symbol END as token_name,
  
    	CASE when contract_in_near = 'aurora' then 18 -- ETH
      	when contract_in_near = 'wrap.near' then 24 -- NEAR
      	when explode.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 18 -- MODA
      	else decimals -- ERC20-tokens
    	END as token_decimals,
    
    	price as token_price,
      asset_amount/pow(10, token_decimals) as amount,
    	amount*token_price as amount_usd
    
    from explode_log explode
    left join ethereum.core.dim_contracts_extended e_c on explode.token_address=e_c.contract_address
    left join all_prices p on (explode.token_address=p.token_address and explode.date=p.date)
  )
  
  -- select * from attach_symbols
  
  select trunc(date, 'day') as date, 
  	'NEAR->Aurora' as way,
    sum(amount) as total_amount, sum(amount_usd) as total_amount_usd,
    count(distinct tx_hash) as bridge_txs, count(distinct sender_near_address) as unique_senders,
    sum(total_amount_usd) over (partition by way order by trunc(date, 'day')) as cum_total_amount_usd
  from attach_symbols
  group by 1,2
  order by 1
), ethereum_to_near as ( -- ==================================================================================
  with bridge_erc20_to_near as (
    select el.block_timestamp::date as date, 
      tx_hash, 
      event_inputs:accountId as receiver, 
      event_inputs:sender as sender, 
      event_inputs:token as token_address,
      symbol, 
      (event_inputs:amount)/pow(10, decimals) as amount
    from ethereum.core.fact_event_logs el
    left join ethereum.core.dim_contracts_extended ec on el.event_inputs:token=ec.contract_address
    where ORIGIN_TO_ADDRESS = '0x23ddd3e3692d1861ed57ede224608875809e127f' -- Near: Rainbow bridge
    and CONTRACT_NAME = 'ERC20Locker' and el.block_timestamp >= current_date-INTERVAL '1 MONTH'
    and EVENT_NAME = 'Locked'
    and ORIGIN_FUNCTION_SIGNATURE = '0x0889bfe7'
    and symbol is not null
    and receiver like '%.near'
  ), erc20_prices as (
    select hour::date as date, symbol, token_address, avg(price) as price
    from ethereum.core.fact_hourly_token_prices
    where token_address in (select distinct token_address from bridge_erc20_to_near)
    and date >= current_date-INTERVAL '1 MONTH'
    group by 1,2,3
  ), near_prices as (
    with swaps as (
      select 
        block_timestamp::date as date,
        logs[0] as log, 
        substring(log, 1, CHARINDEX(' wrap.near for', log)) as first_part, 
        regexp_replace(first_part, '[^0-9]', '')/pow(10, 24) as near_amount,
      
        substring(log, CHARINDEX('for', log), 100) as second_part,
        substring(second_part, 1, CHARINDEX('dac', second_part)-2) as second_part_amount,
        regexp_replace(second_part_amount, '[^0-9]', '')/pow(10,6) as usdt_amount
      from near.core.fact_receipts
      where logs[0] like 'Swapped % wrap.near for % dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
    	and date >= '2021-03-15'
        and block_timestamp >= current_date-INTERVAL '1 MONTH'
    )
    
    select date, 'NEAR' as symbol, 'NEAR' as token_address, avg(usdt_amount)/avg(near_amount) as price
    from swaps
    group by 1
    
  ), all_prices as (
    select * from erc20_prices
    union all
    select * from near_prices
  )
  
  , bridge_erc20_to_near_with_amount_usd as (
    select 
      n.date, 
      tx_hash::string as tx_hash, 
      receiver::string as receiver,
      sender::string as sender, 
      n.token_address::string as token_address, 
      n.symbol::string as symbol, 
      amount*price as amount_usd
    from bridge_erc20_to_near n
    left join all_prices p on (n.token_address = p.token_address and n.date = p.date)
  ), bridge_eth_to_near_with_amount_usd as (
    select block_timestamp::date as date, tx_hash::string as tx_hash, origin_from_address::string as receiver, origin_from_address::string as sender, 'ETH' as token_address, 'ETH' as symbol, amount_usd
    from ethereum.core.ez_eth_transfers
    where ORIGIN_TO_ADDRESS = '0x6bfad42cfc4efc96f529d786d643ff4a8b89fa52' -- Rainbow ETH transfers
    and ORIGIN_FUNCTION_SIGNATURE = '0xa8eb3b51' -- Deposit to Near
    and block_timestamp >= current_date-INTERVAL '1 MONTH'
  ), bridge_near_to_near_with_amount_usd as ( --====================== Bridge NEAR asset to Near chain
    select 
      block_timestamp::date as date, 
      tx_hash::string as tx_hash, 
      EVENT_INPUTS:accountId::string as receiver, 
      EVENT_INPUTS:sender::string as sender,
    	'0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4' as token_address,
    	'NEAR' as symbol,
      (EVENT_INPUTS:amount/pow(10,24))*price as amount_usd
    from ethereum.core.fact_event_logs l
    left join near_prices p on l.block_timestamp::date = p.date
    where CONTRACT_ADDRESS = '0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4'
    and ORIGIN_TO_ADDRESS = '0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4'
    and CONTRACT_NAME = 'eNear' and l.block_timestamp >= current_date-INTERVAL '1 MONTH'
    and ORIGIN_FUNCTION_SIGNATURE = '0xe3113e3b'
    and EVENT_NAME = 'TransferToNearInitiated'
    and receiver like '%.near'
  ), all_bridges as (
    select * from bridge_erc20_to_near_with_amount_usd
    union all
    select * from bridge_eth_to_near_with_amount_usd
    union all 
    select * from bridge_near_to_near_with_amount_usd
  )

  select 
    trunc(date, 'day') as date, 
  	'Ethereum->NEAR' as way,
    sum(amount_usd) as total_amount, sum(amount_usd) as total_amount_usd,
    count(distinct tx_hash) as bridge_txs, count(distinct sender) as unique_senders,
    sum(total_amount_usd) over (partition by way order by trunc(date, 'day')) as cum_total_amount_usd
  from all_bridges
  group by 1,2
  order by 1

)
  
  , near_to_ethereum as ( -- ======================================================================================
  with bridge_ERC20_from_near_to_ethereum as (
    select block_timestamp::date as date, tx_hash::string as tx_hash, (parse_json(trim(args))):amount as asset_amount, (parse_json(args)):recipient as eth_address
    from near.core.fact_actions_events_function_call
    where action_name = 'FunctionCall' and block_timestamp >= current_date-INTERVAL '1 MONTH'
    and method_name = 'withdraw' -- ERC20 tokens
        and args::string like '%"amount%'
    and (parse_json(trim(args))):amount::string is not null
    and (parse_json(trim(args))):recipient::string is not null
  ), bridge_NEAR_from_near_to_ethereum as (
    select block_timestamp::date as date, tx_hash::string as tx_hash, deposit as asset_amount, (parse_json(args)):eth_recipient as eth_address
    from near.core.fact_actions_events_function_call
    where action_name = 'FunctionCall' and block_timestamp >= current_date-INTERVAL '1 MONTH'
    and method_name = 'migrate_to_ethereum' -- NEAR
    and args::string like '%"amount%'
    and (parse_json(args)):eth_recipient is not null
  ), bridge_all_from_near_to_ethereum as (
    select * from bridge_ERC20_from_near_to_ethereum
    union all
    select * from bridge_NEAR_from_near_to_ethereum
  ), join_with_txs as (
    select date, tx_hash, asset_amount, eth_address, TX_RECEIVER as contract_in_near, TX_SIGNER as sender_near_address, 
    	case 
    		when contract_in_near = 'e-near.near' then 'NEAR' 
    		when contract_in_near = 'aurora' then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
    		else concat('0x', substring(contract_in_near, 1, CHARINDEX('.', contract_in_near)-1)) 
    	end as token_address
    from bridge_all_from_near_to_ethereum left join near.core.fact_transactions using(tx_hash)
  ), erc20_prices as ( -- 
    select hour::date as date, symbol, token_address, avg(price) as price
    from ethereum.core.fact_hourly_token_prices
    where token_address in (select distinct token_address from join_with_txs)
    and hour >= current_date-INTERVAL '1 MONTH'
    group by 1,2,3
  ), near_prices as (
    with swaps as (
      select 
        block_timestamp::date as date,
        logs[0] as log, 
        substring(log, 1, CHARINDEX(' wrap.near for', log)) as first_part, 
        regexp_replace(first_part, '[^0-9]', '')/pow(10, 24) as near_amount,
      
        substring(log, CHARINDEX('for', log), 100) as second_part,
        substring(second_part, 1, CHARINDEX('dac', second_part)-2) as second_part_amount,
        regexp_replace(second_part_amount, '[^0-9]', '')/pow(10,6) as usdt_amount
      from near.core.fact_receipts
      where logs[0] like 'Swapped % wrap.near for % dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
      and block_timestamp >= current_date-INTERVAL '1 MONTH'
    )
    
    select date, 'NEAR' as symbol, 'NEAR' as token_address, avg(usdt_amount)/avg(near_amount) as price
    from swaps
    group by 1
  ), all_prices as (
    select * from erc20_prices
    union all
    select * from near_prices
  ), attach_symbols as ( -- 
    select 
    	bridges.date,
    	tx_hash, 
    	sender_near_address, 
    	contract_in_near,
    	bridges.token_address,
    	CASE when contract_in_near = 'e-near.near' then 'NEAR'
    	when bridges.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 'MODA'
    	else e_c.symbol END as token_name,
  
    	CASE when contract_in_near = 'aurora' then 18 -- ETH
      	when contract_in_near = 'e-near.near' then 24 -- NEAR
      	when bridges.token_address = '0x1117ac6ad6cdf1a3bc543bad3b133724620522d5' then 18 -- MODA
      	else decimals -- ERC20-tokens
    	END as token_decimals,
    
    	price as token_price,
      asset_amount/pow(10, token_decimals) as amount,
    	amount*token_price as amount_usd
    
    from join_with_txs bridges
    left join ethereum.core.dim_contracts_extended e_c on bridges.token_address=e_c.contract_address
    left join all_prices p on (bridges.token_address=p.token_address and bridges.date=p.date)
  )
  
  -- select * from attach_symbols
  
  select trunc(date, 'day') as date, 
  	'NEAR->Ethereum' as way,
    sum(amount) as total_amount, sum(amount_usd) as total_amount_usd,
    count(distinct tx_hash) as bridge_txs, count(distinct sender_near_address) as unique_senders,
    sum(total_amount_usd) over (partition by way order by trunc(date, 'day')) as cum_total_amount_usd
  from attach_symbols
  group by 1,2
  order by 1
)
select * from near_to_aurora
where date >= current_date-INTERVAL '1 MONTH'
union all
select * from near_to_ethereum
where date >= current_date-INTERVAL '1 MONTH'
union all
select * from aur_to_near
where date >= current_date-INTERVAL '1 MONTH'
union all
select * from ethereum_to_near
where date >= current_date-INTERVAL '1 MONTH'
"""


# In[20]:

st.experimental_memo(ttl=86400)
def compute(a):
    data=sdk.query(a)
    return data

results = compute(sql)
df = pd.DataFrame(results.records)
df.info()
st.subheader('Main bridging activity over the past month')
st.markdown('In this part, it can be seen the bridging activity over the past month. Because of the main platform to do bridges on Near is Rainbow, we have taken into account the activity there. Several outflows have been detected and analyzed using number of transactions, active users and volume.')


# In[27]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_bar()
    .encode(x='sum(bridge_txs)', y=alt.Y('way',sort='-x'),color=alt.Color('way', scale=alt.Scale(scheme='dark2')))
    .properties(title='Number of monthly bridges by direction'))


# In[28]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='bridge_txs:Q',color=alt.Color('way', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily bridges by direction'))


# In[29]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='unique_senders:Q',color=alt.Color('way', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily active bridgers by direction'))


# In[30]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_line()
    .encode(x='date:O', y='total_amount_usd:Q',color=alt.Color('way', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily volume bridged (USD) by direction'))


# In[26]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_area()
    .encode(x='date:O', y='cum_total_amount_usd:Q',color=alt.Color('way', scale=alt.Scale(scheme='dark2')))
    .properties(title='Cumulative volume bridged (USD) by direction'))


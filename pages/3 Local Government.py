#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


st.title('Local Government')


# In[3]:


st.markdown('NEAR Protocol uses _Proof-of-Stake (PoS)_ consensus to secure and validate transactions on the blockchain. Validators earn NEAR Token rewards for producing new blocks in the form of a static inflation rate of about 4.5% each year.')
st.markdown('Token holders not interested in being a Validator can stake to a Validator’s staking pool and earn a portion of Token rewards too. This incentivizes Token holders to stay involved with the community and support Validators who are keeping the network running smoothly.')


# In[4]:


st.markdown('One of the main aspects to be aware about the blockchains that use proof of stake is the decentralization. **Decentralization** is by far one of the most important factors in a crypto ecosystem. As you probably already figured out, the validation system in place might fail to honor this by progressively moving towards a more traditional centralized environment. Why and how could this happen?') 
st.write('- As it was briefly mentioned previously, validators are incentivized to hold more and more NEAR to guarantee a spot in the network for themselves.') 
st.write('- Users look at the voting power (= total staked NEAR) of a validator as a good indicator of who to trust and delegate their funds to. This is understandable and that is why there is a need to constantly educate the user base and promote smaller validators.')


# In[30]:


st.markdown('In this dashboard we are gonna asses the state of governance on NEAR in terms of decentralization basing on a different metrics:')
st.write('- Distribution of staking by validators')
st.write('- Power distribution by validator ranks')
st.write('- Most common staking actions currently and over time')
st.write('- Evolution of Nakamoto coefficient')


# In[6]:


sql = f"""
SELECT
  trunc(block_timestamp,'day') as date,
  method_name,
  case when method_name in ('deposit_and_stake','stake','stake_all') then 'staking'
  when method_name in ('unstake','unstake_all') then 'unstaking'
  else method_name end as method_name2,
  count(distinct TX_HASH) as actions
from near.core.fact_actions_events_function_call
  WHERE method_name in ('deposit_and_stake','stake','stake_all','unstake','unstake_all','unbond_delegation','update_validator')
  --'vote', 
  and block_timestamp between current_date - INTERVAL '1 MONTH' and current_date
    group by 1,2,3
  order by 1 asc
"""


# In[7]:

st.experimental_memo(ttl=21600)
@st.cache
def compute(a):
    data=sdk.query(a)
    return data

results = compute(sql)
df = pd.DataFrame(results.records)
df.info()
st.subheader('Main staking activity metrics over the past month')
st.markdown('In this first part, we can take a look at the local government metrics on Near, where it can be seen how the staking actions have been splitted across the protocol, the distribution of validators, the Nakamoto coefficient, as well as some other interesting metrics regarding validators.')


# In[9]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_bar()
    .encode(x='sum(actions)', y=alt.Y('method_name2',sort='-x'),color=alt.Color('method_name2', scale=alt.Scale(scheme='dark2')))
    .properties(title='Type of action by usage'))


# In[10]:


st.altair_chart(alt.Chart(df, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='actions:Q',color=alt.Color('method_name2', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily actions by type'))


# In[11]:


sql2 = f"""
with 
  t1 as (
  SELECT 
    x.block_timestamp as week,
    method_name,
    tx_signer,
    tx_receiver
  FROM near.core.fact_actions_events_function_call x
  JOIN near.core.fact_transactions y ON x.tx_hash = y.tx_hash
  WHERE method_name IN ('deposit_and_stake','unstake_all')
),
t2 as (
  SELECT 
    trunc(week,'week') as date,
  tx_receiver as validator,
  count(distinct tx_signer) as stakers
  from t1 where method_name='deposit_and_stake'
  group by 1,2
  ),
t3 as (
  SELECT 
    trunc(week,'week') as date,
  tx_receiver as validator,
  count(distinct tx_signer) as unstakers
  from t1 where method_name='unstake_all'
  group by 1,2
  )
SELECT
ifnull(t2.date,t3.date) as date,
ifnull(t2.validator,t3.validator) as validator,
ifnull(stakers,0) as stakerss, ifnull(unstakers*(-1),0) as unstakerss,stakerss+unstakerss as net_stakers
from t2
join t3 on t2.date=t3.date and t2.validator=t3.validator where t2.validator not like '%lockup%'
"""


# In[12]:

results2 = compute(sql2)
df2 = pd.DataFrame(results2.records)
df2.info()


# In[40]:


base2=alt.Chart(df2).encode(x=alt.X('date:O', axis=alt.Axis(labelAngle=325)))
line1=base2.mark_line(color='blue').encode(y=alt.Y('sum(stakerss):Q', axis=alt.Axis(grid=True)))
line2=base2.mark_line(color='orange').encode(y='sum(unstakerss):Q')
st.altair_chart((line1 + line2).properties(title='Daily stakers vs unstakers over the past month',width=600))


# In[24]:


st.altair_chart(alt.Chart(df2, height=500, width=500)
    .mark_bar(color='green')
    .encode(x='date:O', y='sum(net_stakers):Q')
    .properties(title='Daily net stakers over the past month'))


# In[16]:


st.altair_chart(alt.Chart(df2, height=500, width=500)
    .mark_bar()
    .encode(x='date:O', y='net_stakers:Q',color=alt.Color('validator', scale=alt.Scale(scheme='dark2')))
    .properties(title='Daily net_stakers by validator'))


# In[41]:


st.subheader("Near decentralization")
st.markdown('To close this section, here it can be seen a representation of the current total NEAR staked by each validator as well as the evolution of the Nakamoto Coefficient.')
st.markdown('**Nakamoto Coefficient** is one of the main interesting metrics to measure the decentralization of a blockchain, that represents how many validators are needed to accumulate more than 50% of the total current NEAR staked.')
sql3='''
 WITH 
   transactions as (
   SELECT tx_hash
FROM near.core.fact_actions_events_function_call
  WHERE method_name IN ('deposit_and_stake')
   ),
   stakes as (
SELECT 
  block_timestamp,
  tx_hash as tx,
  tx_receiver as validator, 
  tx_signer as delegator,
  tx:actions[0]:FunctionCall:deposit/pow(10,24) near_staked
FROM near.core.fact_transactions
  WHERE tx_hash in (select * from transactions)
),
   transactions2 as (
   SELECT tx_hash
FROM near.core.fact_actions_events_function_call
  WHERE method_name ='unstake_all'
   ),
   unstakes as (
SELECT 
  block_timestamp,
  tx_hash as tx,
  tx_receiver as validator, 
  tx_signer as delegator,
  tx:actions[0]:FunctionCall:deposit/pow(10,24) near_unstaked
FROM near.core.fact_transactions
  WHERE tx_hash in (select * from transactions2)
),
weekly as (
   SELECT 
   trunc(x.block_timestamp,'week') as weeks,
   x.tx,
   x.validator,
   near_staked
   --amount_staked/pow(10,24) as near_staked
FROM stakes x
   WHERE near_staked is not null 
),
   weekly2 as (
   SELECT 
   trunc(x.block_timestamp,'week') as weeks,
   x.tx,
   x.validator,
   near_unstaked
   --amount_staked/pow(10,24) as near_staked
FROM unstakes x
   WHERE near_unstaked is not null 
),
  totals as (
   SELECT
   x.weeks,
   sum(near_staked) as week_near_staked,
   sum(week_near_staked) over (order by x.weeks)as total_near_staked,
   sum(near_unstaked) as week_near_unstaked,
   sum(week_near_unstaked) over (order by x.weeks)as total_near_unstaked
   from weekly x
   full outer join weekly2 y on x.weeks=y.weeks
   group by 1 order by 1
   ),
   totals2 as (
   SELECT
   weeks,
   week_near_staked- week_near_unstaked as week_near_staked,
   total_near_staked-total_near_unstaked as total_near_staked
   from totals
   ),
ranking1 as (
   SELECT 
   weeks,
   validator,
   count(distinct tx) as txs,
   sum(near_staked) as total_near_delegated,
sum(total_near_delegated) over (partition by validator order by weeks) as cumulative_near_delegated
FROM weekly 
group by 1,2
),
   ranking2 as (
   SELECT 
   weeks,
   validator,
   count(distinct tx) as txs,
   sum(near_unstaked) as total_near_undelegated,
sum(total_near_undelegated) over (partition by validator order by weeks) as cumulative_near_undelegated
FROM weekly2 
group by 1,2
),
   ranking3 as (
   SELECT
   ifnull(x.weeks,y.weeks) as weeks,
   ifnull(x.validator,y.validator) as validator,
   ifnull(total_near_delegated,0)-ifnull(total_near_undelegated,0) as total_near_delegated,
   ifnull(cumulative_near_delegated,0)-ifnull(cumulative_near_undelegated,0) as cumulative_near_delegated
   from ranking1 x
   full outer join ranking2 y on x.weeks=y.weeks and x.validator=y.validator
   ),
stats as (
  SELECT
  weeks,
33 as bizantine_fault_tolerance,
total_near_staked,
(total_near_staked*bizantine_fault_tolerance)/100 as threshold--,
--sum(total_sol_delegated) over (partition by weeks order by validator_ranks asc) as total_sol_delegated_by_ranks,
--count(distinct vote_accounts) as validators
from totals2
), 
stats2 as (
   select *,
1 as numbering,
sum(numbering) over (partition by weeks order by cumulative_near_delegated desc) as rank 
from ranking3
   )
SELECT
weeks,
validator,
cumulative_near_delegated,
rank,
sum(cumulative_near_delegated) over (partition by weeks order by rank asc) as total_staked
--count(case when total_staked)
--sum(1) over (partition by weeks order by stake_rank) as nakamoto_coeff
  from stats2 where cumulative_near_delegated is not null and weeks>=CURRENT_DATE-6 and rank<100
order by rank asc


'''


# In[42]:

results3 = compute(sql3)
df3 = pd.DataFrame(results3.records)


# In[44]:


st.altair_chart(alt.Chart(df3, height=500, width=600)
    .mark_bar()
    .encode(x=alt.X('validator',sort='-y'), y=('cumulative_near_delegated'),color=alt.Color('cumulative_near_delegated'))
    .properties(title='Current NEAR delegated by validator'))


# In[31]:


sql4='''
 WITH 
   transactions as (
   SELECT tx_hash
FROM near.core.fact_actions_events_function_call
  WHERE method_name IN ('deposit_and_stake')
   ),
   stakes as (
SELECT 
  block_timestamp,
  tx_hash as tx,
  tx_receiver as validator, 
  tx_signer as delegator,
  tx:actions[0]:FunctionCall:deposit/pow(10,24) near_staked
FROM near.core.fact_transactions
  WHERE tx_hash in (select * from transactions)
),
weekly as (
   SELECT 
   trunc(block_timestamp,'week') as weeks,
   tx,
   validator,
   near_staked
   --amount_staked/pow(10,24) as near_staked
FROM stakes WHERE near_staked is not null 
),
  totals as (
   SELECT
   weeks,
   sum(near_staked) as week_near_staked,
   sum(week_near_staked) over (order by weeks)as total_near_staked
   from weekly
   group by 1 order by 1
   ),
ranking as (
   SELECT 
   weeks,
   validator,
   count(distinct tx) as txs,
   sum(near_staked) as total_near_delegated,
sum(total_near_delegated) over (partition by validator order by weeks) as cumulative_near_delegated
FROM weekly 
group by 1,2
),
stats as (
  SELECT
  weeks,
50 as bizantine_fault_tolerance,
total_near_staked,
(total_near_staked*bizantine_fault_tolerance)/100 as threshold--,
--sum(total_sol_delegated) over (partition by weeks order by validator_ranks asc) as total_sol_delegated_by_ranks,
--count(distinct vote_accounts) as validators
from totals
), 
stats2 as (
   select *,
1 as numbering,
sum(numbering) over (partition by weeks order by cumulative_near_delegated desc) as rank 
from ranking
   ),
stats3 as (
SELECT
weeks,
validator,
cumulative_near_delegated,
rank,
sum(cumulative_near_delegated) over (partition by weeks order by rank asc) as total_staked
--count(case when total_staked)
--sum(1) over (partition by weeks order by stake_rank) as nakamoto_coeff
  from stats2
order by rank asc),
   final_nak as (
SELECT
a.weeks,
validator,
count(case when total_staked <= threshold then 1 end) as nakamoto_coeff
from stats3 a 
join stats b 
on a.weeks = b.weeks where a.weeks >=CURRENT_DATE-INTERVAL '1 MONTH'
group by 1,2
order by 1 asc
   )
SELECT
weeks,sum(nakamoto_coeff) as nakamoto_coeff
from final_nak
group by 1 order by 1 asc 
'''


# In[32]:

results4 = compute(sql4)
df4 = pd.DataFrame(results4.records)


# In[33]:


st.altair_chart(alt.Chart(df4, height=500, width=500)
    .mark_bar()
    .encode(x='weeks:N', y='nakamoto_coeff:Q',color=alt.Color('nakamoto_coeff'))
    .properties(title='Weekly Nakamoto Coefficient over the past month'))


# In[ ]:





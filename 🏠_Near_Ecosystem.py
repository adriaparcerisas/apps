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


# In[4]:


st.sidebar.markdown("Created by Adrià Parcerisas")
st.title('Near Ecosystem Dashboard')


# In[10]:


st.markdown("**NEAR Protocol** is a decentralized platform that seeks to facilitate the development and deployment of dApps on its blockchain technology. More precisely, **NEAR Protocol** is a decentralized smart contract platform that focuses on creating a developer and user-friendly experience. Its consensus mechanism is Proof-of-Stake and it uses sharding technology to achieve speed and scalability. NEAR also provides a bridge and scaling solution for the Ethereum blockchain.")


# In[14]:


st.markdown("The main idea of this app is to show an overview of the entire **NEAR Procotol** ecosystem through a dive deep analysis of each area of interest. You can find information about each different section by navigating on the sidebar pages.")


# In[17]:


st.markdown("These includes:") 
st.markdown("1. _The Capital of Near_") 
st.markdown("2. _The Financial District_")
st.markdown("3. _Local Government_")
st.markdown("4. _The Art of District_")
st.markdown("5. _Your Bridge Passport: Rainbow Bridge_")


# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[163]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, inspect, func


# In[164]:


engine = create_engine("sqlite:///hawaii.sqlite")


# In[165]:


from sqlalchemy.orm import Session

session = Session(bind=engine)


# In[166]:


inspector = inspect(engine)
inspector.get_table_names()


# In[167]:


columns = inspector.get_columns('measurement')
for c in columns:
    print(c['name'], c["type"])


# In[168]:


engine.execute('SELECT * FROM measurement LIMIT 20').fetchall()


# In[169]:


engine.execute("SELECT * FROM measurement WHERE date < '2016-09-14'").fetchall()


# In[170]:


from sqlalchemy.ext.automap import automap_base

Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[171]:


results = session.query(Measurement.tobs).all()

tobs_values = list(np.ravel(results))
tobs_values


# In[172]:


# Query for last 12 months of precipitation
last_12_months_precipitation = session.query(Measurement.date, Measurement.prcp).        filter(Measurement.date >= '2018-04-01').filter(Measurement.date <= '2019-04-01').order_by(Measurement.date).all()


# In[173]:


df_last12months_precipitation = pd.DataFrame(data=last_12_months_precipitation)
df_last12months_precipitation.head(40)


# In[174]:


plt.title("Precipitation for last 12 Months")
plt.xlabel("Month")
plt.ylabel("Precipitation in inches")

months = ["Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"]

y = df_last12months_precipitation["prcp"].tolist()
x = np.arange(0, len(df_last12months_precipitation.index.tolist()), 1)


# In[ ]:


month_total = len(y)
month_step_xticks = int((month_total / 12)*1.03)
plt.ylim = max(y) + 1
tick_locations = [x+55 for x in range(1, month_total, month_step_xticks)]


# In[ ]:


plt.bar(x, y, width=30, color="blue", alpha=0.5, align="edge")
plt.xticks(tick_locations, months)


# In[ ]:


plt.show()


# In[ ]:


from sqlalchemy import func

totalnumber_of_stations = session.query(func.count(Station.station)).first()


# In[ ]:


print(f"Total number of stations: {str(totalnumber_of_stations[0])}")


# In[ ]:


engine.execute("SELECT count(station), station FROM measurement GROUP BY station ORDER BY count(station) DESC").fetchall()


# In[ ]:


active_stations_descending = session.query(Measurement.station, func.count(Measurement.station)).        group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()

df_active_stations_descending = pd.DataFrame(data=active_stations_descending, columns=['Station', 'Count'])
df_active_stations_descending.head()


# In[ ]:


station_with_most_observations = df_active_stations_descending["Station"][0]
most_observations = df_active_stations_descending["Count"][0]
print(f"Station with most observations ({most_observations}): {station_with_most_observations}")


# In[ ]:


temperature_frequencies = session.query(Measurement.tobs).    filter(Measurement.date >= '2016-08-24').    filter(Measurement.station == station_with_most_observations).    order_by(Measurement.tobs).all()
    
temperature_frequencies


# In[ ]:


hist, bins = np.histogram(temperature_frequencies, bins=12)

width = bins[1] - bins[0]

plt.bar(center, hist, width=width)
plt.show()


# In[ ]:


def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
def last_year_dates(start_date, end_date):
    """ Corresponding dates from previous year
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        start_date (string)
        end_date (string)
    """
    lst_start_date = start_date.split('-')
    lst_end_date = end_date.split('-')
    lastyear_start_year = int(lst_start_date[0]) - 1
    lastyear_end_year = int(lst_end_date[0]) - 1
    ly_start_date = f"{lastyear_start_year}-{lst_start_date[1]}-{lst_start_date[2]}"
    ly_end_date = f"{lastyear_end_year}-{lst_end_date[1]}-{lst_end_date[2]}"
    
    return (ly_start_date, ly_end_date)


# In[ ]:


trip_start = '2015-04-20'
trip_end = '2015-04-28'

average_trip_temps = calc_temps(trip_start, trip_end)

(lastyear_start_date, lastyear_end_date) = last_year_dates(trip_start, trip_end)


# In[ ]:


import seaborn 

yerr_val = average_trip_temps[0][2] - average_trip_temps[0][0]

y = [average_trip_temps[0][1]]
x = 0


# In[ ]:


fig, ax = plt.subplots()

ax.set_ylabel("Temperature (F)", fontsize=14)
ax.set_title("Trip Average Temps", fontsize=18)

ax.bar(x, y, width=.1, color="blue", yerr=yerr_val)
ax.set_xlim(-.1, .1)
ax.set_ylim(0, 100)
ax.set_xbound(lower=-.1, upper=.1)
ax.tick_params(axis='x', which='both', bottom='off', top='off', labelbottom='off') 
plt.show()


# In[ ]:



rainfall_by_station_lastyear = session.query(Measurement.station, func.sum(Measurement.prcp)).    filter(Measurement.date >= lastyear_start_date).    filter(Measurement.date <= lastyear_end_date).    group_by(Measurement.station).    order_by(func.sum(Measurement.prcp).desc()).all()
rainfall_by_station_lastyear


# In[ ]:



query_to_run = f"SELECT station, sum(prcp) FROM measurement WHERE date >= '{lastyear_start_date}' AND date <= '{lastyear_end_date}' "            "GROUP BY station "            "ORDER BY sum(prcp) DESC"
print(query_to_run)
engine.execute(query_to_run).fetchall()


# In[ ]:





# In[ ]:





# In[ ]:





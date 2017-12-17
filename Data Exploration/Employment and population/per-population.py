import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.style.use('ggplot')


df1 = pd.read_csv("C:\Python27\date-borough-code-cleaned.csv", parse_dates=['CMPLNT_FR_DT'])
df2 = pd.read_csv("C:\Python27\census-estimate.csv")

#df2 = df2[df2.County != 'New York City']

#BRONX
dfBronx = df2[df2.County == 'Bronx'].sort_values('Year')
dfBronx['Count'] = df1[df1.BORO_NM == "BRONX"].CMPLNT_FR_DT.dt.year.value_counts().sort_index().values
dfBronx = dfBronx.assign(Per100k = lambda dfBronx: ((dfBronx['Count']/dfBronx['Population Estimate'])*100000))

#BROOKLYN
dfBrooklyn = df2[df2.County == 'Brooklyn'].sort_values('Year')
dfBrooklyn['Count'] = df1[df1.BORO_NM == "BROOKLYN"].CMPLNT_FR_DT.dt.year.value_counts().sort_index().values
dfBrooklyn = dfBrooklyn.assign(Per100k = lambda dfBrooklyn: ((dfBrooklyn['Count']/dfBrooklyn['Population Estimate'])*100000))

#MANHATTAN
dfManhattan = df2[df2.County == 'Manhattan'].sort_values('Year')
dfManhattan['Count'] = df1[df1.BORO_NM == "MANHATTAN"].CMPLNT_FR_DT.dt.year.value_counts().sort_index().values
dfManhattan = dfManhattan.assign(Per100k = lambda dfManhattan: ((dfManhattan['Count']/dfManhattan['Population Estimate'])*100000))

#QUEENS
dfQueens = df2[df2.County == 'Queens'].sort_values('Year')
dfQueens['Count'] = df1[df1.BORO_NM == "QUEENS"].CMPLNT_FR_DT.dt.year.value_counts().sort_index().values
dfQueens = dfQueens.assign(Per100k = lambda dfQueens: ((dfQueens['Count']/dfQueens['Population Estimate'])*100000))

#STATEN ISLAND
dfSI = df2[df2.County == 'Staten Island'].sort_values('Year')
dfSI['Count'] = df1[df1.BORO_NM == "STATEN ISLAND"].CMPLNT_FR_DT.dt.year.value_counts().sort_index().values
dfSI = dfSI.assign(Per100k = lambda dfSI: ((dfSI['Count']/dfSI['Population Estimate'])*100000))


dfTest = pd.DataFrame(index = [2006,2007,2008,2009,2010,2011,2012,2013,2014,2015])
dfTest['Bronx'] = dfBronx.Per100k.values
dfTest['Brooklyn'] = dfBrooklyn.Per100k.values
dfTest['Manhattan'] = dfManhattan.Per100k.values
dfTest['Queens'] = dfQueens.Per100k.values
dfTest['Staten Island'] = dfSI.Per100k.values


ax = dfTest.plot(figsize=(15,13))
ax.set_title('Reported Crime Numbers Per 100k Population')
ax.set_ylabel('Number of Incidents')
plt.savefig("./per100k-all-boroughs.png")
plt.clf()
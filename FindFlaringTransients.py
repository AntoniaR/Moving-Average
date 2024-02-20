import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pylab

import numpy as np 
import pandas as pd 
import functions as func



# Inputs to edit
dataFile = 'ds330_lightcurves.csv' #Insert here the name of the .csv files that need to be analysed
window_size = 12
cutoff_val = 3
minDatapoints = 12
sigma =3. # the number of sigma the deviation has to be above the flux uncertainties

##############################

def plothist(x, threshold, filename):
# Create a histogram of the data t
    plt.hist(x,bins=50,histtype='stepfilled')
    plt.axvline(x=threshold, linewidth=2, color='k',linestyle='--')
    plt.xlabel('Deviation')
    plt.ylabel('Number of sources')
    plt.savefig(filename)
    plt.close()
    return

def checkCand(row):
    if (row.flux - row.MAvg) > row.fluxErr:
        val = 1
    else:
        val = 0
    return val

##############################



data = pd.read_csv(dataFile)

finalData = pd.DataFrame(columns = ["srcID","runcat","time","flux","fluxErr","dpts","MAvg","deviation","candidate"])
runcats = data.runcat.unique()

for runcat in runcats:
    lc = data.loc[data['runcat'] == runcat]
    lc = lc.reset_index()
    lc["dpts"] = lc.index+1
    lc['MAvg'] = lc['flux'].rolling(window=window_size, min_periods=1).mean()
    std = lc.flux.std()
    lc['deviation'] = (lc['flux'] - lc['MAvg']) / std
    lc['candidate'] = np.where(np.abs(lc['flux'] - lc['MAvg']) > (sigma * lc['fluxErr']), 1, 0)
    lc = lc.fillna(0)
    lc = lc.drop('index',axis=1)
    finalData = finalData._append(lc, ignore_index=True)

# remove all rows with a zero deviation
finalData = finalData.loc[finalData['deviation'] != 0]

# Find transients that have deviation values that lie above threshold value
all_deviations = finalData.deviation
params_med = np.median(all_deviations), np.sqrt(np.mean([(i-np.median(all_deviations))**2. for i in all_deviations]))
threshold = cutoff_val * params_med[1]
# Save plot of histogram and threshold
plothist(all_deviations, threshold, 'LOFAR_deviation_hist.png')
print(cutoff_val, '*', params_med[1], '=', threshold)

candidates = finalData.loc[(finalData['deviation'] > threshold) & (finalData['dpts'] >= minDatapoints) & (finalData['candidate'] == 1)]

if len(candidates) == 0:
    print('No candidate variable sources found.')
else:
    print(candidates)
    candidates.to_csv('candidates.csv', index=False)

    
exit()

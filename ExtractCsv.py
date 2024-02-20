import numpy as np 
import pandas as pd 
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import relationship

from dblogin import *
from databaseTools import dbtools 

import tkp.db
from tkp.db.model import Varmetric
from tkp.db.model import Runningcatalog
from tkp.db.model import Newsource
from tkp.db.model import Extractedsource
from tkp.db.model import Assocxtrsource
from tkp.db.model import Image

outputname = 'GRBdata' #Name for the output .csv file
database = 'AR_testing6' 
dataset_id = 330

global db 

# Connect to the database and run the queries
session = dbtools.access(engine,host,port,user,password,database)
lightcurves = dbtools.GetAllLightcurves(session,dataset_id)  # get all the lightcurves








plotdata = [[VarParams[i].Runningcatalog.id, VarParams[i].Varmetric.eta_int, VarParams[i].Varmetric.v_int, VarParams[i].Varmetric.lightcurve_max, 
            VarParams[i].Varmetric.lightcurve_median, (VarParams[i].Varmetric.band.freq_central/1e6), VarParams[i].Runningcatalog.datapoints, VarParams[i].Varmetric.newsource, 
            VarParams[i].Runningcatalog.wm_ra, VarParams[i].Runningcatalog.wm_decl] for i in range(len(VarParams))]
plotdata = pd.DataFrame(data=plotdata,columns=['runcat','eta','V','maxFlx','avgFlx','freq','dpts','newSrc','ra','dec'])
plotdata = plotdata.fillna('N')

plotdata = plotdata.loc[(plotdata['eta'] > 0) & (plotdata['V'] > 0) & (plotdata['dpts']>1) & (plotdata['newSrc']=='N')]

# Save the data for plotting
plotdata.to_csv(outputname+'.csv', index=False)


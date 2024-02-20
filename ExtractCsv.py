import numpy as np 
import pandas as pd 
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import relationship

from dblogin import *
import dbtools 

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
lightcurves.to_csv('ds'+str(dataset_id)+'_lightcurves.csv', index=False)

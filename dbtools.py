import tkp.db
from tkp.db.model import Varmetric
from tkp.db.model import Runningcatalog
from tkp.db.model import Newsource
from tkp.db.model import Extractedsource
from tkp.db.model import Assocxtrsource
from tkp.db.model import Image

from sqlalchemy import *
from sqlalchemy.orm import relationship
import pandas as pd



def access(engine,host,port,user,password,database):
    """ Access the database using sqlalchemy"""
    # make db global in order to be used in GetPandaExtracted
    global db
    db = tkp.db.Database(engine=engine, host=host, port=port,
                     user=user, password=password, database=database)
    db.connect()
    session = db.Session()
    print ('connected!')
    return session

def GetVarParams(session,dataset_id):
    # Returns all the variability parameters for sources in a given dataset
    VarParams = session.query(Varmetric,Runningcatalog).select_from(join(Varmetric,Runningcatalog)).filter(Runningcatalog.dataset_id == dataset_id).all()
    return VarParams

def GetAllLightcurves(session,dataset_id):
    # Returns all the light curves for the unique sources in a given dataset

    x = session.query(Runningcatalog).filter(Runningcatalog.dataset_id == dataset_id)
    dx = pd.read_sql_query(x.statement,db.connection)
    dx = dx.rename(index=str,columns={'id' : 'runcat'})

    #for src in dx.runcat:
    #print(src)
    y = session.query(Extractedsource,Assocxtrsource).select_from(join(Extractedsource,Assocxtrsource)).filter(Assocxtrsource.runcat_id.in_(dx.runcat)).all()
    #print(y)
    data = [[y[i].Extractedsource.id, y[i].Assocxtrsource.runcat_id, y[i].Extractedsource.image.taustart_ts, y[i].Extractedsource.f_int, y[i].Extractedsource.f_int_err] for i in range(len(y))]
    #print(data)
    data=pd.DataFrame(data=data, columns =['srcID','runcat','time','flux','fluxErr'])
    data=data.sort_values(['runcat','time'])
    print(data)

#plotdata = [[VarParams[i].Runningcatalog.id, VarParams[i].Varmetric.eta_int, VarParams[i].Varmetric.v_int, VarParams[i].Varmetric.lightcurve_max, VarParams[i].Varmetric.lightcurve_median, (VarParams[i].Varmetric.band.freq_central/1e6), VarParams[i].Runningcatalog.datapoints, VarParams[i].Varmetric.newsource] for i in range(len(VarParams)) if VarParams[i].Runningcatalog.id not in matchSrcs]
#plotdata = pd.DataFrame(data=plotdata,columns=['runcat','eta','V','maxFlx','avgFlx','freq','dpts','newSrc'])
#plotdata = plotdata.fillna('N')




        
#        dy = pd.read_sql_query(y.statement,db.connection)
#        print(dy)
    #exit()

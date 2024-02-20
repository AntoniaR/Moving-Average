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
    
    full_datasets = dump_trans_full_dataset(DATASET_NR, SIGMA)
    full_dataset_query = """
    SELECT assocxtrsource.runcat, runningcatalog.datapoints
    FROM extractedsource
    INNER JOIN image
    ON extractedsource.image = image.id
    INNER JOIN assocxtrsource
    ON extractedsource.id = assocxtrsource.xtrsrc
    INNER JOIN runningcatalog
    ON extractedsource.id = runningcatalog.xtrsrc
    WHERE extractedsource.det_sigma>{}
    AND image.dataset={}
    AND runningcatalog.datapoints={}
    """.format(sigma, dataset_nr, datapoints)
    
    cursor = tkp.db.execute(full_dataset_query)
    transients = tkp.db.generic.get_db_rows_as_dicts(cursor)
    
    return pd.DataFrame(transients)

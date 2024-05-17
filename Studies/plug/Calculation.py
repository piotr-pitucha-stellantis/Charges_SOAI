import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv
import numpy as np
import datetime as dt
import pytz
import pandas as pd
import shutil
import os
import json
from datetime import datetime




class Calculation:

    def __init__(self):
        pass


    def translate_AFNOR_INFO_CODE(df,initial_naming):
        # Modif Ugo :
        ### prise en charge du datamap
        path =dbutils.widgets.get('BTA_DATA_map') 
        datamap_spk = (spark.read
            .format("csv")
            .option("header",True)
            .options(delimiter=';')
            .load(path)
            )
        datamap = datamap_spk.toPandas()
        #datamap = pd.read_csv(path, sep=";")
        #datamap = pd.read_csv(path, sep=";", encoding="latin-1")

        #Creation d'un dictionnaire nouveau code -> ancien code
        #Si nouveau code alors on conserve le nouveau
        if initial_naming=='NAME_STND2021':
            dict_col=dict(datamap[['NAME_STND2021','INFO_CODE']].values.tolist())
        elif initial_naming=='INFO_CODE':
            dict_col=dict(datamap[['INFO_CODE','NAME_STND2021']].values.tolist())
        else:
            print('error in initial_naming')
            return None
            
        def translate_col_to_INFO_CODE(name):
            try :
                return dict_col[name]
            except :
                return name

        #Renommage des colonnes
        from functools import reduce
        oldcol=df.columns
        newcol=[translate_col_to_INFO_CODE(x) for x in oldcol]
        
        return reduce(lambda data, idx: data.withColumnRenamed(oldcol[idx], newcol[idx]), range(len(oldcol)), df)

    def convert_relative_ts_to_seconds(rts):
        rts_left=rts.partition('T')[0][1:]
        rts_right=rts.partition('T')[2]

        result=0
        rest=rts_left
        if 'Y' in rest:
            result=result+((365.25*24.0*3600.0)*float(rest.partition('Y')[0]))
            rest=rest.partition('Y')[2]
        if 'M' in rest:
            result=result+((365.25*24.0*3600.0/12.0)*float(rest.partition('M')[0]))
            rest=rest.partition('M')[2]
        if 'D' in rest:
            result=result+((24.0*3600.0)*float(rest.partition('D')[0]))
            rest=rest.partition('D')[2]
        rest=rts_right
        if 'H' in rest:
            result=result+(3600.0*float(rest.partition('H')[0]))
            rest=rest.partition('H')[2]
        if 'M' in rest:
            result=result+(60.0*float(rest.partition('M')[0]))
            rest=rest.partition('M')[2]
        if 'S' in rest:
            result=result+float(rest.partition('S')[0])
            rest=rest.partition('S')[2]
        return result

    udf_relts_to_sec=udf(lambda rts: convert_relative_ts_to_seconds(rts),FloatType())


    def clean_invalid(typelev, valeur, invalid, invalid_bev, invalid_phev, typnum):
        if (typelev == 25) & (len(invalid_bev) > 0):
            invalid = invalid + ',' + invalid_bev
        if (typelev == 26) & (len(invalid_phev) > 0):
            invalid = invalid + ',' + invalid_phev
        invalid = invalid.split(',')
        
        # Ajouter une vérification pour éviter les valeurs None
        invalid = [float(valinval) if valinval is not None else None for valinval in invalid]
        
        result = valeur
        
        # Ajouter une vérification pour éviter la conversion si la valeur est None
        if valeur in invalid and valeur is not None:
            result = 999
            
        if typnum == 'float' and result is not None:
            result = float(result)
        
        return result

    udf_clean_long=udf(lambda x,y,z,t,u,v: clean_invalid(x,y,z,t,u,v),LongType())
    udf_clean_double=udf(lambda x,y,z,t,u,v: clean_invalid(x,y,z,t,u,v),DoubleType())

    def SOCestim(typelev, ssstch):
        if ssstch is None or ssstch == 999:
            result = 999
        elif typelev == 25:
            result = int(ssstch)
        elif typelev == 26:
            result = (ssstch - 21.897) / 0.7246
            if result > 100:
                result = 100
            elif result < 0:
                result = 0
            result = int(np.round(result, 0))
        else:
            result = 999
        return result
        
    udf_SOCestim=udf(lambda x,y: SOCestim(x,y),IntegerType())

    def extract_battery_plug_data(start_date,end_date,part,list_vin):

        df_79 = tcv.read(
            spark, 79, start_date, end_date, False, list_vin, 'carbide'
        )
        batsum=translate_AFNOR_INFO_CODE(df_79,'NAME_STND2021')
        batsum=batsum.withColumn('RTS',udf_relts_to_sec(batsum.HETSSS))
        batsum=batsum.dropDuplicates(['HEAVIN','HESEID','HMSG'])

        df_68 = tcv.read(
            spark, 68, start_date, end_date, False, list_vin, 'carbide'
        )
        batrem=translate_AFNOR_INFO_CODE(df_68,'NAME_STND2021')
        batrem=batrem.withColumn('RTS',udf_relts_to_sec(batrem.HETSSS))
        batrem=batrem.dropDuplicates(['HEAVIN','HESEID','HMSG'])

        batsum=batsum.withColumn('BALOLE',lit(None))
        batrem=batrem.withColumn('SSSTCH',lit(None))

        batsum=batsum.withColumn('eSOC',udf_clean_double(batsum.HFTY,batsum.SSSTCH,lit('102.2,102.3'),lit('0'),lit(''),lit('float')))

        batsum=batsum.withColumn('eSOC',udf_SOCestim(batsum.HFTY,batsum.eSOC))
        batrem=batrem.withColumn('eSOC',udf_clean_long(batrem.HFTY,batrem.BALOLE,lit('127'),lit(''),lit('66'),lit('int')))

        batrem=batrem.select('HFTY','HEAVIN','HESEID',lit(68).alias('HMST'),'HMSG','HEDOCO','RTS','BALOLE','SSSTCH','eSOC',\
                            batrem.BACHMO.alias('CHMO'),batrem.BACHST.alias('CHST'))
        
        batsum=batsum.select('HFTY','HEAVIN','HESEID',lit(79).alias('HMST'),'HMSG','HEDOCO','RTS','BALOLE','SSSTCH','eSOC',\
                            batsum.SSCHMO.alias('CHMO'),batsum.SSCHST.alias('CHST'))
        
        batall=batrem.union(batsum)
        
        print(f'extract battery data {start_date} part {part} done')
        return batall


### take a reliable SOC value in the mn right after session start 
    def reliable_soc_session_start(soc,rts,socnextmn,rtsnextmn,param_start):
        if rts>param_start:
            result=soc 
        else:
            
            if len(socnextmn)>1:
                socprev=soc
                for i in range(len(socnextmn)):
                    socval=socnextmn[i]
                    rtsval=rtsnextmn[i]
                    if rtsval>param_start:
                        break
                    else:
                        socprev=socval
                        continue
                result=socprev
            
            else:
                result=soc
            
        return result

    udf_reliable_soc_session_start_long=udf(lambda x,y,z,t,u: reliable_soc_session_start(x,y,z,t,u),LongType())
    udf_reliable_soc_session_start_float=udf(lambda x,y,z,t,u: reliable_soc_session_start(x,y,z,t,u),FloatType())


    ### collect the soc value of the first mn after session start, and pick up a reliable value of SOC
    def cleaned_soc(databloc,socfield,param_start,typnum):
        wnextmn=Window.partitionBy('HEAVIN','HESEID').orderBy('RTS').rangeBetween(0,param_start)

        databloc=databloc.withColumn(socfield+'nextMN',collect_list(socfield).over(wnextmn))\
        .withColumn('RTSnextMN',collect_list('RTS').over(wnextmn))
        
        if typnum=='int':
            databloc=databloc.withColumn(socfield,udf_reliable_soc_session_start_long(col(socfield)\
                                                                ,databloc.RTS,col(socfield+'nextMN')\
                                                                ,databloc.RTSnextMN,lit(param_start)))
        elif typnum=='float':
            databloc=databloc.withColumn(socfield,udf_reliable_soc_session_start_float(col(socfield)\
                                                                ,databloc.RTS,col(socfield+'nextMN')\
                                                                ,databloc.RTSnextMN,lit(param_start)))
        else:
            return None
        
        databloc=databloc.drop(socfield+'nextMN').drop('RTSnextMN')
        return databloc


    ### replace invalid soc value with the next valid value

    def replace_null_soc(soc,ts,socnextmn,tsnextmn,param_lag):
        result=soc
        if soc==999:
            for i in range(len(socnextmn)):
                tsval=tsnextmn[i]
                gapts=(tsval-ts).total_seconds()
                if gapts>param_lag:
                    break
                elif socnextmn[i]==999:
                    continue
                else:
                    result=socnextmn[i]
                    break
        if result==999:
            result=None
        return result

    udf_replace_null_soc_long=udf(lambda x,y,z,t,u: replace_null_soc(x,y,z,t,u),LongType())
    udf_replace_null_soc_float=udf(lambda x,y,z,t,u: replace_null_soc(x,y,z,t,u),FloatType())


    def recover_soc(databloc,socfield,param_lag,typnum,d):
        wnextrows=Window.partitionBy('HEAVIN').orderBy('HMSG').rowsBetween(0,10)

        databloc=databloc.withColumn(socfield+'nextrows',collect_list(socfield).over(wnextrows))\
        .withColumn('TSnextrows',collect_list('HEDOCO').over(wnextrows))
        
        if typnum=='int':
            databloc=databloc.withColumn(socfield,\
                                        udf_replace_null_soc_long(col(socfield)\
                                                                ,databloc.HEDOCO,col(socfield+'nextrows')\
                                                                ,databloc.TSnextrows,lit(param_lag)))
        elif typnum=='float':
            databloc=databloc.withColumn(socfield,udf_replace_null_soc_float(col(socfield)\
                                                                ,databloc.HEDOCO,col(socfield+'nextrows')\
                                                                ,databloc.TSnextrows,lit(param_lag)))
        else:
            return None
        
        databloc=databloc.drop(socfield+'nextrows').drop('TSnextrows')
        print(f'calcul recover soc {d} done')
        return databloc

    def tag_plug(sschst,psschst):
        if psschst==None:
            result=None
        elif psschst!=1:
            if sschst==1:
                result='plug'
            else:
                result=None
        else: #psschst==1
            if sschst==1:
                result=None
            else:
                result='unplug'
        return result

    udf_tag_plug=udf(lambda x,y: tag_plug(x,y),StringType())

    def selectevent(evt,pevt,nevt):
        if evt=='plug':
            if (nevt=='unplug')|(nevt==None):
                return True
            else:
                return False
        elif evt=='unplug':
            if (pevt=='plug')|(pevt==None):
                return True
            else:
                return False
        else:
            return False

    udf_selectevent=udf(lambda x,y,z: selectevent(x,y,z),BooleanType())


    def cleanevents(events,evttype,d):
        events=events.filter(~(col(evttype)).isNull())
        we=Window.partitionBy('HEAVIN').orderBy('HMSG')
        events=events.withColumn('pevt',lag(evttype).over(we)).withColumn('nevt',lead(evttype).over(we))
        events=events.filter(udf_selectevent(col(evttype),events.pevt,events.nevt)).drop('pevt').drop(('nevt'))
        print(f'calcul clean envent {d} done')
        return events

    def clock_correction(TS,RTS,TSfin,RTSfin):
        if (TSfin==None)|(RTSfin==None)|(RTS==None):
            result=TS
        else:
            deltats=np.round((RTSfin-RTS),0)
            result=TSfin-dt.timedelta(0,deltats,0)
        return result

    udf_clock_correction=udf(lambda x,y,z,t: clock_correction(x,y,z,t),TimestampType())

    def calc_plug_events(batall,d):
        
        batall=cleaned_soc(batall,'eSOC',60,'int')
        batall=recover_soc(batall,'eSOC',300,'int',d)

        wplug=Window.partitionBy('HEAVIN').orderBy('HMSG')
        batall=batall.withColumn('pCHST',lag('CHST').over(wplug))
        batall=batall.withColumn('EVT',udf_tag_plug(batall.CHST,batall.pCHST))

        eventsAL=batall.filter(batall.EVT.isin(['plug','unplug']))
        eventsAL=cleanevents(eventsAL,'EVT',d)
        print(f'calcul plug envent {d} done')
        return eventsAL


    def calc_raw_charges(eventsAL):

        startsAL=eventsAL.filter(eventsAL.EVT=='plug')
        startsAL=startsAL.select(
            'HFTY','HEAVIN',\
            startsAL.HESEID.alias('SEIDstart'),\
            startsAL.HMST.alias('HMSTstart'),\
            startsAL.HMSG.alias('HMSGstart'),\
            startsAL.HEDOCO.alias('TSstart'),\
            startsAL.RTS.alias('RTSstart'),\
            startsAL.BALOLE.alias('BALOLEstart'),\
            startsAL.SSSTCH.alias('SSSTCHstart'),\
            startsAL.eSOC.alias('eSOCstart'),\
            startsAL.CHMO.alias('CHMOstart'),\
            startsAL.CHST.alias('CHSTstart'),\
            startsAL.part
        )


        stopsAL=eventsAL.filter(eventsAL.EVT=='unplug')
        stopsAL=stopsAL.select(
            stopsAL.HEAVIN.alias('HEAVIN2'),\
            stopsAL.HESEID.alias('SEIDstop'),\
            stopsAL.HMST.alias('HMSTstop'),\
            stopsAL.HMSG.alias('HMSGstop'),\
            stopsAL.HEDOCO.alias('TSstop'),\
            stopsAL.RTS.alias('RTSstop'),\
            stopsAL.BALOLE.alias('BALOLEstop'),\
            stopsAL.SSSTCH.alias('SSSTCHstop'),\
            stopsAL.eSOC.alias('eSOCstop'),\
            stopsAL.CHMO.alias('CHMOstop'),\
            stopsAL.CHST.alias('CHSTstop')
        )


        wns=Window.partitionBy('HEAVIN').orderBy('HMSGstart')
        startsAL2=startsAL.withColumn('nextHMSGstart',lead(startsAL.HMSGstart).over(wns))

        chargesAL=startsAL2.join(stopsAL,\
                        (startsAL2.HEAVIN==stopsAL.HEAVIN2)\
                        &(stopsAL.HMSGstop>startsAL2.HMSGstart)&((stopsAL.HMSGstop<startsAL2.nextHMSGstart)|(startsAL2.nextHMSGstart.isNull())),\
                        how='outer').drop('HEAVIN2')
        
        return chargesAL

    ### GROUPING CHARGES WITH A SMALL BREAK

    def gapts(ts1,ts2):
        if (ts1==None)|(ts2==None):
            result=None
        else:
            result=int((ts2-ts1).total_seconds())
        return result

    udf_gapts=udf(lambda x,y: gapts(x,y),IntegerType())

    def chargesgap(charges,d):
        wp=Window.partitionBy('HEAVIN').orderBy('HMSGstart')
        charges=charges.withColumn('pHMSGstop',lag('HMSGstop').over(wp))\
        .withColumn('pTSstop',lag('TSstop').over(wp))\
        .withColumn('nHMSGstart',lead('HMSGstart').over(wp))\
        .withColumn('nTSstart',lead('TSstart').over(wp))

        charges=charges.withColumn('pgapTS',udf_gapts(charges.pTSstop,charges.TSstart))\
        .withColumn('ngapTS',udf_gapts(charges.TSstop,charges.nTSstart))
        return charges

    def tagstart(pgapTS):
        if pgapTS==None:
            result=True
        elif pgapTS<=60:
            result=False
        else:
            result=True
        return result

    def tagstop(ngapTS):
        if ngapTS==None:
            result=True
        elif ngapTS<=60:
            result=False
        else:
            result=True
        return result

    udf_tagstart=udf(lambda x: tagstart(x),BooleanType())
    udf_tagstop=udf(lambda x: tagstop(x),BooleanType())

    def group_charges(charges,d):
        
        newstarts=charges.filter(udf_tagstart(charges.pgapTS)).select('HFTY','HEAVIN','SEIDstart','HMSTstart','HMSGstart',\
                                                                        'TSstart','RTSstart',\
                                                                    'BALOLEstart','SSSTCHstart','eSOCstart',\
                                                                        'CHMOstart','CHSTstart')
            

        newstops=charges.filter(udf_tagstop(charges.pgapTS)).select('HEAVIN','SEIDstop','HMSTstop','HMSGstop',\
                                                                        'TSstop','RTSstop',\
                                                                    'BALOLEstop','SSSTCHstop','eSOCstop',\
                                                                        'CHMOstop','CHSTstop')
            
        
        wp=Window.partitionBy('HEAVIN').orderBy('HMSGstart')
        newstarts=newstarts.withColumn('nHMSGstart',lead('HMSGstart').over(wp))
        newstops=newstops.withColumnRenamed('HEAVIN','HEAVIN2')

        charges=newstarts.join(newstops,(newstarts.HEAVIN==newstops.HEAVIN2)\
                            &(newstarts.HMSGstart<newstops.HMSGstop)\
                            &((newstarts.nHMSGstart>newstops.HMSGstop)|(newstarts.nHMSGstart.isNull()))\
                                ,how='left_outer')

        charges=charges.filter(~charges.HEAVIN2.isNull()).drop('HEAVIN2')
        charges=charges.withColumn('SOCevol',charges.eSOCstop-charges.eSOCstart)
        charges=charges.filter(charges.SOCevol>=0).filter(charges.HFTY.isin([25,26]))
        return charges
    
    ## INDICATORS DURATION, SPEED, POWER

    def charge_speed(charge,dur):
        if dur<=0:
            return None
        elif charge==None:
            return None
        else:
            return int(36000*charge/dur)/10

    udf_charge_speed=udf(lambda x,y: charge_speed(x,y),FloatType())

    def chpower(vehtype,chspeed):
        if chspeed==None:
            result=None
            return result
        elif vehtype==25:
            conversion_rate=0.56
        elif vehtype==26:
            conversion_rate=0.12
        else:
            result=None
            return result
        result=conversion_rate * chspeed
        return result

    udf_chpower=udf(lambda x,y: chpower(x,y),FloatType())

    def powercat(chPOWER):
        result='6-not classified'
        if chPOWER==None:
            return result
        elif chPOWER<0:
            return result
        elif chPOWER <1.2:
            result="0-Uncertain <1.2kW"
        elif chPOWER <2.7:
            result="1-Standard 1.8kW(<2.7)"
        elif chPOWER < 5.5:
            result="2-GreenUp 3.7kW(<5.5)"
        elif chPOWER < 9.2:
            result="3-WallBox 7kW(<9.2)"
        elif chPOWER <20:
            result="4-WallBox 11kW(<20)"
        elif chPOWER <80:
            result="5-DCcharger<80kW"
        return result
            
    udf_chpowercat=udf(lambda x: powercat(x),StringType())
        
    def charge_power(charges,d):
        charges=charges.withColumn('chDUR',udf_gapts(charges.TSstart,charges.TSstop))
        charges=charges.withColumn('chSPEED',udf_charge_speed(charges.SOCevol,charges.chDUR))
        charges=charges.withColumn('chPOWER',udf_chpower(charges.HFTY,charges.chSPEED))
        charges=charges.withColumn('chPWRCAT',udf_chpowercat(charges.chPOWER))
        return charges


    def launch():
        # start_date=dt.date(2022,1,1)
        # end_date=dt.date(2022,2,1)
        from datetime import datetime
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        nbdays=int((end_date-start_date).total_seconds()/(3600*24))+1
        print(nbdays)
        start=1
        list_vin=vin
        for d in range(nbdays-1):
            rolling_start=start_date+dt.timedelta(d)
            rolling_end=rolling_start+dt.timedelta(1)
            wyear=rolling_start.year
            wmonth=rolling_start.month
            wday=rolling_start.day
            wpath='/'+str(wyear)+'/'+str(wmonth).zfill(2)
            rolling_start_ts=dt.datetime(wyear,wmonth,wday,0,0,0,0,pytz.UTC)
            
            
            ## calculation plugs
            
            # batall=extract_battery_plug_data(start_date,end_date)
            print("tcv done")
            
            if start==1:
                batall_part1=extract_battery_plug_data(rolling_start,rolling_start,1,list_vin)
                data_temp_bat1 = dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/battery_part1"
                batall_part1.write.mode('Overwrite').parquet(data_temp_bat1)
                
            batall_part1=spark.read.parquet(data_temp_bat1)
            batall_part1=batall_part1.withColumn('part',lit(1))
            
                
            batall_part2=extract_battery_plug_data(rolling_end,rolling_end,2,list_vin)
            data_temp_bat2 = dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/battery_part2"
            batall_part2.write.mode('Overwrite').parquet(data_temp_bat2)
            batall_part2=batall_part2.withColumn('part',lit(2))
            
            batall=batall_part1.union(batall_part2)
            eventsAL=calc_plug_events(batall,d)

            ##################################################################################################################################
            ##################################################################################################################################
            ##################################################################################################################################
            #eventsAL=correct_ts(eventsAL,start_date,end_date,d)
            ##################################################################################################################################
            ##################################################################################################################################
            ##################################################################################################################################
            
            plugs=calc_raw_charges(eventsAL)
            
            # plugs=plugs.filter(plugs.part==1).drop('part')
            plugs=plugs.withColumn('CalcLastRecDay',lit(rolling_end))
            
            ### filtering inconsitent timestamps
            plugs=plugs.filter((plugs.TSstart>=(rolling_start_ts-dt.timedelta(30)))\
                            &(plugs.TSstart<=(rolling_start_ts+dt.timedelta(2))))
            plugs=plugs.filter((plugs.TSstop>=(rolling_start_ts-dt.timedelta(30)))\
                            &(plugs.TSstop<=(rolling_start_ts+dt.timedelta(2))))
            
            plugs=chargesgap(plugs,d)
            plugs=group_charges(plugs,d)
            plugs=charge_power(plugs,d)
            ## cleaning duration >5mn and <48h
            plugs=plugs.filter((plugs.chDUR>300)&(plugs.chDUR<2*3600*24))

            # filtering of plugs calculated in the previous loop and ending in the current loop (partial plugs)
            if start!=1:
                plugs_prev_path = dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/plugs_prev"
                plugs_prev=spark.read.parquet(plugs_prev_path)
                plugs_prev=plugs_prev.select(plugs_prev.HEAVIN.alias('HEAVIN2')\
                                            ,plugs_prev.HMSGstart.alias('HMSGstart2'),plugs_prev.HMSGstop.alias('HMSGstop2'))
                plugs=plugs.join(plugs_prev,(plugs.HEAVIN==plugs_prev.HEAVIN2)\
                                &(((plugs.HMSGstart>=plugs_prev.HMSGstart2)&(plugs.HMSGstart<=plugs_prev.HMSGstop2))\
                                |((plugs.HMSGstop==plugs_prev.HMSGstop2))), how='left_outer')
                plugs=plugs.filter(plugs.HEAVIN2.isNull()).drop('HEAVIN2').drop('HMSGstart2').drop('HMSGstop2')
            
            plugs_current_path = dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/plugs_current"
            plugs.write.parquet(plugs_current_path)
            
            plugs=spark.read.parquet(plugs_current_path)
            
            
            print(rolling_start,plugs.count())
            spark.conf.set('spark.sql.shuffle.partitions',6)
            plugs=plugs.repartition('HEAVIN')
            
            
            plugs.write.parquet(dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/plugs"+wpath,mode='append')
            dbutils.fs.rm(data_temp_bat1, recurse=True)
            dbutils.fs.mv(data_temp_bat2,data_temp_bat1,recurse=True)
            if start!=1:
                dbutils.fs.rm( dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/plugs_prev", recurse=True)
            
            dbutils.fs.mv(plugs_current_path, dbutils.widgets.get('Data_path_S3') +folder_name+"/levdata/temp/plugs_prev",recurse=True)
        
            spark.conf.set('spark.sql.shuffle.partitions',200)
            start=0


if __name__ == '__main__':
    task = Calculation()
    task.launch()


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
import matplotlib.pyplot as plt
import seaborn as sns
import builtins
import hashlib
import pickle
from pyspark.sql import types as T



def anonym(x):
    filename = '/Workspace/Users/ugo.merlier@stellantis.com/data/key/APRS_key'
    code=pickle.load(open(filename,"rb"))
    return hashlib.md5(x.encode('utf-8')+code.encode('utf-8')).hexdigest()

# print('Verif  : 7dc519c5b42887df1ba5a0d398719717')
# print('anonym : ' + anonym('VR3xxxxxxxxxxxxxx'))

# udf spark  function :
udf_hash = F.udf(anonym, T.StringType())



def Min_Max_date(df):
    min_date_start_plug = df.select(min("START")).first()[0]
    max_date_start_plug = df.select(max("START")).first()[0]

    min_date_stop_plug = df.select(min("STOP")).first()[0]
    max_date_stop_plug = df.select(max("STOP")).first()[0]

    print("Min date start :", min_date_start_plug)
    print("Max date start :", max_date_start_plug)
    print("Min date stop :", min_date_stop_plug)
    print("Max date stop :", max_date_stop_plug)




def anonymise_dataframe(df, VIN_col, VAN_col='VAN'):
    VIN_column = F.col(VIN_col)
    list_columns = df.columns
    df = df.withColumn('VAN', udf_hash(VIN_column))
    df.drop(VIN_column)
    new_list_columns = [VAN_col if col == VIN_col else col  for col in list_columns ]
    df = df.select(*new_list_columns)
    return df

def vin_pas_en_commun(list_vin_Avenir,list_vin_Dase):
    
    seulement_liste1 = list(set(list_vin_Avenir) - set(list_vin_Dase))

    seulement_liste2 = list(set(list_vin_Dase) - set(list_vin_Avenir))

    resultat = seulement_liste1 + seulement_liste2

    print(resultat)


def transform_to_plot(df_plug):
    df_spark_start = df_plug.select("START","HEAD_VAN",'SESSION_START','MESSAGE_START','Diff_soc','STOP-START_heures').withColumn("EVNT", F.lit("plug"))
    df_spark_stop = df_plug.select("STOP","HEAD_VAN",'SESSION_STOP',"MESSAGE_STOP",'Diff_soc','STOP-START_heures').withColumn("EVNT", F.lit("unplug"))
    df_union =df_spark_start.union(df_spark_stop)
    df_union = df_union.withColumnRenamed("START", "time")
    df_union = df_union.orderBy("HEAD_VAN")
    df_union = df_union.dropDuplicates()
    window_spec = Window.partitionBy('HEAD_VAN').orderBy('time')
    df_union = df_union.withColumn('row_num', F.row_number().over(window_spec))
    df_union = df_union.orderBy('HEAD_VAN', 'row_num').drop('row_num')
    return df_union


def plot(
        df1,
        df2,
        df_68,
        df_74,
        df_79,
        VIN,
        start_time=None,
        end_time=None,
        col_name1 = "BATT_STTS_SUMM_SOC",
        col_name2 = "VEHC_STTS_CHRG_STT",
        col_name3 ="BATT_CHRG_STT",
        col_name4="BATT_STTS_SUMM_CHRG_STT",
        col_name5="VEHC_STTS_LIFT_MILG",
        col_name6="BATT_LOAD_LEVL"):
        for i, vin in enumerate(VIN):
            # Filtrer les données selon le VIN et trier par le temps
            df1_temp = df1[df1['HEAD_VAN'] == vin]
            df1_temp = df1_temp.sort_values(by='time')
            df2_temp = df2[df2['HEAD_VAN'] == vin]
            df2_temp = df2_temp.sort_values(by='time')
            df_79_temp = df_79[df_79['HEAD_VAN'] == vin]
            df_79_temp = df_79_temp.sort_values(by='HEAD_COLL_TIMS')
            df_74_temp = df_74[df_74['HEAD_VAN'] == vin]
            df_74_temp = df_74_temp.sort_values(by='HEAD_COLL_TIMS') 
            df_68_temp = df_68[df_68['HEAD_VAN'] == vin]
            df_68_temp = df_68_temp.sort_values(by='HEAD_COLL_TIMS') 

            # Convertir les chaînes de caractères en objets datetime si spécifié
            if start_time is not None:
                start_time_datetime = datetime.strptime(start_time, '%Y-%m-%d')
            if end_time is not None:
                end_time_datetime = datetime.strptime(end_time, '%Y-%m-%d')
            
            # Créer une figure avec deux sous-tracés empilés verticalement
            fig, (ax1, ax2, ax3,ax4,ax5,ax6, ax7, ax8) = plt.subplots(8, 1, figsize=(17, 13), sharex=True)
            
            # Dase plug unplug Subplot
            ax1.step(df2_temp['time'], df2_temp['EVNT'], marker='o', linestyle='-', color='purple', label=vin, where='post')
            ax1.legend()
            ax1.invert_yaxis()
            ax1.set_ylabel('EVNT')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_title('Plug and unplug graph Dase')

            #Avenir plug unplug Subplot
            ax2.step(df1_temp['time'], df1_temp['EVNT'], marker='o', linestyle='-', color='b', label=vin, where='post')
            ax2.legend()
            ax2.invert_yaxis()
            ax2.set_ylabel('EVNT')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_title('Plug and unplug graph Avenir')
        
            # First avenir data subplot
            ax3.plot(df_79_temp['HEAD_COLL_TIMS'], df_79_temp[col_name1], marker='o', linestyle='-', color='green')
            ax3.set_title('Evolution du SOC avec la colonne ' + col_name1 + " venant du message 79  -- AVENIR --")
            ax3.set_xlabel('Time')
            ax3.set_ylabel('State of Charge (%)')
            ax3.grid(True)
            ax3.tick_params(axis='x', rotation=45)
            
            #Second avenir data subplot
            ax4.plot(df_74_temp['HEAD_COLL_TIMS'], df_74_temp[col_name2], marker='o', linestyle='-', color='cyan')
            ax4.set_title('Evolution du SOC avec la colonne ' + col_name2 + " venant du message 74  -- AVENIR --")
            ax4.set_xlabel('Time')
            ax4.set_ylabel('State of Charge (%)')
            ax4.grid(True)
            ax4.tick_params(axis='x', rotation=45)
            
            #Third avenir data subplot
            ax5.plot(df_74_temp['HEAD_COLL_TIMS'], df_74_temp[col_name5], marker='o', linestyle='-', color='#f0006a')
            ax5.set_title('Evolution des kilometres parcourus avec la colonne ' + col_name5 + " venant du message 74 -- AVENIR -- ")
            ax5.set_xlabel('Time')
            ax5.set_ylabel('KM')
            ax5.grid(True)
            ax5.tick_params(axis='x', rotation=45)
            
            #First dase data subplot
            ax6.plot(df_68_temp['HEAD_COLL_TIMS'], df_68_temp[col_name3], marker='o', linestyle='-', color='orange')
            ax6.set_title('Evolution du SOC avec la colonne ' + col_name3 + " venant du message 68  -- DASE -- ")
            ax6.set_xlabel('Time')
            ax6.set_ylabel('State value (0,1,3,4)')
            ax6.grid(True)
            ax6.tick_params(axis='x', rotation=45)
            
            #Second dase data subplot
            ax7.plot(df_79_temp['HEAD_COLL_TIMS'], df_79_temp[col_name4], marker='o', linestyle='-', color='#ff2a2e')
            ax7.set_title('Evolution du SOC avec la colonne ' + col_name4 + " venant du message 79  -- DASE -- ")
            ax7.set_xlabel('Time')
            ax7.set_ylabel('State value (0,1,3,4)')
            ax7.grid(True)
            ax7.tick_params(axis='x', rotation=45)
            
            #Third dase data subplot
            ax8.plot(df_68_temp['HEAD_COLL_TIMS'], df_68_temp[col_name6], marker='o', linestyle='-', color='#96a816')
            ax8.set_title('Niveau de charge ' + col_name6 + " venant du message 68  -- DASE -- ")
            ax8.set_xlabel('Time')
            ax8.set_ylabel('KM')
            ax8.grid(True)
            ax8.tick_params(axis='x', rotation=45)
            
            # Limiter l'intervalle de temps si spécifié
            if start_time is not None and end_time is not None:
                ax8.set_xlim(start_time_datetime, end_time_datetime)

            plt.tight_layout()

            plt.show()
            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------")


import matplotlib.pyplot as plt

def plot_comparison(df_dase, df_avenir, list_van, start_time=None, end_time=None):
    for i, van in enumerate(list_van):

        df_temp_dase = df_dase[df_dase['HEAD_VAN'] == van].sort_values(by='time')
        df_temp_avenir = df_avenir[df_avenir['HEAD_VAN'] == van].sort_values(by='time')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

        ax1.step(df_temp_dase['time'], df_temp_dase['EVNT'], marker='o', linestyle='-', color='purple', label=van, where='post', alpha=0.5)
        ax1.invert_yaxis()
        ax1.set_ylabel('EVNT')
        ax1.set_title('Plug and unplug graph Dase')

        ax_twinx = ax1.twinx()
        ax_twinx.bar(x=df_temp_dase['time'], height=df_temp_dase['Diff_soc'], width=0.1, color='orange', alpha=1,label="Quantité de soc gagné durant la charge")

        ax2.step(df_temp_avenir['time'], df_temp_avenir['EVNT'], marker='o', linestyle='-', color='b', label=van, where='post', alpha=0.5)
        ax2.invert_yaxis()
        ax2.set_ylabel('EVNT')
        ax2.set_title('Plug and unplug graph Avenir')

        ax_twinx2 = ax2.twinx()
        ax_twinx2.bar(x=df_temp_avenir['time'], height=df_temp_avenir['Diff_soc'], width=0.1, color='orange', alpha=1, label="Quantité de SOC gagné durant la charge")
        ax_twinx2.set_ylim(0, 100)

        # Positionnement des légendes
        ax1.legend(loc='upper right', bbox_to_anchor=(1, 1))
        ax_twinx.legend(loc='upper right', bbox_to_anchor=(1, 0.8))
        ax2.legend(loc='upper right', bbox_to_anchor=(1, 1))
        ax_twinx2.legend(loc='upper right', bbox_to_anchor=(1, 0.8))
        ax1.tick_params(axis='x', rotation=45)
        ax2.tick_params(axis='x', rotation=45)
        plt.tight_layout()
        plt.show()
        print("----------------------------------------------------------------------------------------------------------------------------------------------------------------")


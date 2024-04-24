from databricks.sdk.runtime import *
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
#import osy
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats
import seaborn as sns
from datetime import timedelta
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.dates as mdates
import json
from datetime import datetime

class Read_and_plot():

    def __init__(self, Charge_Data_Path_S3, folder_name, nouvelle_start_date):
        df_plug = (spark.read
                .format("parquet")
                .option("header",True)
                .options(delimiter=';')
                .load(Charge_Data_Path_S3+folder_name +"/levdata/plugs/"+ nouvelle_start_date)
                )
        self.df_plug = df_plug

        df_79 = (spark.read
                .format("parquet")
                .option("header",True)
                .options(delimiter=';')
                .load(Charge_Data_Path_S3+folder_name +"/Raw/79")
                )
        self.df_79 = df_79

        df_74   = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load(Charge_Data_Path_S3+folder_name +"/Raw/74")
                    )
        self.df_74 = df_74

    def SOC_correction(spark_df74):
        window_vin = Window.partitionBy('HEAD_VIN').orderBy('HEAD_COLL_TIMS')
        df_vehicle_statuts_trips=spark_df74.withColumn('DELTA_SOC',
                                                                    (F.col('BATT_STTS_SUMM_SOC')-F.lead(F.col('BATT_STTS_SUMM_SOC')).over(window_vin))
                                                                    )
        df_vehicle_statuts_trips = df_vehicle_statuts_trips.filter(abs(F.col("DELTA_SOC")) < 10)
        return df_vehicle_statuts_trips


    def transform_to_plot(df_plug):
        df_spark_start = df_plug.select("TSstart","HEAVIN",'SEIDstart','HMSGstart').withColumn("EVNT", F.lit("plug"))
        df_spark_stop = df_plug.select("TSstop","HEAVIN",'SEIDstop',"HMSGstop").withColumn("EVNT", F.lit("unplug"))
        df_union =df_spark_start.union(df_spark_stop)
        df_union = df_union.withColumnRenamed("TSstart", "time")
        df_union = df_union.orderBy("HEAVIN")
        df_union = df_union.dropDuplicates()
        window_spec = Window.partitionBy('HEAVIN').orderBy('time')
        df_union = df_union.withColumn('row_num', F.row_number().over(window_spec))
        df_union = df_union.orderBy('HEAVIN', 'row_num').drop('row_num')
        return df_union


    def plot_v1(df,VIN, start_time=None, end_time=None):
        df = df[df['HEAVIN'] == VIN]
        df = df.sort_values(by='time')
        plt.step(df['time'], df['EVNT'], marker='o', linestyle='-', color='b', label=VIN, where='post')
        plt.legend()
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.xticks(rotation=45)
        
        if start_time is not None:
            start_time = datetime.strptime(start_time, '%Y-%m-%d')
        if end_time is not None:
            end_time = datetime.strptime(end_time, '%Y-%m-%d')
            
        if start_time is not None and end_time is not None:
            plt.xlim(start_time, end_time)

        plt.show()


        import matplotlib.pyplot as plt
    from datetime import datetime

    def plot(df, df_79, col_name, VIN, start_time=None, end_time=None):
        for i, vin in enumerate(VIN):
            # Filtrer les données selon le VIN et trier par le temps
            df_temp = df[df['HEAVIN'] == vin]
            df_temp = df_temp.sort_values(by='time')
            
            # Convertir les chaînes de caractères en objets datetime si spécifié
            if start_time is not None:
                start_time_datetime = datetime.strptime(start_time, '%Y-%m-%d')
            if end_time is not None:
                end_time_datetime = datetime.strptime(end_time, '%Y-%m-%d')
            
            # Créer une figure avec deux sous-tracés empilés verticalement
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
            
            # Première subplot pour les données df
            ax1.step(df_temp['time'], df_temp['EVNT'], marker='o', linestyle='-', color='b', label=vin, where='post')
            ax1.legend()
            ax1.invert_yaxis()
            ax1.set_ylabel('EVNT')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_title('Plug and unplug graph')
            
            # Limiter l'intervalle de temps si spécifié
            if start_time is not None and end_time is not None:
                ax1.set_xlim(start_time_datetime, end_time_datetime)

            df_79_temp = df_79[df_79['HEAD_VIN'] == vin]
            # Deuxième subplot pour les données df_79
            df_79_temp = df_79_temp.sort_values(by='HEAD_COLL_TIMS')  # Tri par HEAD_COLL_TIMS
            ax2.plot(df_79_temp['HEAD_COLL_TIMS'], df_79_temp[col_name], marker='o', linestyle='-', color='orange')
            ax2.set_title('SOC evolution')
            ax2.set_xlabel('Time')
            ax2.set_ylabel('State of Charge (%)')
            ax2.grid(True)
            ax2.tick_params(axis='x', rotation=45)
            
            # Limiter l'intervalle de temps si spécifié
            if start_time is not None and end_time is not None:
                ax2.set_xlim(start_time_datetime, end_time_datetime)
            
            # Ajuster l'espacement entre les sous-tracés pour éviter les chevauchements
            plt.tight_layout()
            # Afficher le graphique

            plt.show()
            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------")

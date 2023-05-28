from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python_operator import PythonOperator


import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.task_group_execute_spark_application import execute_spark_application
from functions.task_group_execute_trino_file import execute_trino_file
from datetime import datetime, timedelta

global_dag_config = {
    "job_name": "PROVA-HIVE-CCMA",
    "description":"Ingesta GFK",
    "owner":"ccma",
    "email_dest":[],
    "application_s3_location": "s3a://"+Variable.get("ccma_entorn")+"/enterprise/zapping/etl/ccma-etl-0.2314.4-jar-with-dependencies.jar",
    "application_main_class": "com.pragsis.ccma.etl.control.ControlProcess"
}
current_path = "dags"

dag_arguments =  {
    #"end_date": datetime(),
    #"depends_on_past": False,
    "email": global_dag_config['email_dest'],
    "email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    #"dagrun_timeout": timedelta(minutes=60)
    "max_active_runs": 1,
    "start_date": datetime(2022, 12, 1),
    "provide_context": True
}


def execute_hive_query():
    hive_hook = HiveCliHook(hive_cli_conn_id='hiveserver2_default')  # Connection ID for Hive
    hive_query = """
-- La select de PRGASIS era incorrecta: a l'hora de determinar que per una sortida no hi ha rebot nomes tenia en compte la seguent del mateix canal. Es podia donar que la seguent no provoques REBOT, pero que n'hi hagues una altra
--                                      posterior, encara dins del GAP d'ubna hora que si que provoques el REBOT.
-- V03 (28/06/2018) ==============================================================================================================================
-- Afegim 'Apagado de television' a descripcion_emision_hacia i descripcion_emision_desde (igual que a canal_hacia i canal_desde).
--
-- Canvi de concepte del moment de l'ABANDONAMENT: 
--    * Fins ara es considerava el moment de l'abandonament com a l'hora de finalitzacio del consum (que compleix condicions d'abandonament)
--    * A partir d'ara (V03) considerem el moment de l'abandonament com el SEGON POSTERIOR a l'hora de finalitzacio del consum (que compleix condicions de l'abandonament). 
--      Com que totes les audiencies acaben al segon 59, el rebot passara a ser el minut seguent.
--
-- Consum minim de 5 min per considerar que hi ha ABANDONAMENT:
--    * Pot passar que diferència entre l’inici de visionat (25:05:00) i la fi del mateix (25:09:09) sigui de 00:04:59  ( < de 5 min). Cal considerar aquests consums com a valids per comptabilitzar ABANDONAMENT. Modifiquem la condicio:
--        . unix_timestamp(B.hora_salida) - unix_TIMESTAMP(B.hora_inicio_visionado)   >=  300 ---->> unix_timestamp(B.hora_salida) - unix_TIMESTAMP(B.hora_inicio_visionado)   >=  299 
--
-- V04a (03/01/2019) ==============================================================================================================================
-- Els dos dies de l'any que hi ha canvi horari (ultims diumenges de març [01:59:59 -> 03:00:00] i octubre [02:59:59 -> 02:00:00]) generen problemes: 
--     kantar envia les dades d'audiencies amb els hores corregides de forma que no es visualitzen salts.
--     Quan transformem aquestes hores amb unix_timestamp / from_unixtime (per comparar dates o calcular consum), HIVE aplica el canvi horari, perdent-se el valor original.
--     Partim de la base (verificada) que TOTS ELS CAMPS DE KANTAR_SORTIDES que tenen valor data_hora tenen la data de "fecha" SEMPRE. És la part hora que fa que saltin de dia.
--     Per solucionar el problema, farem les comparacions i calculs amb dates auxiliars ficticies. 
--     Aixi podem substituir la part "data" amb un dia fix que no tingui canvi horari (pe:"2018-01-01"), mantenint la part "hora"

SET mapred.reduce.tasks=-1;
SET hive.exec.reducers.bytes.per.reducer=235929600;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.tezfiles=true;
SET hive.merge.smallfiles.avgsize=128000000;
SET hive.merge.size.per.task=128000000;
SET hive.tez.container.size=16384;
INSERT INTO TABLE ccma_analytics.kantar_abandonament
SELECT   s.*, 
         --p.descripcion_emision_actual as descripcion_emision_siguiente --<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
         --CASE WHEN s.canal_siguiente = s.canal_actual OR s.hora_inicio_visionado_siguiente is null
         --     THEN 'Apagado de television'
         --     ELSE p.descripcion_emision_actual 
         --END AS descripcion_emision_siguiente
         
         CASE WHEN s.canal_siguiente = 'Apagado de television'
              THEN 'Apagado de television'
              ELSE p.descripcion_emision_actual -- de la join amb kantar_sortides "siguiente"
         END AS descripcion_emision_siguiente

FROM    (   SELECT   DISTINCT s.fecha, 
                     s.individuo, 
                     s.canal_actual, 
                     s.hora_inicio_visionado,
                     hora_abandono,
                     hora_inicio_bloque,                                   
                     hora_fin_bloque,     
                     descripcion_emision_actual,                                
                     codigo_emision_actual,                                                               
                     s.codigo_tipo_bloque_actual,                    
                     s.descripcion_tipo_bloque_actual,                                    
                     genero_actual,
                     hora_inicio_visionado_siguiente,
                     hora_fin_visionado_siguiente,                                
                     --CASE WHEN s.canal_siguiente = s.canal_actual OR hora_inicio_visionado_siguiente is null
                     CASE WHEN hora_inicio_visionado_siguiente is null
                          THEN 'Apagado de television'
                          ELSE s.canal_siguiente 
                     END AS canal_siguiente,
                     hora_inicio_visionado_sig_mism_canal,
                     hora_fin_visionado_sig_mism_canal,
                     comunidad_autonoma,                                      
                     numero_habitantes_municipio,                                      
                     provincia,                                               
                     clase_social ,                                             
                     tamano_hogar ,                                             
                     lengua_autonomica_hogar,                                      
                     numero_televisiones_hogar,                                      
                     genero,                                                    
                     edad_tramo,                                              
                     estado_civil,                                              
                     nivel_estudios,                                            
                     numero_automoviles_hogar,                                      
                     actividad_individuo,                                       
                     equipamiento_hogar,                                      
                     lugar_nacimiento,                                          
                     posesion_television_segunda_residencia,                                      
                     posesion_animales  ,                                      
                     posesion_telefono_movil ,                                     
                     uso_internet,                                         
                     ciclo_vida_aimc,
                     id_plataforma,
                     dia_semana,
                     programa_asociado_bloque,
                     ciclo_vida_tns,
                     tv_iptv,
                     lengua_autonomica_individuo,
                     indice_socioeconomico,
                     tv_cable_concesion,
                     tv_ott,
                     operador_movistar,
                     operador_vodafone,
                     operador_otros,
                     substr(hora_abandono,      6,2) as mes_hora_abandono,
                     substr(hora_abandono,      9,2) as dia_hora_abandono,
                     substr(hora_abandono,     12,2) as hora_hora_abandono,
                     substr(hora_abandono,     15,2) as minuto_hora_abandono,
                     substr(hora_inicio_bloque,12,2) as hora_hora_inicio_bloque,
                     substr(hora_inicio_bloque,15,2) as minuto_hora_inicio_bloque,
                     substr(hora_inicio_bloque,18,2) as segundos_hora_inicio_bloque,
                     if (codigo_tipo_bloque_actual='1',hora_inicio_bloque,         null)                 as hora_inicio_programa,
                     if (codigo_tipo_bloque_actual='1',hora_fin_bloque,            null)                 as hora_fin_programa,
                     if (codigo_tipo_bloque_actual='1',descripcion_emision_actual,'Otro tipo de bloque') as programa
            FROM    (   SELECT   fecha, 
                                 individuo, 
                                 id_plataforma,
                                 canal_actual, 
                                 hora_inicio_visionado,
                                 
                                 --hora_salida AS hora_abandono,                                                                                                                         -- V04a
                                 -- Rectifiquem, si cal, l'hora de rebot pq el format de data-hora ha de ser sempre amb el mateix dia amb hora que calgui (00,01,...,23,24,25,....).     -- V04a
                                 -- IF ( substring(hora_abandono_sense_rectificar,1,10) = fecha,                                                                                         -- V04a
                                 --      hora_abandono_sense_rectificar, -- seguim al mateix dia,                                                                                        -- V04a
                                 --      concat(fecha,' ',  hour(hora_abandono_sense_rectificar) +24  , from_unixtime(unix_timestamp(hora_abandono_sense_rectificar)  , ':mm:ss'    ))   -- V04a
                                 --    ) as hora_abandono,                                                                                                                               -- V04a
                                 -- eliminem tot aquest codi perque amb la versio V04a ja definim hora_rebote a la select mes interna i amb el format correcte                           -- V04a
                                 hora_abandono,                                                                                                                                          -- V04a
                                 
                                 hora_salida,
                                 hora_inicio_bloque,
                                 hora_fin_bloque,
                                 descripcion_emision_actual,
                                 codigo_emision_actual,
                                 codigo_tipo_bloque_actual,                    
                                 descripcion_tipo_bloque_actual, 
                                 genero_actual,                
                                 hora_inicio_visionado_hacia as hora_inicio_visionado_siguiente,
                                 hora_fin_visionado_hacia as hora_fin_visionado_siguiente,
                                 canal_hacia as canal_siguiente,
                                 hora_inicio_visionado_sig_mism_canal,
                                 hora_fin_visionado_sig_mism_canal,
                                 comunidad_autonoma,
                                 numero_habitantes_municipio,
                                 provincia,clase_social,
                                 tamano_hogar,
                                 lengua_autonomica_hogar,
                                 numero_televisiones_hogar,
                                 genero,
                                 edad_tramo,
                                 estado_civil,
                                 nivel_estudios,
                                 numero_automoviles_hogar,
                                 actividad_individuo, 
                                 equipamiento_hogar,
                                 lugar_nacimiento,                                          
                                 posesion_television_segunda_residencia,
                                 posesion_animales,
                                 posesion_telefono_movil,
                                 uso_internet, 
                                 ciclo_vida_aimc,
                                 dia_semana,
                                 programa_asociado_bloque,
                                 ciclo_vida_tns,
                                 tv_iptv,
                                 lengua_autonomica_individuo,
                                 indice_socioeconomico,
                                 tv_cable_concesion,
                                 tv_ott,
                                 operador_movistar,
                                 operador_vodafone,     
                                 operador_otros                                                
                        FROM    ( 
                                    SELECT   B.*,
                                             -- Fem que l'hora de abandono sigui al minut seguent de la sortida. Li sumem 1 segon pq el segon de hora_salida sempre es 59
                                             --from_unixtime(unix_timestamp(B.hora_salida) + 1,'yyyy-MM-dd HH:mm:ss') as hora_abandono_sense_rectificar  -- V04a
                                             if (  from_unixtime(unix_timestamp(concat('2018-01-01', ' ', SUBSTR(B.hora_salida,12, 8)))+1,'yyyy-MM-dd') = '2018-01-01'                                                                                                                                              -- V04a
                                                 , concat( SUBSTR(B.hora_salida, 1,10) , ' ',                                                                                                                 from_unixtime(unix_timestamp(concat('2018-01-01', ' ', SUBSTR(B.hora_salida,12, 8)))+1,'HH:mm:ss') )  -- V04a
                                                 , concat( SUBSTR(B.hora_salida, 1,10) , ' ', cast(from_unixtime(unix_timestamp(concat('2018-01-01', ' ', SUBSTR(B.hora_salida,12, 8)))+1,'HH') +24 as int ), from_unixtime(unix_timestamp(concat('2018-01-01', ' ', SUBSTR(B.hora_salida,12, 8)))+1,  ':mm:ss') )  -- V04a
                                                ) as hora_abandono       -- amb aquesta versio (V04a) hora_abandono ja tindra el format correcte yyyy-MM-dd HH:mm:ss del dia "fecha" i "hora" fins que calgui 23,24,25,26,....  Podem prescindir de hora_abandono_sense_rectificar                                       -- V04a


                                    FROM    (   
                                                SELECT   B.* 
                                                FROM     ccma_analytics.kantar_sortides B  
                                                WHERE    B.fecha NOT IN ( 
                                                                          SELECT   DISTINCT a.FECHA 
                                                                          FROM     ccma_analytics.kantar_abandonament a 
                                                                        )
                                                --FROM     ccma_treball.treb_kantar_sortides_2018_04_03 B    ---------------------------------------------------------------- TAULA DE TEST *************************************************
                                            ) B
                                    LEFT OUTER JOIN
                                            ccma_analytics.kantar_rebots R
                                            --ccma_treball.treb_kantar_rebots_2018_04_03 R    ---------------------------------------------------------------- TAULA DE TEST *************************************************
                                    ON       B.fecha                 = R.fecha        
                                      AND    B.individuo             = R.individuo    
                                      AND    B.id_plataforma         = R.id_plataforma
                                      AND    B.canal_actual          = R.canal_actual
                                      AND    B.hora_inicio_visionado = R.hora_inicio_visionado
                                    
                                    WHERE    --unix_timestamp(                                 B.hora_salida        ) - unix_TIMESTAMP(                                 B.hora_inicio_visionado        )   >=  299 -- El visionat actual >= 5Min   V04a
                                             unix_timestamp  (concat('2018-01-01', ' ', SUBSTR(B.hora_salida,12, 8))) - unix_TIMESTAMP(concat('2018-01-01', ' ', SUBSTR(B.hora_inicio_visionado,12, 8)))   >=  299 -- El visionat actual >= 5Min   V04a
                                      AND    R.fecha IS NULL						

                                ) t
                    ) s 
        ) s
LEFT OUTER JOIN 
         ccma_analytics.kantar_sortides p --se cruza consigo misma para poder coger la descripcion de la emision siguiente
         --ccma_treball.treb_kantar_sortides_2018_04_03 p    ---------------------------------------------------------------- TAULA DE TEST *************************************************
ON       s.fecha                           = p.fecha 
  AND    s.canal_siguiente                 = p.canal_actual  
  AND    s.id_plataforma                   = p.id_plataforma 
  AND    s.individuo                       = p.individuo 
  AND    s.hora_inicio_visionado_siguiente = p.hora_inicio_visionado
    """
    hive_query = """SELECT *
    FROM ccma_analytics.kantar_pelicules
    LIMIT 100
    ;   """

    hive_hook.run_cli(hql=hive_query)

with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:


    hive_task = PythonOperator(
    task_id='hive_query_task',
    python_callable=execute_hive_query,
    dag=dag,
    )
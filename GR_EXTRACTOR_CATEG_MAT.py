import pyodbc
import pandas as pd
import datetime as dt 
import configparser
import vertica_python

try:
    consulta="""
    select
        "DBGR"."DW_GR"."D_ARTICULO_SAP"."CODARTICULO",
        "DBGR"."DW_GR"."D_ARTICULO_SAP"."GRUPO_DEPARTAMENTO"
    from
        "DBGR"."DW_GR"."D_ARTICULO_SAP"
    """


    config = configparser.ConfigParser()
    config.read('C:\DevProjects\Codes\config.ini')
    vertica_con=config['vertica_dwh']

    vertica_config = {
    'host': config['vertica_dwh']['server'],
    'port': int(config['vertica_dwh']['port']),
    'user': config['vertica_dwh']['username'],
    'password': config['vertica_dwh']['password'],
    'database': config['vertica_dwh']['database'],
    'autocommit': True
}

# Connect to Vertica
    with vertica_python.connect(**vertica_config) as connection:
        cursor = connection.cursor()
        cursor.execute(consulta)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(r'C:\DevProjects\DATASETS\GR_CATEGORIZACION_MAT.csv', index=False)


except pyodbc.Error as e:
    print("Database error:", e)
    #conn.close()
except pyodbc.DataError as e:
    print("Database error:", e)
    #conn.close()
except pyodbc.DatabaseError as e:
    print("Database error:", e)
    #conn.close()
finally:
    if 'conn' in locals():
        cursor.close()

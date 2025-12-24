import pyodbc
import pandas as pd
import datetime as dt 
import configparser
import vertica_python


try:
    yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
    onda=['LA ONDA']
    formatted_yesterday = yesterday.strftime('%Y-%m-%d')
    day_mes_ayer = int(yesterday.strftime('%d'))
    mes_actual = int(yesterday.strftime('%m'))
    year_actual = int(yesterday.strftime('%Y'))

    consulta="""
    SELECT fpds.Codalmacen, 
        fpds.CATEGORIA, 
        fpds.Fecha, 
        YEAR(fpds.FECHA) AS AÑO_FECHA,
        MONTH(fpds.FECHA) AS MES_FECHA,
        DAY(fpds.FECHA) AS DIA_FECHA,
        CAT.DPTO AS COD_CATEGORIA,
        CAT.NOMBRE_DEPARTAMENTO AS NOM_CATEGORIA,
        "Presupuesto Venta $"  
        FROM "DBGR"."DW_GR".F_PRESUPUESTO_DIARIO_SAP  fpds 
        LEFT JOIN (
         select DPTO,NOMBRE_DEPARTAMENTO
    from
        "DBGR"."DW_GR"."D_ARTICULO_SAP"
        GROUP BY DPTO,NOMBRE_DEPARTAMENTO)CAT
        ON  fpds.CATEGORIA=CAT.NOMBRE_DEPARTAMENTO
        WHERE year(fpds.fecha) = 2025 
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

        df.to_csv(r'C:\DevProjects\DATASETS\GR_PRESUPUESTO.csv', index=False)
        #df.to_csv(r'C:\DevProjects\DATASETS\GR_PPTO_COMPRADOR.csv', index=False)


    plantas=pd.read_csv('C:\DevProjects\DATASETS\GR_PLANTAS.csv')
    #----------------------------------------------------------------------------------
    #PRESUPUESTO 
    presupuesto=pd.read_csv(r'C:\DevProjects\DATASETS\PPTO_LAONDA_2025.csv')
    presupuesto['Fecha'] = pd.to_datetime(presupuesto['Fecha'])
    presupuesto['AÑO_FECHA'] = presupuesto['Fecha'].dt.year

    presupuesto['MES_FECHA'] = presupuesto['Fecha'].dt.month
    presupuesto['DIA_FECHA'] = presupuesto['Fecha'].dt.day


    ppto_filter=presupuesto[(presupuesto['MES_FECHA']==mes_actual)& 
                            (presupuesto['AÑO_FECHA']==year_actual) &
                            (presupuesto['DIA_FECHA']<=day_mes_ayer)  ]  

    df_ppto_ag=ppto_filter.groupby(['Fecha','Codalmacen','ID_CATEGORIA','ID_SUBCATEGORIA','ID_SEGMENTO','ID_SUBSEGMENTO','CATEGORIA',
            'SUBCATEGORIA',
    'SEGMENTO',
    'SUBSEGMENTO'   ]).agg({
        'Presupuesto_Venta':'sum',
        'Presupuesto_Costo':'sum'
    }).reset_index()
    df_ppto_ag = df_ppto_ag.rename(columns={
        'Presupuesto_Venta':'PPTO_VENTAS', 
        'Presupuesto_Costo':'PPTO_COSTO'
    })
    df_ppto_ag['key'] =  (
        df_ppto_ag['ID_CATEGORIA'].astype(str) +
        df_ppto_ag['ID_SUBCATEGORIA'].astype(str)+
        df_ppto_ag['ID_SEGMENTO'].astype(str)+
        df_ppto_ag['ID_SUBSEGMENTO'].astype(str)
    )


    def definidorComprador(df_ppto_ag): 
        elements_to_check = [235,250,260,265,285,290]
        ehud_elements = [280,240]
        isaac_elemnts=[210,215,220,230]
        bryan_elements=[270,295]
        rob_elements=[225]
        centro_onda=['O1','O2','O3','O5','O7','O9']
        
        if df_ppto_ag['ID_SEGMENTO']       == 2950102  : return "EL FUERTE"

        elif df_ppto_ag['ID_SEGMENTO']     == 2100501 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SEGMENTO']     == 2150401 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SEGMENTO']     == 2200501 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 21007 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 21507 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 22505 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 22007 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 27013 : return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_SUBCATEGORIA'] == 27014 : return "GABRIEL HOMSANY"

        elif df_ppto_ag['ID_SUBCATEGORIA'] ==  21006 : return "EHUD BOTTARO"
        elif df_ppto_ag['ID_SUBCATEGORIA'] ==  21506 : return "EHUD BOTTARO"
        elif df_ppto_ag['ID_SUBCATEGORIA'] ==  22006 : return "EHUD BOTTARO"
        elif df_ppto_ag['ID_SUBCATEGORIA'] ==  22506 : return "EHUD BOTTARO"
        elif df_ppto_ag['ID_SUBCATEGORIA'] ==  23005 : return "EHUD BOTTARO"

        elif df_ppto_ag['ID_CATEGORIA']    in elements_to_check: return "GABRIEL HOMSANY"
        elif df_ppto_ag['ID_CATEGORIA']    in ehud_elements: return "EHUD BOTTARO"
        elif df_ppto_ag['ID_CATEGORIA']    in isaac_elemnts: return "ISAAC ZEBEDE"
        elif df_ppto_ag['ID_CATEGORIA']    in bryan_elements: return "BRYAN NEIMANN"
        elif df_ppto_ag['ID_CATEGORIA']    in rob_elements: return "ROBERTO BENAIM"
        elif df_ppto_ag['Codalmacen']      in centro_onda: return "SUPERMERCADO"
        return "SUPERMERCADO"
    df_ppto_ag['COMPRADOR'] = df_ppto_ag.apply(definidorComprador, axis=1)


    #print(f'PPTO \n\n {df_ppto_ag.columns}\n')
    #print(f'PPTO \n\n {df_ppto_ag.dtypes}\n')

    df_final=pd.merge(df_ppto_ag,plantas,how="left", left_on=['Codalmacen'], right_on=['Plant'])
    union_resultado=df_final.groupby(['COMPRADOR']).agg({
        'PPTO_VENTAS': 'sum', 
        'PPTO_COSTO': 'sum'
    }).reset_index()

    union_resultado.to_excel('C:\DevProjects\DATASETS\GR_PPTO_AGG.xlsx', index=True)


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

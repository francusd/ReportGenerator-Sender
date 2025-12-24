import pyodbc
import pandas as pd
import datetime as dt 
import configparser


try:
 QUERY = """

SELECT avc.FECHA,
concat(concat(year(avc.FECHA),'-'),month(avc.FECHA))as periodo,
 (case when avl.CODALMACEN like ('F%') then 'El Fuerte'
 when avl.CODALMACEN like ('O%') then 'La Onda'
 when avl.CODALMACEN like ('P%') then 'Pharmacy' end ) as CENTRO,
AVL.CODALMACEN,
A.DEPARTAMENTO,
A.SECCION,
A.FAMILIA,
A.SUBFAMILIA,
A.CODDEPARTAMENTO,
A.CODSECCION,
A.CODFAMILIA,
A.CODSUBFAMILIA,
count(distinct AVL.CODARTICULO) AS CNT_DISTINCT_ARTICULO,
count(distinct concat(avl.NUMSERIE,avl.NUMALBARAN)) as CNT_DISTINCT_ticket,
Sum(AVL.UNIDADESTOTAL) as UNIDADES,
Sum(AVL.UNIDADESTOTAL * (AVL.PRECIO - (AVL.PRECIO * (AVL.DTO / 100)) - (AVL.PRECIO * (AVC.DTOCOMERCIAL / 100)))) as VENTA,
Sum(AVL.UNIDADESTOTAL * AVL.COSTE) as COSTO,
Sum(AVL.UNIDADESTOTAL * (AVL.PRECIO - (AVL.PRECIO * (AVL.DTO / 100)) - (AVL.PRECIO * (AVC.DTOCOMERCIAL / 100)))) as VENTA_BRUTA,
SUM( AVL.UNIDADESTOTAL* ( AVL.PRECIOIVA - (AVL.PRECIO*(AVL.DTO/100)) ) ) AS VENTA_NETA
FROM ALBVENTACAB avc With(NoLock)
left join ALBVENTALIN avl With(NoLock)
on avc.NUMSERIE=avl.NUMSERIE	
and avc.NUMALBARAN=avl.NUMALBARAN
AND AVC.N=AVL.N 
left join Z_ICG_ARTICULOSMAESTROS A With(NoLock) on
AVL.CODARTICULO=A.CODARTICULO 
where avc.FECHA>='20250101' 
and AVC.TIQUET = 'T'
and AVC.TIPODOC = 13
group by 
avc.FECHA,
AVL.CODALMACEN,
A.DEPARTAMENTO,
A.SECCION,
A.FAMILIA,
A.SUBFAMILIA,
A.CODDEPARTAMENTO,
A.CODSECCION,
A.CODFAMILIA,
A.CODSUBFAMILIA
    """
 
 config = configparser.ConfigParser()
 config.read('C:\DevProjects\Codes\config.ini')
 icg_sales=config["ventas_icg"]

 #connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
 
 connection_string = f"DRIVER={icg_sales['DRIVER']};SERVER={icg_sales['server']};DATABASE={icg_sales['database']};UID={icg_sales['username']};PWD={icg_sales['password']}"
 conn = pyodbc.connect(connection_string)
 df = pd.read_sql(QUERY, conn)
 df.to_csv(r'C:\DevProjects\DATASETS\VENTAS_ICG_2025.csv')
 print("EXPORTACION COMPLETADA")

 
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
        conn.close()

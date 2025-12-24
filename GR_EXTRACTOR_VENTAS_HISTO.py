import pyodbc
import pandas as pd
import datetime as dt 
import configparser


try:
 QUERY = """
    select
	[A].[CODDEPARTAMENTO],
 	[A].[DEPARTAMENTO],
 	[A].[CODSECCION],
    [A].[SECCION],
    [A].[CODFAMILIA],
    [A].[FAMILIA],
    [A].[CODSUBFAMILIA],
    [A].[SUBFAMILIA],
	[AVC].[FECHA],
	[AVC].[NUMSERIEFAC],
	[AVC].[NUMFAC],
	[AVC].[N],
	[AVL].[CODALMACEN],
	Count(Distinct Concat([AVC].[NUMSERIEFAC], Str([AVC].[NUMFAC]))) as [tickets],
	Sum([AVL].[UNIDADESTOTAL]) as [UNIDADES],
	Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) as [VENTA],
	Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE]) as [COSTO],
	Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) - Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE]) as [BENEFICIO],
	(Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) - Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE])) / Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) as [MARGEN],
	[A].[CODARTICULO]
from
	[BD1].[dbo].[ALBVENTACAB] [AVC] With(NoLock)
left join [BD1].[dbo].[ALBVENTALIN] [AVL] With(NoLock) on
	[AVC].[NUMSERIE] = [AVL].[NUMSERIE]
	and [AVC].[NUMALBARAN] = [AVL].[NUMALBARAN]
	and [AVC].[N] = [AVL].[N]
left join [BD1].[dbo].[Z_ICG_ARTICULOSMAESTROS] [A] With(NoLock) on
	[A].[CODARTICULO] = [AVL].[CODARTICULO]
where
	[AVC].[FECHA] >= case
		when Day(GetDate()) = 1 then DateAdd(MONTH, DateDiff(MONTH, 0, DateAdd(YEAR, -1, GetDate())) - 1, 0)
		else DateAdd(MONTH, DateDiff(MONTH, 0, DateAdd(YEAR, -1, GetDate())), 0)
	end
	and [AVC].[FECHA] <= case
		when Day(GetDate()) = 1 then DateAdd(DAY, 0, EOMonth(DateAdd(YEAR, -1, GetDate()), -1))
		else DateAdd(DAY, -1, DateAdd(YEAR, -1, Cast(GetDate() as [DATE])))
	end
	and [AVC].[TIQUET] = 'T'
	and [AVC].[TIPODOC] = 13
group by
	[A].[CODDEPARTAMENTO],
 	[A].[DEPARTAMENTO],
 	[A].[CODSECCION],
    [A].[SECCION],
    [A].[CODFAMILIA],
    [A].[FAMILIA],
    [A].[CODSUBFAMILIA],
    [A].[SUBFAMILIA],
	[AVC].[FECHA],
	[AVC].[NUMSERIEFAC],
	[AVC].[NUMFAC],
	[AVC].[N],
	[AVL].[CODALMACEN],
	[A].[CODARTICULO]
having
	Sum([AVL].[TOTAL]) <> 0
    """
 
 config = configparser.ConfigParser()
 config.read('C:\DevProjects\Codes\config.ini')
 icg_sales=config["ventas_hist_icg"]
 print(icg_sales)   
 #connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
 
 connection_string = f"DRIVER={icg_sales['DRIVER']};SERVER={icg_sales['server']};DATABASE={icg_sales['database']};UID={icg_sales['username']};PWD={icg_sales['password']}"
 conn = pyodbc.connect(connection_string)
 df = pd.read_sql(QUERY, conn)
 #print("\n\n", df.head(10))
 df.to_csv(r'C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')


 
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

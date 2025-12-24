import time
import pandas as pd
import re 

start = time.perf_counter()
yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
onda=['LA ONDA']
formatted_yesterday = yesterday.strftime('%Y-%m-%d')
day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))

#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')
ventas_act['VENTA HIST']=0
ventas_act_filt = ventas_act.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT'
})

#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo_filt = ventas_histo.rename(columns={
     'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA HIST'
})


plantas=pd.read_csv('C:\DevProjects\DATASETS\GR_PLANTAS.csv')

#----------------------------------------------------------------------------------
#PRESUPUESTO 
df_ppto_ag=pd.read_excel(r'C:\DevProjects\DATASETS\GR_PPTO_AGG.xlsx')

#----------------------------------------------------------------------------------
##RELACIONA LAS VENTAS(ACTUALES VS HISTORICA)

dfventas=pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)
result = dfventas.loc[:,~dfventas.columns.duplicated()].copy()

result['CODSECCION'] = result['CODSECCION'].apply(lambda x: f"{x:02}")
result['CODFAMILIA'] = result['CODFAMILIA'].apply(lambda x: f"{x:02}")
result['CODSUBFAMILIA'] = result['CODSUBFAMILIA'].apply(lambda x: f"{x:02}")
result['key'] =  (
    result['CODDEPARTAMENTO'].astype(str) +
    result['CODSECCION'].astype(str)+
    result['CODFAMILIA'].astype(str)+
    result['CODSUBFAMILIA'].astype(str)
)

#----------------------------------------------------------------------------------
# CREACION DE COLUMNA DE COMPRADOR 
#
def definidorComprador(row): 
  elements_to_check = [235,250,260,265,285,290]
  ehud_elements = [280,240]
  isaac_elemnts=[210,215,220,230]
  bryan_elements=[270,295]
  rob_elements=[225]
  centro_onda=['O1','O2','O3','O5','O7','O9']
  key = row['key']

  if '295.01.02.' in key: return "EL FUERTE"
  elif '210.05.01.' in key: return "GABRIEL HOMSANY"
  elif '215.04.01.' in key: return "GABRIEL HOMSANY"
  elif '220.05.01.' in key: return "GABRIEL HOMSANY"
  elif '210.07.' in key: return "GABRIEL HOMSANY"
  elif '215.07.' in key: return "GABRIEL HOMSANY"
  elif '225.05.' in key: return "GABRIEL HOMSANY"
  elif '220.07.' in key: return "GABRIEL HOMSANY"
  elif '270.013.' in key: return "GABRIEL HOMSANY"      
  elif '270.014.' in key: return "GABRIEL HOMSANY"
  elif '210.06.' in key: return "EHUD BOTTARO"
  elif '215.06.' in key: return "EHUD BOTTARO"
  elif '220.06.' in key: return "EHUD BOTTARO"
  elif '225.06.' in key: return "EHUD BOTTARO"
  elif '230.05.' in key: return "EHUD BOTTARO"
  elif row['CODDEPARTAMENTO'] in elements_to_check: return "GABRIEL HOMSANY"
  elif row['CODDEPARTAMENTO'] in ehud_elements: return "EHUD BOTTARO"
  elif row['CODDEPARTAMENTO'] in isaac_elemnts: return "ISAAC ZEBEDE"
  elif row['CODDEPARTAMENTO'] in bryan_elements: return "BRYAN NEIMANN"
  elif row['CODDEPARTAMENTO'] in rob_elements: return "ROBERTO BENAIM"
  elif row['ALMACEN'] in centro_onda: return "SUPERMERCADO"
  return  "EL FUERTE"
result['COMPRADOR'] = result.apply(definidorComprador, axis=1)

#----------------------------------------------------------------------------------
# 
result_agg=result.groupby(['ALMACEN','CODDEPARTAMENTO','DEPARTAMENTO','COMPRADOR']).agg({
       'VENTA ACT': 'sum',
       'VENTA HIST':'sum', 
       'COSTO':'sum',
       'BENEFICIO':'sum',
       'MARGEN':'sum',  
}).reset_index()

awx=result.groupby(['CODDEPARTAMENTO','DEPARTAMENTO','COMPRADOR','key']).agg({
       'VENTA ACT': 'sum',
       'VENTA HIST':'sum', 
       'COSTO':'sum',
       'BENEFICIO':'sum',
       'MARGEN':'sum',  
}).reset_index()
awx.to_excel('C:\DevProjects\DATASETS\PPTO_COMPRADOR_PRUEBA.xlsx', index=True)

#----------------------------------------------------------------------------------
# Ventas de Ayer 
df_ventas_ayer=result[(result['FECHA']==formatted_yesterday)]
df_ayer_comparativo=df_ventas_ayer.groupby(['ALMACEN','CODDEPARTAMENTO','DEPARTAMENTO','COMPRADOR']).agg({
       'VENTA ACT': 'sum'
}).reset_index()

df_ayer_comparativo_agg = df_ayer_comparativo.rename(columns={
    'VENTA ACT': 'VENTA AYER'
})

#----------------------------------------------------------------------------------
# VENTAS AYER VS ACTUALES VS HISTORICAS
venta_ayer_hoy_histo=pd.merge(result_agg, df_ayer_comparativo_agg,how="left", left_on=['ALMACEN','CODDEPARTAMENTO','COMPRADOR'], right_on=['ALMACEN','CODDEPARTAMENTO','COMPRADOR'])
mod_venta_ayer_hoy_histo=pd.merge(venta_ayer_hoy_histo,plantas,how="left", left_on=['ALMACEN'], right_on=['Planta_Antigua'])
mod_venta_ayer_hoy_histo=mod_venta_ayer_hoy_histo[['Plant','Plant_ICG','Planta_Antigua','ALMACEN','CODDEPARTAMENTO','DEPARTAMENTO_x','COMPRADOR','VENTA AYER','VENTA ACT','VENTA HIST','COSTO','BENEFICIO','MARGEN']]
mod_venta_ayer_hoy_histo.to_excel('C:\DevProjects\DATASETS\LA_ONDA_venta_ayer_hoy_histo.xlsx', index=True)
mod_venta_ayer_hoy_histo=mod_venta_ayer_hoy_histo.groupby(['COMPRADOR']).agg({
       'VENTA AYER': 'sum',
       'VENTA ACT': 'sum',
       'VENTA HIST':'sum'
}).reset_index()

#----------------------------------------------------------------------------------
# UNION VENTAS VS PPTO 

venta_ayer_hoy_histo_ppto=pd.merge(mod_venta_ayer_hoy_histo, df_ppto_ag,how="left", left_on=['COMPRADOR'], right_on=['COMPRADOR'])
union_resultado= venta_ayer_hoy_histo_ppto.loc[:,~venta_ayer_hoy_histo_ppto.columns.duplicated()].copy()

#----------------------------------------------------------------------------------
#CALCULOS
union_resultado['CRECIMIENTO $']=union_resultado['VENTA ACT'].round(2)-union_resultado['VENTA HIST'].round(2)
union_resultado['CRECIMIENTO %']=((union_resultado['VENTA ACT'].round(2)/union_resultado['VENTA HIST'].round(2))-1)*100
union_resultado['DIFERENCIA $']=union_resultado['VENTA ACT'].round(0)  -union_resultado['PPTO_VENTAS'].round(0)  
union_resultado['CUMPLIMIENTO %']=(union_resultado['VENTA ACT'].round(1)  /union_resultado['PPTO_VENTAS'].round(1)  )*100
union_resultado['CUMPLIMIENTO %']=union_resultado['CUMPLIMIENTO %'].round(1)

union_resultado=union_resultado[['COMPRADOR','VENTA AYER','VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %','PPTO_VENTAS','DIFERENCIA $','CUMPLIMIENTO %']]
 
union_resultado['PPTO_VENTAS']=union_resultado['PPTO_VENTAS'].round(2)
union_resultado['VENTA AYER']=union_resultado['VENTA AYER'].round(2)
union_resultado['VENTA ACT']=union_resultado['VENTA ACT'].round(2)
union_resultado['VENTA HIST']=union_resultado['VENTA HIST'].round(2)
union_resultado['CRECIMIENTO $']=union_resultado['CRECIMIENTO $'].round(2)
union_resultado['CRECIMIENTO %']=union_resultado['CRECIMIENTO %'].round(2)
union_resultado['DIFERENCIA $']=union_resultado['DIFERENCIA $'].round(2)
union_resultado['CUMPLIMIENTO %']=union_resultado['CUMPLIMIENTO %'].round(2)

#----------------------------------------------------------------------------------
# Excluye de los compradores las categorias del FUERTE
union_resultado = union_resultado[union_resultado['COMPRADOR'] != 'EL FUERTE']

#----------------------------------------------------------------------------------
union_resultado = union_resultado.sort_values(by='VENTA AYER', ascending=False)
#----------------------------------------------------------------------------------
#
# Compute total for numeric columns
total_row = union_resultado.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CUMPLIMIENTO %'] = ((total_row['VENTA ACT']/total_row['PPTO_VENTAS']))*100
total_row['CUMPLIMIENTO %'] = total_row['CUMPLIMIENTO %'].round(1)
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
total_row['PPTO_VENTAS']=total_row['PPTO_VENTAS'].round(0)
total_row['VENTA AYER']=total_row['VENTA AYER'].round(0)
total_row['VENTA ACT']=total_row['VENTA ACT'].round(0)
total_row['VENTA HIST']=total_row['VENTA HIST'].round(0)
total_row['CRECIMIENTO $']=total_row['CRECIMIENTO $'].round(0)
total_row['COMPRADOR'] = 'TOTAL'

df_total = pd.concat([union_resultado, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %','PPTO_VENTAS','DIFERENCIA $','CUMPLIMIENTO %']
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))

df_total.to_excel('C:\DevProjects\DATASETS\LA_ONDA_COMPRADOR_VENTAS.xlsx', index=False)
print(f'DATAFRAME FINAL RESUMIDO X COMPRADOR: \n{union_resultado}')

end = time.perf_counter()
print(f"⏱ Ejecutado en {end - start:.4f} segundos o en {((end - start)/60):.4f} minutos")
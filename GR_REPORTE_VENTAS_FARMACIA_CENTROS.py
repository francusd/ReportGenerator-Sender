import pandas as pd

depart=['FARMACIA']
centro=['P1','P2','P3','P4','P5']

yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)

formatted_yesterday = yesterday.strftime('%Y-%m-%d')

day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))

#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')
ventas_act['VENTA_HIST']=0
ventas_act['BENEFICIO_HIST']=0
ventas_act['COSTO HIST']=0
ventas_act_filt=ventas_act[
    ventas_act['CODALMACEN'].isin(centro) & 
    ventas_act['DEPARTAMENTO'].isin(depart)
    ]
ventas_act_filt = ventas_act_filt.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT',
    'BENEFICIO':'BENEFICIO_ACT',
    'COSTO':'COSTO ACT'
})
act=ventas_act_filt[['ALMACEN','VENTA ACT','COSTO ACT', 'BENEFICIO_ACT', 'MARGEN','VENTA_HIST', 'BENEFICIO_HIST', 'COSTO HIST']]


#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')


ventas_histo['VENTA ACT']=0
ventas_histo['BENEFICIO_ACT']=0
ventas_histo['COSTO ACT']=0
ventas_histo_filt=ventas_histo[
    ventas_histo['CODALMACEN'].isin(centro) & 
    ventas_histo['DEPARTAMENTO'].isin(depart)
                            ] 
ventas_histo_filt = ventas_histo_filt.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA_HIST',
    'BENEFICIO':'BENEFICIO_HIST',
    'COSTO':'COSTO HIST'
})

histo=ventas_histo_filt[['ALMACEN','VENTA_HIST', 'COSTO HIST', 'BENEFICIO_HIST', 'MARGEN', 
       'VENTA ACT', 'BENEFICIO_ACT', 'COSTO ACT']]

#----------------------------------------------------------------------------------
# Une Ventas Actuales e Historia

result = pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)
result['Fecha_Ayer']=formatted_yesterday

#----------------------------------------------------------------------------------
# Ventas de Ayer
df_ventas_ayer=result[(result['FECHA']==formatted_yesterday)]
df_ayer_comparativo=df_ventas_ayer.groupby(["ALMACEN"]).agg({
       'VENTA ACT': 'sum'
}).reset_index()

df_ayer_comparativo = df_ayer_comparativo.rename(columns={
    'VENTA ACT': 'VENTA AYER'
})

#----------------------------------------------------------------------------------
# Genera parte comparativa de Ventas Actual vs Historia

df_group_comparativo=result.groupby(["ALMACEN"]).agg({
       'VENTA_HIST':'sum',
       'VENTA ACT': 'sum',
       'COSTO ACT':'sum',
       'COSTO HIST':'sum'
}).reset_index()


df_group_comparativo['CRECIMIENTO']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['VENTA_HIST'].round(2)
df_group_comparativo['CRECIMIENTO %']=((df_group_comparativo['VENTA ACT'].round(2)/df_group_comparativo['VENTA_HIST'].round(2))-1)*100
df_group_comparativo['BENEFICIO_ACTUAL']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['COSTO ACT'].round(2)
df_group_comparativo['BENEFICIO_HISTO']=df_group_comparativo['VENTA_HIST'].round(2)-df_group_comparativo['COSTO HIST'].round(2)
df_group_comparativo['MARGEN_ACT_%']=((df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['COSTO ACT'].round(2))/df_group_comparativo['VENTA ACT'].round(2))*100
df_group_comparativo['MARGEN_HIST_%']=((df_group_comparativo['VENTA_HIST'].round(2)-df_group_comparativo['COSTO HIST'].round(2))/df_group_comparativo['VENTA_HIST'].round(2))*100
df_group_comparativo['BENEFICIO_ACTUAL']=df_group_comparativo['BENEFICIO_ACTUAL'].round(0)
df_group_comparativo['BENEFICIO_HISTO']=df_group_comparativo['BENEFICIO_HISTO'].round(0)
df_group_comparativo['CRECIMIENTO %']=df_group_comparativo['CRECIMIENTO %'].round(1)
df_group_comparativo['CRECIMIENTO']=df_group_comparativo['CRECIMIENTO'].round(1)
df_group_comparativo['MARGEN_ACT_%']=df_group_comparativo['MARGEN_ACT_%'].round(0)

#----------------------------------------------------------------------------------
#unifica Ventas comparativa (Actual e Historia) con las Ventas del Ayer 

result2= pd.concat([df_group_comparativo, df_ayer_comparativo], axis=1, join="inner")
result2['VENTA AYER']=result2['VENTA AYER'].round(0)
result2['VENTA ACT']=result2['VENTA ACT'].round(0)
result2['VENTA_HIST']=result2['VENTA_HIST'].round(0)
result2['MARGEN_HIST_%']=result2['MARGEN_HIST_%'].round(0)
result2= result2[['ALMACEN','VENTA AYER' ,'VENTA ACT','COSTO ACT','VENTA_HIST','COSTO HIST','CRECIMIENTO','CRECIMIENTO %','BENEFICIO_ACTUAL',  'BENEFICIO_HISTO' , 'MARGEN_ACT_%' ,'MARGEN_HIST_%']]
result2 = result2.loc[:,~result2.columns.duplicated()].copy()

plantas=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PLANTAS.csv')
result2=pd.merge(result2, plantas,how="inner", left_on='ALMACEN', right_on='Plant_ICG')
result2= result2[['Plant','VENTA AYER' ,'VENTA ACT','COSTO ACT','VENTA_HIST','COSTO HIST','CRECIMIENTO','CRECIMIENTO %','BENEFICIO_ACTUAL',  'BENEFICIO_HISTO' , 'MARGEN_ACT_%' ,'MARGEN_HIST_%']]

#----------------------------------------------------------------------------------
# Compute total for numeric columns
total_row = result2.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA_HIST'])-1)*100
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
total_row['VENTA ACT']=total_row['VENTA ACT'].round(0)
total_row['VENTA_HIST']=total_row['VENTA_HIST'].round(0)
total_row['MARGEN_ACT_%']=((total_row['VENTA ACT'].round(2)-total_row['COSTO ACT'].round(2))/total_row['VENTA ACT'].round(2))*100
total_row['MARGEN_ACT_%']=total_row['MARGEN_ACT_%'].round(0)
total_row['MARGEN_HIST_%']=total_row['MARGEN_HIST_%'].round(0)
# Add label for the first column
total_row['Plant'] = 'TOTAL'

df_total = pd.concat([result2, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT',
                     'VENTA_HIST','CRECIMIENTO','CRECIMIENTO %','BENEFICIO_ACTUAL','MARGEN_ACT_%','BENEFICIO_HISTO','MARGEN_HIST_%']
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))

columns_to_filter = ['Plant','VENTA AYER' ,'VENTA ACT',
                     'VENTA_HIST','CRECIMIENTO','CRECIMIENTO %','BENEFICIO_ACTUAL','MARGEN_ACT_%','BENEFICIO_HISTO','MARGEN_HIST_%']

df_total=df_total[columns_to_filter]
#----------------------------------------------------------------------------------
#
df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FARMACIA_CENTRO.xlsx',index=False)

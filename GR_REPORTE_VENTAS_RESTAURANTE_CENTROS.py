import pandas as pd

centro=['R1','R2','R3','R4','R5','R6']
depart=['RESTAURANTE']
yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)

formatted_yesterday = yesterday.strftime('%Y-%m-%d')

day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))

#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')
ventas_act['VENTA_HIST']=0
ventas_act_filt=ventas_act[ventas_act['CODALMACEN'].isin(centro)]
                           #& ventas_act['DEPARTAMENTO'].isin(depart)
                           
ventas_act_filt = ventas_act_filt.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT',
    'COSTO':'COSTO ACT'
})
#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo['COSTO ACT']=0
ventas_histo_filt=ventas_histo[ventas_histo['CODALMACEN'].isin(centro)]
                               #& ventas_histo['DEPARTAMENTO'].isin(depart)
                               
ventas_histo_filt = ventas_histo_filt.rename(columns={
     'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA HIST'
})
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
       'VENTA HIST':'sum',
       'VENTA ACT': 'sum',
       'COSTO ACT':'sum'
}).reset_index()

df_group_comparativo['CRECIMIENTO']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['VENTA HIST'].round(2)
df_group_comparativo['CRECIMIENTO %']=((df_group_comparativo['VENTA ACT'].round(2)/df_group_comparativo['VENTA HIST'].round(2))-1)*100
df_group_comparativo['BENEFICIO']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['COSTO ACT'].round(2)
df_group_comparativo['MARGEN %']=((df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['COSTO ACT'].round(2))/df_group_comparativo['VENTA ACT'].round(2))*100
df_group_comparativo['BENEFICIO']=df_group_comparativo['BENEFICIO'].round(0)
df_group_comparativo['MARGEN %']=df_group_comparativo['MARGEN %'].round(0)
#----------------------------------------------------------------------------------
#unifica Ventas comparativa (Actual e Historia) con las Ventas del Ayer 

result2= pd.concat([df_group_comparativo, df_ayer_comparativo], axis=1, join="inner")
#
result2= result2[['ALMACEN','VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO','CRECIMIENTO %']]

result2 = result2.loc[:,~result2.columns.duplicated()].copy()
result2['VENTA AYER'] =result2['VENTA AYER'].round(0)
result2['VENTA ACT']=result2['VENTA ACT'].round(0)
result2['VENTA HIST']=result2['VENTA HIST'].round(0)
result2['CRECIMIENTO']=result2['CRECIMIENTO'].round(0)
result2['CRECIMIENTO %']=result2['CRECIMIENTO %'].round(0)


plantas=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PLANTAS.csv')
#print(f'PLANTAS: \n\n{plantas}\n\n')
result2=pd.merge(result2, plantas,how="inner", left_on='ALMACEN', right_on='Plant_ICG')

result2= result2[['Plant','VENTA AYER','VENTA ACT','VENTA HIST','CRECIMIENTO','CRECIMIENTO %']]

#----------------------------------------------------------------------------------
#
# Compute total for numeric columns

total_row = result2.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
# Add label for the first column
total_row['Plant'] = 'TOTAL'

df_total = pd.concat([result2, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO','CRECIMIENTO %',]
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))


df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_RESTAURANTE_CENTROS.xlsx',index=False)
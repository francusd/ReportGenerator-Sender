import pandas as pd

#----------------------------------------------------------------------------------
centro=['F1','F2','F3','F4','F5','F6','O1','O2','O3','O5','O7','O9']
yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
formatted_yesterday = yesterday.strftime('%Y-%m-%d')
day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))

#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')
ventas_act['VENTA_HIST']=0
ventas_act_filt=ventas_act[ventas_act['CODALMACEN'].isin(centro)]
ventas_act_filt = ventas_act_filt.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT'
})

#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo_filt=ventas_histo[ventas_histo['CODALMACEN'].isin(centro)]
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
df_ventas_ayer=ventas_act_filt[(result['FECHA']==formatted_yesterday)]
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
       'VENTA ACT': 'sum'
}).reset_index()
df_group_comparativo['DIFERENCIA EN VENTAS $']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['VENTA HIST'].round(2)
df_group_comparativo['CRECIMIENTO EN VENTAS %']=((df_group_comparativo['VENTA ACT'].round(2)/df_group_comparativo['VENTA HIST'].round(2))-1)*100

#----------------------------------------------------------------------------------
#unifica Ventas comparativa (Actual e Historia) con las Ventas del Ayer 

result2= pd.concat([df_group_comparativo, df_ayer_comparativo], axis=1, join="inner")
#
result2= result2[['ALMACEN','VENTA AYER' ,'VENTA ACT','VENTA HIST','DIFERENCIA EN VENTAS $','CRECIMIENTO EN VENTAS %']]

result2 = result2.loc[:,~result2.columns.duplicated()].copy()

result2['VENTA ACT'] = result2['VENTA ACT'].astype(int)
result2['VENTA AYER'] = result2['VENTA AYER'].astype(int)
result2['VENTA HIST'] = result2['VENTA HIST'].astype(int)

#----------------------------------------------------------------------------------
# Cambio de códigos de los Centros 
plantas=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PLANTAS.csv')
resultado_df=pd.merge(plantas,result2, how="inner", left_on='Plant_ICG', right_on='ALMACEN')

resultado_df= resultado_df[['Plant','VENTA AYER' ,'VENTA ACT','VENTA HIST']]

#----------------------------------------------------------------------------------
#resultado['CRECIMIENTO $']=resultado['VENTA ACT'].round(2)-resultado['VENTA HIST'].round(2)
resultado_df['VENTA AYER']=resultado_df['VENTA AYER'].round(2)
resultado_df['VENTA ACT']=resultado_df['VENTA ACT'].round(2)
resultado_df['VENTA HIST']=resultado_df['VENTA HIST'].round(2)


resultado_df['DIFERENCIA EN VENTAS $']=((resultado_df['VENTA ACT'].round(2)-resultado_df['VENTA HIST'].round(2)))
resultado_df['DIFERENCIA EN VENTAS $']=resultado_df['DIFERENCIA EN VENTAS $'].round(2)
resultado_df['CRECIMIENTO EN VENTAS %']=((resultado_df['VENTA ACT'].round(2)/resultado_df['VENTA HIST'].round(2))-1)*100
resultado_df['CRECIMIENTO EN VENTAS %']=resultado_df['CRECIMIENTO EN VENTAS %'].round(2)

total_row = resultado_df.select_dtypes(include='number').sum()

total_row['CRECIMIENTO EN VENTAS %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CRECIMIENTO EN VENTAS %'] = total_row['CRECIMIENTO EN VENTAS %'].round(1)

total_row['Plant'] = 'TOTAL'

df_total = pd.concat([resultado_df, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','DIFERENCIA EN VENTAS $']
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))


#df_total= df_total[['Plant','VENTA AYER' ,'VENTA ACT','VENTA HIST']]
#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------
print(f'\nDATAFRAME RESULTADO \n{df_total}')

df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FUERTE_ONDA.xlsx', index=True)

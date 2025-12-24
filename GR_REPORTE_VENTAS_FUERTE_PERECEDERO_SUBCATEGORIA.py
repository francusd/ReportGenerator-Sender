import pandas as pd
import re 


yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
formatted_yesterday = yesterday.strftime('%Y-%m-%d')
day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))
perecedero=['PERECEDERO']
centro=['F1','F2','F3','F4','F5','F6']


#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')
ventas_act['VENTA_HIST']=0
ventas_act_filt = ventas_act.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT'
})

ventas_act_filt=ventas_act_filt[ventas_act_filt['DEPARTAMENTO'].isin(perecedero) & 
                                ventas_act_filt['ALMACEN'].isin(centro)
                                ]

#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo_filt = ventas_histo.rename(columns={
     'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA HIST'
})

ventas_histo_filt=ventas_histo_filt[ventas_histo_filt['DEPARTAMENTO'].isin(perecedero) & 
                                ventas_histo_filt['ALMACEN'].isin(centro) ]



#----------------------------------------------------------------------------------
#RELACIONA LAS VENTAS(ACTUALES VS HISTORICA) Y LAS CATGORIAS
dfventas=pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)
#dfventas=dfventas[dfventas['ALMACEN'].isin(centro) ]

ventas_agg=dfventas.groupby(["SECCION"]).agg({
       'VENTA HIST':'sum',
       'VENTA ACT' :'sum' 
}).reset_index()


ventas_agg['VENTA HIST']=ventas_agg['VENTA HIST'].round(2)
ventas_agg['VENTA ACT']=ventas_agg['VENTA ACT'].round(2)
#print(ventas_agg.head())

#----------------------------------------------------------------------------------
# Ventas de Ayer 
df_ventas_ayer=dfventas[(dfventas['FECHA']==formatted_yesterday)]
df_ayer_comparativo=df_ventas_ayer.groupby(["SECCION"]).agg({
       'VENTA ACT': 'sum'
}).reset_index()

df_ayer_comparativo = df_ayer_comparativo.rename(columns={
    'VENTA ACT': 'VENTA AYER'
})
df_ayer_comparativo['VENTA AYER'] = df_ayer_comparativo['VENTA AYER'].round(2)

resultado=pd.merge(ventas_agg, df_ayer_comparativo,how="inner", left_on='SECCION', right_on='SECCION')
resultado= resultado.loc[:,~resultado.columns.duplicated()].copy()
resultado['CRECIMIENTO $']=resultado['VENTA ACT'].round(2)-resultado['VENTA HIST'].round(2)
resultado['CRECIMIENTO %']=((resultado['VENTA ACT'].round(2)/resultado['VENTA HIST'].round(2))-1)*100

resultado['CRECIMIENTO %']=resultado['CRECIMIENTO %'].round(2)
resultado['CRECIMIENTO $']=resultado['CRECIMIENTO $'].round(2)
resultado['VENTA AYER']=resultado['VENTA AYER'].round(0)
resultado['VENTA ACT']=resultado['VENTA ACT'].round(0)
resultado['VENTA HIST']=resultado['VENTA HIST'].round(0)
resultado= resultado[['SECCION','VENTA AYER','VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %']]

#----------------------------------------------------------------------------------
#
# Compute total for numeric columns
total_row = resultado.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
total_row['CRECIMIENTO $'] = total_row['CRECIMIENTO $'].round(1)
# Add label for the first column
total_row['SECCION'] = 'TOTAL'

df_total = pd.concat([resultado, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %']
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))
df_total.to_excel('C:\DevProjects\DATASETS\\REPORTE_GERENCIAL_VENTAS_FUERTE_PERECEDERO_SUBCATEGORIA.xlsx', index=True)

import pandas as pd
yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)

formatted_yesterday = yesterday.strftime('%Y-%m-%d')

day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))
centro=['F1','F2','F3','F4','F5','F6','O1','O2','O3','O5','O7','O9']

#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')

ventas_act['VENTA_HIST']=0
ventas_act_filt=ventas_act[ventas_act['CODALMACEN'].isin(centro) ]
ventas_act_filt = ventas_act_filt.rename(columns={
    #'CODALMACEN':'ALMACEN',
    'DEPARTAMENTO':'CATEGORIA',
    'VENTA': 'VENTA ACT'
})

#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo_filt=ventas_histo[ventas_histo['CODALMACEN'].isin(centro) ]
ventas_histo_filt = ventas_histo_filt.rename(columns={
    #'CODALMACEN':'ALMACEN',
    'DEPARTAMENTO':'CATEGORIA',
    'VENTA': 'VENTA HIST'
})

#----------------------------------------------------------------------------------
# Ventas de Ayer 
df_ventas_ayer=ventas_act_filt[(ventas_act['FECHA']==formatted_yesterday)]
df_ayer=df_ventas_ayer.groupby(["CATEGORIA"]).agg({
       'VENTA ACT': 'sum'
}).reset_index()

df_ayer = df_ayer.rename(columns={
    'VENTA ACT': 'VENTA AYER'
})
#----------------------------------------------------------------------------------
# Une Ventas Actuales e Historia

result = pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)
##result['Fecha_Ayer']=formatted_yesterday

df_actual_histo=result.groupby(["CATEGORIA"]).agg({
       'VENTA HIST':'sum',
       'VENTA ACT': 'sum'
}).reset_index()

resultado=pd.merge(df_actual_histo, df_ayer,how="inner", left_on='CATEGORIA', right_on='CATEGORIA')


plantas=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PLANTAS.csv')
finale_df=resultado
#pd.merge(resultado, plantas,how="left", left_on='ALMACEN', right_on='Planta_Antigua')

finale_df['CRECIMIENTO $']=finale_df['VENTA ACT'].round(2)-finale_df['VENTA HIST'].round(2)
finale_df['CRECIMIENTO %']=((finale_df['VENTA ACT'].round(2)/finale_df['VENTA HIST'].round(2))-1)*100

finale_df['VENTA AYER']=finale_df['VENTA AYER'].round(0)
finale_df['VENTA ACT']=finale_df['VENTA ACT'].round(0)   
finale_df['VENTA HIST']=finale_df['VENTA HIST'].round(0)
finale_df['CRECIMIENTO $']=finale_df['CRECIMIENTO $'].round(0)
finale_df['CRECIMIENTO %']=finale_df['CRECIMIENTO %'].round(0)   
#----------------------------------------------------------------------------------
#finale_df=finale_df[finale_df['ALMACEN'].isin(centro)]
finale_df=finale_df[['CATEGORIA','VENTA AYER','VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %']]

#----------------------------------------------------------------------------------
# Compute total for numeric columns
total_row = finale_df.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
# Add label for the first column
total_row['CATEGORIA'] = 'TOTAL'

df_total = pd.concat([finale_df, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %']
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))

df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FUERTE_ONDA_CATEGORIA.xlsx',index=False)

print(f'\nDATAFRAME RESULTADO \n{finale_df}')

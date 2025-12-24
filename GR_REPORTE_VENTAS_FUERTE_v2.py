import pandas as pd


fuerte=["EL FUERTE"]
yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
formatted_yesterday = yesterday.strftime('%Y-%m-%d')
day_mes_ayer = int(yesterday.strftime('%d'))
mes_actual = int(yesterday.strftime('%m'))
year_actual = int(yesterday.strftime('%Y'))

#----------------------------------------------------------------------------------
# Ventas Actuales
ventas_act=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_ACTUAL.csv')

ventas_act['VENTA_HIST']=0
ventas_act_filt=ventas_act
"""[(
                           (ventas_act['CODDEPARTAMENTO'] == '295') &
                           (ventas_act['CODSECCION'] == '1') &
                           (ventas_act['CODFAMILIA'] == '2'))   |
                           ventas_act['CODALMACEN'].isin(centro) & 
                           ventas_act['DEPARTAMENTO'].isin(depart)]"""
ventas_act_filt = ventas_act_filt.rename(columns={
    'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA ACT'
})

#----------------------------------------------------------------------------------
# Ventas Historica
ventas_histo=pd.read_csv('C:\DevProjects\DATASETS\VENTAS_ICG_HISTO.csv')
ventas_histo['VENTA ACT']=0
ventas_histo_filt=ventas_histo
"""[((ventas_histo['CODDEPARTAMENTO'] == '295') &
                          (ventas_histo['CODSECCION'] == '1') &
                          (ventas_histo['CODFAMILIA'] == '2'))|
                           ventas_histo['CODALMACEN'].isin(centro) & 
                           ventas_histo['DEPARTAMENTO'].isin(depart)] """
ventas_histo_filt = ventas_histo_filt.rename(columns={
     'CODALMACEN':'ALMACEN',
    'VENTA': 'VENTA HIST'
})

#----------------------------------------------------------------------------------
# Une Ventas Actuales e Historia
result = pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)


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
# Metodo para filtrar por las categorias especificas del hipermercado
def filtro_fuerte(result):
    depart=['COMESTIBLE','AUTOMOTRIZ','COLCHONES','ELECTRO','FERRETERIA HARDWARE',
        'NO COMESTIBLE','PERECEDERO','PROMOCION Y ACTIVIDADES']
    #,'NAVIDAD'
    centro=['F1','F2','F3','F4','F5','F6']
   
    if((result['DEPARTAMENTO'] == 'NAVIDAD') and (result['SECCION'] == 'ARBOLITOS') and (result['FAMILIA'] == 'NATURALES')):
        return "EL FUERTE"
    elif((result['CODDEPARTAMENTO'] == 190) and result['ALMACEN'] in centro ):
         return "EL FUERTE"
    elif(result['DEPARTAMENTO'] in depart and  result['ALMACEN'] in centro ): 
        return "EL FUERTE"
    else:
        return "S/N"
    
result['GrupoNegocio'] = result.apply(filtro_fuerte, axis=1)

result=result[result['GrupoNegocio'].isin(fuerte) ]


result_agg=result.groupby(['ALMACEN']).agg({
       'VENTA ACT': 'sum',
       'VENTA HIST':'sum'
}).reset_index()

#----------------------------------------------------------------------------------
#unifica Ventas comparativa (Actual e Historia) con las Ventas del Ayer 
resultado=pd.merge(result_agg, df_ayer_comparativo,how="inner", left_on='ALMACEN', right_on='ALMACEN')
df_group_comparativo = resultado.loc[:,~resultado.columns.duplicated()].copy()

df_group_comparativo['DIFERENCIA EN VENTAS $']=df_group_comparativo['VENTA ACT'].round(2)-df_group_comparativo['VENTA HIST'].round(2)
df_group_comparativo['CRECIMIENTO EN VENTAS %']=((df_group_comparativo['VENTA ACT'].round(2)/df_group_comparativo['VENTA HIST'].round(2))-1)*100


#----------------------------------------------------------------------------------
#PRESUPUESTO 
presupuesto=pd.read_excel('C:\DevProjects\DATASETS\presupuesto mes dic 2023 El Fuerte.xlsx')
ppto_filter=presupuesto[(presupuesto['MES']==mes_actual)& 
                        (presupuesto['AÑO']==year_actual)  & 
                        (presupuesto['DÍA']<=day_mes_ayer)  ] 

df_melted = pd.melt(ppto_filter, 
                    id_vars=['DÍA'], 
                    value_vars=['AÑO', 'F1', 'F2', 'F3', 'F4', 'F5','F6'], 
                    var_name='Name', 
                    value_name='Value')

df_melted_ag=df_melted.groupby(["Name"]).agg({
       'Value':'sum'
}).reset_index()

df_melted_ag = df_melted_ag.rename(columns={
    'Name': 'Almacen',
    'Value':'PPTO VENTAS'
})


finale = pd.merge(df_group_comparativo, df_melted_ag, left_on='ALMACEN', right_on='Almacen', how='inner')

finale['DIFERENCIA EN VENTAS $']=finale['DIFERENCIA EN VENTAS $'].round(0)
finale['CRECIMIENTO EN VENTAS %']=finale['CRECIMIENTO EN VENTAS %'].round(1)
finale['PPTO VENTAS']=finale['PPTO VENTAS'].round(0)  
finale['DIFERENCIA vs PPTO $']=finale['VENTA ACT'].round(0)  -finale['PPTO VENTAS'].round(0)  
finale['CUMPLIMIENTO %']=(finale['VENTA ACT'].round(1)  /finale['PPTO VENTAS'].round(1)  )*100
finale['CUMPLIMIENTO %']=finale['CUMPLIMIENTO %'].round(1)#
finale['VENTA AYER']=finale['VENTA AYER'].round(0)
finale['VENTA ACT']=finale['VENTA ACT'].round(0)   
finale['VENTA HIST']=finale['VENTA HIST'].round(0)
finale= finale[['Plant','VENTA AYER' ,'VENTA ACT','VENTA HIST','DIFERENCIA EN VENTAS $','CRECIMIENTO EN VENTAS %','PPTO VENTAS','DIFERENCIA vs PPTO $','CUMPLIMIENTO %']]

#----------------------------------------------------------------------------------
print(f'{finale}')
finale.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FUERTE_v2.xlsx',index=False)

"""
#----------------------------------------------------------------------------------
#unifica Ventas comparativa (Actual e Historia) con las Ventas del Ayer 

result2= pd.concat([df_group_comparativo, df_ayer_comparativo], axis=1, join="inner")
#
result2= result2[['ALMACEN','VENTA AYER' ,'VENTA ACT','VENTA HIST','DIFERENCIA EN VENTAS $','CRECIMIENTO EN VENTAS %']]

result2 = result2.loc[:,~result2.columns.duplicated()].copy()



#----------------------------------------------------------------------------------

plantas=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PLANTAS.csv')
finale=pd.merge(finale, plantas,how="inner", left_on='ALMACEN', right_on='Plant_ICG')


finale.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FUERTE_raw.xlsx',index=False)

#----------------------------------------------------------------------------------
#
# Compute total for numeric columns
total_row = finale.select_dtypes(include='number').sum()

# Optional: average the percentage column (instead of summing)
total_row['CRECIMIENTO EN VENTAS %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CUMPLIMIENTO %'] = ((total_row['VENTA ACT']/total_row['PPTO VENTAS']))*100
total_row['CUMPLIMIENTO %'] = total_row['CUMPLIMIENTO %'].round(1)
total_row['CRECIMIENTO EN VENTAS %'] = total_row['CRECIMIENTO EN VENTAS %'].round(1)
# Add label for the first column
total_row['Plant'] = 'TOTAL'

df_total = pd.concat([finale, pd.DataFrame([total_row])], ignore_index=True)

columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','DIFERENCIA EN VENTAS $','PPTO VENTAS','DIFERENCIA vs PPTO $',]
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))

df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_FUERTE.xlsx',index=False)
#df_total.to_excel(f'C:/gralteryx2/Users/adminbi/Desktop/ALTERYXREPORTE_GERENCIAL_VENTAS_FUERTE.xlsx',index=False)
"""
import pandas as pd
import numpy as np
import re 


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

#----------------------------------------------------------------------------------
#CATEGORIAS DE MATERIALES
categoriasmat=pd.read_csv('C:\DevProjects\DATASETS\GR_CATEGORIZACION_MAT.csv')
categoriasmat['CODARTICULO'] = categoriasmat['CODARTICULO'].astype(int)

#----------------------------------------------------------------------------------
#PRESUPUESTO 

presupuesto=pd.read_csv(r'C:\DevProjects\DATASETS\GR_PRESUPUESTO.csv')
ppto_filter=presupuesto[(presupuesto['MES_FECHA']==mes_actual)& 
                        (presupuesto['AÑO_FECHA']==year_actual)  & 
                        (presupuesto['DIA_FECHA']<=day_mes_ayer)  ] 

df_ppto_ag=ppto_filter.groupby(["CATEGORIA"]).agg({
       'Presupuesto Venta $':'sum'
}).reset_index()
df_ppto_ag = df_ppto_ag.rename(columns={
    'Presupuesto Venta $':'PPTO VENTAS'   
}).reset_index()

df_ppto_ag["CATEGORIA"] = df_ppto_ag["CATEGORIA"].str.strip()

#df_ppto_ag.to_excel('C:\DevProjects\DATASETS\VENTAS_PPTO_ONDA_DEPTO.xlsx', index=True)

#----------------------------------------------------------------------------------
#RELACIONA LAS VENTAS(ACTUALES VS HISTORICA) Y LAS CATGORIAS
dfventas=pd.concat([ventas_act_filt, ventas_histo_filt], ignore_index=True)

dfventas_cat= pd.merge(dfventas, categoriasmat, how='inner', left_on='CODARTICULO', right_on='CODARTICULO')
result = dfventas_cat.loc[:,~dfventas_cat.columns.duplicated()].copy()
#-------------------------------------------------------------------------------------
#

def asignar_grupo_negocio(result):
    cod = result['ALMACEN']
    grupo = result['GRUPO_DEPARTAMENTO']

    if ('P' in cod or 'F' in cod) and grupo == "FARMACIA":
        return "FARMACIA"
    elif ('R' in cod and grupo in ["RESTAURANTE", "EL FUERTE"]) or ('F' in cod and grupo == "RESTAURANTE"):
        return "RESTAURANTE"
    elif any(x in cod for x in ['O', 'S', 'V', 'EO']):
        return "LA ONDA"
    elif 'F' in cod and grupo == "LA ONDA":
        return "LA ONDA"
    elif 'F' in cod and grupo == "EL FUERTE":
        return "EL FUERTE"
    else:
        return "S/N"

def grupo_negocio_filtro(result):
    farmacia = {"FP01", "FP02", "FP03", "FP04", "FP05", "FP06"}
    restaurante = {"RE01", "RE02", "RE03", "RE04", "RE05", "RE06"}
    la_onda = {"ON01", "ON02", "ON03", "ON04", "ON05", "ON06"}
    fo = {"FO01", "FO02", "FO03", "FO04", "FO05", "FO06"}
    categorias_la_onda = {"210", "215", "220", "225", "230", "235", "260", "240", "265", "250", "270", "280", "290", "295", "296"}

    # Apply the same logic as your SQL/IF
    result["GRUPO_NEGOCIO"] = np.select(
        [
            result["Plant"].isin(farmacia),
            result["Plant"].isin(restaurante),
            result["Plant"].isin(la_onda),
            result["SubSegment"].isin({"295010201"}),
            (result["Plant"].isin(fo)) & (result["Category"].isin(categorias_la_onda))
        ],
        [
            "FARMACIA",
            "RESTAURANTE",
            "LA ONDA",
            "EL FUERTE",
            "LA ONDA"
        ],
        default="EL FUERTE"
    )


result['GrupoNegocio'] = result.apply(asignar_grupo_negocio, axis=1)
result=result[result['ALMACEN']!='EO2']

result=result[result['GrupoNegocio'].isin(onda) ]
result['ALMACEN'] = result['ALMACEN'].apply(
    lambda x: 'O4' if x == 'O7' else 'O6' if x == 'O9' else x
)
result_agg=result.groupby(['DEPARTAMENTO']).agg({
       'VENTA ACT': 'sum',
       'VENTA HIST':'sum'
}).reset_index()

#----------------------------------------------------------------------------------
# Ventas de Ayer 
df_ventas_ayer=result[(result['FECHA']==formatted_yesterday)]
df_ayer_comparativo=df_ventas_ayer.groupby(["DEPARTAMENTO"]).agg({
       'VENTA ACT': 'sum'
}).reset_index()

df_ayer_comparativo = df_ayer_comparativo.rename(columns={
    'VENTA ACT': 'VENTA AYER'
})

resultado=pd.merge(result_agg, df_ayer_comparativo,how="inner", left_on='DEPARTAMENTO', right_on='DEPARTAMENTO')
union_vnt_act_histo = resultado.loc[:,~resultado.columns.duplicated()].copy()

union_vnt_act_histo['CRECIMIENTO $']=union_vnt_act_histo['VENTA ACT'].round(2)-union_vnt_act_histo['VENTA HIST'].round(2)
union_vnt_act_histo['CRECIMIENTO %']=((union_vnt_act_histo['VENTA ACT'].round(2)/union_vnt_act_histo['VENTA HIST'].round(2))-1)*100


df_ppto_ag.reset_index()
finale_df=pd.merge(union_vnt_act_histo, df_ppto_ag, how='inner', left_on='DEPARTAMENTO', right_on='CATEGORIA')
finale_df = finale_df.loc[:,~finale_df.columns.duplicated()].copy()


finale_df['CRECIMIENTO $']=finale_df['CRECIMIENTO $'].round(0)
finale_df['CRECIMIENTO %']=finale_df['CRECIMIENTO %'].round(1)
finale_df['PPTO VENTAS']=finale_df['PPTO VENTAS'].round(0)  
finale_df['DIFERENCIA $']=finale_df['VENTA ACT'].round(0)  -finale_df['PPTO VENTAS'].round(0)  
finale_df['CUMPLIMIENTO %']=(finale_df['VENTA ACT'].round(1)  /finale_df['PPTO VENTAS'].round(1)  )*100
finale_df['CUMPLIMIENTO %']=finale_df['CUMPLIMIENTO %'].round(1)
finale_df['VENTA AYER']=finale_df['VENTA AYER'].round(0)
finale_df['VENTA ACT']=finale_df['VENTA ACT'].round(0)   
finale_df['VENTA HIST']=finale_df['VENTA HIST'].round(0)

finale_df= finale_df[['DEPARTAMENTO','VENTA AYER','VENTA ACT','VENTA HIST','CRECIMIENTO $','CRECIMIENTO %','PPTO VENTAS','DIFERENCIA $','CUMPLIMIENTO %']]
finale_df = finale_df.sort_values(by='VENTA AYER', ascending=False)


# Compute total for numeric columns
total_row = finale_df.select_dtypes(include='number').sum()
total_row['CRECIMIENTO %'] = ((total_row['VENTA ACT']/total_row['VENTA HIST'])-1)*100
total_row['CUMPLIMIENTO %'] = ((total_row['VENTA ACT']/total_row['PPTO VENTAS']))*100
total_row['CUMPLIMIENTO %'] = total_row['CUMPLIMIENTO %'].round(1)
total_row['CRECIMIENTO %'] = total_row['CRECIMIENTO %'].round(1)
# Add label for the first column
total_row['DEPARTAMENTO'] = 'TOTAL'

# Append total row to the DataFrame
df_total = pd.concat([finale_df, pd.DataFrame([total_row])], ignore_index=True)


columns_to_format = ['VENTA AYER' ,'VENTA ACT','VENTA HIST','CRECIMIENTO $','PPTO VENTAS','DIFERENCIA $',]
df_total[columns_to_format] = df_total[columns_to_format].map(lambda x: "{:,}".format(x))

df_total.to_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_ONDA_DEPARTAMENTO.xlsx', index=True)

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import ssl 
from pathlib import Path

df_onda_centro=pd.read_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_RESTAURANTE_CENTROS.xlsx')
df_onda_centro = df_onda_centro.loc[:, ~df_onda_centro.columns.str.contains('^Unnamed')]

df_onda_depa=pd.read_excel('C:\DevProjects\DATASETS\REPORTE_GERENCIAL_VENTAS_RESTAURANTE_SUBCATEGORIA.xlsx')
df_onda_depa = df_onda_depa.loc[:, ~df_onda_depa.columns.str.contains('^Unnamed')]

html_table = df_onda_centro.to_html(index=False, border=3)
html_table_sub=df_onda_depa.to_html(index=False, border=3)

yesterday = pd.Timestamp.today() - pd.Timedelta(days=1)
formatted_yesterday = yesterday.strftime('%Y-%m-%d')
context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')

# ===============================
# 2- Helper functions
# ===============================

def color_growth(val):
    """Color red if negative, green if positive."""
    try:
        val_float = float(str(val).replace(',', ''))
        if val_float < 0:
            return f'<td style="color:red;text-align:right;">{val}</td>'
        elif val_float > 0:
            return f'<td style="color:green;text-align:right;">{val}</td>'
    except:
        pass
    return f'<td style="text-align:right;">{val}</td>'


def color_default(val):
    """Default right-aligned cell."""
    return f'<td style="text-align:right;">{val}</td>'


def build_html_table(df, title):
    """Build formatted HTML table with alternating rows and color logic."""
    html = f"""
    <h3 style="font-family:Arial; color:#333; margin-top:20px;">📊 {title}</h3>
    <table style="border-collapse:collapse; 
          width:100%; 
          font-family:Arial,sans-serif;
          font-size:10px; 
          border:1px solid #ccc;">
    <thead>
        <tr style="background-color:#f2f2f2; text-align:center;">
    """
    for col in df.columns:
        html += f'<th style="border:1px solid #ddd; padding:6px;">{col}</th>'
    html += "</tr></thead><tbody>"

    for i, row in df.iterrows():
        bg_color = "#f9f9f9" if i % 2 == 0 else "white"
        html += f'<tr style="background-color:{bg_color};">'
        for j, val in enumerate(row):
            colname = df.columns[j]
            if colname in ["CRECIMIENTO $", "CRECIMIENTO %", "DIFERENCIA $","DIFERENCIA $","CUMPLIMIENTO %"]:
                html += color_growth(val)
            elif colname in ["Plant", "CATEGORIA"]:
                html += f'<td style="text-align:left; font-weight:bold;">{val}</td>'
            else:
                html += color_default(val)
        html += "</tr>"
    html += "</tbody></table>"
    return html

# ===============================
# 3- Build combined HTML body
# ===============================

html_intro = f"""
<h5 style="font-family:Arial; 
color:#004080;">REPORTE GERENCIAL DIARIO DE VENTAS RESTAURANTE</h5>
<p style="font-family:Arial; font-size:10px;">
Buen día a todos,<br>
Ventas Diarias RESTAURANTE <strong>CENTRO, CATEGORIA</strong> acumuladas al corte: 
<strong>{formatted_yesterday}</strong><br><br>
</p>
"""

# Create both tables
html_centro = build_html_table(df_onda_centro, "Ventas RESTAURANTE - CENTROS")
html_depa = build_html_table(df_onda_depa, "Ventas RESTAURANTE - SUBCATEGORIA")

# Combine everything
html_full = html_intro + html_centro + html_depa 

# ===============================
# 4- Send email
# ===============================

msg = MIMEMultipart('alternative')
msg['Subject'] = "📈 REPORTE GERENCIAL DIARIO DE VENTAS RESTAURANTE"
msg['From'] = "alertabi@corpporativo.com.pa"
recipients = ["email@corporativo.com"]
msg['To'] = ", ".join(recipients)
msg.attach(MIMEText(html_full, 'html'))

smtp_server = 'mail.smtp2go.com'
smtp_port = 587

context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')

with smtplib.SMTP(smtp_server, smtp_port) as server:    
    server.ehlo()
    server.starttls(context=context)
    server.login("PruebasInternasFree", "xxxx")
    server.sendmail(msg['From'], recipients, msg.as_string())

print("✅ Email sent successfully!")



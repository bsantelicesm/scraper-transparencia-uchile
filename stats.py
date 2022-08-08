#Procesador de estadísticas Transparencia Universidad de Chile
#Por Benjamín Santelices Melis, Estudiante
#Departamento de Ingeniería Eléctrica, FCFM

#Estoy bastante seguro que no es ni la forma más eficiente ni la más elegante, si alguien me quiere ayudar a mejorarlo bienvenido sea.
#Está lleno de trucos truchos y resolución manual de excepciones.

import mysql.connector #requiere instalar via pip.
import statistics
import unidecode as ud #requiere instalar via pip.

SQL = mysql.connector.connect(host="SERVER", user="benjamin", passwd="pass", db="fcfm_transparencia") #Conector SQL.
cur = SQL.cursor()

cur.execute("select TABLE_NAME, TABLE_ROWS from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'fcfm_transparencia' AND TABLE_NAME NOT LIKE \"stats%\";") #obtener los nombres de todas las tablas mensuales. Todas las tablas de estadísticas empiezan con stats.
listaTablas = cur.fetchall()


def dotaciones_beauchef_total(): #tabla de dotación mensual total
    for tabla in listaTablas:
        cur.execute(f"select contrato, count(*) from {tabla[0]} where unidad = 1206 group by contrato;")
        data = cur.fetchall()
        planta = data[0][1] #cantidad planta
        contrata = data[1][1] #cantidad contrata
        total = planta + contrata
        cur.execute(f"update stats_dotacion_fcfm set total = {total}, planta = {planta}, contrata = {contrata} where tabla = \"{tabla[0]}\"") #subir a la tabla por mes.

def dotaciones_beauchef_estamento():
    for tabla in listaTablas:
        cur.execute(f"select contrato, estamento, count(*) from {tabla[0]} where unidad = 1206 group by estamento, contrato") #1206 es beauchef, otros codigos permiten otras unidades.
        data = cur.fetchall()
        cmd = ""
        cmd += "update stats_dotacion_estamento_fcfm set " #los resultados de la cuenta no siempre llegan en el mismo orden, por lo que hay que armar el comando para responder a lo que llegue.
        for line in data:
            set = f"{line[0].lower()}_{ud.unidecode(line[1]).lower().replace('tcnico', 'tecnico').replace('acadmico', 'academico')} = {line[2]}, " #atrapar problemas con acentos, no es consistente en las tablas por lo que no todos quedan bien
            cmd += set
        cmd = cmd[:-2] + f" where tabla = \"{tabla[0]}\""
        try:
            cur.execute(cmd)
        except:
            print(tabla[0], data) #si no funciona, imprime el mes de la tabla para debugging.
    SQL.commit()

def remuneracion_promedio_beauchef_total(): #tabla de remuneraciones por estamento.
    for tabla in listaTablas:
        cur.execute(f"SELECT contrato, estamento, round(avg(B_remuneracion)) FROM {tabla[0]} WHERE unidad = 1206 group by contrato, estamento;") #1206 es beauchef. Redondea al peso más cercano.
        data = cur.fetchall()
        cmd = "" #misma construcción de comando que función anterior.
        cmd += "update stats_remuneracionmedia_estamento_fcfm set "
        for line in data:
            set = f"{line[0].lower()}_{ud.unidecode(line[1]).lower().replace('tcnico', 'tecnico').replace('acadmico', 'academico')} = {line[2]}, "
            cmd += set
        cmd = cmd[:-2] + f" where tabla = \"{tabla[0]}\""
        try:
            cur.execute(cmd)
        except:
            print(tabla[0], data)
    SQL.commit()

remuneracion_promedio_beauchef_total()
dotaciones_beauchef_estamento()

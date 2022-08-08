#Scraper Transparencia Universidad de Chile
#Por Benjamín Santelices Melis, Estudiante
#Departamento de Ingeniería Eléctrica, FCFM

#Estoy bastante seguro que no es ni la forma más eficiente ni la más elegante, si alguien me quiere ayudar a mejorarlo bienvenido sea.
#Está lleno de trucos truchos y resolución manual de excepciones.

from bs4 import BeautifulSoup #se debe instalar via pip.
import mysql.connector #se debe instalar via pip.
import urllib.request #se debe instalar via pip.
import time

start = time.time() #Para determinación de tiempo de ejecución

print("Scraper HTML-SQL Transparencia UChile")

dictMeses = {"Enero":"01", "Febrero":"02", "Marzo":"03", "Abril":"04", "Mayo":"05", "Junio":"06", "Julio":"07", "Agosto":"08", "Septiembre":"09", "Octubre":"10", "Noviembre":"11", "Diciembre":"12"} #Para pasar de nombres de meses a números

urlPlanta = "https://uchile.cl/presentacion/informacion-publica/dotacion-de-personal/personal-de-planta"
urlContrata = "https://uchile.cl/presentacion/informacion-publica/dotacion-de-personal/personal-a-contrata"
urlHonorarios = "https://uchile.cl/presentacion/informacion-publica/dotacion-de-personal/dotacion-a-honorarios" #URL de páginas de planta y contrata

def scrapeTransparencia(url): #Paso 1: entra a las URL de transparencia y obtiene las URL para todos los meses
    print("Scraping meses en", url)
    fuente = BeautifulSoup(urllib.request.urlopen(url).read(), "html.parser")
    enlaces = list(str(fuente.find_all("div", {"class": "content__description"})).split("\n")) #Los enlaces se retornan como líneas, por lo que se separan como lista por línea
    paginas = []
    for line in enlaces:
        if "pdf" not in line and "<a href" in line and "transparencia" in line and "2012" not in line: #ignorar enlaces que no sean de transparencia, que sean PDFs (pre 2012) y que sean del 2012 (los de ese año que siguen la lógica usual tienen otra estructura.)
            primeraComilla = line.find("\"")
            segundaComilla = line.find("\"", primeraComilla + 1)
            paginas.append(line[primeraComilla + 1:segundaComilla]) #el tag HTML es <a href= "URL">, por lo que busca las dos comillas y guarda la URL

    return paginas

def scrapeLetras(paginas): #Paso 2: para cada mes, se obtienen los enlaces para todas las letras de apellidos.
    links = []
    links += paginas #el enlace orginal contiene la tabla AB, por lo que se pueden agregar inmediatamente
    for link in paginas:
        print("Obteniendo tablas por letras en", link)
        fuente = BeautifulSoup(urllib.request.urlopen(link).read(), "html.parser")
        letras = str(fuente.find_all("h1")[2]) #Buscar h1 con los links, ignorar los primeros dos que son el header de la página.
        enlacesLetras = letras.split(" - ") #separar cada enlace usando los guiones
        for line in enlacesLetras:
            if "<a href" in line:
                primeraComilla = line.find("\"")
                segundaComilla = line.find("\"", primeraComilla + 1)
                links.append(line[primeraComilla + 1:segundaComilla]) #mismo approach para levantar la URL que en el paso 2
    return links

def scrapeTablas(links): #Paso 3: levantar las tablas y ordenarlas como lista
    tablasRotas = [] #todas las listas separadas, se consolidarán por fecha más tarde.
    for link in links:
        tablaData = []
        print("Scraping", link)
        fuente = BeautifulSoup(urllib.request.urlopen(link).read(), "html.parser")
        cabezal = str(fuente.find_all("div", {"class":"contenido"})).split("\n") #buscar todos los h1 para encontrar el mes y año al que pertenecen la tabla
        h1 = []
        for line in cabezal:
            if "<h1>" in line:
                h1.append(line)
        for headers in h1:
            if "Dotación" in headers: #buscar la palabra "Dotación" en el header para encontrar la línea con la fecha
                fechaRAW = headers.split("-")[1].replace(" ", "").replace("</h1>", "").split("20") #separar por guiones eliminar espacios y tagas, y separar con el 20- del año
                if fechaRAW[1].startswith("_"):
                    fechaRAW[1] = "20" + fechaRAW[1] #forma re turbia de atrapar que el split se come el año 2020.
                fecha = fechaRAW[1]+"_"+dictMeses[fechaRAW[0]] #convertir la fecha bruta de la forma "Agosto2021" a forma numerica de tabla "20_08"

        tabla = fuente.find_all("tr") #encontrar los elementos de la tabla

        lineas = str(tabla)
        lineas = lineas.replace(" align=\"right\"", "").replace("</td>\n<td>", "</td><td>").split("\n") #eliminar ancilarios de estilo y \n entre filas, el retorno del BeautifulSoup no es consistente así que se atrapa todo a mano.
        for line in lineas:
            if "td" in line:
                data = line[4:-5].split("</td><td>") #separar usando los quiebres de fila
                tablaData.append(data)
        tablasRotas.append((fecha, tablaData)) #agregar tupla de fecha con tabla a la salida

    return tablasRotas

def consolidarFechas(tablas): #Paso 4: unir todas las tablas que tengan la misma fecha
    dictTablas = {}
    for tabla in tablas:
        if tabla[0] in dictTablas.keys(): #busca si la fecha es llave en el diccionario, si no lo es agrega la tabla vacia como una nueva entrada, si no, le hace append al final de la ya existente.
            dictTablas[tabla[0]] += tabla[1]
        else:
            dictTablas[tabla[0]] = tabla[1]
    return(dictTablas)

def cargarSQL(sql, dictTablas, toggleHonorarios): #Paso 5: cargar tablas a la base SQL
    cur = sql.cursor()
    for mes in dictTablas:
        print("Creando tabla", mes)
        createCmd = ""
        if toggleHonorarios:
            createCmd = f"create table H{mes} (id int NOT NULL AUTO INCREMENT, apellido_paterno varchar(255), apellido_materno varchar(255), nombres varchar(255), funcion varchar(255), calificacion varchar(255), grado varchar(255), region varchar(255), moneda varchar(255), honorario int, mensual varchar(255), cuotas int, inicio varchar(255), termino varchar(255), unidad int, PRIMARY KEY(id));" #variante para honorarios, las tablas son distintas asi que reciben otra tabulación.
        else:
            createCmd = f"create table {mes} (id int NOT NULL AUTO_INCREMENT , contrato varchar(255), estamento varchar(255), apellido_paterno varchar(255), apellido_materno varchar(255), nombres varchar(255), grado int, calificacion varchar(255), cargo varchar(255), region varchar(255), A_asignaciones int, moneda varchar(255), B_remuneracion int, C_extdiurnas  int, D_extnocturnas int, inicio varchar(255), termino varchar(255), unidad int, PRIMARY KEY (id));"
        cur.execute(createCmd) #crear tabla. IMPORTANTE: la tabla no debe existir de antemano.
        print("Cargando datos a", mes)
        for dataRow in dictTablas[mes]: #cargar datos con una instrucción por fila. Los datos siempre están en el mismo lugar, por lo que se referencia la lista estáticamente.
            try:
                cur.execute(f"""insert into {mes} (contrato, estamento, apellido_paterno, apellido_materno, nombres, grado, calificacion, cargo, region, A_asignaciones, moneda, B_remuneracion, C_extdiurnas, D_extnocturnas, inicio, termino, unidad) values (\"{dataRow[0]}\", \"{dataRow[1]}\", \"{dataRow[2]}\", \"{dataRow[3]}\", \"{dataRow[4]}\", {dataRow[5].replace('C', '0').replace('#¡VALOR!', '0')}, \"{dataRow[6]}\", \"{dataRow[7]}\", \"{dataRow[8]}\", {dataRow[9].replace('.', '')}, \"{dataRow[10]}\", {dataRow[11].replace('.', '')}, {dataRow[12].replace('.', '')}, {dataRow[13].replace('.', '')}, \"{dataRow[14]}\", \"{dataRow[15].replace('Indefinido', '')}\", {dataRow[16]})""") #los replace son todos para eliminar inconsictencias y regularizar los formatos para SQL.
            except:
                print(mes, dataRow) #si el dato tiene un error, no lo sube y lo imprime. Me salen datos vacíos a veces, o datos con valores inválidos.
        sql.commit() #commit al SQL, la tabla no se sube si falla antes de llegar acá.


print("\n---ENLACES MENSUALES---")
planta = scrapeTransparencia(urlPlanta)
contrata = scrapeTransparencia(urlContrata)
honorarios = scrapeTransparencia(urlHonorarios)

meses = [*planta, *contrata] #tabla de enlaces de planta y conrata.
print("\n---ENLACES POR LETRA---")
links = scrapeLetras(meses)
linksHonorarios = scrapeLetras(honorarios)

print("Extrayendo tablas")
tablasRAW = scrapeTablas(links)
tablasRAWHonorarios = scrapeTablas(linksHonorarios)
tablasOrdenadas = consolidarFechas(tablasRAW)
tablasOrdenadasHonorarios = consolidarFechas(tablasRAW)

SQL = mysql.connector.connect(host="SERVER", user="benjamin", passwd="completos", db="fcfm_transparencia") #conector SQL

cargarSQL(SQL, tablasOrdenadas, False)
cargarSQL(SQL, tablasOrdenadasHonorarios, True)

print("Time! completado en", time.time() - start) #tiempo de ejecución. En mi caso tomó aprox. 73 minutos.

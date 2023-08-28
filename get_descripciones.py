from bs4 import BeautifulSoup
import yaml
import os

# descripciones = yaml.safe_load('./sistema_informacion/descripciones.yaml')
descripciones = {}
file_list = os.listdir('./sistema_informacion/metadatos_html/')
for nombre_file in file_list:
    if nombre_file == 'plantilla.html':
        continue
    trozos = nombre_file.split('_')
    actividad = trozos[1]
    consulta = trozos[2].split('.')[0]
    print('Extrayendo descripcion de {} {}, ruta: {}'.format(actividad, consulta, nombre_file))

    file = open(os.path.join('./sistema_informacion/metadatos_html/', nombre_file), 'r', encoding='utf-8')
    soup = BeautifulSoup(file.read(), 'html.parser')
    descripcion = soup.find('div', id='R12-r2').text
    
    if actividad not in descripciones:
        descripciones[actividad]={}
    if descripcion == '--':
        descripcion = None
    descripciones[actividad][consulta] = descripcion

yaml.safe_dump(descripciones, open('./sistema_informacion/descripciones.yaml', 'w', encoding='utf-8'))
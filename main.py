import copy
import os
import time

from bs4 import BeautifulSoup

import pandas as pd
import yaml
from mdmpyclient.codelist.codelist import Codelist

from mdmpyclient.mdm import MDM
from mdmpyclient.ckan.ckan import Ckan
import deepl
from ckanapi import RemoteCKAN

from functions import execute_actividades, initialize_codelists_schemes, put_dsds, get_configuracion_completo, \
    put_all_codelist_schemes, create_categories, mappings_variables, create_dataflows, volcado_ckan, create_metadatos

import logging

logger = logging.getLogger("deepl")
logger.setLevel(logging.WARNING)

if __name__ == "__main__":
    with open("configuracion/global.yaml", 'r', encoding='utf-8') as configuracion_global, \
            open("configuracion/ejecucion.yaml", 'r', encoding='utf-8') as configuracion_ejecucion, \
            open("configuracion/actividades.yaml", 'r', encoding='utf-8') as configuracion_actividades, \
            open("configuracion/plantilla_actividad.yaml", 'r',
                 encoding='utf-8') as plantilla_configuracion_actividad, \
            open("sistema_informacion/BADEA/jerarquias/datos_jerarquias.yaml", 'r',
                 encoding='utf-8') as datos_jerarquias, \
            open("sistema_informacion/mapas/conceptos_codelist.yaml", 'r',
                 encoding='utf-8') as mapa_conceptos_codelist_file, \
            open("sistema_informacion/traducciones.yaml", 'r',
                 encoding='utf-8') as traducciones:
        # cache = yaml.safe_load(open('traducciones.yaml'))

        configuracion_global = yaml.safe_load(configuracion_global)
        configuracion_ejecucion = yaml.safe_load(configuracion_ejecucion)
        configuracion_actividades = yaml.safe_load(configuracion_actividades)
        configuracion_plantilla_actividad = yaml.safe_load(plantilla_configuracion_actividad)
        mapa_conceptos_codelist = yaml.safe_load(mapa_conceptos_codelist_file)
        traducciones = yaml.safe_load(traducciones)
        datos_jerarquias = yaml.safe_load(datos_jerarquias)
        traductor = deepl.Translator('6a0fd2f4-27e7-82d7-1036-42a75f8037f7:fx')

    if configuracion_global["extractor"]:
        execute_actividades(configuracion_ejecucion, configuracion_global, configuracion_actividades,
                            configuracion_plantilla_actividad, mapa_conceptos_codelist)

    if configuracion_global['reset_ddb']:
        controller = MDM(configuracion_global, traductor, False)
        controller.delete_all('ESC01', 'IECA_CAT_EN_ES', '1.0')

    if configuracion_global['volcado_mdm']:
        controller = MDM(configuracion_global, traductor, True)
        configuracion_actividades_sdmx = get_configuracion_completo(configuracion_ejecucion)

        put_all_codelist_schemes(configuracion_ejecucion, configuracion_actividades_sdmx, datos_jerarquias,
                                 mapa_conceptos_codelist, controller, configuracion_actividades)

        put_dsds(configuracion_ejecucion, configuracion_actividades_sdmx, mapa_conceptos_codelist, controller)

        category_scheme = controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0']

        if configuracion_global['translate']:
            category_scheme.translate()

        create_categories(category_scheme, configuracion_ejecucion, configuracion_actividades_sdmx)

        create_dataflows(configuracion_ejecucion, configuracion_actividades, configuracion_actividades_sdmx,
                         category_scheme, configuracion_global, mapa_conceptos_codelist, controller)

        create_metadatos(configuracion_ejecucion, configuracion_actividades, category_scheme, controller,
                         configuracion_global, configuracion_actividades_sdmx)

    if configuracion_global['volcado_ckan']:
        volcado_ckan(configuracion_global, configuracion_ejecucion, configuracion_actividades)
    # controller = MDM(configuracion_global, traductor, True)

    controller.logout()

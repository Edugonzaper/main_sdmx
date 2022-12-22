import copy
import os
import time

import pandas as pd
import yaml
from mdmpyclient.codelist.codelist import Codelist

from mdmpyclient.mdm import MDM
from mdmpyclient.ckan.ckan import Ckan
import deepl
from ckanapi import RemoteCKAN

from functions import execute_actividades, initialize_codelists_schemes, put_dsds, get_configuracion_completo, \
    put_all_codelist_schemes, create_categories, mappings_variables, create_dataflows

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
        traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

        # if configuracion_global['volcado_ckan']:
        #     ckan = Ckan(configuracion_global)
        # if configuracion_global['reset_ckan']:
        #     ckan.datasets.remove_all_datasets()
        #
        # if configuracion_global['volcado_mdm']:
        #     controller = MDM(configuracion_global, traductor, True)
        #     category_scheme = controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0']
        #     if configuracion_global['reset_ddb']:
        #         controller.delete_all('ESC01', 'IECA_CAT_EN_ES', '1.0')
        #
        # if configuracion_global['translate']:
        #     controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].translate()
    if configuracion_global["extractor"]:
        execute_actividades(configuracion_ejecucion, configuracion_global, configuracion_actividades,
                            configuracion_plantilla_actividad, mapa_conceptos_codelist)

    if configuracion_global['volcado_mdm']:
        controller = MDM(configuracion_global, traductor, True)
        if configuracion_global['reset_ddb']:
            controller.delete_all('ESC01', 'IECA_CAT_EN_ES', '1.0')

        configuracion_actividades_sdmx = get_configuracion_completo(configuracion_ejecucion)

        put_all_codelist_schemes(configuracion_ejecucion, configuracion_actividades_sdmx, datos_jerarquias,
                                 mapa_conceptos_codelist, controller)

        put_dsds(configuracion_ejecucion, configuracion_actividades_sdmx, mapa_conceptos_codelist, controller)

        category_scheme = controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0']
        create_categories(category_scheme, configuracion_ejecucion, configuracion_actividades_sdmx)

        create_dataflows(configuracion_ejecucion, configuracion_actividades, configuracion_actividades_sdmx,
                         category_scheme, configuracion_global, mapa_conceptos_codelist, controller)
        # try:
        #     variables.remove('TEMPORAL')
        # except:
        #     pass
        # try:
        #     variables.remove('INDICATOR')
        # except:
        #     pass
        # try:
        #     variables.remove('OBS_VALUE')
        # except:
        #     pass
        # try:
        #     variables.remove('OBS_STATUS')
        # except:
        #     pass
        # try:
        #     variables.remove('FREQ')
        # except:
        #     pass

        # # Creación del cubo para la actividad
        # for consulta in actividad.consultas.values():
        #     cube_id = configuracion_global['nodeId'] + "_" + nombre_actividad + "_" + consulta.id_consulta
        #     categories = category_scheme.categories
        #     id_cube_cat = \
        #         categories[categories['id'] == nombre_actividad]['id_cube_cat'].values[0]
        #
        #     id_cubo = controller.cubes.put(cube_id, id_cube_cat, id_dsd,
        #                                    consulta.metadatos['subtitle'],
        #                                    dimensiones)
        #
        #     variables = copy.deepcopy(actividad.configuracion['variables'])
        #     mapa = copy.deepcopy(actividad.configuracion['variables'])
        #     mapa = ['TIME_PERIOD' if variable == 'TEMPORAL' else variable for variable in mapa]
        #
        #     mapping_id = controller.mappings.put(variables, id_cubo,
        #                                          nombre_actividad + '_' + consulta.id_consulta)
        #
        #     try:
        #         mapping = controller.mappings.data[id_cubo].load_cube(
        #             consulta.datos.datos_por_observacion_extension_disjuntos)
        #     except:
        #         controller.mappings.data = controller.mappings.get(True)
        #         mapping = controller.mappings.data[id_cubo].load_cube(
        #             consulta.datos.datos_por_observacion_extension_disjuntos)
        #
        #     id_df = f'DF_{nombre_actividad}_{consulta.id_consulta}'
        #     nombre_df = {'es': consulta.metadatos['title']}
        #     if consulta.metadatos['subtitle']:
        #         nombre_df = {'es': consulta.metadatos['title'] + ': ' + consulta.metadatos['subtitle']}
        #
        #     variables_df = ['ID_' + variable if variable != 'OBS_VALUE' else variable for variable in mapa]
        #     if 'ID_OBS_STATUS' not in variables_df:
        #         variables_df += ['ID_OBS_STATUS']
        #
        # controller.dataflows.put(id_df, agencia, '1.0', nombre_df, None, variables_df, id_cubo, dsd,
        #                          category_scheme, nombre_actividad)
        #     controller.dataflows.data = controller.dataflows.get(False)
        #     try:
        #         controller.dataflows.data[agencia][id_df]['1.0'].publish()
        #     except:
        #         print('está publicado')
        #
        #     id_mdf = f'MDF_{nombre_actividad}_{consulta.id_consulta}'
        #     controller.metadataflows.put(agencia, id_mdf, '1.0', nombre_df, None)
        #
        #     id_mds = f'MDF_{nombre_actividad}_{consulta.id_consulta}'
        #     nombre_mds = {'es': consulta.metadatos['title']}
        #     if consulta.metadatos['subtitle']:
        #         nombre_mds = {'es': consulta.metadatos['title'] + ': ' + consulta.metadatos['subtitle']}
        #     categoria = category_scheme.get_category_hierarchy(actividad.actividad)
        #     controller.metadatasets.put(agencia, id_mds, nombre_mds, id_mdf, '1.0', 'IECA_CAT_EN_ES', categoria,
        #                                 '1.0')
        #     controller.metadatasets.data[id_mds].put(
        #         os.path.join(configuracion_global['directorio_reportes_metadatos'],
        #                      actividad.configuracion_actividad['informe_metadatos'] + '.json'))
        #     controller.metadatasets.data[id_mds].init_data()
        #     controller.metadatasets.data[id_mds].publish_all()
        #     # controller.metadatasets.data[id_mds].download_all_reports()
        #     if configuracion_global['volcado_ckan']:
        #         id_dataset = f'DF_{nombre_actividad}_{consulta.id_consulta}'
        #         name_dataset = controller.dataflows.data[agencia][id_df]['1.0'].names['es']
        #         ckan.datasets.create(id_dataset.lower(), name_dataset, ckan.orgs.orgs[nombre_actividad.lower()])
        #         ckan.resources.create(consulta.datos.datos_por_observacion_extension_disjuntos,
        #                               id_dataset, 'csv', id_dataset.lower())
        #         controller.metadatasets.data[id_mds].reports.apply(
        #             lambda x: ckan.resources.create_from_file(
        #                 f'{configuracion_global["directorio_metadatos_html"]}/{x.code}.html', x.code, 'html',
        #                 id_dataset.lower()), axis=1)
    controller.logout()

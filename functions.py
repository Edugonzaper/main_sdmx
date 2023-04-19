import os

import pandas as pd
import yaml
from iecasdmx.ieca.actividad import Actividad
from mdmpyclient.ckan.ckan import Ckan
from mdmpyclient.mdm import MDM


def execute_actividades(configuracion_ejecucion, configuracion_global, configuracion_actividades,
                        mapa_conceptos_codelist):
    for nombre_actividad in configuracion_ejecucion['actividades']:
        actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                              mapa_conceptos_codelist, nombre_actividad)
        actividad.generar_consultas()
        actividad.ejecutar()


def initialize_codelists_schemes(configuracion_actividad, datos_jerarquias, mapa_conceptos_codelist, controller,
                                 configuracion_actividades):
    for dimension in configuracion_actividad['variables']:
        jerarquia = datos_jerarquias[dimension]

        codelist_id = jerarquia['ID']
        codelist_agency = jerarquia['agency']
        codelist_version = jerarquia['version']
        codelist_names = jerarquia['nombre']
        codelist_des = jerarquia['description']

        datos_dimension = mapa_conceptos_codelist[dimension]
        concept_scheme = datos_dimension['concept_scheme']

        concept_scheme_id = concept_scheme['id']
        concept_scheme_agency = concept_scheme['agency']
        concept_scheme_version = concept_scheme['version']
        concept_scheme_names = {'es': concept_scheme_id.replace('CS_', '')}
        concept_scheme_des = None
        concept_scheme_concept = concept_scheme['concepto']

        codelist = controller.codelists.add_codelist(codelist_agency, codelist_id, codelist_version, codelist_names,
                                                     codelist_des)

        for file_path in jerarquia['fichero']:
            consultas = configuracion_actividades[configuracion_actividad['NOMBRE']]['consultas']
            consultas = [str(cons).split('?')[0] for cons in consultas]
            if configuracion_actividad['NOMBRE'] in file_path and any([(str(cons) in file_path) for cons in consultas]):
                data = pd.read_csv(file_path, sep=';', dtype='string')
                codelist.add_codes(data)

        concept_scheme = controller.concept_schemes.add_concept_scheme(concept_scheme_agency, concept_scheme_id,
                                                                       concept_scheme_version,
                                                                       concept_scheme_names, concept_scheme_des)
        concept_scheme.add_concept(concept_scheme_concept, None, concept_scheme_concept, None)

    codelist_medidas = controller.codelists.add_codelist('ESC01', 'CL_UNIT', '1.0',
                                                         {'es': 'Unidades de Medida (Indicadores)',
                                                          'en': 'Measurement units (Indicators)'},
                                                         {'es': 'Unidades de Medida (Indicadores)',
                                                          'en': 'Measurement units (Indicators)'})
    data_medidas = pd.read_csv('sistema_informacion/BADEA/jerarquias/INDICATOR', sep=';',
                               dtype='string')  # Esto es feisimo
    codelist_medidas.add_codes(data_medidas)


def put_dsds(configuracion_ejecucion, configuracion_actividades_completo, mapa_conceptos_codelist, controller):
    for nombre_actividad in configuracion_ejecucion['actividades']:
        configuracion_actividad = configuracion_actividades_completo[nombre_actividad]
        dsd_id = 'DSD_' + nombre_actividad
        dsd_agency = 'ESC01'
        dsd_version = '1.0'
        dsd_names = {'es': configuracion_actividad['subcategoria']}
        dsd_des = None
        dimensions = {variable: mapa_conceptos_codelist[variable] for variable in
                      configuracion_actividad['variables']}

        validFrom = "3500-01-01"
        validTo = "1500-01-01"
        for c_id in list(configuracion_actividad["periodicidad"].keys()):
            v_from = configuracion_actividad["periodicidad"][c_id]["validFrom"]
            v_to = configuracion_actividad["periodicidad"][c_id]["validTo"]
            if v_from < validFrom:
                validFrom = v_from
            if v_to > validTo:
                validTo = v_to

        controller.dsds.put(dsd_agency, dsd_id, dsd_version, dsd_names, dsd_des, dimensions, validFrom, validTo)


def get_configuracion_completo(configuracion_ejecucion):
    configuracion_actividades_completo = {}
    for nombre_actividad in configuracion_ejecucion['actividades']:
        with open(f'sistema_informacion/SDMX/datos/{nombre_actividad}/configuracion.yaml', 'r',
                  encoding='utf-8') as file:
            configuracion_actividad = yaml.safe_load(file)
            configuracion_actividades_completo[nombre_actividad] = configuracion_actividad
    return configuracion_actividades_completo


def put_all_codelist_schemes(configuracion_ejecucion, configuracion_actividades_completo, datos_jerarquias,
                             mapa_conceptos_codelist, controller, configuracion_actividades):
    for nombre_actividad in configuracion_ejecucion['actividades']:
        configuracion_actividad = configuracion_actividades_completo[nombre_actividad]
        initialize_codelists_schemes(configuracion_actividad, datos_jerarquias, mapa_conceptos_codelist,
                                     controller, configuracion_actividades)
    controller.codelists.put_all_codelists()
    controller.concept_schemes.put_all_concept_schemes()
    controller.codelists.put_all_data()
    controller.concept_schemes.put_all_data()


def create_categories(category_scheme, configuracion_ejecucion, configuracion_actividades_completo):
    category_scheme.init_categories()
    for nombre_actividad in configuracion_ejecucion['actividades']:
        configuracion_actividad = configuracion_actividades_completo[nombre_actividad]
        category_scheme.add_category(nombre_actividad, configuracion_actividad['categoria'],
                                     configuracion_actividad['subcategoria'], None)
    category_scheme.put()


def mappings_variables(variables, mapa_conceptos_codelist):
    variables_mapped = {}
    for variable in variables:
        try:
            variables_mapped[variable] = mapa_conceptos_codelist[variable]['nombre_dimension']
        except:
            variables_mapped[variable] = variable
    return variables_mapped


def create_dataflows(configuracion_ejecucion, configuracion_actividades, configuracion_actividades_sdmx,
                     category_scheme, configuracion_global, mapa_conceptos_codelist, controller):
    for nombre_actividad in configuracion_ejecucion['actividades']:
        for consulta in configuracion_actividades[nombre_actividad]['consultas']:
            configuracion_actividad = configuracion_actividades_sdmx[nombre_actividad]
            consulta_id = str(consulta).split('?')[0]
            categories = category_scheme.categories
            id_cube_cat = \
                categories[categories['id'] == nombre_actividad]['id_cube_cat'].values[0]

            cube_code = configuracion_global['nodeId'] + "_" + nombre_actividad + "_" + consulta_id
            dimensiones = {variable: mapa_conceptos_codelist[variable] for variable in
                           configuracion_actividad['variables']}
            cube_id = controller.cubes.put(cube_code, id_cube_cat, 'DSD_' + nombre_actividad,
                                           configuracion_actividades_sdmx[nombre_actividad]['metadatos_title'][
                                               consulta_id], dimensiones)

            variables = configuracion_actividad['variables'] + ['INDICATOR', 'TEMPORAL',
                                                                'FREQ',
                                                                'OBS_VALUE']
            variables = mappings_variables(variables, mapa_conceptos_codelist)

            mapping_id = controller.mappings.put(variables, cube_id, nombre_actividad + '_' + consulta_id)

            cube_data = pd.read_csv(
                f'{configuracion_global["directorio_datos"]}/{nombre_actividad}/procesados/{consulta_id}.csv',
                sep=';', dtype='string')
            cube_data = script_provisional(cube_data, configuracion_actividad['variables'])
            controller.mappings.data[cube_id].load_cube(cube_data)

            df_id = f'DF_{nombre_actividad}_{consulta_id}'
            df_title = configuracion_actividades_sdmx[nombre_actividad]["metadatos_title"][consulta_id] + \
                       configuracion_actividades_sdmx[nombre_actividad]["metadatos_subtitle"][consulta_id]
            df_name = {'es': configuracion_actividades_sdmx[nombre_actividad]["metadatos_title"][consulta_id]}
            dataflow_columns = [
                f'ID_{column}' if column not in ['OBS_VALUE', 'TEMPORAL'] else column.replace('TEMPORAL',
                                                                                              'ID_TIME_PERIOD') for
                column in
                variables.values()]
            validFrom = configuracion_actividad["periodicidad"][consulta_id]["validFrom"]
            validTo = configuracion_actividad["periodicidad"][consulta_id]["validTo"]
            print("debug antes de creación,", validTo, validFrom)

            df = controller.dataflows.put(df_id, 'ESC01', '1.0', df_name, None, dataflow_columns, cube_id,
                                          controller.dsds.data['ESC01'][f'DSD_{nombre_actividad}']['1.0'],
                                          category_scheme, nombre_actividad, validFrom, validTo)
            df.publish()

            controller.synchronizeAuthDB()


def script_provisional(cube_data, columns):
    for column in columns:
        if column not in cube_data.columns:
            cube_data.insert(len(cube_data.columns), column, '_Z')
    return cube_data


def volcado_ckan(configuracion_global, configuracion_ejecucion, configuracion_actividades,
                 configuracion_actividades_sdmx, descripciones):
    ckan = Ckan(configuracion_global)
    if configuracion_global['reset_ckan']:
        ckan.datasets.remove_all_datasets()

    if configuracion_global['create_groups']:
        category_scheme = pd.read_csv(configuracion_global['directorio_esquema_categorias'], sep=';')
        category_scheme.columns = ['id', 'name_es', 'Description', 'parent', 'Order', 'FullName', 'IsDefault']
        ckan.groups.create_groups(category_scheme)
        ckan.groups.groups = ckan.groups.get_groups()

    for nombre_actividad in configuracion_ejecucion['actividades']:
        for consulta in configuracion_actividades[nombre_actividad]['consultas']:
            id_consulta = str(consulta).split('?')[0]
            id_dataset = f'{nombre_actividad}_{id_consulta}'
            title = configuracion_actividades_sdmx[nombre_actividad]['metadatos_title'][str(id_consulta)]
            if configuracion_actividades_sdmx[nombre_actividad]['metadatos_subtitle'][str(id_consulta)]:
                title = f'{title}. {configuracion_actividades_sdmx[nombre_actividad]["metadatos_subtitle"][str(id_consulta)]}'
            extras = [{'key': 'Actividad', 'value': configuracion_actividades_sdmx[nombre_actividad]['subcategoria']},
                      {'key': 'Periocidad',
                       'value': f'{configuracion_actividades_sdmx[nombre_actividad]["periodicidad"][id_consulta]["frecuencia"]}'}]
            tags = [{'name': tag} for tag in descripciones[f'{nombre_actividad}_{id_consulta}']['tags']]
            ckan.datasets.create(id_dataset.lower(),
                                 title, 'instituto-de-estadistica-y-cartografia-de-andalucia',
                                 ckan.groups.groups[nombre_actividad.lower()],
                                 descripciones[f'{nombre_actividad}_{id_consulta}']['descripcion'], extras, tags,
                                 'Creative Commons Attribution')
            path = os.path.join(configuracion_global['directorio_datos'], nombre_actividad, 'procesados')

            data = pd.read_csv(f'{path}/{id_consulta}.csv', sep=';')
            ckan.resources.create(data, f'DATOS_{id_dataset}', 'csv',
                                  id_dataset.lower())
            ckan.resources.create_from_file(
                f'{configuracion_global["directorio_metadatos_html"]}/REPORT_{id_dataset}.html',
                f'METADATOS_DE_REFERENCIA_{id_dataset}', 'html', id_dataset.lower())


def create_metadatos(configuracion_ejecucion, configuracion_actividades, category_scheme, controller,
                     configuracion_global, configuracion_actividades_sdmx):
    for nombre_actividad in configuracion_ejecucion['actividades']:

        for consulta in configuracion_actividades[nombre_actividad]['consultas']:

            consulta_id = str(consulta).split('?')[0]
            nombre_df = configuracion_actividades_sdmx[nombre_actividad]['metadatos_title'][consulta_id]
            if configuracion_actividades_sdmx[nombre_actividad]['metadatos_subtitle'][consulta_id]:
                nombre_df = configuracion_actividades_sdmx[nombre_actividad]['metadatos_title'][consulta_id] + ': ' \
                            + configuracion_actividades_sdmx[nombre_actividad]['metadatos_subtitle'][consulta_id]
            id_mdf = f'MDF_{nombre_actividad}_{consulta_id}'
            controller.metadataflows.put('ESC01', id_mdf, '1.0', {'es': nombre_df}, None)
            id_mds = f'MDS_{nombre_actividad}_{consulta_id}'
            nombre_mds = {'es': nombre_df}
            categoria = category_scheme.get_category_hierarchy(nombre_actividad)
            controller.metadatasets.put('ESC01', id_mds, nombre_mds, id_mdf, '1.0',
                                        'IECA_CAT_EN_ES', categoria, '1.0')
            path = os.path.join(configuracion_global['directorio_reportes_metadatos'], nombre_actividad,
                                configuracion_actividades[nombre_actividad][
                                    'informe_metadatos'] + f'_{consulta_id}.json')
            controller.metadatasets.data[id_mds].put(path)
            controller.metadatasets.data[id_mds].init_data()
            controller.metadatasets.data[id_mds].publish_all()
            controller.metadatasets.data[id_mds].get_report()
            controller.metadatasets.data[id_mds].extract_info_html()

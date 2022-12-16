import pandas as pd
from iecasdmx.ieca.actividad import Actividad


def execute_actividades(configuracion_ejecucion, configuracion_global, configuracion_actividades,
                        configuracion_plantilla_actividad, mapa_conceptos_codelist):
    for nombre_actividad in configuracion_ejecucion['actividades']:
        actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                              configuracion_plantilla_actividad, mapa_conceptos_codelist, nombre_actividad)
        actividad.generar_consultas()
        actividad.ejecutar()


def initialize_codelists_schemes(configuracion_actividad, datos_jerarquias, mapa_conceptos_codelist, controller):
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
            if configuracion_actividad['NOMBRE'] in file_path:
                data = pd.read_csv(file_path, sep=';')
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
    # codelist_medidas.add_codes()

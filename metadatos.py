import copy
import json
import os

import yaml

with open('sistema_informacion/report_plantilla.json', 'r', encoding='utf-8') as report_file, open(
        'configuracion/actividades.yaml', 'r', encoding='utf-8') as actividades_file, open(
    'configuracion/ejecucion.yaml',
    'r', encoding='utf-8') \
        as ejecucion_file:
    reporte = json.load(report_file)
    actividades = yaml.safe_load(actividades_file)
    ejecucion = yaml.safe_load(ejecucion_file)
    report_file.close()
    actividades_file.close()
    ejecucion_file.close()

for actividad in ejecucion['actividades']:
    report = copy.deepcopy(reporte)
    with open(f'sistema_informacion/reportes_yaml/{actividad}.yaml', 'r', encoding='utf-8') as metadato_file:
        metadato = yaml.safe_load(metadato_file)
        metadato_file.close()
    for group in report['data']['metadataSets'][0]['reports'][0]['attributeSet']['reportedAttributes']:
        for erre in group['attributeSet']['reportedAttributes']:
            erre['texts']['es'] = metadato[erre['id']]

    for consulta in actividades[actividad]['consultas']:
        report_consulta = copy.deepcopy(report)
        consulta = str(consulta)
        consulta_id = consulta.find('?')
        if consulta_id != -1:
            consulta_id = consulta[:consulta_id]
        else:
            consulta_id = consulta

        report_consulta['data']['metadataSets'][0]['id'] = f"{metadato['mds_id']}_{consulta_id}"
        report_consulta['data']['metadataSets'][0]['names']['es'] = metadato['name']
        report_consulta['data']['metadataSets'][0]['annotations'][0]['texts'][
            'en'] = f"{metadato['mdf_id']}_{consulta_id}"
        report_consulta['data']['metadataSets'][0]['annotations'][0]['text'] = f"{metadato['mdf_id']}_{consulta_id}"
        report_consulta['data']['metadataSets'][0]['reports'][0]['id'] = f"{metadato['report_id']}_{consulta_id}"
        report_consulta['data']['metadataSets'][0]['reports'][0]['target']['referenceValues'][0][
            'object'] = f"{metadato['dataflow']}_{consulta_id}"

        try:
            os.mkdir(f'sistema_informacion/reportes_metadatos/{actividad}')
        except:
            pass
        with open(f'sistema_informacion/reportes_metadatos/{actividad}/REPORT_{actividad}_{consulta_id}.json', 'w',
                  encoding='utf-8') as file:
            json.dump(report_consulta, file)
            file.close()

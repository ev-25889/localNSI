"""Модуль взаимодействия с ГИС СЦОС. Загрузка, обновление, ?удаление? объектов."""
import psycopg2
import requests
import json
from psycopg2 import Error

atributes = {'disciplines' : ['external_id', 'title'],
             'educational_programs' : ['external_id', 'title','direction', 'code_direction', 'start_year', 'end_year'],
             'study_plans' : ['external_id', 'title', 'direction', 'code_direction', 'start_year', 'end_year',
                              'education_form', 'educational_program'],
             'contingent_flows': ['student', 'contingent_flow', 'flow_type', 'date', 'faculty', 'education_form',
                                  'form_fin', 'details', 'external_id'],
             'study_plans_disciplines': ['study_plan', 'discipline', 'semester'],
             'study_plans_students': ['study_plan', 'student']
             }

object_names = {'RootRegistryElement' : ['disciplines', 'disciplines'],  # [адрес/таблицы, массив]
                'EducationalPrograms' : ['educational_programs','educational_programs'],
                'EduPlanVersion' : ['study_plans','study_plans'],
                'StudentOrderExtract' : ['contingent_flows','contingent_flows'],
                'StudyPlanDiscipline' : ['study_plans_disciplines', 'study_plan_disciplines'],
                'StudyPlanStudent': ['study_plans_students', 'study_plan_students']}

def change_status(query_type, table, responce=None, external_id=None, status=None, number=None):
    """
    Метод изменяет статус записи  в заввисимости от действия (обновление, добавление... (удаление?))
    Параметры: query_type:responce:
    """
    update_query = ''
    if query_type == 'post':
        print(len(responce['results']))
        # for i in range(len(responce['results'])):
        external_id = "'" + responce['results'][number]['external_id'] + "'"
        gis_id = "'" + responce['results'][number]['id'] + "'"
        result = "'" + responce['results'][number]['additional_info'] + "'"
        update_query = '''update {table} set "gisscos_id" = {gis_id}, "status" = 'equal', 
                          "responce" = {result}, "date_sync" = NOW() where "external_id" = {external_id}'''.\
            format(table=table, gis_id=gis_id, result=result, external_id=external_id)
        print(update_query)
        print('externaal_id: ', external_id, ', gis_id: ', gis_id, ', result: ', result)
        cursor.execute(update_query)
        connection.commit()

    if query_type == 'error':
        external_id = "'" + responce['results'][number]['external_id'] + "'"
        result = "'" + responce['results'][number]['additional_info'] + "'"
        update_query = '''update {table} set "status" = 'error',
                                        "responce" = {result},
                                        "date_sync" = NOW()
                     where "external_id" = {external_id} '''.\
            format(table=table, result=result, external_id=external_id)
        # print('externaal_id: ', external_id, ', gis_id: ', gis_id, ', result: ', result)
        cursor.execute(update_query)
        connection.commit()

# получить ид
def id_list(object, status=None, limit=None):
    """
    Получить список ид для выбранного типа объектов с определенным статусом
    Возвращает:
    список идентификаторов ID из таблицы **object = object**, для которых **status = status**
	"""
    print('id_list')
    if limit == None:
        limit = 500
    table = object_names[object][0]
    if status is None:                              # не указан статус - возвращаем записи со всеми статусами и без них
        select_query = '''select "external_id" from {table} limit({limit})'''.format(table=table, limit=limit)
    elif status == '':                              # указан пустой статус - возвращаем записи без статуса
        select_query = '''select "external_id" from {table} where "status" is null or "status" = '' '''.format(
            table=table)
    else:
        select_query = '''select "external_id" from {table} where "status" = '{status}' limit({limit})'''.format(
            table=table, status=status, limit=limit)

    if object == 'StudentOrderExtract':
        select_query = '''select cf."external_id" from contingent_flows cf 
                          join student_status ss on ss.external_id = cf.student 
                          where ss.status = 'equal' and cf.status = '{}' limit({}) '''.format(status, limit)
    cursor.execute(select_query)
    select = cursor.fetchall()
    list_of_id = list()
    for row in select:
        list_of_id.append(str(row[0]))
    id_param = "'" + "','".join(list_of_id) + "'"
    if len(id_param) > 2:
        return id_param  # тип параметра - строка
    else:
        return 'записи не найдены'


def make_dict(object, count):
    # получить инфу по ид из бд в словарь
    id = id_list(object=object, status='new', limit=count)
    if id == 'записи не найдены' or id == 'такой таблицы нет':
        return 'получить словарь невозможно: {}'.format(id)
    else:
        atribut = ', '.join(atributes[object_names[object][0]])
        table = object_names[object][0]
        select_query = '''select {atrib} from {table} where "external_id" in ({id})'''\
            .format(atrib=atribut,table=table,id=id)            # я могу пытаться в запрос вместо списка ид
        cursor.execute(select_query)                            # засунуть другие ретерны 'записи не найдены' или
        result = cursor.fetchall()                              # 'такой таблицы нет'
        info_list = list()
        for res in result:
            one_subject_info = list()
            for r in res:
                one_subject_info.append(r)
            dictant = dict(zip(atributes[object_names[object][0]], one_subject_info))
            info_list.append(dictant)
        finall_dict = {'organization_id': 'f07f886b-d609-4a23-8ada-53e1717c1b9c',
                       object_names[object][1]: info_list}
        return finall_dict

def save(object, file, count):
    # сохранить итоговый словарь в файл
    body = make_dict(object=object, count=count)
    with open(file, 'w', encoding='utf-8') as fp:  # открываем файл для записи
        json.dump(body, fp, ensure_ascii=False)

def send_to_gis(object, count):
    save(object=object, file='forGIS.json', count=count)
    with open('forGIS.json', 'r', encoding="utf8") as f:
        data = json.load(f)
    print("Отправляем данные {} в ГИС СЦОС".format(object_names[object][0]))
    responce = requests.post(url='https://test.online.edu.ru/vam/api/v2/{}'.format(object_names[object]),
                             headers={"X-CN-UUID": "e52e03e0-7038-4090-a9f6-628367c0094d"}, json=data, verify=False)
    try:
        responce = responce.json()
        print(responce)
        for i in range(len(responce['results'])):
            try:
                change_status(query_type='post', responce=responce, table=object_names[object], number=i)
                "Статус изменен успешно"
            except Exception:
                change_status(query_type='error', responce=responce, table=object_names[object], number=i)
                "Статус изменен с ошибкой "
    except Exception:
        print('(((')


try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="donsitest", password="TS4d#dkpf3WE1",
                                  host="192.168.25.103", port="5432", database="doubnsitest")
    cursor = connection.cursor()

    print(send_to_gis(object='StudentOrderExtract'))
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")


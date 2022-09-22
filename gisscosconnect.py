"""Модуль взаимодействия с ГИС СЦОС. Загрузка, обновление, ?удаление? объектов."""
import psycopg2
import json
from psycopg2 import Error

atributes_discipline = ['external_id', 'title']
atributes_eduprogram = ['external_id', 'title','direction', 'code_direction', 'start_year',
                        'end_year']
def change_status(query_type, responce=None, external_id=None, status=None):
    """
    Метод изменяет статус записи  в заввисимости от действия (обновление, добавление... (удаление?))
    Параметры: query_type:responce:
    """
    update_query = ''
    if query_type == 'post':
        for i in range(len(responce['results'])):
            external_id = "'" + responce['results'][i]['external_id'] + "'"
            gis_id = "'" + responce['results'][i]['id'] + "'"
            update_query = '''update student_status set "gisscos_id" = {gis_id}, "status" = 'equal'
                              where "external_id" = {external_id}'''.format(gis_id=gis_id, external_id=external_id)

    if query_type == 'compare' or query_type == 'put':
       update_query = '''update student_status set "status" = '{}'
                         where "external_id" = {} '''.format(status, external_id)
    cursor.execute(update_query)
    connection.commit()

# получить ид
def id_list(object, status=None, limit=None):
    """
    Получить список ид для выбранного типа объектов с определенным статусом
    Возвращает:
    список идентификаторов ID из таблицы **object = object**, для которых **status = status**
	"""
    if limit == None:
        limit = 500
    if object == 'RootRegistryElement':
        table = 'discipline'
    elif object == 'EducationLevelHighschool':
        table = 'eduprogram'
    else:
        return 'такой таблицы нет'

    if status is None:                              # не указан статус - возвращаем записи со всеми статусами и без них
        select_query = '''select "external_id" from {table} limit({limit})'''.format(table=table, limit=limit)
    elif status == '':                              # указан пустой статус - возвращаем записи без статуса
        select_query = '''select "external_id" from {table} where "status" is null or "status" = '' '''.format(
            table=table)
    else:
        select_query = '''select "external_id" from {table} where "status" = '{status}' limit({limit})'''.format(
            table=table, status=status, limit=limit)

    cursor.execute(select_query)
    select = cursor.fetchall()
    list_of_id = list()
    for row in select:
        list_of_id.append(row[0])
    id_param = "'" + "','".join(list_of_id) + "'"
    if len(id_param) > 2:
        return id_param  # тип параметра - строка
    else:
        return 'записи не найдены'


def make_dict(object):
    # получить инфу по ид из бд в словарь
    id = id_list(object=object, status='new', limit=10)
    if id == 'записи не найдены' or id == 'такой таблицы нет':
        return 'получить словарь невозможно: {}'.format(id)
    else:
        if object == 'RootRegistryElement':
            table = 'discipline'
            atrib = atributes_discipline
        elif object == 'EducationLevelHighschool':
            table = 'eduprogram'
            atrib = atributes_eduprogram
        else:
            return "пустая выборка"
        atribut = ', '.join(atrib)
        select_query = '''select {atrib} from {table} where "external_id" in ({id})'''\
            .format(atrib=atribut,table=table,id=id)            # я могу пытаться в запрос вместо списка ид
        cursor.execute(select_query)                            # засунуть другие ретерны 'записи не найдены' или
        result = cursor.fetchall()                              # 'такой таблицы нет'
        info_list = list()
        for res in result:
            one_subject_info = list()
            for r in res:
                one_subject_info.append(r)
            dictant = dict(zip(atrib, one_subject_info))
            info_list.append(dictant)
        finall_dict = {'organization_id': '2a0fc6e23c7744478ab6114add556f3e',
                       table: info_list}
        return finall_dict

def save(object, file):
    # сохранить итоговый словарь в файл
    body = make_dict(object=object)
    with open(file, 'w', encoding='utf-8') as fp:  # открываем файл для записи
        json.dump(body, fp, ensure_ascii=False)


try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="donsitest", password="TS4d#dkpf3WE1",
								  host="192.168.25.103", port="5432", database="doubnsitest")
    cursor = connection.cursor()
    #print(make_dict(object='EducationLevelHighschool'))
    # print(dictant(object='RootRegistryElement'))
    # print(make_dict(object='RootRegistryElement'))
    save(object='RootRegistryElement', file='forGIS.json')
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")


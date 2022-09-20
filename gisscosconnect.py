import psycopg2
from psycopg2 import Error

atributes_discipline = ['"ID"', '"RootRegistryElementName"']
atributes_eduprogram = ['external_id', 'title', 'start_year',
                        'end_year']
def change_status(query_type, responce=None, external_id=None, status=None):
    """
    Метод изменяет статус студента в заввисимости от действия (обновление, добавление... (удаление?))
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
def id_list(object, status=None):
    """
    Получить список идентификаторов для объектов с определенным статусом
    Возвращает:
    список идентификаторов ID из таблицы **object = object**, для которых **status = new**
	"""
    if object == 'RootRegistryElement':
        table = 'discipline'
    elif object == 'EducationLevelHighschool':
        table = 'eduprogram'
    else:
        return 'такой таблицы нет'
    if status is None:
        select_query = '''select "external_id" from {table}'''.format(table=table)
    elif status == '':
        select_query = '''select "external_id" from {table} where "status" is null '''.format(
            table=table)
    else:
        select_query = '''select "external_id" from {table} where "status" = '{status}' '''.format(
            table=table, status=status)

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


# плучить инфу по ид из бд в словарь
def make_dict(object):
    id = id_list(object=object, status='new')
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
        .format(atrib=atribut,table=table,id=id)
    cursor.execute(select_query)
    result = cursor.fetchall()
    info_list = list()
    for res in result:
        one_subject_info = list()
        for r in res:
            one_subject_info.append(r)
        dictant = dict(zip(atrib, one_subject_info))
        info_list.append(dictant)
    finall_dict = {'organization_id': '2a0fc6e23c7744478ab6114add556f3e',
                   'educational_programs': info_list}
    return finall_dict


# сохранить список словарей в словарь

# сохранить итоговый словарь в файл

# отправить файл в гис



def save(object, file):
    pass

try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="myprojectuser", password="password",
								  host="localhost", port="5432", database="myproject")
    cursor = connection.cursor()
    # print(id_list(object='EducatioLevelHighschool', status='new'))
    # print(dictant(object='RootRegistryElement'))
    print(make_dict(object='RootRegistryElement'))
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")


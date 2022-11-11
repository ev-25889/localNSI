"""Модуль взаимодействия с ГИС СЦОС. Загрузка, обновление, ?удаление? объектов."""
import psycopg2
import requests
import json
from psycopg2 import Error

atributes = {
             'lkssessionresult': ['discipline', 'study_plan', 'student','mark_type', 'mark_value', 'semester',
                                  'teacher', 'date', 'external_id']
             }

object_names = {
                'LksSessionResult': ['marks', 'lkssessionresult', 'marks']
                }

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
        #external_id = "'" + responce['results'][number]['external_id'] + "'"
        #result = "'" + responce['results'][number]['additional_info'] + "'"
        if number is None:
            update_query = '''update {table} set "status" = 'error',
                                            "responce" = '{result}',
                                            "date_sync" = NOW()
                         where "ID" = '{external_id}' '''.\
                format(table=table, result=responce, external_id=external_id)
        else:
            external_id = "'" + responce['results'][number]['external_id'] + "'"
            print(external_id)
            result = "'" + responce['results'][number]['additional_info'] + "'"
            print(result)
            update_query = '''update {table} set "status" = 'error',
                                                        "responce" = {result},
                                                        "date_sync" = NOW()
                                     where "ID" = {external_id} '''. \
                format(table=table, result=result, external_id=external_id)
        # print('externaal_id: ', external_id, ', gis_id: ', gis_id, ', result: ', result)
        print(update_query)
        cursor.execute(update_query)
        connection.commit()

# получить ид
def id_list(object, status=None, limit=None, type_id=None):
    """
    Получить список ид для выбранного типа объектов с определенным статусом
    Возвращает:
    список идентификаторов ID из таблицы **object = object**, для которых **status = status**
	"""


    select_query = '''select l."ID" from lkssessionresult l 
                          where l.status = '{status}' limit({limit}) '''.format(status=status,limit=limit)


    cursor.execute(select_query)
    select = cursor.fetchall()
    list_of_id = list()
    count = 0
    for row in select:
        count += 1
        list_of_id.append(str(row[0]))
    id_param = "'" + "','".join(list_of_id) + "'"
    print(count)
    if len(id_param) > 2:
        return id_param  # тип параметра - строка
    else:
        return 'записи не найдены'

def get_study_plan(mark_id):
    sql1 = """SELECT entityid_p FROM lkssessionresult_t AS sr
        WHERE sr.guid_p = '{}'""".format(mark_id)
    curTan.execute(sql1)
    row = curTan.fetchone()
    #print('row: ', row[0])
    sql2 = """select wpb.guid_p from epp_workplan_base_t as wpb
                inner join 
                epp_wprow_t as wpr on wpb.id=wpr.workplan_id inner join 
                epp_student_wpe_t as wpe on wpr.id=wpe.sourcerow_id 
                where wpe.id='{}';""".format(row[0])
    curTan.execute(sql2)
    row = curTan.fetchone()
    #print('row[0]: ', row[0])
    if row is None:
        return ''
    else:
        return row[0]


def get_mark_type(cred, exam):
    mark_type = None
    if not cred is None:
        mark_type = "CREDIT"
    if not exam is None:
        mark_type = "MARK"

    return mark_type


def get_mark_value(cred, exam):
    if cred == "Зачтено":
        mark_value = 1
    else:
        mark_value = 0

    if exam == "Удовлетворительно":
        mark_value = 3
    elif exam == "Хорошо":
        mark_value = 4
    elif exam == "Отлично":
        mark_value = 5
    elif exam == "не удовлетворительно":
        mark_value = 2
    else:
        mark_value = 1

    return mark_value

def make_dict(object, count):
    # получить инфу по ид из бд в словарь
    print(id_list(object=object, status='new', limit=count))
    id = id_list(object=object, status='new', limit=count)
    print(id)
    if id == 'записи не найдены' or id == 'такой таблицы нет':
        return 'получить словарь невозможно: {}'.format(id)
    else:
        atribut = ', '.join(atributes[object_names[object][1]])

        table = object_names[object][1]

        select_query = '''select ds."discipline" as discipline, 
                                 l."StudentID" as student, 
                                 l."LksSessionResultCredit" as credit, 
                                 l."LksSessionResultExam" as exam, 
                                 l."LksSessionResultSemesterNumber" as semester, 
                                 l."LksSessionResultCommission" as teacher,
                                 l."LksSessionResultDate" as date, 
                                 l."ID" as external_id 
                          from lkssessionresult l 
                          join disciplines_subjects ds on l."LksSubjectID" = ds."subject" 
                          where "ID" in ({id}) '''\
                          .format(atrib=atribut,table=table,id=id)            # я могу пытаться в запрос вместо списка ид
        cursor.execute(select_query)                            # засунуть другие ретерны 'записи не найдены' или
        result = cursor.fetchall()                              # 'такой таблицы нет'
        print(result)
        info_list = list()
        for res in result:
            mark_type = get_mark_type(cred=res[2], exam=res[3])
            mark_value = get_mark_value(cred=res[2], exam=res[3])
            external_id = res[7]
            study_plan = get_study_plan(mark_id=external_id)
            print(mark_type)
            if mark_type is not None:
                one_subject_info = list()
                one_subject_info.append(res[0]) # discipline
                one_subject_info.append(study_plan) # study_plan
                one_subject_info.append(res[1])
                one_subject_info.append(mark_type)
                one_subject_info.append(mark_value)
                one_subject_info.append(res[4])
                one_subject_info.append(res[5])
                one_subject_info.append(res[6])
                one_subject_info.append(external_id)
            else:
                print(change_status(query_type='error', table='lkssessionresult', responce='Нет оценки', external_id=external_id, status='error'))
            dictant = dict(zip(atributes[object_names[object][1]], one_subject_info))
            info_list.append(dictant)
        #print(info_list)
        finall_dict = {'organization_id': 'f07f886b-d609-4a23-8ada-53e1717c1b9c',
                       object_names[object][2]: info_list}
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
    responce = requests.post(url='https://test.online.edu.ru/vam/api/v2/{}'.format(object_names[object][0]),
                             headers={"X-CN-UUID": "e52e03e0-7038-4090-a9f6-628367c0094d"}, json=data, verify=False)
    try:
        responce = responce.json()
        print(responce)
        for i in range(len(responce['results'])):
            try:
                change_status(query_type='post', responce=responce, table=object_names[object], number=i)
                "Статус изменен успешно"
            except Exception:
                change_status(query_type='error', responce=responce, table=object_names[object][1], number=i)
                "Статус изменен с ошибкой "
    except Exception:
        print('(((')


try:
    # Подключение к базе данных
    connection = psycopg2.connect(user="donsitest", password="TS4d#dkpf3WE1",
                                  host="192.168.25.103", port="5432", database="doubnsitest")
    connection2 = psycopg2.connect(user="nsn001",password="esz7jba1cm",
                                  host="192.168.25.101",port="5432", database="tdmdb")
    cursor = connection.cursor()
    curTan = connection2.cursor()

    #print(send_to_gis(object='StudentOrderExtract'))
    print(send_to_gis(object='LksSessionResult', count=1))
    #print(save(object='LksSessionResult',file='forGIS.json', count=10))
    # print(id_list(object='LksSessionResult', status='new', limit=10))
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection or connection2:
        cursor.close()
        connection.close()
        print("Соединение с базой TandemDB закрыто")
        curTan.close()
        connection2.close()
        print("Соединение с базой DoublerNSItest закрыто")


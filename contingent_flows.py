import psycopg2
from psycopg2 import Error

def izm_param(fac_old,fac_new,fo_old,fo_new,fin_old,fin_new,flow):
    ''''
       метод анализует была ли смена института, формы обучения или формы финансирования. Используется для отсечения приказов с типом перевод, при которых не было смены института, формы оучения и финансирования
    Параметры:
        fac_old: институт предыдущего приказа
        fac_new: институт текущего приказа
        fo_old: форма обучения предыдущего приказа
        fo_new: форма обучения текущего приказа
        fin_old: форма финансирования предыдущего приказа
        fin_new: форма финансирования текущего приказа

    Возвращаемое значение:
        izm: есть/нет изменение (True/False)
    '''
    if fac_old != fac_new or fo_old != fo_new or fin_old != fin_new:
        izm = True
    elif str.lower(flow).find('перевод') != -1 and str.lower(flow).find('исключение') != -1:
        izm = True
    else:
        izm = False
    return izm
def fac_krat(str_fac):
    ''''
        метод получает краткое название института
    Параметры:
        str_fac: название института из приказа (str)
    Возвращаемое значение:
        fac: краткое название института (str)
    '''
    if str.lower(str_fac).find('прокуратуры') != -1:
        fac = 'ИП'
    elif str.lower(str_fac).find('предприним') != -1:
        fac = 'ИПиП'
    elif str.lower(str_fac).find('юстиции') != -1:
        fac = 'ИЮ'
    elif str.lower(str_fac).find('специал') != -1:
        fac = 'ИСОП'
    elif str.lower(str_fac).find('международ') != -1:
        fac = 'ИГиМП'
    else:
        fac = None

    return fac
def get_fin(str_fin):
    ''''
    !!! метод не доделан для бюджетников
    метод преобразует источник фонансирования тандем к виду финансировая справочника
    Параметры:
        str_fin: источник финансирования из тандем
    Возвращаемое значение:
        fin_form: требуенмое для словаря значания источника финансирования (str)

    '''
    if str.lower(str_fin).find('договор') != -1 or str.lower(str_fin).find('сверхплан') != -1:
        fin_form = 'Полное возмещение затрат'
    else:
        fin_form = str_fin
        if str_fin == "В рамках квоты лиц, имеющих особые права":
            fin_form = "Госбюджетное место"
        if str_fin == "Целевой прием":
            fin_form = "Госбюджетное место"
        if str_fin == "Общий конкурс":
            fin_form = "Госбюджетное место"

    return fin_form
def trans_develop_form(s):
    if s=='Очная':
        return 'FULL_TIME'
    elif s=='Очно-заочная':
        return 'PART_TIME'
    elif s=='Заочная':
        return 'EXTRAMURAL'
    elif s=='Экстернат':
        return 'EXTERNAL'
    else:
        raise Exception(s)
def get_flow_type(flow):
    ''''
    метод получает тип события для справочника
    Параметры:
        flow: тип события из  тандем ( метод get_flow_name)
    Возвращаемое значение:
        flow_t: требуенмое для словаря значания источника финансирования (str)

    '''
    if flow != None:
        if str.lower(flow).find('зачисл') != -1:
            flow_t='ENROLLMENT'
        elif str.lower(flow).find('отчисл') != -1:
            flow_t = 'DEDUCTION'
        elif str.lower(flow).find('перевод') != -1:
            flow_t = 'TRANSFER'
        elif str.lower(flow).find('восстанов') != -1:
            flow_t = 'REINSTATEMENT'
        elif str.lower(flow).find('академ') != -1:
            flow_t = 'SABBATICAL_TAKING'
        else:
            flow_t = ''
    else:
        flow_t = ''

    return flow_t
def get_flow_name(code_pr):
    ''''
    метод получает тип события тандем по коду выписки. Если требуемая выписка не выходит у студента, ее код нужно добавить в нужный справочник метода.
    Параметры:
        code_pr: код выписки из  тандем
    Возвращаемое значение:
        flow_name: тип события (str)

    '''
    vid_pr_zach = ('1', '1.11', '1.15', '1.14', '516', '521', '525', '2.49', '2.49.1')
    vid_pr_otch = ('1.26','1.27','1.62','1.63','1.64','1.7','1.85','234','2.4','2.4.1','2.8','2.9','502','503','504','505','509','510','519','unidip-2')
    vid_pr_per = ('1.18','1.3','1.5','1.6','1.65','1.67','1.68','1.69','1.70','1.72','1.76','1.79','1.92','2.39','2.41','2.43','2.47','514','515','520','522')
    vid_pr_vost = ('1.10','1.17','1.61','1.73','1.83','500','523','1.9','512','517')
    vid_pr_akadem = ('1.16','1.2','518','1.37')

    if code_pr == '1':
        flow_name = 'Зачисление в вуз'
    elif code_pr in vid_pr_zach:
        flow_name = 'Зачисление'
    elif code_pr in vid_pr_otch:
        flow_name = 'Отчисление'
    elif code_pr in vid_pr_per:
        flow_name = 'Перевод'
    elif code_pr in vid_pr_vost:
        flow_name = 'Восстановление'
    elif code_pr in vid_pr_akadem:
        flow_name = 'Выход в академ.отпуск'
    else:
        flow_name = None
    return flow_name


def get_flow(guid_st, guid_order):
    """"
    метод получает движение контингента по студенту
    Параметры:
        guid_st: guid студента из таблицы personrole_t
    Возвращаемое значение:
        dan_prikaz_st: данные по приказам (словарь)

    """

    faculty_old = None
    fo_old = None
    fin_old = None
    str_dict = ''
    dan_flow =dict()
    dan_prikaz_st=[]

    # Зачисление ПК

    cur1 = connection.cursor()
    sql1 = """SELECT r.guid_p,pt.title_p,p.number_p,p.commitDate_p,i.shorttitle_p,df.title_p,fin.title_p,pt.code_p,i2.shorttitle_p 
                 FROM enr14_order_extract_abs_t  a 
                    inner join  enr14_order_enr_extract_t s on a.id=s.id 
                    inner join personrole_t r on s.student_id=r.id 
                    inner join  enr14_order_par_abs_t ap on a.paragraph_id=ap.id 
                    inner join   enr14_order_enr_par_t ep on a.paragraph_id=ep.id  
                    inner join developform_t df on ep.developForm_id=df.id 
                    inner join  enr14_c_comp_type_t fin on ep.competitionType_id=fin.id 
                    inner join enr14_order_abs_t p  on   ap.order_id=p.id 
                    inner join  enr14_order_t pp on ap.order_id = pp.id  
                    inner join enr14_c_order_type_t pt on pp.type_id=pt.id 
                    inner join orgunit_t i on ep.formativeOrgUnit_id=i.id 
                    left join enr14_requested_comp_t k on s.entity_id=k.id  
                    left join  enr_requested_program_t vk on k.id=vk.requestedcompetition_id  
                    left join enr14_program_set_item_t ps on vk.programSetItem_id=ps.id 
                    left  join educationorgunit_t edu on ps.educationOrgUnit_id=edu.id 
                    left join orgunit_t i2 on  edu.formativeOrgUnit_id=i2.id
                 where p.state_id =1594713222843617009 
                 and k.state_id=1594713222153652315 
                 and r.guid_p like '""" + str(guid_st) + """'"""

    cur1.execute(sql1)
    recordP = cur1.fetchone()
    if recordP != None:


        student = recordP[0]
        flow_n = get_flow_name(recordP[7])
        continget_flow = flow_n
        flow_type = get_flow_type(flow_n)
        date = recordP[3]
        if recordP[8] != None:
            faculty = recordP[8]
        else:
            faculty = recordP[4]
        education_form = trans_develop_form(recordP[5])
        fin_form = get_fin(recordP[6])
        details =  'Приказ №' + recordP[2] + ' от ' + recordP[3].strftime('%d-%m-%Y')

        dan_flow['student'] = recordP[0]
        dan_flow['continget_flow'] = get_flow_name(recordP[7])
        dan_flow['flow_type']  = get_flow_type(recordP[1])
        dan_flow['date'] = recordP[3]
        dan_flow['faculty'] = recordP[4]
        dan_flow['education_form']  = trans_develop_form(recordP[5])
        dan_flow['fin_form'] = get_fin(recordP[6])
        dan_flow['details']  = 'Приказ №' + recordP[2] + ' от ' + recordP[3].strftime('%d-%m-%Y')

        #str_dict='''{'student':\'''' + student + '''\','continget_flow':\'''' + continget_flow + '''\','flow_type':\'''' + flow_type + '''\','date': datetime.date(''' + str(date.year)+',' +  str(date.month) +','+ str(date.day)+'''),'faculty':\'''' + faculty + '''\','education_form':\'''' + education_form + '''\','fin_form':\'''' + fin_form + '''\','details':\'''' + details + '''\'}'''

        dan_prikaz_st.append(dan_flow.copy())
        faculty_old = dan_flow['faculty']
        fo_old = dan_flow['education_form']
        fin_old = dan_flow['fin_form']



    # приказы по студенту
    cur2 = connection.cursor()
    sql2 = """SELECT r.guid_p,pt.title_p,p.number_p,p.commitDate_p,i.shorttitle_p,s.developFormStr_p,
                     s.compensationTypeStr_p,pt.code_p,fo2.title_p,fin2.title_p,s.formativeOrgUnitStr_p,pt.title_p 
              FROM abstractstudentextract_t s 
                inner join personrole_t r on s.entity_id=r.id 
                inner join  abstractstudentparagraph_t  ap on s.paragraph_id = ap.id 
                inner join abstractstudentorder_t p on    ap.order_id=p.id 
                inner join  baseextracttype_t pt on s.type_id = pt.id 
                left join orgunit_t i on p.orgunit_id= i.id  
                left join commonStuExtract_t dop on  s.id=dop.id 
                left join educationorgunit_t op2 on  dop.educationOrgUnitNew_id=op2.id 
                left join developform_t fo2 on op2.developform_id=fo2.id 
                left join  compensationtype_t fin2 on dop.compensationTypeNew_id=fin2.id
              where p.state_id =1594713222843617009 
              and s.guid_p like '""" + str(guid_order) + """' 
              and r.guid_p like '""" + str(guid_st) + """'  order by p.commitDate_p """

    cur2.execute(sql2)
    recordP2 = cur2.fetchall()
    if recordP2 != None:

        for row in recordP2:
            dan_flow['student'] = row[0]
            flow_n = get_flow_name(row[7])
            dan_flow['continget_flow'] = row[11]
            dan_flow['flow_type'] = get_flow_type(flow_n)
            dan_flow['date'] = row[3]
            if row[4] != None:
                dan_flow['faculty'] = row[4]
            else:
                dan_flow['faculty'] = fac_krat(row[10])
            if row[8] != None:
                dan_flow['education_form'] = trans_develop_form(row[8])
            else:
                dan_flow['education_form']= trans_develop_form(row[5])
            if row[9] != None:
                dan_flow['fin_form']  = get_fin(row[9])
            else:
                dan_flow['fin_form']  = get_fin(row[6])
            dan_flow['details']  = 'Приказ №' + row[2] + ' от ' + row[3].strftime('%d-%m-%Y')


            if flow_n != None and (flow_n != 'Перевод' or (flow_n== 'Перевод' and izm_param(faculty_old, dan_flow['faculty'], fo_old,dan_flow['education_form'], fin_old,dan_flow['fin_form'],dan_flow['continget_flow']) == True)):

                dan_prikaz_st.append(dan_flow.copy())


            faculty_old = dan_flow['faculty']
            fo_old = dan_flow['education_form']
            fin_old = dan_flow['fin_form']
    return dan_flow

# Этот метод уже не нужен, потому что в таблице contingent_flows_status только нужные типы приказов.
def choose_flows_for_gis(guid_st):
    flow = get_flow(guid_st=guid_st)
    if flow['flow_type'] in ('ENROLLMENT', 'DEDUCTION', 'TRANSFER', 'REINSTATEMENT','SABBATICAL_TAKING'):
        print('flows')
    else:
        print('not flows for gis')

def get_from_testdb():
    cur3 = connection2.cursor()
    sql3 = """select * from student_status where external_id = '45ab0956-0df8-426e-9eed-8b44fee3e591'"""

    cur3.execute(sql3)
    recordP = cur3.fetchone()
    print(recordP)


#  функция возвращает ид студента из таблицы contingent_flows_status со статусом new
def get_id(field):
    select_query = """select {field} from contingent_flows_status where status = 'new' limit(1)""".format(field=field)
    cursor2.execute(select_query)
    selection = cursor2.fetchone()
    if type(selection[0]) == str:
        id_param = selection[0]
        # id_param = "'" + str(selection[0]) + "'"
        return id_param  # тип параметра - строка
    else:
        return print("Нет таких {field} в таблице.".format(field=field))

def fill_table():
    guid_st = get_id(field='student')
    guid_order = get_id(field='external_id')
    info = get_flow(guid_st=guid_st, guid_order=guid_order)
    print(info)
    print(info['date'], type(info['date']))
    update_query = """update contingent_flows_status set contingent_flow = '{contingent_flow}', 
                                                         flow_type = '{flow_type}', 
                                                         date = '{date}', 
                                                         faculty = '{faculty}',
                                                         education_form = '{education_form}', 
                                                         form_fin = '{fin_form}', 
                                                         details = '{details}', 
                                                         status = 'readyToSend', 
                                                         date_sync = NOW(), 
                                                         response = 'come from LDNSI'
                      where external_id = '{guid_order}'""".format\
        (contingent_flow=info['continget_flow'], flow_type=info['flow_type'], date=str(info['date']), faculty=info['faculty'],
         education_form=info['education_form'], fin_form=info['fin_form'], details=info['details'], guid_order=guid_order)

    cursor2.execute(update_query)
    connection2.commit()

try:
  # Подключение к базе данных
  connection = psycopg2.connect(user="nsn001",
                                password="esz7jba1cm",
                                host="192.168.25.101",
                                port="5432",
                                database="tdmdb")

  connection2 = psycopg2.connect(user="donsitest",
                                password="TS4d#dkpf3WE1",
                                host="192.168.25.103",
                                port="5432",
                                database="doubnsitest")
  cursor = connection.cursor()
  cursor2 = connection2.cursor()
  # print(get_flow(get_id(field='student'), get_id(field='external_id')))
  print(fill_table())

except (Exception, Error) as error:
  print("Ошибка при работе с PostgreSQL", error)
finally:
  if connection or connection2:
     cursor.close()
     connection.close()
     print("Соединение с базой TandemDB закрыто")
     cursor2.close()
     connection2.close()
     print("Соединение с базой DoublerNSItest закрыто")

""" Модуль для взаимодействия с базой данных УНИ напрямую

Еще здесь есть функция для чтени из indigo
надо будет убрать.
"""
from collections import OrderedDict
import psycopg2
import logging

class TandemDB():
  def __init__(self,conf):
    super().__init__()
    self.con = psycopg2.connect(
                database=conf['database'], 
                user=conf['user'], 
                password=conf['password'], 
                host=conf['host'], 
                port=conf['port']
                )

  def __del__(self):
    self.con.close()
    
  def entityid_to_guid(self,s):
    d=dict()
    cur = self.con.cursor()  
    for i in s:
      sql = "SELECT guid_p FROM nsientity_t where entityid_p='%s'" % (
        str(i)
        )
      cur.execute(sql)
      rows = cur.fetchall()
      if len(rows)==1:
        d.update({i:str(rows[0][0])})
      else:
        d.update({i: None})
    return d

  def guid_to_entityid(self,s):
    d=dict()
    cur = self.con.cursor()  
    for i in s:
      sql = "SELECT id FROM personrole_t where guid_p ='%s'" % (
        str(i)
        )
      cur.execute(sql)
      rows = cur.fetchall()
      if len(rows)==1:
        d.update({i:str(rows[0][0])})
      else:
        d.update({i: None})
    return d
  
  def get_dip_obj(self,entityid):
    d = list()
    cur = self.con.cursor()  
    sql = "SELECT content_id FROM dip_object_t where student_id=%s"%(entityid)
    cur.execute(sql)
    rows = cur.fetchall()
    for i in rows:
      d.append(i)
    return d

  def get_edu_program(self,psid):
    d = list()
    cur = self.con.cursor()  
    sql = "SELECT * FROM edu_c_pr_subject__2013_t where id=%s"%(psid)
    cur.execute(sql)
    rows = cur.fetchall()
    for i in rows:
      d.append(i)
    return d

  def get_edu_special(self,ids):
    d = list()
    cur = self.con.cursor()  
    sql = "SELECT * FROM edu_specialization_base_t where id=%s"%(ids)
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][5]

  def get_edu_c_pr_subject(self,ids):
    d = list()
    cur = self.con.cursor()  
    sql = "SELECT * FROM edu_c_pr_subject_t where id=%s"%(ids)
    cur.execute(sql)
    rows = cur.fetchall()
    return rows[0][7]

  def get_dip_content(self,dcid):
    d = OrderedDict()
    cur = self.con.cursor()  
    sql = """SELECT * FROM dip_content_t where 
        (type_id=1607626543224737569 or 
        type_id=1607626543214251809 or 
        type_id=1607626543217397537 or 
        type_id=1607626543223688993 or
        type_id=1607626543221591841
         )and id=%s"""%(dcid)
    cur.execute(sql)
    rows = cur.fetchall()
    if len(rows)==0:
      return None
    edu_special_id = rows[0][8]
    # красный диплом
    d['redDiplom'] = rows[0][11]
    # Направленность образовательной программы
    if edu_special_id is None:
      d['EduSpecializationProfile'] = None
    else:
      d['EduSpecializationProfile'] = self.get_edu_special(edu_special_id)
    d['DiplomID'] = rows[0][0]
    subid = rows[0][7]
    # Направление подготовки
    d['EduProgramName'] = self.get_edu_c_pr_subject(subid)
    if rows[0][5] is None:
      d['loadAll'] = 0
    else:  
      d['loadAll'] = int(rows[0][5])/100
    if rows[0][6] is None:
      d['audloadAll'] = 0
    else:
      d['audloadAll'] = int(rows[0][6])/100
    d['DiplomContentRows'] = self.get_discipl(dcid,rows[0][2])

    return d

  def get_discipl(self,dcid,dip_type):
    d = list()
    cur = self.con.cursor()  
    sql = "SELECT * FROM dip_row_t where owner_id=%s order by number_p"%(dcid)
    # print(dcid)
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
      item = OrderedDict()
      item['discipName'] = row[3]
      k = 0
      item['RowType'] = row[1]
      if row[1] == 520:
        item['RowType'] = 'disciple'
        k = 1
        if row[13] is None:
          continue
      if row[1] == 2521:
        item['RowType'] = 'facultydiscip'
        k = 6
        if row[13] is None:
          continue
      
      if row[1] == 3038:
        item['RowType'] = 'praktiks'
        k = 2
#        if row[13] is None:
#          continue

      if row[1] == 3566:
        item['RowType'] = 'gosekzamen'
        k = 3

      if row[1] == 3708:
        item['RowType'] = 'VKR'
        k = 4
        if not row[16] is None:
          item['VKRTheme'] = str(row[16])
        else:
          item['VKRTheme'] = ''


      if row[1] == 2604:
        item['RowType'] = 'kursovay'
        k = 5


      item['loadedinc'] = 'з.е.'
      #12
      if dip_type==1607626543217397537:
        if row[9] is None: 
          item['load'] = 0
        else:
          item['load'] = int(row[9]/100)
      else:
        if row[12] is None: 
          item['load'] = 0
        else:
          item['load'] = int(row[12]/100)
      
      item['audloadedinc'] = 'час'
      if row[10] is None:
        item['audload'] = 0
      else:
        item['audload'] = int(int(row[10])/100)
      item['mark'] = row[13]
      
      item['number'] = k*100+row[6]
      d.append(item)
    d = sorted(
      d,
      key=lambda a: a['number']
      )
    return d


  def get_diplom(self,sid):
    res = list()
    w = self.guid_to_entityid([sid])
    if w[sid] is None:
      return res

    entityid = w[sid]
    dip = self.get_dip_obj(entityid)

    for dip_content_id in dip:
      w = self.get_dip_content(dip_content_id[0])
      if not w is None:
        res.append(w)
    return res

  def get_group_guid(self,s):
    d=dict()
    cur = self.con.cursor()  
    cur.execute("SELECT guid_p FROM group_t where id='"+str(s)+"'")
    rows = cur.fetchall()
    if len(rows)==1:
      return rows[0][0]
    else:
      return None

  def get_group_id_by_guid(self,s):
    d=dict()
    cur = self.con.cursor()  
    cur.execute("SELECT id FROM group_t where guid_p='"+str(s)+"'")
    rows = cur.fetchall()
    if len(rows)==1:
      return rows[0][0]
    else:
      return None


def entityid_to_guid(s):
  d=dict()
  con = psycopg2.connect(
  database="",
  user="",
  password="",
  host="",
  port=""
  )

  cur = con.cursor()  
  #SELECT id,discriminator,guid_p,entityid_p,entitytype_p,type_p FROM nsientity_t where entityid_p='1672348176574991991'
  for i in s:
    cur.execute("SELECT guid_p FROM nsientity_t where entityid_p='"+str(i)+"'")
    rows = cur.fetchall()
    
    if len(rows)==1:
      d.update({i:str(rows[0][0])})
    else:
      d.update({i: None})
  con.close()  
  return d
  
def get_group_guid(s):
  d=dict()
  con = psycopg2.connect(
  database="tdmdb", 
  user="nsn001", 
  password="esz7jba1cm", 
  host="192.168.25.101", 
  port="5432"
  )

  cur = con.cursor()  
  #SELECT id,discriminator,guid_p,entityid_p,entitytype_p,type_p FROM nsientity_t where entityid_p='1672348176574991991'
  cur.execute("SELECT guid_p FROM group_t where id='"+str(s)+"'")
  rows = cur.fetchall()
  
  if len(rows)==1:
    return rows[0][0]
  else:
    return None
  

def sql():
  d=dict()
  con = psycopg2.connect(
  database="tdmdb", 
  user="nsn001", 
  password="esz7jba1cm", 
  host="192.168.25.101", 
  port="5432"
  )

  cur = con.cursor()  
  #SELECT id,discriminator,guid_p,entityid_p,entitytype_p,type_p FROM nsientity_t where entityid_p='1672348176574991991'
  cur.execute("SELECT id,discriminator,guid_p,entityid_p,entitytype_p,type_p FROM nsientity_t where entityid_p='1674614297058494488'")
  #cur.execute("SELECT * FROM pg_catalog.pg_tables")
  rows = cur.fetchall()
  
  for row in rows:
    print(row)
  con.close()  
  return d


def indigo_result():
  con = psycopg2.connect(
    database="indigodb", 
    user="bot54700", 
    password="7344fce3d8e832f190411a877da83e93", 
    host="192.168.25.86", 
    port="5436"
  )
  cur = con.cursor()  
  cur.execute("SELECT * FROM res.results_view where ph_test_id='1089' and user_group='гр.ИЮ112Б'")
  
  rows = cur.fetchall()
  
  for row in rows:
    print(row)
  print(len(row))
  con.close()  
  return 1

if __name__=='__main__':
  #sql()
  conf = {
    'database':"test", 
    'user':"tandemdb", 
    'password':"FEfs$*dcn2slts", 
    'host':"192.168.25.103", 
    'port':"5432"
  }
  #obj = TandemDB(conf)
  #obj.show_tables()
  #print(obj.guid_to_entityid(['c81f7746-8d76-40a0-9ddf-c66f595270e4']))
  #print(obj.get_diplom('c81f7746-8d76-40a0-9ddf-c66f595270e4'))

import requests
def get_info_from_gis(object=None, id=None):
  """
  Метод получения данных из теста ГИС СЦОС
  """

  data = requests.get(url='https://test.online.edu.ru/vam/api/v2/{}'.format(object),
                 headers={"X-CN-UUID": "e52e03e0-7038-4090-a9f6-628367c0094d"}, verify=False)
  print(data)
  print(data.json())
  '''
  if "404" in str(data):
      print("Данные в ГИС СЦОС не найдены.")
      return False
  elif "400" in str(data):
      print("Ошибка в запросе, Bad Request")
      return False
  elif "200" in str(data):
      print(data.json()['results'][0].keys())
      li_ex = list()
      li_name = list()
      for i in range(len(data.json()['results'])):
        # print(data.json()['results'][i]['education_form'])
        li_ex.append(data.json()['results'][i]['external_id'])
        li_name.append(data.json()['results'][i]['title'])
      # print(object, data.json()['results'][1]['title'], data.json()['results'][1]['start_year'], data.json()['results'][1]['end_year'])
      print(li_ex)
      print(li_name)
      print(data.json()['results'][1])
      return True
  else:
      print("Unknown error")
    '''

# get_info_from_gis(object='students', id='6e5b38e9-5b49-4702-ba79-889574bba778')
get_info_from_gis(object='educational_programs')
# get_info_from_gis(object='study_plans')
# get_info_from_gis(object='disciplines')
# get_info_from_gis()

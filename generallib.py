""" Модуль с функциями и классами общего назначения, которые
используется в многих модулях для работы с НСИ.
Модуль свалка.
"""

import os
import pip
import json
import shutil

import zipfile
from datetime import datetime
import logging

def save_data_to_local_file(nsi_table,data):
    """Метод записывает данные справочника в json файл

    :param nsi_table: Имя файла для записи
    :type nsi_table: str
    :param data: словарь содержащий справочник
    :type data: dict

    """

    work_dir = 'data/'
    with open(work_dir+nsi_table+'.json',encoding='utf-8',mode='w') as f: 
        json.dump(data, f,ensure_ascii=False, sort_keys=True, indent=2)
    f.close()


def load_data_from_local_file(nsi_table):
    """Метод читает справочник из файла json

    :param nsi_table: Имя файла справочника
    :type nsi_table: str
    :return: словарь содержащий справочник nsi_table
    :rtype: dict

    """
    
    work_dir = 'data/'
    with open(work_dir+nsi_table+'.json','rb') as f:
        templates = json.load(f) #,encoding='utf-8')
    f.close()
    #TODO: Обработать ошибку открытия файла. Критическая ошибка
    # запись в лог, остановка программы
    return templates

def make_backup(filename):
    """Создает бекап файла, хранит 7 бекапов.
    """

    n=datetime.weekday(datetime.now())
    i=filename.rfind('.')
    newFilename=filename[0:i]+str(n)+filename[i:]

    if(os.path.isfile(newFilename)):
        d1=datetime.fromtimestamp(os.path.getmtime(newFilename))
        d2=datetime.today()
        delta=d2-d1
        if(delta.days>0):
            shutil.copy(filename,newFilename)
    else:
        shutil.copy(filename,newFilename)


def generate_logfile_name(fn,period=None):
    """Генерирует новое имя файла лога
    
    Если указать параметр period='day' имя будет иметь
    вид ukserver-date.log. Все записи в день будут писаться
    в один лог.
    В противном случае имя имеет структуру ukserver-date(n).log. 
    Проверяется подпапка logs рабочего каталога скрипта
    """

    ext='.log'
    now = datetime.now()
    #now.strftime("%Y-%m-%d")
    dir_script = os.path.dirname(__file__)+'/'
    # print(dir_script)
    fn = dir_script+'logs/'+fn
    name=fn+now.strftime("%Y-%m-%d")
    if period=='day':
        return name+ext

    if os.path.exists(name+ext):
        i=1
        while os.path.exists(name+'('+str(i)+')'+ext):
            i=i+1
        return name+'('+str(i)+')'+ext
    else:
        return name+ext

def str_compare_e(a,b):
    """Метод сравнивает две строки без учета ё и е
    
    :param a: строка для сравнения
    :type a: str
    :param b: строка для сравнения
    :type b: str
    :return: возвращает правду если строки равны(е и ё считаем тождественными), ложь если не равны
    :rtype: boolean

    """

    if a is None and b is None:
        return True
    if a is None and b=='':
        return True
    if a=='' and b is None:
        return True
    if a is None and not b is None:
        return False
    if not a is None and b is None:
        return False
    
        
    c=a.replace('ё','е')
    d=b.replace('ё','е')        
    if c==d:
        return True
    else:
        return False
    
def csvdateconvert(csvbdate):
    wdate = datetime.strptime(csvbdate,"%d.%m.%Y")
    return wdate.strftime("%Y-%m-%d")

def exract_zip_inplace(fn,path):
    """ Функция разархивирует только xml файлы из архива

    :param fn:Имя файла с архивом
    :type fn:str
    :param path: Папка куда разархивировать файлы
    :type path: str
    """

    # Поверяем, что файл с данным именем существует
    if os.path.isfile(fn):
        # Открываем архив на чтение
        zf = zipfile.ZipFile(fn,'r')
        # Читаем содержание архива
        for z in zf.infolist():
            if z.filename.endswith('.xml'):
                # Извлекаем нужные файлы из архива
                zf.extract(z,path=path)
        zf.close()
        # Удаляем файл архива
        os.remove(fn)

def get_xmlfile_from_dir(dn):
    """ Функция получения имени xml файла в папке входящих

    :param dn: Папка входящих, сюда класть архивы выгруженные
    из НСИ
    :type dn:str
    :return: Имя xml-файла содержащего справочник НСИ
    :rtype:str или None

    Функция смотрит в папку и берет первый xml файл. Если
    в папке нет xml, а есть zip-архив, его разархивируем
    и берем первый xml.
    """

    zips = list()
    xmls = list()
    # Берем содержание папки
    files = os.listdir(dn)
    # Бьем на два списка, с xml и zip файлами
    for f in files:
        if f.endswith('.zip'):
            zips.append(f)
        if f.endswith('.xml'):
            xmls.append(f)
    # Вначале берем xml файлы
    for x in xmls:
        return x
    # Когда xml закончились, начинаем извлекать из архивов
    for z in zips:
        exract_zip_inplace(dn+'/'+z,dn)
        # Отправляемся в рекурсию
        fn = get_xmlfile_from_dir(dn)
        if not fn is None:
            return fn
    # Если в папке ничего нет, возвращаем None
    return None

class FileHasInvalidData(Exception):
    pass

class ConfigFileNotFound(Exception):
    pass

class ConfigFileError(Exception):
    pass

class projectConfig():
    """ Класс projectConfig хранит конфигурацию проекта

    Конфигурационная информация( строки подключения к базам данных
    , логины пароли исползуемые при работе программы) для проекта 
    хранится в корневом каталоге проекта в файле config.json
    в формате json.
    При инициализации объекта файл читается и помещаеться
    в dict переменную объекта config.
    Для получения нужной инфы существуют конкретные методы, например
    метод sql_connection_string формирует строку подключения к БД.

      
    """
    config = dict()

    def __init__(self,path):
        file_name = path+'config.json'
        if os.path.isfile(file_name):
            with open(file_name,'rb') as f:
                try: 
                    self.config = json.load(f) #,encoding='utf-8')
                except Exception as e:
                    logging.exception(e)
                    raise ConfigFileError
            f.close()
        else:
            raise ConfigFileNotFound
    
    def sql_connection_string(self):
        """ Возвращает стороку подключения к БД

        Используется в некоторых скриптах проекта,
        строет строит стандартную строку подключения.
        Если в конфигурации нет ветки sql выдается ошибка.
        """
        
        if 'sql' in self.config:
            w = self.config['sql']
            conf = w['server_type']+'://'+w['user']+':'+w['password']+'@'+\
                w['server']+':'+w['port']+'/'+w['database']
        else:
            raise ConfigFileError
        return conf

    def wsgi_port(self):
        return int(self.config['wsgi_app']['port'])
    
    def nsi_config(self):
        """ Возвращает конфигурацию НСИ.

        Если нет в конфигурационном файле, вызывает
        ошибку ConfigFileError
        """

        if 'nsi' in self.config:
            w = self.config['nsi']
        else:
            raise ConfigFileError
        return w
    
    def tandemdb_config(self):
        if 'TandemBD' in self.config:
            w = self.config['TandemBD']
        else:
            raise ConfigFileError
        return w
        
def to_frdo_snils(snils):
    res = str()
    if snils is None:
        return ''
    res = snils.replace("-", "")
    if len(res)<11:
        return res
    res = res[:9]+' '+res[9:]
    return res

def print_info(msg):
    logging.info(msg)
    #print(msg)

if __name__=="__main__":
    print(to_frdo_snils('159-251-220-62'))
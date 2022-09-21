""" SOAP НСИ клиент
Здесь описан один класс NSI, с помощью которого можно
работать с НСИ напрямую.
"""
import pip
try:
    from zeep import Client,Settings
except ModuleNotFoundError as e:
    pip.main(['install','zeep'])
    from zeep import Client,Settings
from zeep import xsd
from zeep.wsse.username import UsernameToken
from zeep.wsse import utils
from zeep.plugins import HistoryPlugin
from zeep import ns
from zeep import Plugin

from lxml import etree

import requests
import logging
import traceback
import sys
from datetime import datetime, timedelta, tzinfo, timezone


class TZ(tzinfo):
    def utcoffset(self, dt): 
        return timedelta(hours=0)
    def dst(self, dt):
        return timedelta(0)



class NSI():
    """Класс для работы с НСИ

    Имеет базовые методы для работы
        retrieve
        update
    Класс является оберткой для пакета zeep работы с SOAP
    адаптированый для работы с НСИ

    """

    def __init__(self, conf):
        """Конструктор класса обеспечивает инициализацию настроек
         соединения

        :param conf: словарь с описанием параметров соединения.
        Например conf={'sourceId':'qwe',
            'destinationId':'nsi',
            'elementsCount':25000,
            'wsdl' : 'http://192.168.25.104:8085/services/NSIService?wsdl',
            'username':'qwe',
            'password':'qweqweqwe2'
        } Все приведенные в примере ключи являются обязательными для подключения
        :type conf: dict

        """

        self.sourceId=conf['sourceId']
        self.destinationId=conf['destinationId']
        self.elementsCount=conf['elementsCount']
        self.wsdl = conf['wsdl']
        self.username=conf['username']
        self.password=conf['password']
        self.client=Client(wsdl=self.wsdl,wsse=UsernameTokenTimestamp(username=self.username, password=self.password),
        settings = Settings(strict=False, xml_huge_tree=True), plugins=[HistoryPlugin(),MyLoggingPlugin()])
        self.routingHeader=self.client.get_type('{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Service}RoutingHeaderType')
        self.xdatagramtype=self.client.get_type('{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Service}datagram')

    def retrieve(self,req):
        """Метод запроса данных у НСИ

        :param req: Запрос к НСИ
        :type req: '{http://www.tandemservice.ru/Schemas/Tandem/
            Nsi/Service}datagram'
        :return: SOAP пакет ответа НСИ
        :rtype: '{http://www.tandemservice.ru/Schemas/Tandem/Nsi/
            Service}datagram'

        """
        if isinstance(req,str):
            now = datetime.now(tz=TZ())
            n = now.isoformat(sep='T', timespec='seconds')
            hour = timedelta(hours=10)
            exp = now+hour
            e = exp.isoformat(sep='T', timespec='seconds')
            body = """
<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope">
	<soap-env:Header>
		<wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
			<wsu:Timestamp xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
			<wsu:Created>%s</wsu:Created>
			<wsu:Expires>%s</wsu:Expires>
			</wsu:Timestamp>
			<wsse:UsernameToken>
				<wsse:Username>uks</wsse:Username>
				<wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">D6Ks6#ip</wsse:Password>
				<wsse:Created>%s</wsse:Created>
			</wsse:UsernameToken>
		</wsse:Security>
	</soap-env:Header>
	<soap-env:Body>
		<retrieveRequest xmlns="http://www.tandemservice.ru/Schemas/Tandem/Nsi/Service">
			<routingHeader>
				<operationType>retrieve</operationType>
				<sourceId>uks</sourceId>
				<destinationId>nsi</destinationId>
				<elementsCount>1</elementsCount>
                <async>1</async>
			</routingHeader>
			<datagram><x-datagram xmlns="http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0">
					<%s/>
				</x-datagram>
			</datagram>
		</retrieveRequest>
	</soap-env:Body>
</soap-env:Envelope>"""%(n,e,n,req)
            body = body.encode('utf-8')
            session = requests.session()
            session.headers = {"Content-Type": "text/xml; charset=utf-8"}
            session.headers.update({"Content-Length": str(len(body))})
            response = session.post(url=self.wsdl, data=body, verify=True)
            return (response.text)
        else:
            header = self.routingHeader(
                operationType='retrieve',sourceId=self.sourceId,
                destinationId=self.destinationId,
                elementsCount=self.elementsCount)
            try:
                return self.client.service.retrieve(header,req)
            except Exception as err:
                trace=traceback.format_exc()
                logging.error(str(err)+' '+str(trace))
                sys.exit()

    def update(self,req):
        """Метод обновления данных у НСИ

        :param req: Запрос к НСИ
        :type req: '{http://www.tandemservice.ru/Schemas/Tandem/Nsi/
            Service}datagram'
        :return: SOAP пакет ответа НСИ
        :rtype: '{http://www.tandemservice.ru/Schemas/Tandem/Nsi/
            Service}datagram'

        """

        try:
            return self.client.service.update(self.routingHeader(
                operationType='update',sourceId=self.sourceId,
                destinationId=self.destinationId,
                elementsCount=self.elementsCount
                ),req)
        except Exception as err:
            trace=traceback.format_exc()
            logging.error(str(err)+' '+str(trace))
            sys.exit()

    def insert(self,req):
        """Метод добавления данных в НСИ

        :param req: Запрос к НСИ
        :type req: '{http://www.tandemservice.ru/Schemas/Tandem/Nsi/
            Service}datagram'
        :return: SOAP пакет ответа НСИ
        :rtype: '{http://www.tandemservice.ru/Schemas/Tandem/Nsi/
            Service}datagram'

        """

        try:
            return self.client.service.insert(self.routingHeader(
                operationType='insert',sourceId=self.sourceId,
                destinationId=self.destinationId,
                elementsCount=self.elementsCount
                ),req)
        except Exception as err:
            trace=traceback.format_exc()
            logging.error(str(err)+' '+str(trace))
            sys.exit()

    def get_type(self,userType):
        """Метод возвращает требуемы тип данных из схемы wdsl"""

        try:
            return self.client.get_type(userType)
        except Exception as err:
            trace=traceback.format_exc()
            logging.error(str(err)+' '+str(trace))
            sys.exit()
    
    def create_request(self,nsi_table,data=None,no_id_request=False):
        """Метод создает запроса к справочнику nsi_table с телом data в НСИ

        :param nsi_table: Имя справочника к которому создается запрос
        :type nsi_table: str
        :param data: Необязательный параметр определяющий структуру запроса, представляет
        собой словарь, который потом пребразуется в SOAP-пакет запроса.
        :type data: dict
        :param no_id_request: Если True свойство ID  не формируктся.
        Преднозначено для формирования запросов на создание  новых записей в НСИ.
        :type no_id_request: boolean
        :return: объект типа, требуемого wdsl содержащий запрос для
        вызова SOAP-клиента.
        :rtype:{http://www.tandemservice.ru/Schemas/Tandem/Nsi/Service}datagram

        создаем запрос. это объект класс etree.Element преведенный к типу 
        требуемому wdsl
        """
        
        root=etree.Element('x-datagram')
        #добовлем ему атрибут
        root.set('xmlns','http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0')
        #добовляем подчиненный элемент
        
        #в xml данный запрос выглядит както так
        #<x-datagram xmlns=http://www.tandemservice.ru/Schemas/Tandem/Nsi/Datagram/1.0'
        #<AcademicDegree/>
        #</x-datagram>
        #теперь создаем объект требуемого операцией retieve класса согласно спецификации wsdl
        #print(data)
        if not(data is None):
            #Если словарь data не пуст, он может представлять из себя сложную
            #структуру из других словорей. Первый уровнь обрабатываем здесь
            if isinstance(data,dict):
                for i in data:
                    el=etree.Element(nsi_table)
                    if not no_id_request:
                        ide=etree.Element('ID')
                        ide.text=str(i)
                        el.append(ide)
                    #print(data)
                    for v in data[i].keys():
                        el1=etree.Element(v)
                        if isinstance(data[i][v],dict):
                            #Рекурсивно обрабатываем сложные словари содержащие другие словари
                            el1=self.create_request_requrcive(data[i][v],el1)
                        else:
                            if data[i][v] is None:
                                pass
                            elif data[i][v]=='None':
                                pass
                            else:
                                el1.text=str(data[i][v])
                        el.append(el1)
                    root.append(el)
        else:
            root.append(etree.Element(nsi_table))
        #print(etree.tostring(root, pretty_print=True))# убрать после отладки
        #полученный объект класс etree.Element приводим к требуемому
        #типу данных согласно схеме wdsl
        req=self.xdatagramtype(root)
        return req

    def create_request_requrcive(self,data,root):
        """Метод рекрсивного постороения тела запроса SOAP для НСИ

        :param data: словарь струкры запроса SOAP
        :type data: dict
        :param root: корневой элемент к которому присоединяем 
        элементы словоря data
        :return: Объект запроса преобразованный из словаря data в etree.Element
        :rtype: etree.Element
        """

        for v in data.keys():
            el1=etree.Element(v)
            if isinstance(data[v],dict):
                el1=self.create_request_requrcive(data[v],el1)
            else:
                el1.text=data[v]
            root.append(el1)
        return root


class MyLoggingPlugin(Plugin):
    """Класс плагин описывает кастомные преобразования пакетов SOAP 
    
    Используется основным классом NSI для настройки пакетов SOAP, чтобы 
    соответсовавать особеностям реализации НСИ

    """

    def ingress(self, envelope, http_headers, operation):
        """Метод обрабатывает ответ сервера прежде чем отдать его клиенту"""
        
        # Здесь выводим на экран ответ сервера
        # print(etree.tostring(envelope, pretty_print=True))
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        """Метод обрабатывает исходящий пакет SOAP прежде чем отправить
        его на сервер
        
        Здесь подготавливаем правильный пакет запроса для отправки на
        сервер. Дело в том что zeep готовит несколько не такой пакет
        какой ждет сервер. Глюки или нарушения стандарта, не знаю.
        просто убираем лишнее.
        
        """

        string1 = etree.tostring(envelope)
        s = string1.replace(b'ns0:',b'')
        s = s.replace(b':ns0',b'')
        # Это пакет запроса на сервер.
        # print(s)
        envelope = etree.fromstring(s)
        return envelope, http_headers


class UsernameTokenTimestamp:
    """Вспомогательный класс для класса NSI"""

    def __init__(self, username, password=None):
        self.username = username
        self.password = password
        self.username_token_profile_ns = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0"

    def apply(self, envelope, headers):
        security = utils.get_security_header(envelope)

        created = datetime.now()
        expired = created + timedelta(seconds=5 * 60)

        token = security.find("{%s}UsernameToken" % ns.WSSE)
        if token is None:
            token = utils.WSSE.UsernameToken()
            security.append(token)
        token.extend([
            utils.WSSE.Username(self.username),
            utils.WSSE.Password(
                self.password, Type="%s#PasswordText" % self.username_token_profile_ns
            ),
            utils.WSSE.Created(utils.get_timestamp(created)),
        ])

        timestamp = utils.WSU('Timestamp')
        timestamp.append(utils.WSU('Created', utils.get_timestamp(created)))
        timestamp.append(utils.WSU('Expires', utils.get_timestamp(expired)))
        security.append(timestamp)
        security.append(token)
        return envelope, headers

    def verify(self, envelope):
        pass
    def _create_password_text(self):
        return [
            utils.WSSE.Password(
                self.password, Type="%s#PasswordText" % self.username_token_profile_ns
            )
        ]

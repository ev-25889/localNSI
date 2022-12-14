""" Local doubler NSI module
It's define a class DoublerNSI? which use SQLAlchemy
to store data in some SQL server



"""

import pip
import inspect
import sys
from collections import OrderedDict
try:
    import xmltodict
except ModuleNotFoundError:
    pip.main([['install','xmltodict'],['trusted-host','pypi.org']])
    import xmltodict
import traceback
import logging
import os

try:
    from dill.source import getname
except ModuleNotFoundError:
    pip.main(['install','dill'])
    from dill.source import getname

import argparse

from functools import partial

try:
    from sqlalchemy import Column, DateTime, String, Integer
except ModuleNotFoundError:
    pip.main(['install','psycopg2'])
    pip.main(['install','sqlalchemy'])
    from sqlalchemy import Column, DateTime, String, Integer
from sqlalchemy import ForeignKey, func,Index


from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,Boolean 
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

from generallib import generate_logfile_name
from generallib import get_xmlfile_from_dir
from generallib import projectConfig
from generallib import FileHasInvalidData

# from dekanatapi import Dekanat
from postgres import TandemDB
from nsiclient import NSI

DEP_IU = 'ea51250e-530a-42d8-bfdd-1d758e162d83'
DEP_IGIMP = '61b88eb2-9bb6-4fc6-a8b5-93ba07d7d6ec'
DEP_AID = 'fc8e6515-fbd0-4509-b98e-172e5d5e34d3'
DEP_IPIP = 'ea16875f-686a-4f2c-918d-1a2e8db8a5cb'
DEP_IP = '5a81ba4f-a5ce-45b7-bea3-02174a010807'
DEP_ISOP = '3eb83c20-a717-4a10-8b1d-09e3a296e9e4'
DEP_IDO = '1006f98a-885a-403b-98eb-26d0bec37e59'

DEP_IU = 'ea51250e-530a-42d8-bfdd-1d758e162d83'
DEVEL_COND_1 = '1cd45549-cc9c-4f55-97cf-bb050a97f420'
DEVEL_COND_2 = '5af8152e-6e22-4d8b-b7c4-8c20ef69cea1'
DEVEL_COND_3 = '896a94c2-f191-46c5-a19a-ec6f938cfaf5'
DEVEL_COND_4 = 'baf3ca5d-1aff-48a7-99b2-470d87a3c915' 

Base = declarative_base() 


class Nsiconvert():
    """ Класс содержит обобщенные методы взаимодействия объектов

    """
    
    def init_from_dict(self,a,flds):
        for i in a:
            if hasattr(self,i):
                if i in flds.keys():
                    if a[i] is None:
                        setattr(self,i,None)
                    else:
                        setattr(self,i,a[i][flds[i]]['ID'])
                else:
                    setattr(self,i,a[i])
    
    def to_dict_base(self,flds):
        """Метод преобразует объект к словарю специального вида
        
        :param flds: Словарь, в котором содержится особенности
        преобразования к результирующему словарю. По умолчанию
        поле объекта является ключем, а значения поля это значение
        по этому ключу.
        Но для некоторых полей объекта требуется более сложное
        преобразования.result[self.поле][fld[self.поле]]['ID']
        :type flds: dict
        :result: Словарь специального вида, готовый для 
        взаимодействия с НСИ
        :rtype: dict
        """
        
        res = dict()
        # print(self.__dict__)
        # Пробегаем по всем атрибутам экземпляра класса
        for fld in self.__dict__:
            if fld in ['_sa_instance_state','flds']:
                continue
            # и если они не  ['_sa_instance_state','flds']
            # то  заносим их в dict 
            try:
                # Читаем значение атрибута преобразуя его к строке
                # либо None 
                if self.__dict__[fld] is None:
                    v = None
                else:
                    v = str(self.__dict__[fld])
                # Проверяем являеться ли атрибут особым.
                # Особые атрибуты предаются в метод в параметре flds 
                if fld in flds.keys():
                    # Если значение атрибута пусто, вид словаря 
                    # схлопывается до простого представления(так в НСИ)
                    if v is None:
                        it = {
                            fld:v
                        }
                    else:
                    # Особый случай для специальных атрибутов
                        it = {
                            fld:{
                                flds[fld]:{
                                    'ID':v
                                }
                            }
                        }
                else:
                    # Все не специальные атрибуты имеют простое
                    # представлние в виде словаря
                    it = {
                        fld:v
                    }
            except AttributeError as e:
                logging.error('not found fields '+str(self.__dict__))
                raise e
            res.update(it)
        return res

    def base_update(self,a):
        """Метод обновляет поля класса по словарю

        :param a: Словарь с ключами равными названиям полей класса,
        ииспользуется для заполения своими значениями соответсвующих
        полей объекта
        :type a: dict
        """

        for fld in self.__dict__:
            if fld in ['_sa_instance_state','flds']:
                continue
            w = getattr(a,fld)
            setattr(self,fld,w)

class DiplomUpdateStatus(Base):
    __tablename__ = 'diplomupdatestatus'

    diplom_id = Column(String,primary_key=True)
    student_id = Column(String)
    hash_string = Column(String)
    change_date = Column(DateTime)
    created_on = Column(DateTime, default=func.now())


class ServiceRequest(Base):
    __tablename__ = 'packages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    operationType = Column(String)
    messageId = Column(String)
    sourceId = Column(String)
    destinationId = Column(String)
    last = Column(Boolean)
    datagram=Column(String)
    callCC = Column(Integer)
    callRC = Column(String)

class EnrEntrant(Base,Nsiconvert):
    __tablename__ = 'enrentrant'
    ID = Column(String,primary_key=True)
    EnrEntrantPersonalNumber = Column(String)
    EnrEntrantRegistrationDate = Column(String)
    EnrEntrantArchival = Column(String)
    HumanID = Column(String)
    EnrEnrollmentCampaignID = Column(String)
    EnrEntrantStateID = Column(String)
    EducationalOrganizationID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'HumanID':'Human',
            'EnrEnrollmentCampaignID':'EnrEnrollmentCampaign',
            'EnrEntrantStateID':'EnrEntrantState',
            'EducationalOrganizationID':'Department',
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'HumanID':'Human',
            'EnrEnrollmentCampaignID':'EnrEnrollmentCampaign',
            'EnrEntrantStateID':'EnrEntrantState',
            'EducationalOrganizationID':'Department',
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class EnrEnrollmentCampaign(Base,Nsiconvert):
    __tablename__ = 'enrenrollmentcampaign'
    ID = Column(String,primary_key=True)
    EnrEnrollmentCampaignName = Column(String)
    EnrEnrollmentCampaignDateFrom = Column(String)
    EnrEnrollmentCampaignDateTo = Column(String)
    EnrEnrollmentCampaignEducationYear = Column(String)
    EnrEnrollmentCampaignCourierApplyFormAvailable = Column(String)
    EnrEnrollmentCampaignElectronApplyFormAvailable = Column(String)
    DepartmentID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'DepartmentID':'Department',
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'DepartmentID':'Department',
        }

        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))
class EnrRequestedCompetition(Base,Nsiconvert):
    __tablename__ = 'enrrequestedcompetition'
    ID = Column(String,primary_key=True)
    EnrRequestedCompetitionType = Column(String)
    EnrEnrollmentPlanID = Column(String)
    StateID = Column(String)
    EnrEntrantRequestID = Column(String)
    EnrProgramSetOrgUnitID = Column(String)
    EnrProgramSetItemID = Column(String)
    CompensationTypeID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'EnrEnrollmentPlanID':'EnrEnrollmentPlan',
            'StateID':'EnrEntrantState',
            'EnrEntrantRequestID':'EnrEntrantRequest',
            'EnrProgramSetOrgUnitID':'EnrProgramSetOrgUnit',
            'EnrProgramSetItemID':'EnrProgramSetItem',
            'CompensationTypeID':'CompensationType',
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'EnrEnrollmentPlanID':'EnrEnrollmentPlan',
            'StateID':'EnrEntrantState',
            'EnrEntrantRequestID':'EnrEntrantRequest',
            'EnrProgramSetOrgUnitID':'EnrProgramSetOrgUnit',
            'EnrProgramSetItemID':'EnrProgramSetItem',
            'CompensationTypeID':'CompensationType',
        }

        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))

class EnrEntrantRequest(Base,Nsiconvert):
    __tablename__ = 'enrentrantrequest'
    ID = Column(String,primary_key=True)
    EnrEntrantRequestType = Column(String)
    EnrEntrantRequestRegNumber = Column(String)
    EnrEntrantRequestRegDate = Column(String)
    EnrEntrantRequestTakeAwayDocumentDate = Column(String)
    EnrEntrantRequestEduInstDocOriginalRef = Column(String)
    EnrEntrantRequestBenefitCategory = Column(String)
    EnrEntrantRequestBenefitType = Column(String)
    EnrEntrantRequestEnrollmentCommission = Column(String)
    EnrEntrantRequestEntrantachievementmarksum = Column(String)
    EnrEntrantRequestFiledByTrustee = Column(String)
    EnrEntrantRequestOriginalSubmissionWay = Column(String)
    EnrEntrantRequestOriginalReturnWay = Column(String)
    EnrEntrantRequestInternalExamReason = Column(String)
    EnrEntrantRequestInternationalTreatyContractor = Column(String)
    EnrEntrantRequestGeneralEduSchool = Column(String)
    EnrEntrantRequestReceiveEduLevelFirst = Column(String)
    EnrEntrantID = Column(String)
    IdentityCardID = Column(String)
    PersonEduDocumentID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'EnrEntrantID':'EnrEntrant',
            'IdentityCardID':'IdentityCard',
            'PersonEduDocumentID':'PersonEduDocument'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'EnrEntrantID':'EnrEntrant',
            'IdentityCardID':'IdentityCard',
            'PersonEduDocumentID':'PersonEduDocument'
        }

        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class HumanContactData(Base,Nsiconvert):
    __tablename__ = 'humancontactdata'
    ID = Column(String,primary_key=True)
    HumanContactDataEmail = Column(String)
    HumanContactDataOther = Column(String)
    HumanContactDataPhoneDefault = Column(String)
    HumanContactDataPhoneFact = Column(String)
    HumanContactDataPhoneMobile = Column(String)
    HumanContactDataPhoneReg = Column(String)
    HumanContactDataPhoneRegTemp = Column(String)
    HumanContactDataPhoneRelatives = Column(String)
    HumanContactDataPhoneWork = Column(String)
    HumanID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'HumanID':'Human'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'HumanID':'Human'
        }

        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))



class Address(Base,Nsiconvert):
    __tablename__ = 'address'
    ID = Column(String,primary_key=True)
    AddressID = Column(String)
    AddressName = Column(String)
    AddressPostcode = Column(String)
    AddressOkato = Column(String)
    ParentAddressID = Column(String)
    OksmID = Column(String)
    AddressTypeID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'ParentAddressID':'Address',
            'OksmID':'Oksm',
            'AddressTypeID':'AddressType'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'ParentAddressID':'Address',
            'OksmID':'Oksm',
            'AddressTypeID':'AddressType'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class LksSessionResult(Base,Nsiconvert):
    __tablename__ = 'lkssessionresult'
    ID = Column(String,primary_key=True)
    LksSessionResultID = Column(String)
    LksSessionResultSemesterNumber = Column(String)
    LksSessionResultDate = Column(String)
    LksSessionResultResult = Column(String)
    LksSessionResultMaxResult = Column(String)
    LksSessionResultCredit = Column(String)
    LksSessionResultDiffCredit = Column(String)
    LksSessionResultID = Column(String)
    LksSessionResultExam = Column(String)
    LksSessionResultExamAccum = Column(String)
    LksSessionResultID = Column(String)
    LksSessionResultCourseWork = Column(String)
    LksSessionResultCourseProject = Column(String)
    LksSessionResultID = Column(String)
    LksSessionResultControlWork = Column(String)
    LksSessionResultCommission = Column(String)
    EducationYearID = Column(String)
    LksSubjectID = Column(String)
    StudentID = Column(String)
    LksSessionResultRemovalDate = Column(String)

    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'EducationYearID':'EducationYear',
            'LksSubjectID':'LksSubject',
            'StudentID':'Student'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'EducationYearID':'EducationYear',
            'LksSubjectID':'LksSubject',
            'StudentID':'Student'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class PersonEduDocument(Base,Nsiconvert):
    __tablename__ = 'personedudocument'
    ID = Column(String,primary_key=True)
    AddressID = Column(String)
    EduDocumentKindID = Column(String)
    EduLevelID = Column(String)
    GraduationHonourID = Column(String)
    HumanID = Column(String)
    PersonEduDocumentAvgMarkAsLong = Column(String)
    PersonEduDocumentDocumentEducationLevel = Column(String)
    PersonEduDocumentEduOrganization = Column(String)
    PersonEduDocumentEduProgramSubject = Column(String)
    PersonEduDocumentIssuanceDate = Column(String)
    PersonEduDocumentMark3 = Column(String)
    PersonEduDocumentMark4 = Column(String)
    PersonEduDocumentMark5 = Column(String)
    PersonEduDocumentNumber = Column(String)
    PersonEduDocumentQualification = Column(String)
    PersonEduDocumentRegistrationNumber = Column(String)
    PersonEduDocumentSeria = Column(String)
    PersonEduDocumentYearEnd = Column(String)

    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'AddressID':'Address',
            'EduDocumentKindID':'EduDocumentKind',
            'EduLevelID':'EduLevel',
            'GraduationHonourID':'GraduationHonour',
            'HumanID':'Human'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'AddressID':'Address',
            'EduDocumentKindID':'EduDocumentKind',
            'EduLevelID':'EduLevel',
            'GraduationHonourID':'GraduationHonour',
            'HumanID':'Human'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class LksWorkResult(Base,Nsiconvert):
    __tablename__ = 'lksworkresult'
    ID = Column(String,primary_key=True)
    LksWorkResultID = Column(String)
    LksWorkResultDate = Column(String)
    LksWorkResultName = Column(String)
    LksWorkResultComment = Column(String)
    LksWorkResultAbsent = Column(String)
    LksWorkResultValue = Column(String)
    LksWorkResultMaxValue = Column(String)
    LksSessionResultID = Column(String)
    LksWorkResultRemovalDate = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'LksSessionResultID':'LksSessionResult'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'LksSessionResultID':'LksSessionResult'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class Student(Base,Nsiconvert):
    __tablename__ = 'student'
    ID = Column(String,primary_key=True)
    StudentCourseNumber = Column(String)
    StudentTermNumber = Column(String)
    StudentPersonalNumber = Column(String)
    StudentBookNumber = Column(String)
    StudentPrivacyNumber = Column(String)
    StudentArchivePrivacyNumber = Column(String)
    StudentEntranceYear = Column(String)
    StudentIndividualTraining = Column(String)
    StudentTarget = Column(String)
    StudentEconomicGroup = Column(String)
    StudentYearEnd = Column(String)
    StudentArchival = Column(String)
    StudentArchiveDate = Column(String)
    StudentFinalQualifiyngWorkTheme = Column(String)
    StudentFinalQualifiyngWorkAdvisor = Column(String)
    StudentPracticePlace = Column(String)
    StudentEmployment = Column(String)
    StudentEnrollmentOrderDate = Column(String)
    StudentEnrollmentOrderNumber = Column(String)
    StudentEnrollmentOrderEnrDate = Column(String)
    AccountExpirationDate = Column(String)
    StudentDischargeOrderDate = Column(String)
    StudentDischargeOrderNumber = Column(String)
    StudentDischargeOrderDischargeDate = Column(String)
    StudentStartDate = Column(String)
    StudentEndDate = Column(String)
    StudentAnnotation = Column(String)
    StudentComment = Column(String)
    EducationalOrganizationID = Column(String)
    HumanID = Column(String)
    AcademicGroupID = Column(String)
    EducationalProgramID = Column(String)
    CompensationTypeID = Column(String)
    StudentStatusID = Column(String)
    CourseID = Column(String)
    StudentCategoryID = Column(String)
    ProgramQualificationID = Column(String)
    ForeignLanguageID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'EducationalOrganizationID':'Department',
            'HumanID':'Human',
            'AcademicGroupID':'AcademicGroup',
            'EducationalProgramID':'EducationalProgram',
            'CompensationTypeID':'CompensationType',
            'StudentStatusID':'StudentStatus',
            'CourseID':'Course',
            'StudentCategoryID':'StudentCategory',
            'ProgramQualificationID':'ProgramQualification',
            'ForeignLanguageID':'ForeignLanguage'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'EducationalOrganizationID':'Department',
            'HumanID':'Human',
            'AcademicGroupID':'AcademicGroup',
            'EducationalProgramID':'EducationalProgram',
            'CompensationTypeID':'CompensationType',
            'StudentStatusID':'StudentStatus',
            'CourseID':'Course',
            'StudentCategoryID':'StudentCategory',
            'ProgramQualificationID':'ProgramQualification',
            'ForeignLanguageID':'ForeignLanguage'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))

class IdentityCard(Base,Nsiconvert):
    __tablename__ = 'identitycard'
    ID = Column(String,primary_key=True)
    AddressID = Column(String)
    AddressStringID = Column(String)
    HumanID = Column(String)
    IdentityCardBirthDate = Column(String)
    IdentityCardBirthPlace = Column(String)
    IdentityCardDateGive = Column(String)
    IdentityCardDayOfEntryFrom = Column(String)
    IdentityCardDayOfEntryTo = Column(String)
    IdentityCardDepartmentCode = Column(String)
    IdentityCardFirstName = Column(String)
    IdentityCardKindID = Column(String)
    IdentityCardLastName = Column(String)
    IdentityCardMain = Column(String)
    IdentityCardMiddleName = Column(String)
    IdentityCardNationality = Column(String)
    IdentityCardNumber = Column(String)
    IdentityCardPeriodFrom = Column(String)
    IdentityCardPeriodTo = Column(String)
    IdentityCardSeries = Column(String)
    IdentityCardWhoGive = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'AddressID':'Address',
            'HumanID':'Human',
            'AddressStringID':'AddressString',
            'IdentityCardKindID':'IdentityCardKind'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'AddressID':'Address',
            'HumanID':'Human',
            'AddressStringID':'AddressString',
            'IdentityCardKindID':'IdentityCardKind'
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


class Human(Base,Nsiconvert):
    __tablename__ = 'human'
    ID = Column(String,primary_key=True)
    HumanAddressRuID = Column(String)
    HumanAddressStringID = Column(String)
    HumanBasicEmail = Column(String)
    HumanBirthDate = Column(String)
    HumanBirthPlace = Column(String)
    HumanBirthPlaceOKATO = Column(String)
    HumanCitizenshipID = Column(String)
    HumanFirstName = Column(String)
    HumanID = Column(String)
    HumanINN = Column(String)
    HumanLastName = Column(String)
    HumanLogin = Column(String)
    HumanMiddleName = Column(String)
    HumanPhotoContentURL = Column(String)
    HumanPrincipalID = Column(String)
    HumanSNILS = Column(String)
    HumanSex = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'HumanAddressRuID':'AddressRu',
            'HumanAddressStringID':'AddressString',
            'HumanCitizenshipID':'Oksm',
            'HumanPrincipalID':'Principal',
            'CompensationTypeID':'CompensationType',
            'StudentStatusID':'StudentStatus',
            'CourseID':'Course',
            'StudentCategoryID':'StudentCategory',
            'ProgramQualificationID':'ProgramQualification',
            'ForeignLanguageID':'ForeignLanguage'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'HumanAddressRuID':'AddressRu',
            'HumanAddressStringID':'AddressString',
            'HumanCitizenshipID':'Oksm',
            'HumanPrincipalID':'Principal',
            'CompensationTypeID':'CompensationType',
            'StudentStatusID':'StudentStatus',
            'CourseID':'Course',
            'StudentCategoryID':'StudentCategory',
            'ProgramQualificationID':'ProgramQualification',
            'ForeignLanguageID':'ForeignLanguage'
        }

        return self.to_dict_base(fields)
       
    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))

class AcademicGroup(Base,Nsiconvert):
    __tablename__ = 'academicgroup'
    ID = Column(String,primary_key=True)
    AcademicGroupArchival = Column(String)
    AcademicGroupArchiveDate = Column(String)
    AcademicGroupCreationDate = Column(String)
    AcademicGroupDescription = Column(String)
    AcademicGroupID = Column(String)
    AcademicGroupName = Column(String)
    AcademicGroupNumber = Column(String)
    AcademicGroupYearBegin = Column(String)
    AcademicGroupYearEnd = Column(String)
    CaptainStudentID = Column(String)
    CourseID = Column(String)
    DepartmentID = Column(String)
    EducationalProgramID = Column(String)
    EmployeeID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'CaptainStudentID':'Student',
            'CourseID':'Course',
            'DepartmentID':'Department',
            'EducationalProgramID':'EducationalProgram',
            'EmployeeID':'Employee'
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'CaptainStudentID':'Student',
            'CourseID':'Course',
            'DepartmentID':'Department',
            'EducationalProgramID':'EducationalProgram',
            'EmployeeID':'Employee'
        }

        return self.to_dict_base(fields)
        
    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))

class Employee(Base,Nsiconvert):
    __tablename__ = 'employee'
    ID = Column(String,primary_key=True)
    EmployeeContractKind = Column(String)
    EmployeeDepartmentID = Column(String)
    EmployeeDischargeDate = Column(String)
    EmployeeEmploymentDate = Column(String)
    EmployeeEmploymentKind = Column(String)
    EmployeeGradeID = Column(String)
    EmployeeID = Column(String)
    EmployeeMobilePhoneNumber = Column(String)
    EmployeePhoneNumber = Column(String)
    EmployeePostCode = Column(String)
    EmployeeRate = Column(String)
    EmployeeStatusID = Column(String)
    EducationalProgramID = Column(String)
    EmployeeWeekWorkLoad = Column(String)
    EmployeeWorkWeekDuration = Column(String)
    EmploymentTypeID = Column(String)
    HumanID = Column(String)
    OrganizationID = Column(String)
    PostID = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self)
        fields = {
            'EmployeeDepartmentID':'Department',
            'EmployeeGradeID':'EmployeeGrade',
            'EmployeeStatusID':'Status',
            'EmploymentTypeID':'EmploymentType',
            'HumanID':'Human',
            'OrganizationID':'Organization',
            'PostID':'Post',
        }
        self.init_from_dict(a,fields)
    
    def to_dict(self):
        fields = {
            'EmployeeDepartmentID':'Department',
            'EmployeeGradeID':'EmployeeGrade',
            'EmployeeStatusID':'Status',
            'EmploymentTypeID':'EmploymentType',
            'HumanID':'Human',
            'OrganizationID':'Organization',
            'PostID':'Post',
        }
        return self.to_dict_base(fields)

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))

class Principal(Base,Nsiconvert):
    __tablename__ = 'principal'
    ID = Column(String,primary_key=True)
    PrincipalLogin = Column(String)
    PrincipalPasswordHash = Column(String)
    PrincipalPasswordSalt = Column(String)
    created_on = Column(DateTime, default=func.now())

    def __init__(self,a):
        Base.__init__(self) 
        self.init_from_dict(a,{})
    
    def to_dict(self):
        return self.to_dict_base({})

    def update(self,a):
        if self.ID==a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s"%(
                self.ID,a.ID))


# начинаем самодеятельность
class RootRegistryElement(Base, Nsiconvert):
    __tablename__ = 'disciplines'

    # local_id = Column(Integer,primary_key=True)
    ID = Column(String, primary_key=True, name='external_id')
    RootRegistryElementName = Column(String, name='title')
    Status = Column(String, name='status', default='new')
    Date_sync = Column(DateTime, default=func.now(), name='date_sync')

    # Responce = Column(String, name='responce')

    def __init__(self, a):
        Base.__init__(self)
        self.init_from_dict(a, {})

    def to_dict(self):
        return self.to_dict_base({})

    def update(self, a):
        if self.ID == a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s" % (
                self.ID, a.ID))


class EducationLevelsHighSchool(Base, Nsiconvert):
    __tablename__ = 'educational_programs'

    ID = Column(String, primary_key=True, name='external_id')
    EducationLevelsHighSchoolName = Column(String, name='title')
    SubjectID = Column(String, name='direction_id')
    EducationLevelsHighSchoolOpenDate = Column(String, name='start_year')
    EducationLevelsHighSchoolCloseDate = Column(String, name='end_year')
    Status = Column(String, name='status', default='new')

    def __init__(self, a):
        Base.__init__(self)
        fields = {
            'SubjectID': 'EduProgramSubject',
        }
        self.init_from_dict(a, fields)

    def to_dict(self):
        return self.to_dict_base({})

    def update(self, a):
        if self.ID == a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s" % (
                self.ID, a.ID))


class EduProgramSubject(Base, Nsiconvert):
    __tablename__ = 'subject'
    # __table_args__ = {'extend_existing': True}

    ID = Column(String, name='id', primary_key=True)
    EduProgramSubjectName = Column(String, name='direction')
    EduProgramSubjectSubjectCode = Column(String, name='code_direction')

    def __init__(self, a):
        Base.__init__(self)
        self.init_from_dict(a, {})

    def to_dict(self):
        return self.to_dict_base({})

    def update(self, a):
        if self.ID == a.ID:
            self.base_update(a)
        else:
            logging.error("Ошибка присвоения self.ID==a.ID %s == %s" % (
                self.ID, a.ID))

"""
class EppWorkPlanBase(Base, Nsiconvert):
    __tablename__ = 'studyplan'

    ID = Column(String, primary_key=True)
    EppWorkPlanBaseName = Column(String)
    EppWorkPlanBase

class EppEduPlan(Base, Nsiconvert):
    __tablename__ = 'eduplan'
"""

class DoublerNSI():
    """Основной класс работы с базой Локального НСИ

    """

    tables = list()
    Session = sessionmaker(autoflush=False)
    wp = ''

    def __init__(self,conf,nsiclient,tandemcfg,drop_all=False,override=False):
        """Конструктор класс

        :param drop_all: Если True при создании объекта
        удаляются все таблицы из базы данных и создаются 
        вновь. Иначе только добавляются новые объекты базы.
        :type drop_all: boolean
        :param override: Если True то при записи из файла
        данные вначале удаляются. Перезапись таблицы.
        :type override: boolean
        """

        logging.debug('Create object DoublerNSI')
        self.client = nsiclient
        self.tdm = None
        engine = create_engine(conf,pool_size=10, max_overflow=30)
        self.Session.configure(bind=engine)
        self.tables = self._get_class_list()
        self.override = override
        if drop_all:
            Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        self.dapi = None
        self.grp = None
        self.tandemcfg = tandemcfg

    def _get_class_list(self):
        """Метод читает с какими таблицами умеет работать модуль.

        Читаем какие объекты в модуле определены и выбираем из них
        те что соответсвуют таблицам в базе( имеют родителем класс
        Base). 
        :return: лист классов для работы с базой
        :rtype: list
        """

        res = list()
        clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
        for i in clsmembers:
            parents = inspect.getmro(i[1])
            if parents[1].__name__=="Base":
                res.append(i[1])
        res.remove(ServiceRequest)
        return res

    def _parse_datagram_to_dict(self,w):
        """ Метод обработки xml-datagram пакета

        :param w: строка xml содержащаяся в datagram-теле
        пакета пришедшего от НСИ
        :type w:str
        :return: словарь ключами, которого являются 
        названия справочников, а содержанием списки из
        значений записей справочников в виде словарей.
        Возвращаем None, если ничего подходящего не нашли
        :rtype:dict
        """

        try:
            res = xmltodict.parse(w)
        except Exception as e:
            logging.debug(w)
            logging.exception(e)
            return None
        if 'x-datagram' in res:
            res = res['x-datagram']
        elif 'x-datagram:x-datagram' in res:
            res = res['x-datagram:x-datagram']
        else:
            logging.debug(res)
            return None
        
        result = dict()
        for i in res:
            # собираем только данные для словорей
            # содержащихся в self.tables 
            if i in map(getname,self.tables):
                x = [g for g in self.tables if getname(g)==i]
                if not x[0] in result.keys():
                    result[x[0]] = list()
                if isinstance(res[i],list):
                    for j in res[i]:
                        result[x[0]].append(dict(j))
                else:
                    result[x[0]].append(dict(res[i]))
        return result
    
    def _event_new_academicgroup(self,obj):
        """ Метод вызывается при создании новой группы

        :param obj: новая группа из НСИ
        :type obj: AcademicGroup
        :return:
        :rtype:AcademicGroup
        """
        return obj
        # TODO: Довести до ума генерацию нового имени кода
        if obj.AcademicGroupDescription is None \
            or obj.AcademicGroupDescription=='':
            logging.info(obj.ID)
            # Проверим есть ли такая группа в бд Деканат
            if self.dapi is None:
                logging.info('create dekanat access object')
                self.dapi = Dekanat()
            # Создадим словарь групп, где ключем будет
            # guid группы, а значением полное название 
            if self.grp is None:
                logging.info('create list grp')
                self.grp = dict()
                if self.tdm is None:
                    self.tdm = TandemDB(self.tandemcfg)
                wdict = self.dapi.get_dekanat_group()
                print(wdict)
                for gid in wdict:
                    gnid = self.tdm.get_group_guid(int(wdict[gid],16))
                    if not gnid is None:
                        logging.info(str({gnid:gid}))
                        self.grp.update({gnid:gid})

            # Проверим группы из Деканата, если есть
            # группа с нужным ID, то используем ее название
            # обновим в НСИ информацю с описанием группы 
            if obj.ID in self.grp.keys():
                obj.AcademicGroupDescription = self.grp[obj.ID]
                req=self.client.create_request(
                    'AcademicGroup',
                    data=obj.to_dict())
                responce=self.client.update(req)
                logging.info('obj '+obj.AcademicGroupDescription)
                return obj
            # Если в БД Деканат нет, деканате формируем уникалную 
            # строку имени группы по старым правилам из БД Деканат
            # s1 это год набора  
            if obj.AcademicGroupYearBegin is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: Год набора 
                пуст
                """ % (obj.ID,obj.AcademicGroupName)
                logging.error(msg)
                return obj
            elif len(obj.AcademicGroupYearBegin)==0:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: Год набора 
                пуст
                """ % (obj.ID,obj.AcademicGroupName)
                logging.error(msg)
                return obj
            else:
                s1 = obj.AcademicGroupYearBegin
            # s2 форма обучения
            # Для этого нам нужно прочитать другой справочник
            # EducationalProgramID 
            if obj.EducationalProgramID is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                Образовательная программы пуста
                """ % (obj.ID,obj.AcademicGroupName)
                logging.error(msg)
                return obj
            ep = self.find_by_id(
                'EducationalProgram',
                obj.EducationalProgramID
                )
            if ep is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                Образовательная программа с ID %s не найдена
                в справочнике
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID
                    )
                logging.error(msg)
                return obj
            if ep.DevelopFormID is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В образовательной программе с ID %s не определена
                форма обучения
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID
                    )
                logging.error(msg)
                return obj
            df = self.find_by_id('DevelopForm',ep.DevelopFormID)
            if df is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В образовательной программе с ID %s форма обучения %s
                не существует в справочнике
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID,
                    ep.DevelopFormID
                    )
                logging.error(msg)
                return obj
            s2 = df.DevelopFormGroup
            s2_str = df.DevelopFormName
            # s4 тип программы {“П”, “СОП”, “ВВ”}
            if ep.DevelopConditionID is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В образовательной программе с ID %s не определен 
                тип программы
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID
                    )
                logging.error(msg)
                return obj
            if ep.DevelopConditionID==DEVEL_COND_2:
                s4 = 'П'

            if ep.DevelopConditionID==DEVEL_COND_4:
                s4 = 'СОП'

            if ep.DevelopConditionID==DEVEL_COND_3:
                s4 = 'ВВ'
            # s5 подразделение
            if obj.DepartmentID==DEP_AID:
                s5 = 'АиД'
                s5_id = 53
            if obj.DepartmentID==DEP_IDO:
                s5 = 'ИДО'
                s5_id = 81
            if obj.DepartmentID==DEP_IGIMP:
                s5 = 'ИГиМП'
                s5_id = 56
            if obj.DepartmentID==DEP_IP:
                s5 = 'ИП'
                s5_id = 7
            if obj.DepartmentID==DEP_IPIP:
                s5 = 'ИПиП'
                s5_id = 5
            if obj.DepartmentID==DEP_ISOP:
                s5 = 'ИСОП'
                s5_id = 77
            if obj.DepartmentID==DEP_IU:
                s5 = 'ИЮ'
                s5_id = 6
            if s5 is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В группе указан несуществующий факультет %s
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.DepartmentID
                    )
                logging.error(msg)
                return obj
            # S7: [Уровень]
            if ep.EduProgramQualificationID is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В образовательной программе %s нет квалификации %s
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID,
                    ep.EduProgramQualificationID
                    )
                logging.error(msg)
                return obj
            epq = self.find_by_id(
                'EduProgramQualification',
                ep.EduProgramQualificationID
                )
            if epq is None:
                msg = """Возникла ошибка в формировании уникального
                имени группы для группы ID=%s c именем %s: 
                В образовательной программе %s нет квалификации %s
                """ % (
                    obj.ID,
                    obj.AcademicGroupName,
                    obj.EducationalProgramID,
                    ep.EduProgramQualificationID
                    )
                logging.error(msg)
                return obj

            lvl = epq['EduProgramQualificationName']
            crt = [
                DEP_AID,
                DEP_IDO
            ]
            if obj.DepartmentID in crt:
                s7 = 'А'
            else:
                if lvl=='Исследователь. Преподаватель-исследователь':
                    s7 = 'А'
                if lvl=='Магистр':
                    s7 = 'М'
                if lvl=='Бакалавр':
                    s7 = 'Б'
                if lvl=='Юрист':
                    if obj.DepartmentID==DEP_ISOP:
                        s7 = 'СПО'
                    else:
                        s7 = 'С'
            
            #S3: [Название специализации]
            s3 = str(ep.EducationalProgramID)
            # S6: [Номер группы]:
            # Подбор номера
            s6 = 1
            s = s1+'.'+s2+'.'+s3+'.'+s4+'.'+s5+'.'+str(s6)+'.'+s7
            while self._check_group(s):
                s6 = s6 + 1
                s = s1+'.'+s2+'.'+s3+'.'+s4+'.'+s5+'.'+str(s6)+'.'+s7
            # Получили уникальное имя группы, обновим в НСИ 
            obj.AcademicGroupDescription = s
            req=self.client.create_request(
                'AcademicGroup',
                data=obj.to_dict())
            responce=self.client.update(req)
            # Обновим массив self.grp 
            self.grp.update(
                {obj.ID:obj.AcademicGroupDescription}
            )
            gr_item = {
                '[ID_Код]': 0,
                '[псевдоним]': obj.AcademicGroupName,
                '[Количество_студентов]': 0,
                '[Курс]':0,
                '[Поток]':0,
                '[Специальность]':0,
                '[КФакультет]':s5_id,
                '[Форма_обучения]': s2_str,
                '[Уровень]':s7,
                '[Год набора]':obj.AcademicGroupYearBegin,
                '[группа]': obj.AcademicGroupDescription,
                '[id_tandem]':self.tdm.get_group_id_by_guid(obj.ID)
            }
            self.dapi.insert_dict('Группы',gr_item)

            logging.info('obj '+obj.AcademicGroupDescription)

        return obj
    def _check_group(self,s):
        """ Метод ищет есть ли такая группа уже в списке групп

        :param s: строка названия группы
        :type s: str
        :return: True если нет такой группы, иначе False
        :rtype: boolean
        """
        
        session = self.Session()
        cls_name = self._get_cls_by_name('AcademicGroup')
        try:
            q = session.query(cls_name).filter_by(AcademicGroupDescription=s).one()
        except NoResultFound:
            return True
        except MultipleResultsFound:
            return False
        return False



    def _event_new_object(self,obj):
        """ Метод событие создание нового объекта в БД

        :param obj: Объект, который будет создан в БД
        :type obj: object(Base)
        :return: Возвращает измененный объект для создания в БД
        :rtype: object(Base)

        Если перед созданием объекта в БД нужно что-то сделать,
        то сюда помещаем вызов обработчика

        при создании новой группы нам нужно отправить в НСИ значение
        AcademicGroupDescription заполненное специальным образом

        """

        if isinstance(obj,AcademicGroup):
            pass
#            obj = self._event_new_academicgroup(obj)
        return obj

    def _process_package(self,package):
        """ Метод обрабатывает пакет из очереди

        :param package: пакет из очереди
        :type package: ServiceRequest

        Входящий пакет объект класс ServiceRequest.
        Метод опредляет тип действий с пакетом по свойству
        operationType. 
        """
        
        w = package.datagram
        if len(w)==0:
            return True
        if package.operationType=='update':
            # Попытаемся преобразовать пакет к OrderedDict
            res = self._parse_datagram_to_dict(w)
            # Объявляем список в который загоним все объекты
            # которые пришли в пакете.
            nsi_objs = list()
            if res is None:
                return True
            for tn in res:
                for el in res[tn]:
                    nsi_objs.append(tn(el))
            # Пройдемася по полученному списку объектов
            # и загоним их в базу 
            session = self.Session()
            newobjs = list()
            for obj in nsi_objs:
                # найдем если уже существует
                try:
                    q = session.query(obj.__class__).filter_by(ID=obj.ID).one()
                except NoResultFound:
                    # для нового объекта запускаем событие
                    # смотрим нужно ли что то делать с новым объектом
                    obj = self._event_new_object(obj) 
                    newobjs.append(obj)
                    continue
                # если сущствует, применим событие обновления объекта.
                # Затем обновим обновим
                obj = self._event_update_object(obj)
                q.update(obj)
            session.commit()
            session.close()
            session = self.Session()
            for obj in newobjs:
                try:
                    q = session.query(obj.__class__).filter_by(ID=obj.ID).one()
                except NoResultFound:
                    session.add(obj)
                    session.commit()
                    continue
                q.update(obj)
                session.commit()
            session.close()  
            return True      
        if package.operationType=='delete':
            # Попытаемся преобразовать пакет к OrderedDict
            res = self._parse_datagram_to_dict(w)
            session = self.Session()
            for tn in res:
                
                for el in res[tn]:
                    
                    try:
                        q = session.query(tn).\
                             filter_by(ID=el['ID']).one()
                    except NoResultFound:
                        continue
                    logging.debug(str(q.__class__)+' delete ID='+q.ID)
                    session.delete(q)
                session.commit()
            session.close()
            return True

        # TODO: сделать обработку пакета retrive
        if package.operationType=='retrive':
            logging.error('package method retrive '+w)
            return True
        if package.operationType=='insert':
            # Попытаемся преобразовать пакет к OrderedDict
            res = self._parse_datagram_to_dict(w)
            # Объявляем список в который загоним все объекты
            # которые пришли в пакете.
            nsi_objs = list()
            for tn in res:
                for el in res[tn]:
                    nsi_objs.append(tn(el))
            # Пройдемася по полученному списку объектов
            # и загоним их в базу 
            session = self.Session()
            for obj in nsi_objs:
                # найдем если уже существует
                try:
                    q = session.query(obj.__class__).filter_by(ID=obj.ID).one()
                except NoResultFound:
                    # для нового объекта запускаем событие
                    # смотрим нужно ли что то делать с новым объектом
                    obj = self._event_new_object(obj) 
                    session.add(obj)
                    continue
                # если сущствует обновим применим событие
                # обновление объекта
                obj = self._event_update_object(obj)  
                q.update(obj)
            session.commit()
            session.close()  
            return True      


        return False

    def _event_update_object(self,obj):
        """Метод вызывает обработчик события обновление объекта"""
        if isinstance(obj,AcademicGroup):
            # obj = self._event_new_academicgroup(obj)
            pass
        
        return obj
    def serve_requests(self):
        """ Метод обработки очереди пакетов от НСИ

        Метод читает пакеты из таблицы packages и оборабатывает
        их содержание. Поле обработки пакеты удаляются из очерди.
        """
        
        sess = self.Session()
        while True:
            packages = sess.query(ServiceRequest).limit(10).all()
            t = len(packages)
            msg = "len(package)=%s"%(t,)
            logging.debug(msg)
            i = 0
            if len(packages)==0:
                break
            for package in packages:
                if self._process_package(package):
                    i += 1
                    sess.delete(package)
            sess.commit()
            
            if i>0:
                msg = 'Processed '+str(i)+' packages from NSI'
                logging.info(msg)
        sess.close()

    def _get_cls_by_name(self,name):

        for cls_name in self.tables:
            if getname(cls_name)==name:
                return cls_name
        return None

    def update_from_xml(self,file_name,dir_name):
        """ Метод обновляет справочник из файла выгруженного из НСИ

        :param file_name: имя разархивированного файла xml 
        выгруженного из НСИ
        :type file_name: str

        Метод получает имя уже разархивированного файла выгруженного
        из НСИ. Анализирует его содержание. И обновляет нужный 
        спрвочник. После файл удаляется! При ошибке файл не удаляется,
        но пишется сообщение об ошибке и вызывается исключение.
        """
        logging.debug('Trying to process file '+file_name)
        if not os.path.isfile(dir_name+'/'+file_name):
            s = 'File '+file_name+' do not exist'
            logging.error(s)
            return 
        f = open(dir_name+'/'+file_name, 'r+b')
        tag_name = file_name.replace('Type.xml','')
        cls_name = self._get_cls_by_name(tag_name)
        if cls_name is None:
            logging.error('No data in file '+file_name)
            raise FileHasInvalidData
        end_tag = bytes('</'+tag_name+'>',encoding='utf-8')
        b_tag = bytes('<'+tag_name+'>',encoding='utf-8')
        size_in_bytes = 1048576
        res = dict()
        k = 0
        prev = bytes('',encoding='utf-8')
        count = 0
        f_read  = partial(f.read, size_in_bytes)
        for text in iter(f_read, ''):
            if len(text)==0:
                break
            if not text.endswith(end_tag):
                # if file contains a partial line at the end, then don't
                # use it when counting the substring count. 
                rt = text.rsplit(end_tag, 1)
                if len(rt)==1:
                    text = rt[0]+end_tag
                    rest = bytes('',encoding='utf-8')
                else:
                    text = rt[0]+end_tag
                    rest = rt[1]
            
                # pre-pend the previous partial line if any.
                text =  bytes(bytearray(prev) + bytearray(text))
                prev = rest
            else:
                # if the text ends with a '\n' then simple pre-pend the
                # previous partial line. 
                text =  bytes(bytearray(prev) + bytearray(text))
                prev = bytes('',encoding='utf-8')
            for i in self.pars_to_obj(text,b_tag,end_tag,cls_name,tag_name):
                self.add(i)
                count += 1
            k +=1
        ind = k*size_in_bytes-len(prev)
        for i in self.pars_to_obj(prev,b_tag,end_tag,cls_name,tag_name):
            self.add(i)
            count += 1
        # Удаляем файл.
        f.close()
        os.remove(dir_name+'/'+file_name)
        msg = 'Processed file '+file_name+'. Objects '+getname(cls_name)+\
            ' loaded:'+str(count)
        logging.info(msg)

    def pars_to_obj(self,text,b_tag,end_tag,cls_name,tag_name):
        res = dict()
        # TODO: Исправить кривой код поиска нужных
        # объектов в тексте 
        while text.find(b_tag)!=-1:
            i = text.find(b_tag)
            if len(self.wp)>0:
                w = self.wp+text[:i-1]
                self.wp = ''
                res = xmltodict.parse(w.decode(encoding='utf-8'))
                obj = cls_name(dict(res[tag_name]))
                text = text[i:]
                yield obj

            
            text = text[i:]
            w = text[:text.find(end_tag)+len(end_tag)]

            text = text[text.find(end_tag)+len(end_tag):]
            
            if w[len(b_tag):].find(b_tag)>0:
                if len(text)==0:
                    self.wp = text[:text.find(end_tag)+len(end_tag)]
                    continue

                w += text[:text.find(end_tag)+len(end_tag)]
                text = text[text.find(end_tag)+len(end_tag):]

            
            res = xmltodict.parse(w.decode(encoding='utf-8'))
            obj = cls_name(dict(res[tag_name]))
            yield obj

    
    def add(self,obj):
        session = self.Session()
        try:
            q = session.query(obj.__class__).filter_by(ID=obj.ID).one()
        except NoResultFound:
            # для нового объекта запускаем событие
            # смотрим нужно ли что то делать с новым объектом
            obj = self._event_new_object(obj) 
            session.add(obj)
            session.commit()
            return
        # если сущствует обновим.
        q.update(obj)
        session.commit()
        

    def getall(self,name):
        """ Метод возвращает содержания справочника в формате нужном
        для ukserver
        """
        
        cls_name = self._get_cls_by_name(name)
        session = self.Session()
        result = dict()
        q = session.query(cls_name).all()
        for obj in q:
            result.update({obj.ID:dict(obj.to_dict())})
        return result

        

    def update(self,table,data):
        cls_name = self._get_cls_by_name(table)
        for i in data:
            obj = cls_name(data[i])
            self.add(obj)
        
    def find_by_id(self,table,i):
        session = self.Session()
        cls_name = self._get_cls_by_name(table)
        logging.debug("DoublerNSI.find_by_id %s, %s"%(cls_name,table))
        try:
            q = session.query(cls_name).filter_by(ID=i).one()
        except NoResultFound:
            return None
        return q

    def iter_lsr(self,i):
        session = self.Session()
        q = session.query(LksSessionResult).filter_by(StudentID=i).all()
        for obj in q:
            yield obj.to_dict()



if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--debug", action="store_true",
                    help="turn on debug mode")
    parser.add_argument("-D","--drop", action="store_true",
                    help="Drop all existing data in local NSI")
    htxt = """This option with file in input catalog delete 
        correspondine table in database before updating from file"""
    parser.add_argument("-o","--override", action="store_true",
                    help=htxt)

    args = parser.parse_args()
    if args.debug:
        dmm = logging.DEBUG
    else:
        dmm = logging.INFO

    format_str='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=generate_logfile_name('nsiSync',
       period='day'),filemode='a',format=format_str,level=dmm)

    logging.info('start script ldnsi')
    try:
        script_path = os.path.dirname(__file__)
        if len(script_path)>0:
            script_path += '/'
        input_dir = script_path+'input'
        logging.debug('script_path:'+script_path)
        logging.debug('input_dir:'+input_dir)
        config = projectConfig(script_path)
        # conf = 'postgresql://donsi:TS4d#dkpf3WE@192.168.25.103:5432/doubnsi'
        #conf = 'postgresql://donsitest:TS4d#dkpf3WE1@192.168.25.103:5432/doubnsitest'
        conf = config.sql_connection_string()
        nsiconf = config.nsi_config()
        tandemcfg = config.tandemdb_config()
        #conf = 'sqlite:///orm_in_detail.sqlite'
        if args.drop:
            txt = input('Do really want to erease all data in database Y/n[n]')
            if not txt=='Y':
                logging.info('Fail to confirm option "drop_all". Exiting.')
                sys.exit()

            logging.info('Droping all tables in database.')
        dnsi = DoublerNSI(conf,nsiconf,tandemcfg,drop_all=args.drop)
        while True:
            f_name = get_xmlfile_from_dir(input_dir)
            
            
            if f_name is None:
                break
            msg = "find xml file %s to process"%(f_name,)
            logging.debug(msg)
            logging.debug('begin process')
            dnsi.update_from_xml(f_name,input_dir)
            logging.debug("end process of file %s"%(f_name,))


        logging.debug("begin serve_requests")
        dnsi.serve_requests()
        logging.debug("end serve_requests")
        logging.info('stop ldnsi')
    except BaseException as e:
        logging.exception(e)

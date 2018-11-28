######## DESCRIPTION ###########################################
######## HISTORY ###############################################
# 2017/11/22 - Méloée BOUDET - MBT - Initialisation
######## DEPENDANCIES ##########################################




import sys 
import os
import datetime
sys.path.append('./../../') 

import pandas as PANDAS
import numpy as NUMPY
from lxml import etree as ETREE 
import re
import sqlalchemy
from sqlalchemy import create_engine
import ctypes
import json

from __C__.__C__PHASE1.TABLES.TABLE import TABLE
from __C__.__C__PHASE1.DIRECTORIES.DIRECTORY import DIRECTORY


#Connexion Ã  la base de donnÃ©es AWS RDS
with open('RDS_CONFIG.json') as json_data_file:
    data = json.load(json_data_file)

host = data["mysql"]["host"]
user = data["mysql"]["user"]
password = data["mysql"]["passwd"]
db = data["mysql"]["db"]

db_url = 'mysql+pymysql://%s:%s@%s/%s' % (user,password,host,db)
engine = create_engine(db_url)


class STUDY():
    def __init__(self, key, verbose=False):
        self.verbose = verbose
        self.directory = DIRECTORY()
        self.key = key
        self.path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]
        self.TABLES_METADATAS_df = None
        self.TABLES_FILENAMES_df = None
        self.TABLES_dict = {}
        self.TABLES_PART4_df = None
        self.TABLE_PCSA = None
        self.PART_1_etree = None
        self.PART_2_etree = None
        self.PART_3_etree = None
        self.PART_4_etree = None
        self.CHECK_REJECTION = {'status' : False, 'rejection' : {}}
        self.CHECK_REJECTION_LAB = {'status' : False, 'rejection' : {}}
        self.sas_update = []

        self.delete_label_bdd()

        self.read_GROUP_DESCRIPTION()
        self.read_TABLES_METADATAS()
        self.read_TABLES_FILENAMES()

        self.maj_sourefile_table_bdd()

        self.read_PCSA_LISTING()
        # self.read_AESI()
        self.concatenate_table()
        # self.LISTING_TABLE()
        self.ADD_tableLocation()
        self.get_group_metadata()

        ## COMMENTER ##
        self.set_PCSA_TABLES()
        self.set_TABLES()

        self.get_date_last_update()
		
        # self.CHECK_REJECTIONS()

        self.set_PART_1()
        self.set_PART_1_AI4CSR_TOOLS()
        self.set_PART_2()
        self.set_PART_2_AI4CSR_TOOLS()
        self.set_PART_2_bis()
        self.set_PART_2_AI4CSR_TOOLS_bis()
        self.set_PART_3()
        self.set_PART_3_AI4CSR_TOOLS()
        self.set_PART_4()
        self.set_PART_4_AI4CSR_TOOLS()

        ###### UPDATE DB FUNCTIONS - BEGINNING ######
        self.maj_label_LISTING()

        # Remise à zéro des indices de la table LABEL de la bdd
        LABEL_BDD_Id = PANDAS.read_sql_query('select Id from label', engine)['Id'].tolist()
        LABEL_BDD_Id.sort()
        i=0
        for ids in LABEL_BDD_Id:
            i+=1
            query = 'UPDATE label SET Id=%s WHERE Id=%s;' % (i,ids)
            with engine.begin() as conn:
                conn.execute(query)

        # print(PANDAS.read_sql_query('select * from label', engine))

    
    ###### NEW ######  
    def maj_sourefile_table_bdd(self):
        sourcefile_df = PANDAS.read_sql_query('select * from sourcefile', engine)
        new_Name = {'Name' : []}
        for irow,row in self.TABLES_FILENAMES_df.iterrows():

            fileName = row['fileName']
            # print(fileName)
            if str(fileName) != 'nan':
                if fileName not in sourcefile_df['Name'].tolist():
                    new_Name['Name'].append(fileName)

        new_Name = PANDAS.DataFrame(new_Name)
        new_Name.to_sql('sourcefile', engine, if_exists='append', index = False)
        # print(PANDAS.read_sql_query('select * from sourcefile', engine))
        # exit()

        sourcefile_df = PANDAS.read_sql_query('select Name,Id as IdSourceFile from sourcefile', engine)
        self.TABLES_FILENAMES_df = PANDAS.merge(self.TABLES_FILENAMES_df,sourcefile_df,how='left',left_on='fileName',right_on='Name')

    ###### NEW ######  
    def maj_label_LISTING(self):
        IdStudy = PANDAS.read_sql_query('SELECT Id FROM study WHERE Compound = "%s" '\
            'and StudyName = "%s" and PeriodAnalysis = "%s"' % (self.key[0], self.key[1], self.key[2]), engine)['Id'].tolist()[0]
        IdDict = (19,20)

        Label_listing_df = PANDAS.read_sql_query('select * from label where IdStudy = %s and IdDict in %s' % (IdStudy,IdDict), engine)
        Label_listing_df.drop_duplicates(subset=['Text1','Text2','IdStudy','IdDict'], inplace = True)
        LABEL_BDD_Id = PANDAS.read_sql_query('select Id from label where IdStudy = %s and IdDict in %s' % (IdStudy,IdDict), engine)['Id'].tolist()

        for ids in LABEL_BDD_Id:
            query = 'DELETE FROM label WHERE Id=%s;' % (ids);
            # query = 'DELETE FROM label WHERE Id in (1,2);'
            with engine.begin() as conn:
                conn.execute(query)

        Label_listing_df.to_sql('label', engine, if_exists='append', index = False)


    ###### NEW ######  
    def delete_label_bdd(self):
        IdStudy = PANDAS.read_sql_query('SELECT Id FROM study WHERE Compound = "%s" '\
            'and StudyName = "%s" and PeriodAnalysis = "%s"' % (self.key[0], self.key[1], self.key[2]), engine)['Id'].tolist()[0]
        IdDicts = range(23)
        for IdDict in IdDicts:
            LABEL_BDD_Id = PANDAS.read_sql_query('select Id from label where IdDict = %s and IdStudy = %s' % (IdDict,IdStudy), engine)['Id'].tolist()
            for ids in LABEL_BDD_Id:
                query = 'DELETE FROM label WHERE Id=%s;' % (ids);
                # query = 'DELETE FROM label WHERE Id in (1,2);'
                with engine.begin() as conn:
                    conn.execute(query)

            ###### UPDATE DB FUNCTIONS - END ######


    ###### TO DELETE ? ######
    def separate_LAB_TABLE(self, TABLES_df):
        LAB_FILES = TABLES_df[(TABLES_df['documentTableTag'] == 'LAB_PCSA') | (TABLES_df['documentTableTag'] == 'LAB_DESC')]
        LAB_FILES = LAB_FILES[LAB_FILES['HASFILE'] == True]
        LAB_FILES = LAB_FILES[LAB_FILES['studyConceptTag'].isnull()]


        LAB_FILTER_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
        LAB_FILTER_df = PANDAS.read_excel(LAB_FILTER_path, sheet_name='LAB_PCSA_PARAMETER', encoding='utf8')

        # LAB_CATEGORY_path = self.directory['PARAMETERS']['__PCSA']['PATH'] + 'LAB_CATEGORY_MAPPING.xlsx'
        # LAB_CATEGORY_df = PANDAS.read_excel(LAB_CATEGORY_path, sheet_name='MAPPING', encoding='utf8') 
        # LAB_CATEGORY_df.rename(columns = {'Category':'LAB_CATEGORY'}, inplace = True)

        cle = 'lab category mapping'
        requete = 'SELECT dictline.In1 as LABEL, dictline.Out1 as Category, dictline.Id as IdDictLine FROM dictline LEFT JOIN dict on dictline.IdDict = dict.Id WHERE TableTechnical = "' + cle + '";' 
        LAB_CATEGORY_df = PANDAS.read_sql_query(requete, engine)
        LAB_CATEGORY_df.rename(columns = {'Category':'LAB_CATEGORY'}, inplace = True)
        # print(LAB_CATEGORY_df)
        # exit()



        LAB_FILTER_df = PANDAS.merge(LAB_FILTER_df, LAB_CATEGORY_df, how='left', left_on=['Category'] , right_on=['LABEL'])
        # print(LAB_FILTER_df)
        # exit()

        FILE_df = {'fileName':[], 'NAME' : [], 'studyConceptTag' : [], 'IdSourceFile' : []}
        self.list_files_separated = []

        # print(NUMPY.unique(LAB_FILTER_df.File.tolist()))

        for file in NUMPY.unique(LAB_FILTER_df.File.tolist()):
        #     print(file)
        #     print(NUMPY.unique(LAB_FILES.fileName.tolist()))
        #     print(not LAB_FILES.empty)
            if not LAB_FILES.empty:
                if file in NUMPY.unique(LAB_FILES.fileName.tolist()):
                    documentTableTag = TABLES_df[TABLES_df['fileName'] == file].documentTableTag.tolist()[0]
                    NAME = TABLES_df[TABLES_df['fileName'] == file].NAME.tolist()[0]
                    IdSourceFile = TABLES_df[TABLES_df['fileName'] == file].IdSourceFile.tolist()[0]
                    FILTER_df = LAB_FILTER_df[LAB_FILTER_df['File'] == file]
                    dataset_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+file+'.sas7bdat'
                    dataset = PANDAS.read_sas(filepath_or_buffer=dataset_path, format='SAS7BDAT', encoding='iso-8859-1')
                    # print(FILTER_df)
                    parameters = NUMPY.unique(FILTER_df.Parameter.tolist())
                    # print(parameters)
                    current_parameter = None
                    for irow,row in dataset.iterrows():
                        test = False
                        if row['__datatype'] != 'HEAD':
                            for parameter in parameters:
                                # print(parameter)
                                if row['__col_0'].find(parameter) != -1:
                                    current_parameter = FILTER_df[FILTER_df['Parameter'] == parameter].Category.values[0]
                                    dataset.loc[irow,'category_flag'] = current_parameter
                                    test = True

                            if test == False:
                                # print(irow)
                                # print(current_parameter)
                                dataset.loc[irow,'category_flag'] = current_parameter


                    Categories = NUMPY.unique(FILTER_df.Category.tolist())
                    for Category in Categories:
                        TMP_df = dataset[(dataset['category_flag'] == Category) | (dataset['__datatype'] == 'HEAD')]
                        studyConceptTag = Category.replace(' ','_')
                        studyConceptTag = studyConceptTag.replace(',','')
                        LAB_CATEGORY = NUMPY.unique(FILTER_df[FILTER_df['Category'] == Category].LAB_CATEGORY.tolist())[0]
                        filename = documentTableTag+'_'+LAB_CATEGORY+'_'+studyConceptTag
                        path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+filename+'.xlsx'
                        TMP_df.to_excel(excel_writer=path, index=True, sheet_name='DATA')


                        FILE_df['fileName'].append(file)
                        FILE_df['NAME'].append(NAME)
                        FILE_df['studyConceptTag'].append(Category)
                        FILE_df['IdSourceFile'].append(IdSourceFile)

                    self.list_files_separated.append(file)

                    TABLES_df.drop(TABLES_df[TABLES_df['fileName'] == file].index ,inplace = True)



        FILE_df = PANDAS.DataFrame(FILE_df)
        FILE_df = PANDAS.merge(FILE_df, self.TABLES_METADATAS_df, how='left', left_on=['NAME'] , right_on=['NAME'])
        FILE_df.loc[:,'HASFILE'] = PANDAS.notnull(FILE_df['fileName'])
        # print(FILE_df.fileName)
        # print(FILE_df.shape[0])
        # print(TABLES_df[TABLES_df['HASFILE'] == True].shape[0])
        # print(TABLES_df)

        TABLES_df = PANDAS.concat([TABLES_df,FILE_df],ignore_index = True)#, sort = True)

        # print(TABLES_df[TABLES_df['HASFILE'] == True].shape[0])
        # exit()

        return TABLES_df


    ###### TO DELETE ? ######
    # def read_PCSA_LISTING(self):

    #     accents = { 'a': ['à', 'ã', 'á', 'â'],
    #                 'e': ['é', 'è', 'ê', 'ë'],
    #                 'i': ['î', 'ï'],
    #                 'u': ['ù', 'ü', 'û'],
    #                 'o': ['ô', 'ö'] }
    #     ponctuation = ['?',',','.',';',':','/','!']
    #     # pcsa_liste = {'fileName' : [], 'studyConceptTag' : [], 'documentTableTag' : []}
    #     # print(self.TABLES_METADATAS_df)
    #     # print(self.TABLES_FILENAMES_df)
    #     TABLES_df = PANDAS.merge(self.TABLES_FILENAMES_df, self.TABLES_METADATAS_df, how='left', left_on=['NAME'] , right_on=['NAME'])
    #     TABLES_df.loc[:,'KEY'] = TABLES_df['NAME']
    #     TABLES_df.loc[:,'HASFILE'] = PANDAS.notnull(TABLES_df['fileName'])

    #     TABLES_df = self.separate_LAB_TABLE(TABLES_df)
    #     TABLES_df.loc[:,'HASFILE'] = PANDAS.notnull(TABLES_df['fileName'])
    #     # print(TABLES_df[TABLES_df['HASFILE'] == True])

    #     LAB_CATEGORY_path = self.directory['PARAMETERS']['__PCSA']['PATH'] + 'LAB_CATEGORY_MAPPING.xlsx'
    #     LAB_CATEGORY_REJECTION_path = self.directory['PARAMETERS']['__PCSA']['PATH'] + 'LAB_CATEGORY_REJECTION.xlsx'
    #     LAB_CATEGORY_REJECTION_df = PANDAS.read_excel(LAB_CATEGORY_REJECTION_path, sheet_name='REJECTION', encoding='utf8') 
    #     LAB_CATEGORY_df = PANDAS.read_excel(LAB_CATEGORY_path, sheet_name='MAPPING', encoding='utf8') 

    #     # print(TABLES_df)

    #     self.TABLES_PCSA_STUDYCONCEPT = {}
    #     CATEGORY = ['HEMATOLOGY','CHEMISTRY','URINALYSIS','VITAL_SIGNS','ECG','TMP']
    #     for Category in CATEGORY:
    #         self.TABLES_PCSA_STUDYCONCEPT['LAB_DESC_'+Category] = {}
    #         self.TABLES_PCSA_STUDYCONCEPT['LAB_PCSA_'+Category] = {}

    #     # print(self.TABLES_PCSA_STUDYCONCEPT)

    #     self.TABLES_df = PANDAS.merge(TABLES_df, LAB_CATEGORY_df, how='left', left_on=['studyConceptTag'] , right_on=['LABEL'])
    #     self.TABLES_df['mainWording'] = self.TABLES_df['LABEL']
    #     self.TABLES_df = self.TABLES_df.loc[self.TABLES_df['HASFILE'] == True]
    #     self.TABLES_df.reset_index(drop=True, inplace=True)
    #     # print(self.TABLES_df)

    #     ## Check for non Mapping
    #     CHECK_CATEGORY_REJECTION = False
    #     LAB_CATEGORY_REJECTION_new_df = {'LABEL' : [], 'fileName' : []}
    #     for ROW_id, ROW in self.TABLES_df.iterrows():
    #         if ROW['HASFILE']:
    #             if re.findall("LAB_", str(ROW['documentTableTag'])): 
    #                 if str(ROW['Category']) == 'nan':
    #                     self.TABLES_df.loc[ROW_id,('Category')] = 'TMP'
    #                     LAB_CATEGORY_REJECTION_new_df['LABEL'].append(str(ROW['studyConceptTag']))
    #                     LAB_CATEGORY_REJECTION_new_df['fileName'].append(str(ROW['fileName']))
    #                     CHECK_CATEGORY_REJECTION = True

    #     if CHECK_CATEGORY_REJECTION == True:
    #         # LAB_CATEGORY_REJECTION_df['LABEL'] = list(NUMPY.unique(LAB_CATEGORY_REJECTION_df['LABEL']))
    #         # print(LAB_CATEGORY_REJECTION_df['LABEL'])
    #         # print(LAB_CATEGORY_REJECTION_df)
    #         LAB_CATEGORY_REJECTION_new_df = PANDAS.DataFrame(LAB_CATEGORY_REJECTION_new_df)

    #         LAB_CATEGORY_REJECTION_new_df.loc[:,'COMPOUND'] = self.key[0]
    #         LAB_CATEGORY_REJECTION_new_df.loc[:,'STUDY_NAME'] = self.key[1]
    #         LAB_CATEGORY_REJECTION_new_df.loc[:,'PERIOD_ANALYSIS'] = self.key[2]


    #         LAB_CATEGORY_REJECTION_df = PANDAS.concat([LAB_CATEGORY_REJECTION_df,LAB_CATEGORY_REJECTION_new_df],ignore_index = True)
    #         LAB_CATEGORY_REJECTION_df = LAB_CATEGORY_REJECTION_df.reindex(['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'], axis=1)
    #         LAB_CATEGORY_REJECTION_df.drop_duplicates(subset=['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'],inplace = True)



    #         LAB_CATEGORY_REJECTION_df.to_excel(excel_writer=LAB_CATEGORY_REJECTION_path, index=True, sheet_name='REJECTION')

    #         LAB_CATEGORY_REJECTION_df.drop_duplicates(['LABEL'],inplace = True)

    #         self.CHECK_REJECTION['status'] = True
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY'] = {}
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['study'] = self.key
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['title'] = 'Rejections regarding studyConcepTag and Category for LAB tables'
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['rejectionFile'] = LAB_CATEGORY_REJECTION_path
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['mappingFile'] = LAB_CATEGORY_path
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['dataFile'] = None
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['documentTableTag'] = None
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['studyConceptTag'] = None
    #         self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['rejets'] = LAB_CATEGORY_REJECTION_df
    #         # print(LAB_CATEGORY_REJECTION_df)

    #     # print(' ')
    #     # print(TABLES_df)


    #     for ROW_id, ROW in self.TABLES_df.iterrows():
    #         TABLE_hasfile = ROW['HASFILE']
            
    #         if TABLE_hasfile:
    #             if re.findall("LAB_", str(ROW['documentTableTag'])):
    #                 # self.TABLES_PCSA_STUDYCONCEPT[str(ROW['documentTableTag'])]
    #                 documentTableTag = str(ROW['documentTableTag']) + '_' + str(ROW['Category'])
    #                 studyConceptTag = str(ROW['studyConceptTag']).replace(' ','_')
    #                 for ponct in ponctuation:
    #                     studyConceptTag = studyConceptTag.replace(ponct, '')

    #                 for (char, accented_chars) in accents.items():
    #                     for accented_char in accented_chars:
    #                         studyConceptTag = studyConceptTag.replace(accented_char, char)



    #                 if studyConceptTag in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag]:
    #                     self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag].append(str(ROW['fileName']))
    #                 else:
    #                     self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag] = [str(ROW['fileName'])]
 

    ###### NEW ######   
    def read_PCSA_LISTING(self):

        accents = { 'a': ['à', 'ã', 'á', 'â'],
                    'e': ['é', 'è', 'ê', 'ë'],
                    'i': ['î', 'ï'],
                    'u': ['ù', 'ü', 'û'],
                    'o': ['ô', 'ö'] }
        ponctuation = ['?',',','.',';',':','/','!']
        # pcsa_liste = {'fileName' : [], 'studyConceptTag' : [], 'documentTableTag' : []}
        # print(self.TABLES_METADATAS_df)
        # print(self.TABLES_FILENAMES_df)
        TABLES_df = PANDAS.merge(self.TABLES_FILENAMES_df, self.TABLES_METADATAS_df, how='left', left_on=['NAME'] , right_on=['NAME'])
        TABLES_df.loc[:,'KEY'] = TABLES_df['NAME']
        TABLES_df.loc[:,'HASFILE'] = PANDAS.notnull(TABLES_df['fileName'])

        # print(TABLES_df[TABLES_df['HASFILE'] == True])
        TABLES_df = self.separate_LAB_TABLE(TABLES_df)
        # print(TABLES_df.fileName)
        TABLES_df.loc[:,'HASFILE'] = PANDAS.notnull(TABLES_df['fileName'])
        # print(TABLES_df[TABLES_df['HASFILE'] == True])
        # LAB_CATEGORY_path = self.directory['PARAMETERS']['__PCSA'] + 'LAB_CATEGORY_MAPPING.xlsx'
        # LAB_CATEGORY_REJECTION_path = self.directory['PARAMETERS']['__PCSA'] + 'LAB_CATEGORY_REJECTION.xlsx'
        # LAB_CATEGORY_REJECTION_df = PANDAS.read_excel(LAB_CATEGORY_REJECTION_path, sheet_name='REJECTION', encoding='utf8') 
        cle = 'lab category mapping'
        requete = 'SELECT dictline.In1 as LABEL, dictline.Out1 as Category, dictline.Id as IdDictLine FROM dictline LEFT JOIN dict on dictline.IdDict = dict.Id WHERE TableTechnical = "' + cle + '";' 
        LAB_CATEGORY_df = PANDAS.read_sql_query(requete, engine)
        # print(LAB_CATEGORY_df)

        # requete = 'Select * from dictline where IdDict = 21'
        # test = PANDAS.read_sql_query(requete, engine)
        # print(test)
        # LAB_CATEGORY_df = PANDAS.read_excel(LAB_CATEGORY_path, sheet_name='MAPPING', encoding='utf8') 

        # print(TABLES_df)

        self.TABLES_PCSA_STUDYCONCEPT = {}
        CATEGORY = ['HEMATOLOGY','CHEMISTRY','URINALYSIS','VITAL_SIGNS','ECG','TMP']
        for Category in CATEGORY:
            self.TABLES_PCSA_STUDYCONCEPT['LAB_DESC_'+Category] = {}
            self.TABLES_PCSA_STUDYCONCEPT['LAB_PCSA_'+Category] = {}


        # print(self.TABLES_PCSA_STUDYCONCEPT)
        self.TABLES_df = PANDAS.merge(TABLES_df, LAB_CATEGORY_df, how='left', left_on=['studyConceptTag'] , right_on=['LABEL'], indicator = True)
        self.TABLES_df['mainWording'] = self.TABLES_df['LABEL']
        self.TABLES_df = self.TABLES_df.loc[self.TABLES_df['HASFILE'] == True]
        self.TABLES_df.drop_duplicates(subset=['fileName','documentTableTag','studyConceptTag'], inplace = True)
        self.TABLES_df.reset_index(drop=True, inplace=True)
        
        # print(self.TABLES_df.fileName)
        # print(self.TABLES_df)
        # print(LAB_CATEGORY_df)

        ## Check for non Mapping
        CHECK_CATEGORY_REJECTION = False
        LAB_CATEGORY_REJECTION_new_df = {'LABEL' : [], 'fileName' : []}
        for ROW_id, ROW in self.TABLES_df.iterrows():
            if ROW['HASFILE']:
                if re.findall("LAB_", str(ROW['documentTableTag'])): 
                    if str(ROW['Category']) == 'nan':
                        self.TABLES_df.loc[ROW_id,('Category')] = 'TMP'
                        LAB_CATEGORY_REJECTION_new_df['LABEL'].append(str(ROW['studyConceptTag']))
                        LAB_CATEGORY_REJECTION_new_df['fileName'].append(str(ROW['fileName']))
                        CHECK_CATEGORY_REJECTION = True


        ### fill LABEL table
        #get data
        # print(self.TABLES_df)
        LABEL_df = self.TABLES_df[(self.TABLES_df['documentTableTag'] == 'LAB_DESC') | (self.TABLES_df['documentTableTag'] == 'LAB_PCSA')][['studyConceptTag','IdDictLine','IdSourceFile']]
        # print(LABEL_df)
        # LABEL_df['Category'].replace(to_replace = 'TMP', value = 'None', inplace = True)
        LABEL_df['Text2'] = None
        LABEL_df.rename(columns = {'studyConceptTag':'Text1'}, inplace = True)
        LABEL_df.drop_duplicates(subset=['Text1'], inplace = True)
        IdDict = 21
        IdStudy = PANDAS.read_sql_query('SELECT Id FROM study WHERE Compound = "%s" '\
            'and StudyName = "%s" and PeriodAnalysis = "%s"' % (self.key[0], self.key[1], self.key[2]), engine)['Id'].tolist()[0]
        LABEL_BDD_length = PANDAS.read_sql_query('select max(Id) as max from label', engine)['max'].tolist()[0]
        if str(LABEL_BDD_length) == 'None':
            LABEL_BDD_length = 0
        LABEL_BDD_length+=1
        LABEL_df['Text2'] = None
        LABEL_df['IdDict'] = IdDict
        LABEL_df['IdStudy'] = IdStudy
        LABEL_df['Impact'] = 0
        LABEL_df['Id'] = [i for i in range(LABEL_BDD_length,LABEL_BDD_length + LABEL_df.shape[0])]
        # LABEL_df['IdDictLine'] = int(LABEL_df['IdDictLine'])

        # print(LABEL_df)
        # exit()
        LABEL_df.to_sql('label', engine, index = False, if_exists='append')
        

        # print(PANDAS.read_sql_query('select * from label', engine))
        # exit()

        # if CHECK_CATEGORY_REJECTION == True:
        #     # LAB_CATEGORY_REJECTION_df['LABEL'] = list(NUMPY.unique(LAB_CATEGORY_REJECTION_df['LABEL']))
        #     # print(LAB_CATEGORY_REJECTION_df['LABEL'])
        #     # print(LAB_CATEGORY_REJECTION_df)
        #     LAB_CATEGORY_REJECTION_new_df = PANDAS.DataFrame(LAB_CATEGORY_REJECTION_new_df)

        #     LAB_CATEGORY_REJECTION_new_df.loc[:,'COMPOUND'] = self.key[0]
        #     LAB_CATEGORY_REJECTION_new_df.loc[:,'STUDY_NAME'] = self.key[1]
        #     LAB_CATEGORY_REJECTION_new_df.loc[:,'PERIOD_ANALYSIS'] = self.key[2]


        #     LAB_CATEGORY_REJECTION_df = PANDAS.concat([LAB_CATEGORY_REJECTION_df,LAB_CATEGORY_REJECTION_new_df],ignore_index = True)
        #     LAB_CATEGORY_REJECTION_df = LAB_CATEGORY_REJECTION_df.reindex(['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'], axis=1)
        #     LAB_CATEGORY_REJECTION_df.drop_duplicates(subset=['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'],inplace = True)



        #     LAB_CATEGORY_REJECTION_df.to_excel(excel_writer=LAB_CATEGORY_REJECTION_path, index=True, sheet_name='REJECTION')

        #     LAB_CATEGORY_REJECTION_df.drop_duplicates(['LABEL'],inplace = True)

        #     self.CHECK_REJECTION['status'] = True
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY'] = {}
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['study'] = self.key
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['title'] = 'Rejections regarding studyConcepTag and Category for LAB tables'
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['rejectionFile'] = LAB_CATEGORY_REJECTION_path
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['mappingFile'] = LAB_CATEGORY_path
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['dataFile'] = None
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['documentTableTag'] = None
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['studyConceptTag'] = None
        #     self.CHECK_REJECTION['rejection']['LAB_CATEGORY']['rejets'] = LAB_CATEGORY_REJECTION_df
            # print(LAB_CATEGORY_REJECTION_df)

        # print(' ')
        # print(TABLES_df)


        for ROW_id, ROW in self.TABLES_df.iterrows():
            TABLE_hasfile = ROW['HASFILE']
            
            if TABLE_hasfile:
                # print(ROW['fileName'])
                if re.findall("LAB_", str(ROW['documentTableTag'])):
                    # print('1')
                    # print(ROW['fileName'])
                    # self.TABLES_PCSA_STUDYCONCEPT[str(ROW['documentTableTag'])]
                    documentTableTag = str(ROW['documentTableTag']) + '_' + str(ROW['Category'])
                    studyConceptTag = str(ROW['studyConceptTag']).replace(' ','_')
                    for ponct in ponctuation:
                        studyConceptTag = studyConceptTag.replace(ponct, '')

                    for (char, accented_chars) in accents.items():
                        for accented_char in accented_chars:
                            studyConceptTag = studyConceptTag.replace(accented_char, char)



                    if studyConceptTag in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag]:
                        self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag].append(str(ROW['fileName']))
                    else:
                        self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag] = [str(ROW['fileName'])]

        # print(self.TABLES_PCSA_STUDYCONCEPT)
        # exit()


    ###### NOT MODIFIED ######  
    def concatenate_table(self):
        TABLE_PCSA = {}
        CATEGORY = ['HEMATOLOGY','CHEMISTRY','URINALYSIS','VITAL_SIGNS','ECG','TMP']
        for Category in CATEGORY:
            TABLE_PCSA['LAB_DESC_'+Category] = {}
            TABLE_PCSA['LAB_PCSA_'+Category] = {}
        CHECK_STATS = False
        CHECK_NUMBER = False
        INDEX = []
        c = 0

        # print(self.TABLES_PCSA_STUDYCONCEPT)

        for documentTableTag in self.TABLES_PCSA_STUDYCONCEPT:
            # print(documentTableTag)
            # print(' ')
            # print(' ')
            for studyConceptTag in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag]:
                # print(studyConceptTag)
                # print(' ')
                nbfile = 1
                for fileName in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag]:
                    # print(fileName)
                    if fileName not in self.list_files_separated:
                        path = self.path+'ANALYSIS_SDS/'+fileName+'.sas7bdat'
                        if nbfile == 1:
                            INDEX.append(c)
                            TABLE = PANDAS.read_sas(filepath_or_buffer=path, format='sas7bdat', encoding='iso-8859-1')
                            # TABLE = PANDAS.read_excel(path, sheet_name='Dataframe', encoding='utf8') 
                            TABLE_PCSA[documentTableTag][studyConceptTag] = TABLE
                        elif nbfile > 1:
                            # print('TABLE_CONCATENATION')
                            TABLE = PANDAS.read_sas(filepath_or_buffer=path, format='sas7bdat', encoding='iso-8859-1')
                            # TABLE = PANDAS.read_excel(path, sheet_name='Dataframe', encoding='utf8') 
                            TABLE_PCSA[documentTableTag][studyConceptTag] = PANDAS.concat([TABLE_PCSA[documentTableTag][studyConceptTag],TABLE], ignore_index = True)

                    else:
                        if nbfile == 1:
                            INDEX.append(c)


                    nbfile += 1
                    c += 1

        # print(self.TABLES_PCSA_STUDYCONCEPT)
        # exit()

 
        for documentTableTag in TABLE_PCSA:
            # print(documentTableTag)
            # print(' ')
            # print(' ')
            for studyConceptTag in TABLE_PCSA[documentTableTag]:
                # print(studyConceptTag)
                # print(' ')
                # studyConceptTag_Name = studyConceptTag.replace(' ','_')
                # for ponct in ponctuation:
                #     studyConceptTag_Name = studyConceptTag_Name.replace(ponct, '')

                # for (char, accented_chars) in accents.items():
                #     for accented_char in accented_chars:
                #         studyConceptTag_Name = studyConceptTag_Name.replace(accented_char, char)


                fileName = documentTableTag+'_'+studyConceptTag
                path = self.path + 'ANALYSIS_SDS/' + fileName + '.xlsx'
                # print(TABLE_PCSA[documentTableTag][studyConceptTag])
                # print(' ')
                # print(studyConceptTag_Name)
                # print(' ')
                # TABLE_PCSA[documentTableTag][studyConceptTag_Name] = TABLE_PCSA[documentTableTag].pop(studyConceptTag)
                TABLE_PCSA[documentTableTag][studyConceptTag].to_excel(excel_writer=path, index=True, sheet_name='DATA')
                # self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag_Name] = self.TABLES_PCSA_STUDYCONCEPT[documentTableTag].pop(studyConceptTag)


        index_to_remove = []
        PCSA_METADATAS =\
        {
            'fileType' : [],
            'documentTableTag' : [],
            'studyConceptTag' : [],
            'mainWording' : [],
            # 'unit' : [],
            'documentTableID' : [],
            'fileName' : [],
            'key' : [],
            'documentTableLocation': [],
            'documentTablePart': [],
            'ongoingNumber': [],     
            'observationPeriod': [], 
            'referencePopulation' : [],     
            'location1' : [],
            'location2' : [],
            'location3' : [],
            'location4' : [],
            'location5' : [],
            'location6' : [],
            'INDEX' : [],
            'Appendix' : [],
            'Section' : [],
            'Title 2' : [],
            'Number' : [],
            'TLF number' : [],
        }

        path = self.directory['PARAMETERS']['__PCSA']['PATH']+'/'+'LAB_CATEGORY_MAPPING.xlsx'
        LAB_CATEGORY= PANDAS.read_excel(path, sheet_name='MAPPING', encoding='utf8')
        label_rejection_stats = {'studyConceptTag' : []}
        label_rejection_number = {'studyConceptTag' : []}



        for documentTableTag in self.TABLES_PCSA_STUDYCONCEPT:
            for studyConceptTag in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag]:
                # print(studyConceptTag)
                for fileName in self.TABLES_PCSA_STUDYCONCEPT[documentTableTag][studyConceptTag]:
                    # print(fileName)
                    PCSA_METADATAS['fileType'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['fileType'].values[0])
                    PCSA_METADATAS['documentTableTag'].append(documentTableTag)
                    PCSA_METADATAS['studyConceptTag'].append(studyConceptTag)
                    PCSA_METADATAS['documentTableID'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['documentTableID'].values[0])
                    PCSA_METADATAS['fileName'].append(fileName)
                    PCSA_METADATAS['key'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['KEY'].values[0])
                    PCSA_METADATAS['documentTableLocation'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['documentTableLocation'].values[0])
                    PCSA_METADATAS['documentTablePart'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['documentTablePart'].values[0])
                    PCSA_METADATAS['ongoingNumber'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['ongoingNumber'].values[0])
                    PCSA_METADATAS['observationPeriod'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['observationPeriod'].values[0])
                    PCSA_METADATAS['referencePopulation'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['referencePopulation'].values[0])
                    PCSA_METADATAS['location1'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location1'].values[0])
                    PCSA_METADATAS['location2'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location2'].values[0])
                    PCSA_METADATAS['location3'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location3'].values[0])
                    PCSA_METADATAS['location4'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location4'].values[0])
                    PCSA_METADATAS['location5'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location5'].values[0])
                    PCSA_METADATAS['location6'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['location6'].values[0])

                    PCSA_METADATAS['INDEX'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['INDEX'].values[0])
                    PCSA_METADATAS['mainWording'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['mainWording'].values[0])
                            
                    if 'Appendix' in self.TABLES_df.columns:
                        PCSA_METADATAS['Appendix'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['Appendix'].values[0])
                        PCSA_METADATAS['Section'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['Section'].values[0])
                        PCSA_METADATAS['Title 2'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['Title 2'].values[0])
                        PCSA_METADATAS['Number'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['Number'].values[0])
                        PCSA_METADATAS['TLF number'].append(self.TABLES_df[self.TABLES_df['fileName']== fileName]['TLF number'].values[0])

                    else:
                        PCSA_METADATAS['Appendix'].append('[null]')
                        PCSA_METADATAS['Section'].append('[null]')
                        PCSA_METADATAS['Title 2'].append('[null]')
                        PCSA_METADATAS['Number'].append('[null]')
                        PCSA_METADATAS['TLF number'].append('[null]')

                    # if documentTableTag == 'LAB_DESC':

                    #     if len(LAB_CATEGORY[LAB_CATEGORY['studyConceptTag'] == studyConceptTag]['Category']) > 0:
                    #         PCSA_METADATAS['mainWording'].append(LAB_CATEGORY[LAB_CATEGORY['studyConceptTag'] == studyConceptTag]['Category'].values[0])
                    #         # PCSA_METADATAS['unit'].append(LAB_DESC_METADATAS[LAB_DESC_METADATAS['studyConceptTag'] == studyConceptTag]['unit'].values[0])
                    #     else:
                    #         CHECK_STATS = True
                    #         PCSA_METADATAS['mainWording'].append('None')
                    #         # PCSA_METADATAS['unit'].append('None')
                    #         label_rejection_stats['studyConceptTag'].append(studyConceptTag)
                            

                    # elif documentTableTag == 'LAB_PCSA':

                    #     if len(LAB_CATEGORY[LAB_CATEGORY['studyConceptTag'] == studyConceptTag]['Category']) > 0:
                    #         PCSA_METADATAS['mainWording'].append(LAB_CATEGORY[LAB_CATEGORY['studyConceptTag'] == studyConceptTag]['Category'].values[0])
                    #         # PCSA_METADATAS['unit'].append(LAB_PCSA_METADATAS[LAB_PCSA_METADATAS['studyConceptTag'] == studyConceptTag]['unit'].values[0])
                    #     else:
                    #         CHECK_NUMBER = True
                    #         PCSA_METADATAS['mainWording'].append('None')
                    #         # PCSA_METADATAS['unit'].append('None')
                    #         label_rejection_number['studyConceptTag'].append(studyConceptTag)
                    self.TABLES_df = self.TABLES_df.drop(self.TABLES_df[self.TABLES_df['fileName']== fileName].index.tolist()[0])
                    # index_to_remove.append(self.TABLES_df[self.TABLES_df['fileName']== fileName].index.tolist()[0])

        # for key in PCSA_METADATAS.keys():
        #     print(key)
        #     print(len(PCSA_METADATAS[key]))
        #     print(' ')

        # print(PCSA_METADATAS)

        self.PCSA_METADATAS = PANDAS.DataFrame(PCSA_METADATAS)
        # self.TABLES_df.drop(self.TABLES_df.index[index_to_remove], inplace = True)
        self.PCSA_METADATAS = self.PCSA_METADATAS.loc[INDEX]
        # print(self.PCSA_METADATAS)
        # print(index_to_remove)
        # print(self.TABLES_df)
        # print('\n\n\n')
        # print(self.PCSA_METADATAS)
        # exit()


    ###### NOT MODIFIED ######
    def LISTING_TABLE(self):
        if 'ALL_AE_LISTING' in self.TABLES_df.documentTableTag.tolist():
            LISTING_RULES_FILTER_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
            LISTING_RULES_FILTER_df = PANDAS.read_excel(LISTING_RULES_FILTER_path, sheet_name='LISTING', encoding='utf8')
            # print(self.TABLES_df)
            ALL_AE_LISTING_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+str(self.TABLES_df.fileName[self.TABLES_df['documentTableTag'] == 'ALL_AE_LISTING'].values[0])+'.sas7bdat'
            # print(ALL_AE_LISTING_path)
            ALL_AE_LISTING_df = PANDAS.read_sas(filepath_or_buffer=ALL_AE_LISTING_path, format='SAS7BDAT', encoding='iso-8859-1')
            

            if 'DEATHS_AE_LISTING' not in self.TABLES_df.documentTableTag.tolist():
                RULES_VALUE_FILTER_DEATH = LISTING_RULES_FILTER_df.DEATHS_AE_LISTING[1]
                RULES_COLUMN_DEATH = LISTING_RULES_FILTER_df.DEATHS_AE_LISTING[0]
                # print(ALL_AE_LISTING_df[RULES_COLUMN_DEATH])

                DEATHS_AE_LISTING_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+'DEATHS_AE_LISTING'+'.xlsx'

                DEATHS_AE_LISTING_df = ALL_AE_LISTING_df.loc[ALL_AE_LISTING_df[RULES_COLUMN_DEATH] == RULES_VALUE_FILTER_DEATH]
                DEATHS_AE_LISTING_df.reset_index(drop=True, inplace=True)
                if DEATHS_AE_LISTING_df.shape[0] == 0:
                    pass
                else:
                    DEATHS_AE_LISTING_df.to_excel(excel_writer=DEATHS_AE_LISTING_path, index=True, sheet_name='DATA')

                    COLUMNS = self.TABLES_df.columns
                    COLUMNS_to_fill = ['fileType','referencePopulation','documentTablePart','ongoingNumber','observationPeriod']
                    NEW_ROW = {}
                    for COLUMN in COLUMNS:
                        NEW_ROW[COLUMN] = ['null']

                        if COLUMN in COLUMNS_to_fill:
                            NEW_ROW[COLUMN] = [self.TABLES_df[self.TABLES_df['documentTableTag'] == 'ALL_AE_LISTING'][COLUMN].values[0]]

                    NEW_ROW['fileType'] = ['LISTING_EXCEL']
                    NEW_ROW['fileName'] = ['DEATHS_AE_LISTING']
                    NEW_ROW['documentTableTag'] = ['DEATHS_AE_LISTING']

                    NEW_ROW = PANDAS.DataFrame(NEW_ROW)
                    self.TABLES_df = PANDAS.concat([self.TABLES_df,NEW_ROW],ignore_index=True)

                # print('1')


            if 'DISCONTINUATION_AE_LISTING' not in self.TABLES_df.documentTableTag.tolist():
                RULES_VALUE_FILTER_DISCONTINUATION = LISTING_RULES_FILTER_df.DISCONTINUATION_AE_LISTING[1]
                RULES_COLUMN_DISCONTINUATION = LISTING_RULES_FILTER_df.DISCONTINUATION_AE_LISTING[0]

                DISCONTINUATION_AE_LISTING_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+'DISCONTINUATION_AE_LISTING'+'.xlsx'

                DISCONTINUATION_AE_LISTING_df = ALL_AE_LISTING_df.loc[ALL_AE_LISTING_df[RULES_COLUMN_DISCONTINUATION] == RULES_VALUE_FILTER_DISCONTINUATION]
                DISCONTINUATION_AE_LISTING_df.reset_index(drop=True, inplace=True)
                DISCONTINUATION_AE_LISTING_df.to_excel(excel_writer=DISCONTINUATION_AE_LISTING_path, index=True, sheet_name='DATA')

                COLUMNS = self.TABLES_df.columns
                COLUMNS_to_fill = ['documentTableTag','fileType','referencePopulation','documentTablePart','ongoingNumber','observationPeriod']
                NEW_ROW = {}
                for COLUMN in COLUMNS:
                    NEW_ROW[COLUMN] = ['null']

                    if COLUMN in COLUMNS_to_fill:
                        NEW_ROW[COLUMN] = [self.TABLES_df[self.TABLES_df['documentTableTag'] == 'ALL_AE_LISTING'][COLUMN].values[0]]

                NEW_ROW['fileType'] = ['LISTING_EXCEL']
                NEW_ROW['fileName'] = ['DISCONTINUATION_AE_LISTING']
                NEW_ROW['documentTableTag'] = ['DISCONTINUATION_AE_LISTING']

                NEW_ROW = PANDAS.DataFrame(NEW_ROW)
                self.TABLES_df = PANDAS.concat([self.TABLES_df,NEW_ROW],ignore_index=True)

                # print('2')


            if 'SERIOUS_AE_LISTING' not in self.TABLES_df.documentTableTag.tolist():
                RULES_VALUE_FILTER_SERIOUS = LISTING_RULES_FILTER_df.SERIOUS_AE_LISTING[1]
                RULES_COLUMN_SERIOUS = LISTING_RULES_FILTER_df.SERIOUS_AE_LISTING[0]

                SERIOUS_AE_LISTING_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_SDS/'+'SERIOUS_AE_LISTING'+'.xlsx'

                SERIOUS_AE_LISTING_df = ALL_AE_LISTING_df.loc[ALL_AE_LISTING_df[RULES_COLUMN_SERIOUS] == RULES_VALUE_FILTER_SERIOUS]
                SERIOUS_AE_LISTING_df.reset_index(drop=True, inplace=True)
                SERIOUS_AE_LISTING_df.to_excel(excel_writer=SERIOUS_AE_LISTING_path, index=True, sheet_name='DATA')

                COLUMNS = self.TABLES_df.columns
                COLUMNS_to_fill = ['documentTableTag','fileType','referencePopulation','documentTablePart','ongoingNumber','observationPeriod']
                NEW_ROW = {}
                for COLUMN in COLUMNS:
                    NEW_ROW[COLUMN] = ['null']

                    if COLUMN in COLUMNS_to_fill:
                        NEW_ROW[COLUMN] = [self.TABLES_df[self.TABLES_df['documentTableTag'] == 'ALL_AE_LISTING'][COLUMN].values[0]]

                NEW_ROW['fileType'] = ['LISTING_EXCEL']
                NEW_ROW['fileName'] = ['SERIOUS_AE_LISTING']
                NEW_ROW['documentTableTag'] = ['SERIOUS_AE_LISTING']

                NEW_ROW = PANDAS.DataFrame(NEW_ROW)
                self.TABLES_df = PANDAS.concat([self.TABLES_df,NEW_ROW],ignore_index=True)


                # print('3')

            # print(self.TABLES_df)


    ###### NEW ######  
    def read_AESI(self):
        # print(self.TABLES_df)
        AESI_TABLES_index = self.TABLES_df[self.TABLES_df['documentTableTag'] == 'AESI_ANALYSIS'].index
        AESI_TABLES = self.TABLES_df[self.TABLES_df['documentTableTag'] == 'AESI_ANALYSIS']
        AESI_TABLES.reset_index(drop = True, inplace = True)

        if len(AESI_TABLES_index) <2:
            # print('ici !!!!')
            pass

        else:
            i = 0
            

            for index in AESI_TABLES_index:
                # print(self.TABLES_df.loc[index,'fileName'])
                path = self.path+'ANALYSIS_SDS/'+self.TABLES_df.loc[index,'fileName']+'.sas7bdat'

                if i==0:
                    AESI_ANALYSIS_df = PANDAS.read_sas(filepath_or_buffer=path, format='sas7bdat', encoding='iso-8859-1')
                    AESI_ANALYSIS_df['fileName'] = AESI_TABLES.loc[i,'fileName']
                elif i>0:
                    AESI_ANALYSIS_df_tmp = PANDAS.read_sas(filepath_or_buffer=path, format='sas7bdat', encoding='iso-8859-1')
                    AESI_ANALYSIS_df_tmp['fileName'] = AESI_TABLES.loc[i,'fileName']
                    AESI_ANALYSIS_df = PANDAS.concat([AESI_ANALYSIS_df,AESI_ANALYSIS_df_tmp], ignore_index = True)
                i = i+1



            path = self.path+'ANALYSIS_SDS/'+'AESI_ANALYSIS'+'.xlsx'
            AESI_ANALYSIS_df.to_excel(excel_writer=path, index=True, sheet_name='DATA')
            index = self.TABLES_df.shape[0]+1


            self.TABLES_df.loc[index,'fileName'] = self.TABLES_df.loc[AESI_TABLES_index[0],'fileName']
            self.TABLES_df.loc[index,'fileType'] = 'AESI_ANALYSIS'
            self.TABLES_df.loc[index,'documentTableTag'] = 'AESI_ANALYSIS'
            self.TABLES_df.loc[index,'referencePopulation'] = self.TABLES_df.loc[AESI_TABLES_index[0],'referencePopulation']
            self.TABLES_df.loc[index,'documentTablePart'] = self.TABLES_df.loc[AESI_TABLES_index[0],'documentTablePart']
            self.TABLES_df.loc[index,'INDEX'] = self.TABLES_df.loc[AESI_TABLES_index[0],'INDEX']
            self.TABLES_df.loc[index,'HASFILE'] = True
            self.TABLES_df.loc[index,'ongoingNumber'] = '[null]'
            self.TABLES_df.loc[index,'observationPeriod'] = '[null]'


            self.TABLES_df = self.TABLES_df.drop(AESI_TABLES_index)
            self.TABLES_df.reset_index(inplace = True, drop = True)



        # print(self.TABLES_df)


    ###### NOT MODIFIED ######
    def read_GROUP_DESCRIPTION(self):
        TABLES_GROUP_DESCRIPTION_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
        self.TABLES_GROUP_DESCRIPTION_df = PANDAS.read_excel(TABLES_GROUP_DESCRIPTION_path, sheet_name='GROUP_DESCRIPTION', encoding='utf8') 
        TABLE_GROUP_DESCRIPTION_dict = {}

        if 'All' in self.TABLES_GROUP_DESCRIPTION_df.groupName.tolist():
            index_to_remove = self.TABLES_GROUP_DESCRIPTION_df[self.TABLES_GROUP_DESCRIPTION_df.groupName == 'All'].index
            self.TABLES_GROUP_DESCRIPTION_df = self.TABLES_GROUP_DESCRIPTION_df.drop(index_to_remove)



        for ROW_id, ROW in self.TABLES_GROUP_DESCRIPTION_df.iterrows():
            flagName = True if ROW['groupSpecificity'] == 'PLACEBO' else False
            if flagName == False:
                if ROW['groupName'].find('^') != -1:
                    groupName = str(ROW['groupName']).split('^ ',1)[1]
                    name = str(ROW['groupName']).split('^ ',1)[0]

                else:
                    groupName = str(ROW['groupName']).split(' ',1)[0]
                    name = str(ROW['groupName']).split(' ',1)[1:]
                    # print(groupName)
                    # print(name)              
            # print(str(ROW['groupName']).split('^ ',1)[1])
                values = re.findall('[0-9]*\.[0-9]*',groupName)
                # print(values)
                
                # print(values)
            else:
                groupName = "placebo"
                values = None
                name = groupName

            if name == []:
                name = ROW['groupName']
				
            TABLE_GROUP_DESCRIPTION_dict[ROW_id] =\
            {
                # 'identifier':                str(ROW['identifier']),
                'groupId':                   str(ROW['groupId']),
                # 'groupName':                 str(ROW['groupName']),
                'groupSpecificity':          str(ROW['groupSpecificity']), 
                'csrLabel':                  str(ROW['csrLabel']),
				'listingName':               str(ROW['listingName']),
				'flagName':					 flagName,
				'groupName':				 groupName,
				'values':					 values,
                'name':                      name,

            }
        self.TABLE_GROUP_DESCRIPTION_dict = TABLE_GROUP_DESCRIPTION_dict  
        self.TABLE_GROUP_DESCRIPTION_df  = PANDAS.DataFrame(self.TABLE_GROUP_DESCRIPTION_dict)
        self.TABLE_GROUP_DESCRIPTION_df = self.TABLE_GROUP_DESCRIPTION_df.transpose()

        print(self.TABLE_GROUP_DESCRIPTION_df)
        # exit()

    ###### NOT MODIFIED ######
    def read_TABLES_METADATAS(self):
        TABLES_METADATAS_path = self.directory['PARAMETERS']['__METADATAS']['PATH']+'/'+'METADATAS.xlsx'
        self.TABLES_METADATAS_df = PANDAS.read_excel(TABLES_METADATAS_path, sheet_name='METADATAS', encoding='utf8') 
        # print(self.TABLES_METADATAS_df.columns)
        self.TABLES_METADATAS_df.loc[:,'INDEX']= self.TABLES_METADATAS_df['INDEX'].apply(lambda n : str(n).zfill(4))
        self.TABLES_METADATAS_df.loc[:,'INDEX'] = self.TABLES_METADATAS_df['INDEX'].astype(str) + '.' + self.TABLES_METADATAS_df['NAME']


    ###### NOT MODIFIED ######
    def read_TABLES_FILENAMES(self):
        TABLES_FILENAMES_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
        self.TABLES_FILENAMES_df = PANDAS.read_excel(TABLES_FILENAMES_path, sheet_name='FILENAMES', encoding='utf8')


        # print(self.TABLES_FILENAMES_df.head(5))
        # print(self.TABLES_FILENAMES_df.columns)


    ###### NOT MODIFIED ######
    def ADD_tableLocation(self):
        for irow, row in self.PCSA_METADATAS.iterrows():
            if row['Appendix'] != '[null]':
                if row[('Appendix')] != 'yes':
                    self.PCSA_METADATAS.loc[irow,'tableLocation'] = row['Section']+'-'+row['Title 2']+'-'+row['Number']

                else:
                    self.PCSA_METADATAS.loc[irow,'tableLocation'] = row['TLF number']
            else:
                self.PCSA_METADATAS.loc[irow,'tableLocation'] = '[null]'

        # print(self.TABLES_df)
        for irow, row in self.TABLES_df.iterrows():
            if row['fileType'] != "LISTING":
                if 'Appendix' in self.TABLES_df.columns:
                    if row[('Appendix')] != 'yes':
                        self.TABLES_df.loc[irow,'tableLocation'] = row['Section']+'-'+row['Title 2']+'-'+row['Number']

                    else:
                        self.TABLES_df.loc[irow,'tableLocation'] = row['TLF number']

                else:
                    self.TABLES_df.loc[irow,'tableLocation'] = '[null]'
            else:
                self.TABLES_df.loc[irow,'tableLocation'] = '[null]'

        # print(self.TABLES_df)
        # print(self.PCSA_METADATAS)
        #     print(row)
        #     print(' ')
        #     print(' ')


    def get_group_metadata(self):
        TABLES_PART4_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
        self.TABLES_PART4_df = PANDAS.read_excel(TABLES_PART4_path, sheet_name='GROUP_DESCRIPTION', encoding='utf8')

        self.GROUP_METADATA_DICT = \
        {
            'columnTotalType' : [],
            'columnId' :        [],
            'columnLabel' :     [],
            'columnTotal' :     [],
        }


        for ROW_id, ROW in self.TABLES_df.iterrows():
            TABLE_hasfile = ROW['HASFILE']
            if TABLE_hasfile:
                STRUCTURE = self.set_STRUCTURE(ROW['fileType'])

                if STRUCTURE == 'LISTING_EXCEL':
                    TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.xlsx'
                    df = PANDAS.read_excel(io = TABLE_path, sheet_name= 'DATA', encoding='utf8')

                else:
                    TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.sas7bdat'
                    df = PANDAS.read_sas(filepath_or_buffer=TABLE_path, format='SAS7BDAT', encoding='iso-8859-1')

                df = df[df['__datatype'] == 'HEAD']

                COLUMNS_GROUP = []
                COLUMNS_id = df.columns.values
                for COLUMN_id in COLUMNS_id:
                    if re.match('__col_(.)+', COLUMN_id) != None:
                        if COLUMN_id != '__col_0':
                            COLUMNS_GROUP.append(COLUMN_id)

                df = df[COLUMNS_GROUP]

                if df.shape[0] == 1:
                    GROUP_METADATA_check, GROUP_METADATA_DICT = self.get_group_metadata_detail(df = df,
                        GROUP_METADATA_DICT = self.GROUP_METADATA_DICT)

                    # print('\n\n########################################')
                    # print('######## GROUP_METADATA_check ##########')
                    # print(GROUP_METADATA_check)


                    if GROUP_METADATA_check == True:
                        self.GROUP_METADATA_df = PANDAS.DataFrame(GROUP_METADATA_DICT)
                        # print(self.GROUP_METADATA_df)
                        # exit()
                        
                        return True

        return False


    def get_group_metadata_detail(self, df, GROUP_METADATA_DICT):
        GROUP_METADATA_check = False
        GROUPS_desc = df.loc[0].values.tolist()
        # print(GROUPS_desc)
        for ROW_id, ROW in self.TABLES_PART4_df.iterrows():
            # print('\n#####  NEW ROW ######')
            # print(ROW)
            # print('')
            for i, GROUP_desc in enumerate(GROUPS_desc):

                if str(ROW['groupName']).find('^') or GROUP_desc.find('^') != -1:
                    # print('la')
                    GROUP_name_clean = ROW['groupName'].replace('^','')
                    GROUP_desc_clean = GROUP_desc.replace('^','')
                else:
                    GROUP_name_clean = ROW['groupName']
                    GROUP_desc_clean = GROUP_desc

                # print(GROUP_desc_clean)
                # print(GROUP_name_clean)


                if GROUP_desc_clean.find(GROUP_name_clean) != -1:
                    GROUP_id = ROW['groupId']
                    CHECK_POP = False
                    CHECK_PY = False

                    # GROUP_desc_clean = GROUP_desc
                    # print('FINDED !!!!!')
                    # print(GROUP_desc_clean)

                    if re.findall(".*?\(", str(GROUP_desc_clean)):
                        CHECK_POP = True if re.findall("\(N=\d+", str(GROUP_desc_clean)) != [] else False
                        CHECK_PY =  True if  re.findall("\(PY=\d+", str(GROUP_desc_clean)) != [] else False

                        if CHECK_POP == True :
                            value_1 = re.findall("\(N=\d+", str(GROUP_desc_clean))[0]
                            value_2 = re.findall("\d+",str(value_1))
                            value =   GROUP_name_clean
                            GROUP_METADATA_DICT['columnTotalType'].append('POP_NUMBER')
                            GROUP_METADATA_DICT['columnId'].append(str(GROUP_id))
                            GROUP_METADATA_DICT['columnLabel'].append(str(value))
                            GROUP_METADATA_DICT['columnTotal'].append(value_2[0])

                            # print('check !!!!')
                            GROUP_METADATA_check = True



        return GROUP_METADATA_check, GROUP_METADATA_DICT

    ###### TO DELETE ? ######
    # def set_TABLES(self):
        
    #     # Normal Tables
    #     # print(self.TABLES_df)
    #     for ROW_id, ROW in self.TABLES_df.iterrows():
    #         TABLE_hasfile = ROW['HASFILE']
    #         # self.CHECK_REJECTION = False
            
    #         if TABLE_hasfile:
    #             # print(ROW)
    #             TABLE_key = ROW['INDEX']
    #             # print(TABLE_key)
    #             TABLE_name = ROW['fileName']
    #             TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.sas7bdat'
    #             STRUCTURE = self.set_STRUCTURE(ROW['fileType'])

    #             if STRUCTURE == 'LISTING_EXCEL':
    #                 TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.xlsx'

    #             TABLE_METADATAS_dict =\
    #             {
    #                 'fileName':                 str(ROW['fileName']),
    #                 'fileType':                 str(ROW['fileType']),
    #                 'documentTableTag':         str(ROW['documentTableTag']),
    #                 'documentTableID':          str(ROW['documentTableID']),
    #                 'documentTableLocation':    str(ROW['documentTableLocation']),
    #                 'studyConceptTag':          'nan', #que pour pharma 11.4 et 11.5 
    #                 'documentTablePart':        str(ROW['documentTablePart']), #que pour vaccin et peut être ANY_PART (After Any Vaccine Injection) ou BOOSTER (After Booster Vaccine Injection). 
    #                 'ongoingNumber':            str(ROW['ongoingNumber']), #que pour vaccin
    #                 'observationPeriod':        str(ROW['observationPeriod']), #que pour vaccin
    #                 'referencePopulation':      str(ROW['referencePopulation']),
    #                 'tableLocation':            str(ROW['tableLocation']),
    #                 'values' : [
    #                     ROW['location1'], 
    #                     ROW['location2'], 
    #                     ROW['location3'],
    #                     ROW['location4'],
    #                     ROW['location5'],
    #                     ROW['location6']]    
    #             }
    #             # print(TABLE_name)
    #             TABLE_ = TABLE(key=TABLE_key, METADATAS_dict=TABLE_METADATAS_dict, study = self.key)
    #             TABLE_.read_SAS(path=TABLE_path, structure=STRUCTURE)

    #             if not TABLE_.check_empty:
    #                 self.UPDATE_REJECTIONS(NEW_REJECTION = TABLE_.CHECK_REJECTION)
    #                 self.sas_update.append(TABLE_.sas_update)

    #                 TABLE_etree = TABLE_.etree
    #                 self.TABLES_dict[TABLE_name] = {}
    #                 self.TABLES_dict[TABLE_name]['ETREE'] = TABLE_etree
    #                 if self.verbose: print('######## '+'[LOAD]'+' '+TABLE_name)

    ###### NEW ######  
    def set_TABLES(self):
        
        # Normal Tables
        # print(self.TABLES_df)
        for ROW_id, ROW in self.TABLES_df.iterrows():
            TABLE_hasfile = ROW['HASFILE']
            # self.CHECK_REJECTION = False
            
            if TABLE_hasfile:
                # print(ROW)
                TABLE_key = ROW['INDEX']
                # print(TABLE_key)
                TABLE_name = ROW['fileName']
                TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.sas7bdat'
                STRUCTURE = self.set_STRUCTURE(ROW['fileType'])

                if STRUCTURE == 'LISTING_EXCEL':
                    TABLE_path = self.path+'ANALYSIS_SDS/'+ROW['fileName']+'.xlsx'

                if STRUCTURE == 'AESI_ANALYSIS':
                    TABLE_path = self.path+'ANALYSIS_SDS/'+'AESI_ANALYSIS'+'.xlsx'

                #print(self.TABLES_df.columns.tolist())
                if 'table location' not in self.TABLES_df.columns:
                    tableLocation = ROW['fileName']
                else:
                    tableLocation = self.tableLocation(ROW)
                    #print(tableLocation)


                TABLE_METADATAS_dict =\
                {
                    'fileName':                 str(ROW['fileName']),
                    'fileType':                 str(ROW['fileType']),
                    'documentTableTag':         str(ROW['documentTableTag']),
                    'documentTableID':          str(ROW['documentTableID']),
                    'documentTableLocation':    str(ROW['documentTableLocation']),
                    'studyConceptTag':          'nan', #que pour pharma 11.4 et 11.5 
                    'documentTablePart':        str(ROW['documentTablePart']), #que pour vaccin et peut être ANY_PART (After Any Vaccine Injection) ou BOOSTER (After Booster Vaccine Injection). 
                    'ongoingNumber':            str(ROW['ongoingNumber']), #que pour vaccin
                    'observationPeriod':        str(ROW['observationPeriod']), #que pour vaccin
                    'referencePopulation':      str(ROW['referencePopulation']),
                    'tableLocation':            tableLocation,
                    'values' : [
                        ROW['location1'], 
                        ROW['location2'], 
                        ROW['location3'],
                        ROW['location4'],
                        ROW['location5'],
                        ROW['location6']]    
                }
                # print(TABLE_name)
                TABLE_ = TABLE(key=TABLE_key, METADATAS_dict=TABLE_METADATAS_dict, study = self.key, GROUP_METADATA=self.GROUP_METADATA_df)
                TABLE_.read_SAS(path=TABLE_path, structure=STRUCTURE)

                if not TABLE_.dataset_empty:

                    self.UPDATE_REJECTIONS(NEW_REJECTION = TABLE_.CHECK_REJECTION)
                    self.sas_update.append(TABLE_.sas_update)

                    TABLE_etree = TABLE_.etree
                    self.TABLES_dict[TABLE_name] = {}
                    self.TABLES_dict[TABLE_name]['ETREE'] = TABLE_etree
                    if self.verbose: print('######## '+'[LOAD]'+' '+TABLE_name)

                else:
                    pass

    ###### NOT MODIFIED ######
    def set_PCSA_TABLES(self):
        # PCSA tables
        # print(self.PCSA_METADATAS.fileName)
        # exit()
        for ROW_id, ROW in self.PCSA_METADATAS.iterrows():
            # self.CHECK_REJECTION = False
            # print(ROW)
            TABLE_key = ROW['INDEX']
            TABLE_name = ROW['documentTableTag'] +'_'+ ROW['studyConceptTag']
            TABLE_path = self.path+'ANALYSIS_SDS/'+TABLE_name+'.xlsx'
            STRUCTURE = self.set_STRUCTURE(ROW['fileType'])
            TABLE_METADATAS_dict =\
            {
                'fileName':                 str(ROW['fileName']),
                'fileType':                 str(ROW['fileType']),
                'documentTableTag':         str(ROW['documentTableTag']),
                'documentTableID':          str(ROW['documentTableID']),
                'documentTableLocation':    str(ROW['documentTableLocation']),
                'studyConceptTag':          str(ROW['studyConceptTag']),
                'mainWording':              str(ROW['mainWording']),
                # 'unit':                     str(ROW['unit']),
                'documentTablePart':        str(ROW['documentTablePart']), #que pour vaccin et peut être ANY_PART (After Any Vaccine Injection) ou BOOSTER (After Booster Vaccine Injection). 
                'ongoingNumber':            str(ROW['ongoingNumber']), #que pour vaccin
                'observationPeriod':        str(ROW['observationPeriod']), #que pour vaccin
                'referencePopulation':      str(ROW['referencePopulation']),
                'tableLocation':            str(ROW['tableLocation']),
                'values' : [
                    ROW['location1'], 
                    ROW['location2'], 
                    ROW['location3'],
                    ROW['location4'],
                    ROW['location5'],
                    ROW['location6']] 
            }
            # print(TABLE_name)
            # print(TABLE_METADATAS_dict)
            TABLE_ = TABLE(key=TABLE_key, METADATAS_dict=TABLE_METADATAS_dict, study = self.key, GROUP_METADATA=self.GROUP_METADATA_df)
            TABLE_.read_SAS(path=TABLE_path, structure=STRUCTURE)
            self.UPDATE_REJECTIONS(NEW_REJECTION = TABLE_.CHECK_REJECTION)
            self.UPDATE_LAB_REJECTIONS(NEW_REJECTION = TABLE_.CHECK_REJECTION)
            self.sas_update.append(TABLE_.sas_update)
            # self.CHECK_REJECTIONS()
            TABLE_etree = TABLE_.etree
            self.TABLES_dict[TABLE_name] = {}
            self.TABLES_dict[TABLE_name]['ETREE'] = TABLE_etree
            if self.verbose: print('######## '+'[LOAD]'+' '+TABLE_name)

        ## Write LAB_DESC and LAB_PCSA rejections
        if self.CHECK_REJECTION_LAB['status'] == True:
            path_PCSA_REJECTION = self.directory['PARAMETERS']['__PCSA']['PATH']+'/'+'LAB_PARAMETERS_REJECTION.xlsx'
            if self.CHECK_REJECTION_LAB['status'] == True:
                REJECTION_df = PANDAS.read_excel(path_PCSA_REJECTION, sheet_name='REJECTION', encoding='utf8')
                        
                for REJECTION_key in list(self.CHECK_REJECTION_LAB['rejection'].keys()):
                    REJECTION_df = PANDAS.concat([REJECTION_df,self.CHECK_REJECTION_LAB['rejection'][REJECTION_key]['rejets']], ignore_index = True)


                REJECTION_df.drop_duplicates(subset=['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'],inplace = True)
                REJECTION_df = REJECTION_df.reindex(['LABEL','FILE_NAME','COMPOUND','PERIOD_ANALYSIS','STUDY_NAME'], axis=1)
                REJECTION_df.to_excel(excel_writer=path_PCSA_REJECTION, index=True, sheet_name='REJECTION')

    ###### NEW ######  
    def tableLocation(self,ROW):
        tableLocation = ROW['fileName']
        if ROW['table location'] == 'in-text':
            if str(ROW['inTextTableNumber']) != 'nan':
                tableLocation = ROW['inTextTableNumber']
            else:
                tableLocation = ROW['fileName']


        elif ROW['table location'] == 'appendices':
            if str(ROW['appendicesTableNumber']) != 'nan':
                tableLocation = ROW['appendicesTableNumber']
            else:
                tableLocation = ROW['fileName']

        return tableLocation

    ###### TO DELETE ? ######
    # def set_STRUCTURE(self,STRUCTURE):
        if STRUCTURE == 'IN_TEXT':
            STRUCTURE = 'INTEXT'
        elif STRUCTURE == '[TMP]':
            STRUCTURE = 'INTEXT'
        elif STRUCTURE == 'LISTING':
            STRUCTURE = 'LISTING'
        elif STRUCTURE == 'LISTING_EXCEL':
            STRUCTURE = 'LISTING_EXCEL'
        else:
            STRUCTURE = 'INTEXT'

        return STRUCTURE

    ###### NEW ######  
    def set_STRUCTURE(self,STRUCTURE):
        if STRUCTURE == 'IN_TEXT':
            STRUCTURE = 'INTEXT'
        elif STRUCTURE == '[TMP]':
            STRUCTURE = 'INTEXT'
        elif STRUCTURE == 'LISTING':
            STRUCTURE = 'LISTING'
        elif STRUCTURE.find('LISTING_EXCEL') == 0:
            STRUCTURE = 'LISTING_EXCEL'
        elif STRUCTURE == 'AESI_ANALYSIS':
            STRUCTURE = 'AESI_ANALYSIS'
        else:
            STRUCTURE = 'INTEXT'

        return STRUCTURE

    ###### NOT MODIFIED ######
    def set_PART_1(self):
        XML_version = '''<?xml version='1.0' encoding='UTF-8'?>'''
        XML_space = '  '
        XML_input = ETREE.Element(_tag='y_input')
        XML_input.attrib['xmlns_y'] = 'http://www.yseop.com/engine/3'
        XML_input.attrib['xmlns_i18n'] = 'http://apache.org/cocoon/i18n/2.1'
        XML_input.attrib['xmlns_fi'] = 'http://apache.org/cocoon/forms/1.0#instance'
        XML_input.attrib['xmlns_ft'] = 'http://apache.org/cocoon/forms/1.0#template'
        XML_input.attrib['xmlns_fd'] = 'http://apache.org/cocoon/forms/1.0#definition'
        XML_input.attrib['xmlns_fb'] = 'http://apache.org/cocoon/forms/1.0#binding'


        XML_action = ETREE.Element(_tag='y_action')
        XML_action.attrib['command'] = 'init-dialog'
        XML_action.attrib['sub-command'] = ""
        XML_input.append(XML_action)


        XML_data = ETREE.Element(_tag='y_data')
        
        self.PART_1_etree = XML_data

    ###### NOT MODIFIED ######
    def set_PART_1_AI4CSR_TOOLS(self):
        XML_version = '''<?xml version='1.0' encoding='UTF-8'?>'''
        XML_space = '  '
        XML_input = ETREE.Element(_tag='y_input')
        XML_input.attrib['xmlns_y'] = 'http://www.yseop.com/engine/3'
        XML_input.attrib['xmlns_i18n'] = 'http://apache.org/cocoon/i18n/2.1'
        XML_input.attrib['xmlns_fi'] = 'http://apache.org/cocoon/forms/1.0#instance'
        XML_input.attrib['xmlns_ft'] = 'http://apache.org/cocoon/forms/1.0#template'
        XML_input.attrib['xmlns_fd'] = 'http://apache.org/cocoon/forms/1.0#definition'
        XML_input.attrib['xmlns_fb'] = 'http://apache.org/cocoon/forms/1.0#binding'


        XML_action = ETREE.Element(_tag='y_action')
        XML_action.attrib['command'] = 'init-dialog'
        XML_action.attrib['sub-command'] = ""
        XML_input.append(XML_action)


        XML_data = ETREE.Element(_tag='y_data')
        
        self.PART_1_etree_TOOLS_AI4CSR = XML_data

    ###### NOT MODIFIED ######
    def set_PART_2(self):

        self.PART_2_etree = ETREE.Element(_tag='y_instance')
        self.PART_2_etree.attrib['yid']='theGeneralData'

        language = ETREE.Element(_tag='language')
        language.attrib['yid'] = 'LANG_en'
        self.PART_2_etree.append(language)

        loadReference = ETREE.Element(_tag='loadReference') 
        loadReference.attrib['yid'] = 'SINGLE_REFERENCE'
        self.PART_2_etree.append(loadReference) 

        self.PART_1_etree.append(self.PART_2_etree)
        # self.PART_1_etree_TOOLS_AI4CSR.append(self.PART_2_etree)

    ###### NOT MODIFIED ######
    def set_PART_2_AI4CSR_TOOLS(self):

        self.PART_2_etree_TOOLS_AI4CSR = ETREE.Element(_tag='y_instance')
        self.PART_2_etree_TOOLS_AI4CSR.attrib['yid']='theGeneralData'

        language = ETREE.Element(_tag='language')
        language.attrib['yid'] = 'LANG_en'
        self.PART_2_etree_TOOLS_AI4CSR.append(language)

        loadReference = ETREE.Element(_tag='loadReference')
        loadReference.attrib['yid'] = 'SINGLE_REFERENCE'
        self.PART_2_etree_TOOLS_AI4CSR.append(loadReference) 

        self.PART_1_etree_TOOLS_AI4CSR.append(self.PART_2_etree_TOOLS_AI4CSR)

    ###### NOT MODIFIED ######
    def set_PART_2_bis(self):

        self.PART_2_etree_bis = ETREE.Element(_tag='y_instance')
        self.PART_2_etree_bis.attrib['yid']='theClinicalStudyReport'

        language = ETREE.Element(_tag='reportPartToGenerate')
        language.attrib['yclass'] = 'List'
        self.PART_2_etree_bis.append(language)

        values_11 = ETREE.Element(_tag='values')
        values_11.text = '11'
        language.append(values_11)

        values_1 = ETREE.Element(_tag='values')
        values_1.text = '1'
        language.append(values_1)

        hasBeenInitialized = ETREE.Element(_tag='hasBeenInitialized')
        hasBeenInitialized.text = 'false'
        self.PART_2_etree_bis.append(hasBeenInitialized)

        self.PART_1_etree.append(self.PART_2_etree_bis)
        # self.PART_1_etree_TOOLS_AI4CSR.append(self.PART_2_etree)

    ###### NOT MODIFIED ######
    def set_PART_2_AI4CSR_TOOLS_bis(self):

        self.PART_2_etree_TOOLS_AI4CSR_bis = ETREE.Element(_tag='y_instance')
        self.PART_2_etree_TOOLS_AI4CSR_bis.attrib['yid']='theClinicalStudyReport'

        language = ETREE.Element(_tag='reportPartToGenerate')
        language.attrib['yclass'] = 'List'
        self.PART_2_etree_TOOLS_AI4CSR_bis.append(language)

        values_11 = ETREE.Element(_tag='values')
        values_11.text = '11'
        language.append(values_11)

        values_1 = ETREE.Element(_tag='values')
        values_1.text = '1'
        language.append(values_1)

        hasBeenInitialized = ETREE.Element(_tag='hasBeenInitialized')
        hasBeenInitialized.text = 'false'
        self.PART_2_etree_TOOLS_AI4CSR_bis.append(hasBeenInitialized)

        self.PART_1_etree_TOOLS_AI4CSR.append(self.PART_2_etree_TOOLS_AI4CSR_bis)

    ###### NOT MODIFIED ######
    def set_PART_3(self):
        STUDY_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'ANALYSIS_MT/'+'STUDY.xlsx'
        self.STUDY_df = PANDAS.read_excel(STUDY_path, sheet_name='STUDY', encoding='utf8') 
        self.PART_3_etree = ETREE.Element(_tag='referenceClinicalStudy')
        self.PART_3_etree.attrib['yclass']='ClinicalStudy'


        ######## studyNumber ########
        studyNumber = ETREE.Element(_tag='studyNumber')
        studyNumber.text = str(self.STUDY_df.loc[0,'studyNumber'])
        self.PART_3_etree.append(studyNumber)

        ######## studyName ########
        studyType = ETREE.Element(_tag='studyName')
        studyType.text = str(self.STUDY_df.loc[0,'studyNumber'])
        self.PART_3_etree.append(studyType)

        ######## studyType ########
        studyType = ETREE.Element(_tag='studyType')
        studyType.attrib['yid'] = str(self.STUDY_df.loc[0,'studyType'])
        self.PART_3_etree.append(studyType)

        ######## PHASE3 ########
        studyPhase = ETREE.Element(_tag='studyPhase')
        studyPhase.attrib['yid'] = str(self.STUDY_df.loc[0,'studyPhase'])
        self.PART_3_etree.append(studyPhase)

        #### typePeson ####
        typePerson = ETREE.Element(_tag='typePerson')
        typePerson.attrib['yid'] = str(self.STUDY_df.loc[0,'typePerson'])
        self.PART_3_etree.append(typePerson)

        ### compound ####
        compound = ETREE.Element(_tag='compound')
        compound.text = self.key[0]
        self.PART_3_etree.append(compound)


        ##### period Analysis 
        periodAnalysis = ETREE.Element(_tag='periodAnalysis')
        periodAnalysis.text = self.key[2]
        self.PART_3_etree.append(periodAnalysis)


        #### TOC - Table of Content
        if 'TOC' in self.STUDY_df.columns:
            TOC = ETREE.Element(_tag='TOC')
            TOC.attrib['yid'] = str(self.STUDY_df.loc[0,'TOC'])
            self.PART_3_etree.append(TOC)


        sas_update = ETREE.Element(_tag='sasUpdate')
        sas_update.text = str(self.sas_update)
        self.PART_3_etree.append(sas_update)


        current_date = ETREE.Element(_tag='mtUpdate')
        current_date.text = str(self.current_date)
        self.PART_3_etree.append(current_date)


        self.PART_1_etree.append(self.PART_3_etree)

        ######## singleDose ########
        # isSingleDoseStudy = ETREE.Element(_tag='isSingleDoseStudy')
        # isSingleDoseStudy.attrib['yid'] = str(self.STUDY_df.loc[0,'isSingleDoseStudy'])
        # self.PART_3_etree.append(isSingleDoseStudy)

    ###### NOT MODIFIED ######
    def set_PART_3_AI4CSR_TOOLS(self):
        self.PART_3_etree_TOOLS_AI4CSR = ETREE.Element(_tag='referenceClinicalStudy')
        self.PART_3_etree_TOOLS_AI4CSR.attrib['yclass']='ClinicalStudy'


        ######## studyNumber ########
        studyNumber_1 = ETREE.Element(_tag='studyNumber')
        studyNumber_1.text = str(self.STUDY_df.loc[0,'studyNumber'])
        self.PART_3_etree_TOOLS_AI4CSR.append(studyNumber_1)

        ######## studyName ########
        studyName_1 = ETREE.Element(_tag='studyName')
        studyName_1.text = 'AI4CSR'
        self.PART_3_etree_TOOLS_AI4CSR.append(studyName_1)

        ######## studyType ########
        studyType_1 = ETREE.Element(_tag='studyType')
        studyType_1.attrib['yid'] = str(self.STUDY_df.loc[0,'studyType'])
        self.PART_3_etree_TOOLS_AI4CSR.append(studyType_1)

        ######## PHASE3 ########
        studyPhase_1 = ETREE.Element(_tag='studyPhase')
        studyPhase_1.attrib['yid'] = str(self.STUDY_df.loc[0,'studyPhase'])
        self.PART_3_etree_TOOLS_AI4CSR.append(studyPhase_1)

        #### typePeson ####
        typePerson = ETREE.Element(_tag='typePerson')
        typePerson.attrib['yid'] = str(self.STUDY_df.loc[0,'typePerson'])
        self.PART_3_etree_TOOLS_AI4CSR.append(typePerson)

        ### compound ####
        compound_1 = ETREE.Element(_tag='compound')
        compound_1.text = 'TOOLS'
        self.PART_3_etree_TOOLS_AI4CSR.append(compound_1)


        ##### period Analysis 
        periodAnalysis_1 = ETREE.Element(_tag='periodAnalysis')
        periodAnalysis_1.text = 'PH1_'+ str(self.STUDY_df.loc[0,'studyNumber'])
        self.PART_3_etree_TOOLS_AI4CSR.append(periodAnalysis_1)


        #### TOC - TAble of Content
        if 'TOC' in self.STUDY_df.columns:
            TOC = ETREE.Element(_tag='TOC')
            TOC.attrib['yid'] = str(self.STUDY_df.loc[0,'TOC'])
            self.PART_3_etree_TOOLS_AI4CSR.append(TOC)


        
        sas_update = ETREE.Element(_tag='sasUpdate')
        sas_update.text = str(self.sas_update)
        self.PART_3_etree_TOOLS_AI4CSR.append(sas_update)


        current_date = ETREE.Element(_tag='mtUpdate')
        current_date.text = str(self.current_date)
        self.PART_3_etree_TOOLS_AI4CSR.append(current_date)

        self.PART_1_etree_TOOLS_AI4CSR.append(self.PART_3_etree_TOOLS_AI4CSR)

    ###### NOT MODIFIED ######
    def set_PART_4(self):
        # self.PART_4_etree = ETREE.Element(_tag='y_instance')
        # self.PART_4_etree.attrib['yid']='theClinicalStudy'

        ######## patientDisposition ########
        self.PART_4_etree = ETREE.Element(_tag='patientDisposition')
        self.PART_4_etree.attrib['yclass'] = "PatientSubGroups"

        ######## groupId ########
        NB_GROUP = len(self.GROUP_METADATA_df)
        groupId = ETREE.Element(_tag='groupId')
        groupId.text = "Group "+ str(NB_GROUP+2) #qu'est ce qu'il faut mettre ?
        self.PART_4_etree.append(groupId)

        ######## groupName ########
        groupName = ETREE.Element(_tag='groupName')
        groupName.text = "All"
        self.PART_4_etree.append(groupName)
      		 
        ######## parts ########
        parts = ETREE.Element(_tag='parts')
        parts.attrib['yclass'] = "List"
        self.PART_4_etree.append(parts)

        exist = False
        compt_grp = 0
        groupid_check = []
        for irow, row in self.TABLE_GROUP_DESCRIPTION_df.iterrows():
         if self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId'] in groupid_check:
            continue

         groupid_check.append(self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId'])

         if row['groupSpecificity'] == 'PLACEBO_COMP' or row['groupSpecificity'] == 'ACTIVE_COMP':
          ######## Groups indentification ########
          # for GROUP in self.TABLE_GROUP_DESCRIPTION_dict:
           values = ETREE.Element(_tag='values')
           values.attrib['yclass'] = "PatientGroup"
           parts.append(values)
                   
           ######## groupSpecificity ########
           TAGS = ['groupSpecificity']
           for TAG in TAGS:
            groupSpecificity = ETREE.Element(_tag=TAG)
            groupSpecificity.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            values.append(groupSpecificity)

           ######## groupId ########
           groupId = ETREE.Element(_tag='groupId')
           groupId.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId']
           values.append(groupId)

           ######## groupName ########
           # print(self.TABLE_GROUP_DESCRIPTION_dict[irow])
           groupName = ETREE.Element(_tag='groupName')
           groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
           values.append(groupName)

           ####### dataOnGroup ########
           dataOnGroup = ETREE.Element(_tag='dataOnGroup')
           dataOnGroup.attrib['yclass'] = "GroupData"
           values.append(dataOnGroup)
            
           ####### dataOnTreatment ########
           dataOnTreatment = ETREE.Element(_tag='dataOnTreatment')
           dataOnTreatment.attrib['yclass'] = "TreatmentData"
           dataOnGroup.append(dataOnTreatment)

           ###### mainCompound ########
           mainCompound = ETREE.Element(_tag='mainCompound')
           mainCompound.attrib['yclass'] = "StudyData"
           dataOnTreatment.append(mainCompound)
        
           ######## name ########
           TAGS = ['groupName']
           for TAG in TAGS:
            name = ETREE.Element(_tag="name")
            name.text = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            mainCompound.append(name)
         else:
          if exist == False:            
           values = ETREE.Element(_tag='values')
           values.attrib['yclass'] = "PatientSubGroups"
           parts.append(values)
           
           ######## groupId ########
           groupId = ETREE.Element(_tag='groupId')
           groupId.text = "Group "+ str(NB_GROUP+1)
           values.append(groupId)

           ######## groupName ########
           # print(self.TABLE_GROUP_DESCRIPTION_dict[irow])
           groupName = ETREE.Element(_tag='groupName')
           groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
           values.append(groupName)
          
           ######## groupSpecificity ########
           TAGS = ['groupSpecificity']
           for TAG in TAGS:
            groupSpecificity = ETREE.Element(_tag=TAG)
            groupSpecificity.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            values.append(groupSpecificity)
          
           ####### dataOnGroup ########
           dataOnGroup = ETREE.Element(_tag='dataOnGroup')
           dataOnGroup.attrib['yclass'] = "GroupData"
           values.append(dataOnGroup)

           ####### dataOnTreatment ########
           dataOnTreatment = ETREE.Element(_tag='dataOnTreatment')
           dataOnTreatment.attrib['yclass'] = "TreatmentData"
           dataOnGroup.append(dataOnTreatment)

           ###### mainCompound ########
           mainCompound = ETREE.Element(_tag='mainCompound')
           mainCompound.attrib['yclass'] = "StudyData"
           dataOnTreatment.append(mainCompound)
        
           ######## name ########
           TAGS = ['name']
           for TAG in TAGS:
            name = ETREE.Element(_tag="name")
            name.text = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            mainCompound.append(name)
            ######## parts ########
           parts2 = ETREE.Element(_tag='parts')
           parts2.attrib['yclass'] = "List"
           dataOnGroup.append(parts2)
           exist = True
           
         
             ####### Groups indentification ########
           # for GROUP in self.TABLE_GROUP_DESCRIPTION_dict:
          values = ETREE.Element(_tag='values')
          values.attrib['yclass'] = "PatientGroup"
          parts2.append(values)

          ######## Identifier ########
          compt_grp+=1
          groupName = ETREE.Element(_tag='identifier')
          groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['name']+'_'+str(compt_grp)
          values.append(groupName)

          ######## groupSpecificity ########
          TAGS = ['groupSpecificity']
          for TAG in TAGS:
           FILE_INFO = ETREE.Element(_tag=TAG)
           FILE_INFO.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
           values.append(FILE_INFO)

          ######## groupName ########
          groupName = ETREE.Element(_tag='groupName')
          groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
          values.append(groupName)

          ######## groupId ########
          groupId = ETREE.Element(_tag='groupId')
          groupId.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId']
          values.append(groupId)
                
            ####### dataOnGroup ########
          dataOnGroup = ETREE.Element(_tag='dataOnGroup')
          dataOnGroup.attrib['yclass'] = "GroupData"
          values.append(dataOnGroup)
           
            ######## dataOnTreatment ########
          dataOnDosage = ETREE.Element(_tag='dataOnDosage')
          dataOnDosage.attrib['yclass'] = "DosageData"
          dataOnGroup.append(dataOnDosage)
         
            ######## mainCompound ########
          dosages = ETREE.Element(_tag='dosages')
          dosages.attrib['yclass'] = "DataList"
          dataOnDosage.append(dosages)

          ####### values ########
          for val in self.TABLE_GROUP_DESCRIPTION_dict[irow]['values']:
           values = ETREE.Element(_tag='values')
           values.text = val
           dosages.append(values)
         #    # TAGS = ['values']
         #    # for TAG in TAGS:
         #     # values = ETREE.Element(_tag=TAG)
         #     # values.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[GROUP][TAG]
         #     # dosages.append(values)
                
         #        # ######## name ########
         #        # TAGS = ['name']
         #        # for TAG in TAGS:
         #            # name = ETREE.Element(_tag=TAG)
         #            # name.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[GROUP][TAG]
         #            # mainCompound.append(name)
				
        self.PART_3_etree.append(self.PART_4_etree)

    ###### NOT MODIFIED ######
    def set_PART_4_AI4CSR_TOOLS(self):
        # self.PART_4_etree_TOOLS_AI4CSR = ETREE.Element(_tag='y_instance')
        # self.PART_4_etree_TOOLS_AI4CSR.attrib['yid']='theClinicalStudy'
		
        ######## patientDisposition ########
        self.PART_4_etree_TOOLS_AI4CSR = ETREE.Element(_tag='patientDisposition')
        self.PART_4_etree_TOOLS_AI4CSR.attrib['yclass'] = "PatientSubGroups"

        ######## groupId ########
        NB_GROUP = len(self.GROUP_METADATA_df)
        groupId = ETREE.Element(_tag='groupId')
        groupId.text = "Group "+ str(NB_GROUP+2) #qu'est ce qu'il faut mettre ?
        self.PART_4_etree_TOOLS_AI4CSR.append(groupId)

        ######## groupName ########
        groupName = ETREE.Element(_tag='groupName')
        groupName.text = "All"
        self.PART_4_etree_TOOLS_AI4CSR.append(groupName)
      
		######## parts ########
        parts = ETREE.Element(_tag='parts')
        parts.attrib['yclass'] = "List"
        self.PART_4_etree_TOOLS_AI4CSR.append(parts)

        exist = False
        compt_grp = 0
        groupid_check = []
        for irow, row in self.TABLE_GROUP_DESCRIPTION_df.iterrows():
         if self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId'] in groupid_check:
            continue

         groupid_check.append(self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId'])

         if row['groupSpecificity'] == 'PLACEBO_COMP' or row['groupSpecificity'] == 'ACTIVE_COMP':
          ######## Groups indentification ########
          # for GROUP in self.TABLE_GROUP_DESCRIPTION_dict:
           values = ETREE.Element(_tag='values')
           values.attrib['yclass'] = "PatientGroup"
           parts.append(values)

           ######## groupSpecificity ########
           TAGS = ['groupSpecificity']
           for TAG in TAGS:
            groupSpecificity = ETREE.Element(_tag=TAG)
            groupSpecificity.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            values.append(groupSpecificity)

           ######## groupId ########
           groupId = ETREE.Element(_tag='groupId')
           groupId.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId']
           values.append(groupId)

           ######## groupName ########
           groupName = ETREE.Element(_tag='groupName')
           groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
           values.append(groupName)

           ####### dataOnGroup ########
           dataOnGroup = ETREE.Element(_tag='dataOnGroup')
           dataOnGroup.attrib['yclass'] = "GroupData"
           values.append(dataOnGroup)
            
           ####### dataOnTreatment ########
           dataOnTreatment = ETREE.Element(_tag='dataOnTreatment')
           dataOnTreatment.attrib['yclass'] = "TreatmentData"
           dataOnGroup.append(dataOnTreatment)

           ###### mainCompound ########
           mainCompound = ETREE.Element(_tag='mainCompound')
           mainCompound.attrib['yclass'] = "StudyData"
           dataOnTreatment.append(mainCompound)
        
           ######## name ########
           TAGS = ['groupName']
           for TAG in TAGS:
            name = ETREE.Element(_tag="name")
            name.text = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            mainCompound.append(name)
         else:
          if exist == False: 
           # for GROUP in self.TABLE_GROUP_DESCRIPTION_dict:
           values = ETREE.Element(_tag='values')
           values.attrib['yclass'] = "PatientSubGroups"
           parts.append(values)

                    
           ######## identifier, groupId, groupName ########
           # TAGS = ['groupId', 'groupName']#'identifier'
           # for TAG in TAGS:
           #  FILE_INFO = ETREE.Element(_tag=TAG)
           #  FILE_INFO.text = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
           #  values.append(FILE_INFO)

           ######## groupId ########
           groupId = ETREE.Element(_tag='groupId')
           groupId.text = "Group "+ str(NB_GROUP+1)
           values.append(groupId)

           ######## groupName ########
           groupName = ETREE.Element(_tag='groupName')
           groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
           values.append(groupName)

           ######## groupSpecificity ########
           TAGS = ['groupSpecificity']
           for TAG in TAGS:
            groupSpecificity = ETREE.Element(_tag=TAG)
            groupSpecificity.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            values.append(groupSpecificity)

           ####### dataOnGroup ########
           dataOnGroup = ETREE.Element(_tag='dataOnGroup')
           dataOnGroup.attrib['yclass'] = "GroupData"
           values.append(dataOnGroup)
            
           ####### dataOnTreatment ########
           dataOnTreatment = ETREE.Element(_tag='dataOnTreatment')
           dataOnTreatment.attrib['yclass'] = "TreatmentData"
           dataOnGroup.append(dataOnTreatment)

           ###### mainCompound ########
           mainCompound = ETREE.Element(_tag='mainCompound')
           mainCompound.attrib['yclass'] = "StudyData"
           dataOnTreatment.append(mainCompound)
        
           ######## name ########
           TAGS = ['name']
           for TAG in TAGS:
            name = ETREE.Element(_tag="name")
            name.text = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
            mainCompound.append(name)

                       ######## parts ########
           parts2 = ETREE.Element(_tag='parts')
           parts2.attrib['yclass'] = "List"
           dataOnGroup.append(parts2)

           exist = True
                     
             ####### Groups indentification ########
           # for GROUP in self.TABLE_GROUP_DESCRIPTION_dict:
          values = ETREE.Element(_tag='values')
          values.attrib['yclass'] = "PatientGroup"
          parts2.append(values)

          ######## Identifier ########
          compt_grp+=1
          groupName = ETREE.Element(_tag='identifier')
          groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['name']+'_'+str(compt_grp)
          values.append(groupName)

          ######## groupSpecificity ########
          TAGS = ['groupSpecificity']
          for TAG in TAGS:
           FILE_INFO = ETREE.Element(_tag=TAG)
           FILE_INFO.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[irow][TAG]
           values.append(FILE_INFO)

          ######## groupName ########
          groupName = ETREE.Element(_tag='groupName')
          groupName.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupName']
          values.append(groupName)

          ######## groupId ########
          groupId = ETREE.Element(_tag='groupId')
          groupId.text = self.TABLE_GROUP_DESCRIPTION_dict[irow]['groupId']
          values.append(groupId)
                    
            ####### dataOnGroup ########
          dataOnGroup = ETREE.Element(_tag='dataOnGroup')
          dataOnGroup.attrib['yclass'] = "GroupData"
          values.append(dataOnGroup)
            
            ######## dataOnTreatment ########
          dataOnDosage = ETREE.Element(_tag='dataOnDosage')
          dataOnDosage.attrib['yclass'] = "DosageData"
          dataOnGroup.append(dataOnDosage)
            
            ######## mainCompound ########
          dosages = ETREE.Element(_tag='dosages')
          dosages.attrib['yclass'] = "DataList"
          dataOnDosage.append(dosages)

          ####### values ########
          for val in self.TABLE_GROUP_DESCRIPTION_dict[irow]['values']:
           values = ETREE.Element(_tag='values')
           values.text = val
           dosages.append(values)

          # TAGS = ['values']
            # for TAG in TAGS:
             # values = ETREE.Element(_tag=TAG)
             # values.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[GROUP][TAG]
             # dosages.append(values)
                
                # ######## name ########
                # TAGS = ['name']
                # for TAG in TAGS:
                    # name = ETREE.Element(_tag=TAG)
                    # name.attrib['yid'] = self.TABLE_GROUP_DESCRIPTION_dict[GROUP][TAG]
                    # mainCompound.append(name)
                
        self.PART_3_etree_TOOLS_AI4CSR.append(self.PART_4_etree_TOOLS_AI4CSR)

    ###### NOT MODIFIED ######
    def write_XML(self):

        XML_str = ""
        XML_space = '\n'
        XML_version = '''<?xml version='1.0' encoding='UTF-8'?>'''

        etree_referenceTables = ETREE.Element(_tag='ReferenceTables')
        etree_referenceTables.attrib['yclass']='ReferenceTables'

        documentTables = ETREE.Element(_tag='documentTables')
        documentTables.attrib['yclass']='List'
        etree_referenceTables.append(documentTables)

        # XML_SPACE_str = '\n\n'

        # XML_str = XML_str + ETREE.tostring(self.PART_1_etree, pretty_print=True).decode('utf-8') + XML_SPACE_str
        # XML_str = XML_str + ETREE.tostring(self.PART_2_etree, pretty_print=True).decode('utf-8') + XML_SPACE_str
        # XML_str = XML_str + ETREE.tostring(self.PART_3_etree, pretty_print=True).decode('utf-8') + XML_SPACE_str
        # XML_str = XML_str + ETREE.tostring(self.PART_4_etree, pretty_print=True).decode('utf-8') + XML_SPACE_str

        ## COMMENTER ##
        for TABLE_key in self.TABLES_dict:
            TABLE_etree = self.TABLES_dict[TABLE_key]['ETREE']
            TABLE_str = ETREE.tostring(TABLE_etree, pretty_print=True).decode('utf-8')
            documentTables.append(TABLE_etree)
            # XML_str = XML_str + XML_SPACE_str + TABLE_str
            # XML_str = XML_str + TABLE_str

        XML_str = XML_version + XML_space + ETREE.tostring(etree_referenceTables, pretty_print=True).decode('utf-8')

        #replace specific values
        XML_str = XML_str.replace('&lt;=','&#8804;')
        XML_str = XML_str.replace('&gt;=','&#8805;')
        XML_str = XML_str.replace('y_instance', 'y:instance')
        XML_filename = 'referenceStudy'+'.'+'xml'
        XML_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'XML'+'/'+XML_filename

        with open(XML_path, 'w') as XML_file:
            XML_file.write(XML_str)
            if self.verbose: print('######## '+'[WRITE]'+' '+XML_filename)


        XML_input = ETREE.Element(_tag='ReferenceClinicalStudy')
        XML_input.attrib['yclass'] = "ReferenceClinicalStudy" 
        # XML_input.attrib['xmlns_y'] = 'http://www.yseop.com/engine/3'
        # XML_input.attrib['xmlns_i18n'] = 'http://apache.org/cocoon/i18n/2.1'
        # XML_input.attrib['xmlns_fi'] = 'http://apache.org/cocoon/forms/1.0#instance'
        # XML_input.attrib['xmlns_ft'] = 'http://apache.org/cocoon/forms/1.0#template'
        # XML_input.attrib['xmlns_fd'] = 'http://apache.org/cocoon/forms/1.0#definition'
        # XML_input.attrib['xmlns_fb'] = 'http://apache.org/cocoon/forms/1.0#binding'


        # XML_action = ETREE.Element(_tag='y_action')
        # XML_action.attrib['command'] = 'init-dialog'
        # XML_action.attrib['sub-command'] = ""
        # XML_input.append(XML_action)
        XML_input.append(self.PART_3_etree)


        DataFlow = ""
        DataFlow = XML_version + XML_space + ETREE.tostring(XML_input, pretty_print=True).decode('utf-8')

        DataFlow = DataFlow.replace('y_instance', 'y:instance')
        DataFlow = DataFlow.replace('y_input', 'y:input')
        DataFlow = DataFlow.replace('y_action', 'y:action')
        DataFlow = DataFlow.replace('y_data', 'y:data')
        DataFlow = DataFlow.replace('xmlns_y', 'xmlns:y')
        DataFlow = DataFlow.replace('xmlns_i18n', 'xmlns:i18n')
        DataFlow = DataFlow.replace('xmlns_fi', 'xmlns:fi')
        DataFlow = DataFlow.replace('xmlns_ft', 'xmlns:ft')
        DataFlow = DataFlow.replace('xmlns_fd', 'xmlns:fd')
        DataFlow = DataFlow.replace('xmlns_fb', 'xmlns:fb')

        DataFlow_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'XML'+'/'+'referenceClinicalStudy.xml'
        with open(DataFlow_path, 'w') as XML_file:
                XML_file.write(DataFlow)


        ## COMMENTER ##
        XML_FOLDER = 'PH1_'+self.key[1]
        XML_path = self.directory['STUDIES']['TOOLS']['AI4CSR'][XML_FOLDER]+'XML'+'/'+XML_filename

        with open(XML_path, 'w') as XML_file:
            XML_file.write(XML_str)
            if self.verbose: print('######## '+'[WRITE]'+' '+XML_filename)






        XML_input = ETREE.Element(_tag='ReferenceClinicalStudy')
        # XML_input.attrib['xmlns_y'] = 'http://www.yseop.com/engine/3'
        # XML_input.attrib['xmlns_i18n'] = 'http://apache.org/cocoon/i18n/2.1'
        # XML_input.attrib['xmlns_fi'] = 'http://apache.org/cocoon/forms/1.0#instance'
        # XML_input.attrib['xmlns_ft'] = 'http://apache.org/cocoon/forms/1.0#template'
        # XML_input.attrib['xmlns_fd'] = 'http://apache.org/cocoon/forms/1.0#definition'
        # XML_input.attrib['xmlns_fb'] = 'http://apache.org/cocoon/forms/1.0#binding'
        XML_input.attrib['yclass'] = "ReferenceClinicalStudy"

        # XML_action = ETREE.Element(_tag='y_action')
        # XML_action.attrib['command'] = 'init-dialog'
        # XML_action.attrib['sub-command'] = ""
        # XML_input.append(XML_action)
        XML_input.append(self.PART_3_etree_TOOLS_AI4CSR)

        DataFlow = XML_version + XML_space + ETREE.tostring(XML_input, pretty_print=True).decode('utf-8')

        DataFlow = DataFlow.replace('y_instance', 'y:instance')
        DataFlow = DataFlow.replace('y_input', 'y:input')
        DataFlow = DataFlow.replace('y_action', 'y:action')
        DataFlow = DataFlow.replace('y_data', 'y:data')
        DataFlow = DataFlow.replace('xmlns_y', 'xmlns:y')
        DataFlow = DataFlow.replace('xmlns_i18n', 'xmlns:i18n')
        DataFlow = DataFlow.replace('xmlns_fi', 'xmlns:fi')
        DataFlow = DataFlow.replace('xmlns_ft', 'xmlns:ft')
        DataFlow = DataFlow.replace('xmlns_fd', 'xmlns:fd')
        DataFlow = DataFlow.replace('xmlns_fb', 'xmlns:fb')

        XML_FOLDER = 'PH1_'+self.key[1]
        XML_path = self.directory['STUDIES']['TOOLS']['AI4CSR'][XML_FOLDER]+'XML'+'/'+'referenceClinicalStudy.xml'

        with open(XML_path, 'w') as XML_file:
            XML_file.write(DataFlow)

    ###### NOT MODIFIED ######
    def write_separate_XML(self):

        for TABLE_key in self.TABLES_dict:
            TABLE_etree = self.TABLES_dict[TABLE_key]['ETREE']
            TABLE_str = ETREE.tostring(TABLE_etree, pretty_print=True).decode('utf-8')
            XML_str = TABLE_str

            #replace specific values
            XML_str = XML_str.replace('&lt;=','&#8804;')
            XML_str = XML_str.replace('&gt;=','&#8805;')
            XML_str = XML_str.replace('y_instance', 'y:instance')
            XML_filename = 'YSEOP_TABLE'+' - '+TABLE_key+'.'+'xml'
            XML_path = self.directory['STUDIES'][self.key[0]][self.key[1]][self.key[2]]+'XML'+'/'+XML_filename
            with open(XML_path, 'w') as XML_file:
                XML_file.write(XML_str)
                if self.verbose: print('######## '+'[WRITE]'+' '+XML_filename)

    ###### NOT MODIFIED ######
    def UPDATE_REJECTIONS(self, NEW_REJECTION):
        # self.CHECK_REJECTION = True
        if NEW_REJECTION['status'] == True:
            self.CHECK_REJECTION['status'] = True
            NEW_REJECTION_keys = list(NEW_REJECTION['rejection'].keys())
            # print(REJECTION_keys)
                        
            for NEW_REJECTION_key in NEW_REJECTION_keys:
                self.CHECK_REJECTION['rejection'][NEW_REJECTION_key] = NEW_REJECTION['rejection'][NEW_REJECTION_key]


    def UPDATE_LAB_REJECTIONS(self, NEW_REJECTION):
        if NEW_REJECTION['status'] == True:
            self.CHECK_REJECTION_LAB['status'] = True
            NEW_REJECTION_keys = list(NEW_REJECTION['rejection'].keys())
            # print(REJECTION_keys)
                        
            for NEW_REJECTION_key in NEW_REJECTION_keys:
                self.CHECK_REJECTION_LAB['rejection'][NEW_REJECTION_key] = NEW_REJECTION['rejection'][NEW_REJECTION_key]



    def DISPLAY_REJECTIONS(self):
        # print(self.CHECK_REJECTION)
        if self.CHECK_REJECTION['status'] == True:
            PreviousFile = None

            print('\n')
            print('          ####################################')
            print('          #######                      #######')
            print('          ####                            ####')
            print('          ##           Rejections           ##')
            print('          ####                            ####')
            print('          #######                      #######')
            print('          ####################################')
            print('\n')

            for REJECTION_key in self.CHECK_REJECTION['rejection']:
                CurrentFile = self.CHECK_REJECTION['rejection'][REJECTION_key]['dataFile']

                if CurrentFile != PreviousFile:
                    print('\n')
                    print('##########')

                    if self.CHECK_REJECTION['rejection'][REJECTION_key]['studyConceptTag'] == None:
                        print('Rejections regarding the following file : ' + CurrentFile +'  ('+ 
                            self.CHECK_REJECTION['rejection'][REJECTION_key]['documentTableTag'] +')' +'\n')

                    else:
                        print('Rejections regarding the following file : ' + CurrentFile +'  ('+ 
                            self.CHECK_REJECTION['rejection'][REJECTION_key]['documentTableTag']+', '+
                            self.CHECK_REJECTION['rejection'][REJECTION_key]['studyConceptTag']  +')' +'\n') 


                print(' _'+ self.CHECK_REJECTION['rejection'][REJECTION_key]['title'] +' :')
                print('   -New rejections into : '+ self.CHECK_REJECTION['rejection'][REJECTION_key]['rejectionFile'])
                print('   -Update of mapping into : '+ self.CHECK_REJECTION['rejection'][REJECTION_key]['mappingFile'])
                print('   -List of rejected values : '+ 
                    str(self.CHECK_REJECTION['rejection'][REJECTION_key]['rejets'].LABEL.tolist()))

                
                PreviousFile = CurrentFile
                print(' ')


            print('\n\n')
        else:
            # print('\n')
            print('\n###########\n')
            print('There are no rejections for this study')
            print('\n###########')



    def get_date_last_update(self):
        now = datetime.datetime.now()
        self.current_date = now.strftime('%d/%m/%Y')


        check = False 
        self.sas_update = [i for i in self.sas_update if i != None]
        maxi_date = self.sas_update[0]
        for date in self.sas_update:
            if date != None:
                check = True
                if date > maxi_date:
                    maxi_date = date

        if check:
            self.sas_update = maxi_date.strftime('%d/%m/%Y')

        else:
            self.sas_update = None




def MAJ_MAPPING_AND_REJECTION_FILES():
    directory = DIRECTORY()
    FILES_UPDATED = []

    #LISTING et PCSA
    MAPPING_REJECTION = \
    {
        'MAPPING_PATH' :    [directory['PARAMETERS']['__LISTING'] + 'LISTING_COLUMN_MAPPING.xlsx',
                            directory['PARAMETERS']['__LISTING'] + 'LISTING_VALUES_MAPPING.xlsx',
                            directory['PARAMETERS']['__PCSA'] + 'LAB_CATEGORY_MAPPING.xlsx',
                            directory['PARAMETERS']['__PCSA'] + 'LAB_PARAMETERS_MAPPING.xlsx'],

        'REJECTION_PATH' :  [directory['PARAMETERS']['__LISTING'] + 'LISTING_COLUMN_REJECTION.xlsx',
                            directory['PARAMETERS']['__LISTING'] + 'LISTING_VALUES_REJECTION.xlsx',
                            directory['PARAMETERS']['__PCSA'] + 'LAB_CATEGORY_REJECTION.xlsx',
                            directory['PARAMETERS']['__PCSA'] + 'LAB_PARAMETERS_REJECTION.xlsx'],
    }
    MAPPING_REJECTION_df = PANDAS.DataFrame(MAPPING_REJECTION)


    for irow, row in MAPPING_REJECTION_df.iterrows():
        MAPPING_PATH = row['MAPPING_PATH']
        REJECTION_path = row['REJECTION_PATH']

        MAPPING_df = PANDAS.read_excel(MAPPING_PATH, sheet_name='MAPPING', encoding='utf8')
        REJECTION_df = PANDAS.read_excel(REJECTION_path, sheet_name='REJECTION', encoding='utf8')


        if not REJECTION_df.empty:
            CHECK_MAJ = False
            Label_REF = MAPPING_df.LABEL.tolist()


            if 'COLUMN' in REJECTION_df.columns:

                for irow, row in REJECTION_df.iterrows():   
                        LABEL = row['LABEL']
                        COLUMN = row['COLUMN']
                        if LABEL in Label_REF:
                            rows = MAPPING_df[MAPPING_df['LABEL'] == LABEL]
                            if COLUMN in rows.COLUMN.tolist():
                                CHECK_MAJ = True
                                REJECTION_df = REJECTION_df.drop(irow)



            else:
                for irow, row in REJECTION_df.iterrows():   
                    LABEL = row['LABEL']
                    if LABEL in Label_REF:
                        CHECK_MAJ = True
                        REJECTION_df = REJECTION_df.drop(irow)


            if CHECK_MAJ == True:
                FILES_UPDATED.append(REJECTION_path)
                REJECTION_df.reset_index(drop = True,inplace = True)
                REJECTION_df.to_excel(excel_writer=REJECTION_path, index=True, sheet_name='REJECTION')




    #EACH TABLE
    FOLDER_avoid = ['__LISTING','__PCSA','__METADATAS']
    FOLDER_path = "./"

    ######## DISTANCE & RELATIVE PATH TO ROOT ########
    if os.path.exists('./../'+'__C__')==True: 
        FOLDER_path = "./../"
    elif os.path.exists('./../../'+'__C__')==True: 
        FOLDER_path = "./../../"
    elif os.path.exists('./../../../'+'__C__')==True: 
        FOLDER_path = "./../../../"

    FOLDER_path = FOLDER_path + '/PARAMETERS/'
    FOLDER_keys = [ENTRY.name for ENTRY in os.scandir(FOLDER_path) if (ENTRY.is_dir() and ENTRY.name not in FOLDER_avoid)]

    # print(FOLDER_keys)
    for FOLDER_key in FOLDER_keys:
        MAPPING_PATH = directory['PARAMETERS'][FOLDER_key] + 'MAPPING.xlsx'
        REJECTION_path = directory['PARAMETERS'][FOLDER_key] + 'REJECTIONS.xlsx'

        MAPPING_df = PANDAS.read_excel(MAPPING_PATH, sheet_name='MAPPING', encoding='utf8')
        REJECTION_df = PANDAS.read_excel(REJECTION_path, sheet_name='REJECTION', encoding='utf8')
        
        #maj Rejection and mapping
        if not REJECTION_df.empty:
            CHECK_MAJ = False
            Label_REF = MAPPING_df.LABEL.tolist()

            for irow, row in REJECTION_df.iterrows():
                Label = row['LABEL']
                if Label in Label_REF:
                    CHECK_MAJ = True
                    REJECTION_df = REJECTION_df.drop(irow)


            if CHECK_MAJ == True:
                FILES_UPDATED.append(REJECTION_path)
                REJECTION_df.reset_index(drop = True,inplace = True)
                REJECTION_df.to_excel(excel_writer=REJECTION_path, index=True, sheet_name='REJECTION')


    if len(FILES_UPDATED) == 0:
        print('No updates done\n\n')
        
    else:
        print('Updates have been done on follonwing files :')
        for FILES in FILES_UPDATED:
            print('_'+FILES)




import logging
import wget
import zipfile
import json
from pathlib import Path
import sqlite3
from sqlite3 import Error
import  requests
from time import sleep

def greet():
    #print("Hello Airflow!")
    logging.info("hello Airflow 3!!!")

def getFile():
    logging.info("begin down load . . .")
    wget.download("https://ofdata.ru/open-data/download/okved_2.json.zip", "/opt/airflow/dags") 
    logging.info("end download!")  

def handle_bd(name, kodokved, naimokved, inn, ogrn, kpp):
    #print("name = %s, kodokved = %s, naimokved = %s inn = %s, ogrn = %s, kpp = %s"%(name,kodokved,naimokved,inn,ogrn,kpp))
    logging.info(f'name = %s',name)    
    try:
        conn = sqlite3.connect('hw3.db') 
        conn.execute('CREATE TABLE IF NOT EXISTS telecom_companies (company_name TEXT, kodokved TEXT, naimokved TEXT, inn TEXT, ogrn TEXT, kpp TEXT)')
        query = """INSERT INTO telecom_companies (company_name, kodokved, naimokved, inn, ogrn, kpp) VALUES
        (?,?,?,?,?,?)"""
        cursor = conn.cursor()
        value = []
        value.append(name)
        value.append(kodokved)
        value.append(naimokved)
        value.append(inn)
        value.append(ogrn)
        value.append(kpp)
        valTup = tuple(value)
        cursor.execute(query, valTup)
    except Error:
      #print(Error)
      logging.error(f'Error: %s', Error)
    finally:
        if conn:
            conn.commit()
            conn.close() 

def handle_file(hf):    
    #print("handle file: %s"%(hf))
    logging.info(f'handle file: %s',hf)
    with open(hf, encoding='utf-8') as f:
        data = json.load(f)        
        for i in data:  
            try:          
                dt = i['data']
                if 'СвОКВЭД' in dt:
                    if 'СвОКВЭДОсн' in dt['СвОКВЭД']:
                        kodokved = dt['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
                        if str(kodokved).startswith('61.'):                      
                            handle_bd(i['name'], kodokved, dt['СвОКВЭД']['СвОКВЭДОсн']['НаимОКВЭД'], i['inn'], i['ogrn'], i['kpp'])                        
                    else:
                        if 'СвОКВЭДДоп' in dt['СвОКВЭД']:
                            svokveddop = dt['СвОКВЭД']['СвОКВЭДДоп']                                                
                            if 'КодОКВЭД' not in svokveddop:
                                for d in svokveddop:
                                    if str(d['КодОКВЭД']).startswith('61.'):
                                        handle_bd(i['name'], str(d['КодОКВЭД']), dt['СвОКВЭД']['СвОКВЭДОсн']['НаимОКВЭД'], i['inn'], i['ogrn'], i['kpp'])                                
                            else:
                                if str(svokveddop['КодОКВЭД']).startswith('61.'):
                                    handle_bd(i['name'], str(svokveddop['КодОКВЭД']), dt['СвОКВЭД']['СвОКВЭДОсн']['НаимОКВЭД'], i['inn'], i['ogrn'], i['kpp'])                            
            except KeyError:
                #print("KeyError in file: %s"%(hf))
                logging.error(f'KeyError in file: %s',hf)
                continue
            except UnboundLocalError:
                #print("UnboundLocalError in file: %s"%(hf))
                logging.error(f'UnboundLocalError in file: %s',hf)
                continue
            except:
                #print("UnknownError in file: %s"%(hf))
                logging.error(f'UnknownError in file: %s',hf)
                continue

def handle_egrul_json_zip():
    with zipfile.ZipFile('/opt/airflow/dags/egrul.json.zip', 'r') as zipobj:
        file_names = zipobj.namelist()    
        for name in file_names:
            zipobj.extract(name)
            handle_file(name)
            path = Path(name)
            path.unlink()
            #return   
#----------------------------------------------------------------
def check_in_base(name):
    try:
        conn = sqlite3.connect('hw3.db')
        cursor = conn.cursor()
        #query = 'select * from telecom_companies where company_name like %"' + name + '"%'
        query = "select * from telecom_companies where company_name like '%\"" + name + "\"%'"
        cursor.execute(query)
        #print(len(cursor.fetchall()))
        cnt = len(cursor.fetchall())
        cursor.close
    except Error:
      logging.error(f'Error check_in_base: %s',Error)
    finally:
        if conn:            
            conn.close() 
    return cnt

def handle_request_api(url): 
    listLinks = []
    try:                   
        result = requests.get(url)   
        data = json.loads(result.text)    
        vacancies = data.get('items')
        for vacancy in vacancies:
            emp_name = vacancy['employer']['name'].upper()
            if(check_in_base(emp_name) > 0):
               # print(emp_name)
                logging.info(emp_name)
               # print(vacancy['url'])
                listLinks.append(vacancy['url'])            
    except requests.exceptions.HTTPError as errh:
        logging.error(f'Http Error: %s',errh)
    except requests.exceptions.ConnectionError as errc:        
        logging.error(f'Error Connecting %s',errh)
    except requests.exceptions.Timeout as errt:        
        logging.error(f'Timeout Error %s',errh)
    except requests.exceptions.RequestException as err:        
        logging.error(f'OOps: Something Else %s',errh)
    return listLinks    

def handle_single_vacancy_api(url):
    listKeySkills = []
    result = requests.get(url)
    data = json.loads(result.text)
    key_skills = data['key_skills']
    for skill in key_skills:
        listKeySkills.append(skill['name'])    
    return listKeySkills

def handle_key_skills_table(listKeySkills):
    try:
        conn = sqlite3.connect('hw3.db') 
        cursor = conn.cursor()
        conn.execute('CREATE TABLE IF NOT EXISTS key_skills (skill TEXT, count INTEGER)')
        for keySkill in listKeySkills:
            #print(keySkill)
            sleep(0.5)
            logging.info(keySkill)
            query = "select * from key_skills where skill like '" + keySkill + "'"
            cursor.execute(query)
            cnt = len(cursor.fetchall())            
            if cnt > 0:
                query = "update key_skills set count=count+1 where skill ='" + keySkill + "'"
                cursor.execute(query)
            else:
                query = "insert into key_skills(skill, count) values('" + keySkill + "', 1)"
                cursor.execute(query)        
    except Error:
      logging.error(f'Error: %s', Error)
    finally:
        cursor.close
        if conn:
            conn.commit()
            conn.close() 

def getTopSkills():
    logging.warning("Print Top skills!")
    try:
        conn = sqlite3.connect('hw3.db') 
        cursor = conn.cursor()
        query = "select * from key_skills order by count desc"
        cursor.execute(query)
        for i in range(10):
            next_row = cursor.fetchone()
            if next_row:
                #print(next_row)
                logging.warning(next_row)
            else:
                break
    except Error:
      logging.error(f'Error: %s', Error)
    finally:
        cursor.close
        if conn:
            conn.commit()
            conn.close()        

def handle_api_get_top_skills():
    url = "https://api.hh.ru/vacancies?text=middle%20python&per_page=20&page="
    fullListLinksApi = [] 
    listOfListKeySkills = []
    for i in range(10):
        fullListLinksApi = handle_request_api(url + str(i))            
    for url in fullListLinksApi:
        listOfListKeySkills.append(handle_single_vacancy_api(url))          
    for listKeySkills in listOfListKeySkills:
        handle_key_skills_table(listKeySkills)
    getTopSkills()            
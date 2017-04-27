import os
import glob
import time
import datetime
import re

from collections import OrderedDict

import requests
import sqlalchemy
import pdb
import pandas as pd

from bs4 import BeautifulSoup
from itertools import chain

POKEMON_DIRPATH = r'C:\Users\jcarr\Desktop\pokemon_scripts'

#local host 127.0.0.1
#port: 3306
#user root

def main():
    cleanData()
    df_dmg = pd.read_pickle('df_dmg.pkl')
    df_data = pd.read_pickle('df_data.pkl')
    df_stats = pd.read_pickle('df_stats.pkl')
    df_evo = pd.read_pickle('df_evo.pkl')
    
    engine = sqlalchemy.create_engine("mysql+pymysql://root:Kitler4ever@127.0.0.1/pokemon_db?host=localhost?port=3306")
    #df_data.to_sql(name ='data', con=engine, if_exists='append', index=False)
    #df_stats.to_sql(name ='stats', con=engine, if_exists='append', index=False)
    df_evo.to_sql(name ='evo', con=engine, if_exists='append', index=False)
    #df_dmg.to_sql(name ='dmg', con=engine, if_exists='append', index=False)
    
    
def cleanData():
    
    df_dmg = pd.read_csv('pkmn_damage.csv').drop(['timestamp','name'], 1)
    df_evo = pd.read_csv('pkmn_evo.csv').drop(['timestamp'], 1)
    df_stats = pd.read_csv('pkmn_stats.csv').drop(['timestamp'], 1)
    df_data = pd.read_csv('pkmn_data.csv').drop(['timestamp'], 1)
    
    df_data = df_data.rename(columns={'number':'pid'}) 
    df_dmg = df_dmg.rename(columns={'number':'fk_pid'}) 
    
    df_dmg = df_data[['pid', 'typeI','typeII']].merge(df_dmg, how = 'inner', left_on='pid', right_on='fk_pid')
    df_dmg = df_dmg.drop(['fk_pid','pid'],1)
    df_dmg = df_dmg.drop_duplicates(subset =['typeI', 'typeII']).sort_values(['typeI', 'typeII']).reset_index(drop = True)
    df_dmg.index.name = 'did'
    df_dmg = df_dmg.reset_index()
    
    df_data = df_data.merge(df_dmg[['did', 'typeI','typeII']], how = 'left', on=['typeI', 'typeII'])
    dids = df_data.pop('did')
    df_data.insert(1, 'fk_did', dids)
    
    retdec = lambda x: float(''.join(ele for ele in str(x) if ele.isdigit() or ele == '.'))
    df_data['genderM'] = df_data['genderM'].apply(lambda x: float(str(x).split('%')[0])/100.0)
    df_data['genderF'] = df_data['genderF'].apply(lambda x: float(str(x).split('%')[0])/100.0)
    
    for fld in ['height', 'weight', 'eggsteps']:
        df_data[fld] = df_data[fld].apply(retdec)
    
    df_stats = df_data[['pid']].merge(df_stats, left_on='pid', right_on='number')
    df_stats = df_stats.drop(['name', 'number'], 1).rename(columns={'pid':'fk_pid'})
    
    df_evo = cleanEvo(df_evo)
        
    df_dmg.to_pickle('df_dmg.pkl')
    df_data.to_pickle('df_data.pkl')
    df_stats.to_pickle('df_stats.pkl')
    df_evo.to_pickle('df_evo.pkl')
    
def cleanEvo(df):
    
    flds = ['fk_pid', 'fk_pid_from', 'fk_pid_to', 'method']
    
    #df['name'] = df['name'].apply(lambda s: "".join(i for i in s if 31 < ord(i) < 127) 
    pid_ref = dict(df[['number', 'name']].values[:,::-1].tolist())
    df = df.drop(['evo_name'], 1)        
    
    df['evo_num'] = df['evo_num'].apply(lambda x: int(x) if not pd.isnull(x) else 0)
    df['evo_from'] = df['evo_from'].apply(lambda x: int(pid_ref[x]) if not pd.isnull(x) else 0)
    for i in xrange(1,8):
        df = df.drop(['evo_name_alt{0}'.format(i)], 1)
        df['evo_num_alt{0}'.format(i)] = df['evo_num_alt{0}'.format(i)].apply(lambda x: int(x) if not pd.isnull(x) else 0)
        df['evo_from_alt{0}'.format(i)] = df['evo_from_alt{0}'.format(i)].apply(lambda x: int(pid_ref[x]) if not pd.isnull(x) else 0)
    
    df = df.drop(['name'],1).rename(columns={'number':'fk_pid'})
    df.columns = ['fk_pid'] + list(chain.from_iterable( [ ['fk_pid_from_{0}'.format(i), 'method_{0}'.format(i), 'fk_pid_to_{0}'.format(i)] for i in range(1,9) ] ))
    #pdb.set_trace()
    df.index.name = 'eid'
    df = df.reset_index()
    df['eid'] = df['eid']+1
    
    return df
    
    
def readDatabase(query):
    disk_engine = sqlalchemy.create_engine('sqlite:///Pokemon.db') # Initializes database with filename 311_8M.db in current directory
    df = pd.read_sql_query(query, disk_engine)
    return df

    
def executeRaw(raw):
    disk_engine = sqlalchemy.create_engine('sqlite:///Pokemon.db') # Initializes database with filename 311_8M.db in current directory
    with disk_engine.connect() as con:
        cursor = con.execute(raw)
        result = [row for row in cursor]
    #pdb.set_trace()
    return result
    
    
def loadDatabase():
    df = pd.read_pickle('df.pkl')
    df.columns = ['number', 'name', 'hp', 'attack', 'defense', 'spattack', 'spdefense', 'speed', 'total']
    timestamp = datetime.datetime.now()
    df['timestamp'] = timestamp
    
    for num_fld in ['hp', 'attack', 'defense', 'spattack', 'spdefense', 'speed', 'total']:
        df[num_fld] = pd.to_numeric(df[num_fld])
    
    
    disk_engine = sqlalchemy.create_engine('sqlite:///Pokemon.db') # Initializes database with filename 311_8M.db in current directory
    df.to_sql('pokemon_stats', disk_engine, if_exists='append')
    
    
def scanPokemonPages():
    pkmn_pages = glob.glob(os.path.join(POKEMON_DIRPATH, '*'))
    
    StatsData_list = []
    DamageData_list = []
    for idx,page_path in enumerate(pkmn_pages):
        with open(page_path,'rb') as fle:
            pkmn_html = fle.read()
			
        soup = BeautifulSoup(pkmn_html)
        
        StatsData = OrderedDict()
        DamageData = OrderedDict()
        
        #Get Stats Data
        StatsData['number'] = os.path.basename(page_path).split('_')[0]
        StatsData['name'] = os.path.basename(page_path).replace('.html','').split('_')[1]
        StatsData = getStatsData(soup, StatsData)
        StatsData_list.append(StatsData)
        
        #Get Damage Data
        DamageData['number'] = os.path.basename(page_path).split('_')[0]
        DamageData['name'] = os.path.basename(page_path).replace('.html','').split('_')[1]
        DamageData = getDamageData(soup, DamageData)
        DamageData_list.append(DamageData)
        print idx,' --- ', page_path
    
    #df = pd.DataFrame(data_list)
    df_stats = pd.DataFrame(StatsData_list)
    df_damage = pd.DataFrame(DamageData_list)
    #df.to_pickle('df.pkl')
    #df.to_csv('test.csv',index=False)
    df_stats.to_csv('pkmn_stats.csv',index=False)
    df_damage.to_csv('pkmn_damage.csv',index=False)
    pdb.set_trace()
    
    
def getStatsData(soup,data):
    # number = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
    # data['number'] = number.get_text().split('#')[1].split(' ')[0]
    
    # name = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
    # data['name'] = ''.join(name.get_text().split(' ')[1:])
    

    dextables = soup.find_all('table', attrs={'class': 'dextable'})
    for dextable in dextables:
        header = dextable.find('td').get_text()
        if header == 'Stats':
            trs = dextable.find_all('tr')
            
            data['hp'] = trs[2].find_all('td')[1].get_text()
            data['attack'] = trs[2].find_all('td')[2].get_text()
            data['defense'] = trs[2].find_all('td')[3].get_text()
            data['spattack'] = trs[2].find_all('td')[4].get_text()
            data['spdefense'] = trs[2].find_all('td')[5].get_text()
            data['speed'] = trs[2].find_all('td')[6].get_text()
            data['total'] = trs[2].find_all('td')[0].get_text().split(' ')[-1]
    
    """
    paras = soup.find_all('p')
    for pidx,para in enumerate(paras):
        dextables = para.find_all('table', attrs={'class': 'dextable'})
        for dextable in dextables:
            header = dextable.find('td').get_text()
            print pidx, header, '\n'
    """    
    
    return data
    
    pdb.set_trace()

def getDamageData(soup,data):
    # number = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
    # data['number'] = number.get_text().split('#')[1].split(' ')[0]
    
    # name = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
    # data['name'] = ''.join(name.get_text().split(' ')[1:])
    
    dextables = soup.find_all('table', attrs={'class': 'dextable'})
    for dextable in dextables:
        header = dextable.find('td').get_text()
        if header == '\n\t\tDamage Taken\n\t\t':
            trs = dextable.find_all('tr')
            
            data['normal'] = trs[2].find_all('td')[0].get_text()
            data['fire'] = trs[2].find_all('td')[1].get_text()
            data['water'] = trs[2].find_all('td')[2].get_text()
            data['electric'] = trs[2].find_all('td')[3].get_text()
            data['grass'] = trs[2].find_all('td')[4].get_text()
            data['ice'] = trs[2].find_all('td')[5].get_text()
            data['fight'] = trs[2].find_all('td')[6].get_text()
            data['poison'] = trs[2].find_all('td')[7].get_text()
            data['ground'] = trs[2].find_all('td')[8].get_text()
            data['flying'] = trs[2].find_all('td')[9].get_text()
            data['psychic'] = trs[2].find_all('td')[10].get_text()
            data['bug'] = trs[2].find_all('td')[11].get_text()
            data['rock'] = trs[2].find_all('td')[12].get_text()
            data['ghost'] = trs[2].find_all('td')[13].get_text()
            data['dragon'] = trs[2].find_all('td')[14].get_text()
            data['dark'] = trs[2].find_all('td')[15].get_text()
            data['steel'] = trs[2].find_all('td')[16].get_text()
            data['fairy'] = trs[2].find_all('td')[17].get_text()
    """
    paras = soup.find_all('p')
    for pidx,para in enumerate(paras):
        dextables = para.find_all('table', attrs={'class': 'dextable'})
        for dextable in dextables:
            header = dextable.find('td').get_text()
            print pidx, header, '\n'
    """    
    
    return data
    
    pdb.set_trace()

def getPokemonPages():
    
    root_url = "http://www.serebii.net/pokedex-sm/"    
    numbers = [str(i).zfill(3) for i in xrange(1,803)]
   
    errors = []
    for number in numbers:
        try:
            page = root_url + number + '.shtml'
            
            result = requests.get(page).content
            soup = BeautifulSoup(result, "html.parser")
            
            name = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
            name = ''.join(name.get_text().split(' ')[1:])
        
            filename = number + '_' + name + '.html'
            filepath = os.path.join(POKEMON_DIRPATH, filename)
            
            with open(filepath, 'wb') as fle:
                fle.write(result)
            
            print 'Processed {0} ---- {1}'.format(number, name)
            time.sleep(1)
            
            #pdb.set_trace() # comment this
        
        except UnicodeEncodeError:
            errors.append(number)
            
    print ("Completed\n\n")    
    print ("Errors:")
    for error in errors:
        print (error)
    
    
    
        
        
        #pdb.set_trace()
    
    
if __name__=="__main__":
    main()

import os
import glob
import time
import datetime
import pdb
import itertools 

from multiprocessing import Pool
from collections import OrderedDict

import sqlalchemy
import pandas as pd

from bs4 import BeautifulSoup

POKEMON_DIRPATH_PDB = r'C:\Users\jcarr\Desktop\pokemon_scripts\pokemon_pdb'

def main():
    start = datetime.datetime.now()
    ###################################################################
           
    pkmn_pages_pdb = glob.glob(os.path.join(POKEMON_DIRPATH_PDB, '*'))  
    compilePdbTables(pkmn_pages_pdb)
    
    ###################################################################
    end = datetime.datetime.now()
    print "Completed: {0}".format(end-start)

def compilePdbTables(pkmn_pages_pdb):
    #evo_list = scanPdbEvo(pkmn_pages_pdb)
    pkmn_list = scanPdbData(pkmn_pages_pdb)
    
    ###################################################################
    # Create dataframes, enforce numeric types, backup copies of dataframe
    #df_evo = pd.DataFrame(list(evo_list)) 
    df_pkmn = pd.DataFrame(list(pkmn_list))
    
    # numeric_evo_fields = ['number', 'evo_num', 'evo_num_alt1', 'evo_num_alt2', 'evo_num_alt3', 'evo_num_alt3', 'evo_num_alt4', 'evo_num_alt5', 'evo_num_alt6', 'evo_num_alt7']
    # for fld in numeric_evo_fields:
        # df_evo[fld] = pd.to_numeric(df_evo[fld])
        
    numeric_pkmn_fields = ['number','genderM','genderF','height','weight','capture','basehappy']        
    for fld in numeric_pkmn_fields:
        df_pkmn[fld] = pd.to_numeric(df_pkmn[fld])        
    
    #df_evo.to_csv('pkmn_evo.csv',index=False)
    df_pkmn.to_csv('pkmn_df.csv',index=False)

    ###################################################################
    # Load Dataframes into SQLlite DB
    #pdb.set_trace()
    sql_engine = sqlalchemy.create_engine('sqlite:///Pokemon.db')
    #df_evo.to_sql('pkmn_evo', sql_engine, if_exists='replace')
    df_pkmn.to_sql('pkmn_data', sql_engine, if_exists='replace')
       
def scanPdbEvo(pkmn_pages):
    evo_data_list=[]
    
    for idx,page_path in enumerate(pkmn_pages):
        with open(page_path,'rb') as fle:
            pkmn_html = fle.read()
			
        soup = BeautifulSoup(pkmn_html)
        evo_data = OrderedDict()
        
        evo_data['number'] = os.path.basename(page_path).replace('.html','').split('_')[0]
        evo_data = getEvoData(soup, evo_data)
       
        evo_data_list.append(evo_data)
        
        print idx,' --- ', page_path
    # #pdb.set_trace()
    # df_pkmn = pd.DataFrame(pkmnData_list)
    # #df.to_pickle('df.pkl')
    # df_pkmn.to_csv('pkmn_evo_test.csv',index=False)
    return(evo_data_list)
    
def scanPdbData(pkmn_pages):
    pkmn_data_list=[]
    
    for idx,page_path in enumerate(pkmn_pages):
        with open(page_path,'rb') as fle:
            pkmn_html = fle.read()
			
        soup = BeautifulSoup(pkmn_html)
        pkmn_data = OrderedDict()
        
        pkmn_data['number'] = os.path.basename(page_path).replace('.html','').split('_')[0]
        pkmn_data = getPkmnData(soup,pkmn_data)
       
        pkmn_data_list.append(pkmn_data)
        
        print idx,' --- ', page_path
    # #pdb.set_trace()
    # df_pkmn = pd.DataFrame(pkmnData_list)
    # #df.to_pickle('df.pkl')
    # df_pkmn.to_csv('pkmn_evo_test.csv',index=False)
    return(pkmn_data_list)

def getEvoData(soup,evo_data):
    #pdb.set_trace()
    evo_data['timestamp'] = datetime.datetime.now()
    nameDex = soup.find_all('h1')
    evo_data['name'] = str(nameDex).split('<h1>')[1].split('</h1>')[0].replace('\xe9','e').replace('\u2640','F').replace('\u2642','M')
    evo_data['evo_from'] = ''
    evo_data['method'] = ''
    evo_data['evo_num'] = ''
    evo_data['evo_name'] = ''
    
    evo_data['evo_from_alt1'] = ''
    evo_data['method_alt1'] = ''
    evo_data['evo_num_alt1'] = ''
    evo_data['evo_name_alt1'] = ''
    
    evo_data['evo_from_alt2'] = ''
    evo_data['method_alt2'] = ''
    evo_data['evo_num_alt2'] = ''
    evo_data['evo_name_alt2'] = ''
    
    evo_data['evo_from_alt3'] = ''
    evo_data['method_alt3'] = ''
    evo_data['evo_num_alt3'] = ''
    evo_data['evo_name_alt3'] = ''
    
    evo_data['evo_from_alt4'] = ''
    evo_data['method_alt4'] = ''
    evo_data['evo_num_alt4'] = ''
    evo_data['evo_name_alt4'] = ''
    
    evo_data['evo_from_alt5'] = ''
    evo_data['method_alt5'] = ''
    evo_data['evo_num_alt5'] = ''
    evo_data['evo_name_alt5'] = ''
    
    evo_data['evo_from_alt6'] = ''
    evo_data['method_alt6'] = ''
    evo_data['evo_num_alt6'] = ''
    evo_data['evo_name_alt6'] = ''
    
    evo_data['evo_from_alt7'] = ''
    evo_data['method_alt7'] = ''
    evo_data['evo_num_alt7'] = ''
    evo_data['evo_name_alt7'] = ''
    
    # flds = ['name'] + list(chain.from_iterable( [ ['evo_from_{0}'.format(i), 'method_{0}'.format(i), 'evo_num_{0}'.format(i), 'evo_name_{0}'.format(i)] for i in range(1,8) ] ))
    # evo_data.update([ (fld, '') for fld in flds ])
    try:
        #pdb.set_trace()
        evoDex = soup.find_all(attrs={'class':'infocard-evo-list'})# 6 tables from evo_data to pokedex enteries
        spans = evoDex[0].find_all('span')
        if len(spans) == 3:
            evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[1]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[2]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[2]).split('alt="')[1].split('"')[0]
                        
        elif len(spans) == 6:
            evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[2]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[3]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[3]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt1'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt1'] = str(spans[5]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt1'] = str(spans[5]).split('alt="')[1].split('"')[0]
            
        elif len(spans) == 5: #3-stage
            evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[1]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[2]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[2]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt1'] = str(spans[2]).split('alt="')[1].split('"')[0]
            evo_data['method_alt1'] = str(spans[3]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt1'] = str(spans[4]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt1'] = str(spans[4]).split('alt="')[1].split('"')[0]
            
        elif len(spans) == 8: 
            if evo_data['name'].startswith('Hi') or evo_data['name'].startswith('Ty'): #Hitmons
                evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
                evo_data['method'] = str(spans[2]).split('(')[1].split(')')[0].replace('<br>',' ').replace('&gt;','>').replace('&lt;','<')
                evo_data['evo_num'] = str(spans[0]).split('#')[1].split('</small>')[0]
                evo_data['evo_name'] = str(spans[1]).split('alt="')[1].split('"')[0]
                
                evo_data['evo_from_alt1'] = str(spans[0]).split('alt="')[1].split('"')[0]
                evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0].replace('<br>',' ').replace('&gt;','>').replace('&lt;','<')
                evo_data['evo_num_alt1'] = str(spans[5]).split('#')[1].split('</small>')[0]
                evo_data['evo_name_alt1'] = str(spans[5]).split('alt="')[1].split('"')[0]
                
                evo_data['evo_from_alt2'] = str(spans[0]).split('alt="')[1].split('"')[0]
                evo_data['method_alt2'] = str(spans[6]).split('(')[1].split(')')[0].replace('<br>',' ').replace('&gt;','>')
                evo_data['evo_num_alt2'] = str(spans[7]).split('#')[1].split('</small>')[0]
                evo_data['evo_name_alt2'] = str(spans[7]).split('alt="')[1].split('"')[0]
            
            else: #Pikas, Polis, Ralts, Cos, & Odd evo_chains 
                evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
                evo_data['method'] = str(spans[1]).split('(')[1].split(')')[0]
                evo_data['evo_num'] = str(spans[2]).split('#')[1].split('</small>')[0]
                evo_data['evo_name'] = str(spans[2]).split('alt="')[1].split('"')[0]
                
                evo_data['evo_from_alt1'] = str(spans[2]).split('alt="')[1].split('"')[0]
                evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0]
                evo_data['evo_num_alt1'] = str(spans[5]).split('#')[1].split('</small>')[0]
                evo_data['evo_name_alt1'] = str(spans[5]).split('alt="')[1].split('"')[0]
                
                evo_data['evo_from_alt2'] = str(spans[2]).split('alt="')[1].split('"')[0]
                evo_data['method_alt2'] = str(spans[6]).split('(')[1].split(')')[0].replace('<br>', ' ')
                evo_data['evo_num_alt2'] = str(spans[7]).split('#')[1].split('</small>')[0]
                evo_data['evo_name_alt2'] = str(spans[7]).split('alt="')[1].split('"')[0]
            
        elif len(spans) == 19: #eeveelutions
            evo_data['evo_from'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[2]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[1]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[1]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt1'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt1'] = str(spans[3]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt1'] = str(spans[3]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt2'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt2'] = str(spans[6]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt2'] = str(spans[7]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt2'] = str(spans[7]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt3'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt3'] = str(spans[6]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt3'] = str(spans[5]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt3'] = str(spans[5]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt4'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt4'] = str(spans[8]).split('(')[1].split(')')[0].replace('\xe2\x99\xa5','(Heart)').replace('\xc3\xa9','e').replace('<br>',' ')
            evo_data['evo_num_alt4'] = str(spans[7]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt4'] = str(spans[7]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt5'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt5'] = str(spans[11]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt5'] = str(spans[12]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt5'] = str(spans[12]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt6'] = str(spans[9]).split('alt="')[1].split('"')[0]
            evo_data['method_alt6'] = str(spans[13]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt6'] = str(spans[14]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt6'] = str(spans[14]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt7'] = str(spans[9]).split('alt="')[1].split('"')[0]
            linkStr = spans[15].get_text()
            evo_data['method_alt7'] = linkStr.split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt7'] = str(spans[16]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt7'] = str(spans[16]).split('alt="')[1].split('"')[0]
        
        elif len(spans) == 10:  #wurmple
            evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[2]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[3]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[3]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt1'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt1'] = str(spans[5]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt1'] = str(spans[5]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt2'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method_alt2'] = str(spans[7]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt2'] = str(spans[8]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt2'] = str(spans[8]).split('alt="')[1].split('"')[0]    
            
            evo_data['evo_from_alt3'] = str(spans[0]).split('alt="')[1].split('"')[0] 
            evo_data['method_alt3'] = str(spans[10]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt3'] = str(spans[11]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt3'] = str(spans[11]).split('alt="')[1].split('"')[0]   
            
        elif len(spans) == 12: #burmy
            evo_data['evo_from'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method'] = str(spans[1]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num'] = str(spans[2]).split('#')[1].split('</small>')[0]
            evo_data['evo_name'] = str(spans[2]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt1'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method_alt1'] = str(spans[4]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt1'] = str(spans[5]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt1'] = str(spans[5]).split('alt="')[1].split('"')[0]
            
            evo_data['evo_from_alt2'] = str(spans[0]).split('alt="')[1].split('"')[0]
            evo_data['method_alt2'] = str(spans[6]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt2'] = str(spans[7]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt2'] = str(spans[7]).split('alt="')[1].split('"')[0]    
            
            evo_data['evo_from_alt3'] = str(spans[7]).split('alt="')[1].split('"')[0]  
            evo_data['method_alt3'] = str(spans[8]).split('(')[1].split(')')[0].replace('<br>',' ')
            evo_data['evo_num_alt3'] = str(spans[9]).split('#')[1].split('</small>')[0]
            evo_data['evo_name_alt3'] = str(spans[9]).split('alt="')[1].split('"')[0]   
        
        else:
            pdb.set_trace()
            evo_data['method'] = "Span Length: " + len(spans)
            evo_data['evo_num'] = "something went wrong here..."
            evo_data['evo_name'] = 'N/A'
            evo_data['method_alt1'] = 'N/A'
            evo_data['evo_num_alt1'] = 'N/A'
            evo_data['evo_name_alt1'] = 'N/A'
      
    except IndexError:
        #pdb.set_trace()
        evo_data['method'] = str(evo_data['name']) + " does not evolve"
        evo_data['evo_from'] = ""
    
    #Final check to fix unicode text
    if ('\xe2\x99\x80' in evo_data['evo_from']) or ('\xe2\x99\x82' in evo_data['evo_from']):
        evo_data['evo_from'] = evo_data['evo_from'].replace('\xe2\x99\x80','F').replace('\xe2\x99\x82','M')
        
    if ('\xc3\xa9' in evo_data['method']) or ('\xc3\xa9' in evo_data['method_alt1']) or ('\xc3\xa9' in evo_data['method_alt2']):
        evo_data['method'] = evo_data['method'].replace('\xc3\xa9','e')
        evo_data['method_alt1'] = evo_data['method_alt1'].replace('\xc3\xa9','e')
        evo_data['method_alt2'] = evo_data['method_alt2'].replace('\xc3\xa9','e')
    
    if '\xc3\xa9' in evo_data['evo_from'] or '\xc3\xa9' in evo_data['evo_from_alt1']:
        evo_data['evo_from'] = evo_data['evo_from'].replace('\xc3\xa9','e')
        
    if '\xc3\xa9' in evo_data['method_alt1'] or '\xc3\xa9' in evo_data['method_alt2']:
        evo_data['method_alt1'] = evo_data['method_alt1'].replace('\xc3\xa9','e')
        
    if '<a href=' in evo_data['method']:
        newMethod = evo_data['method'].split('>')[1].split('<')[0]
        evo_data['method'] = 'after '+newMethod+' learned'
        
    if '<a href=' in evo_data['method_alt1']:
        if evo_data['method_alt1'].startswith('in'):
            evo_data['method_alt1'] = evo_data['method_alt1'].split('>')[1].split('<')[0]
            evo_data['method_alt1'] = 'In a ' + evo_data['method_alt1'] + ' area'
        elif evo_data['method_alt1'].startswith('after'):
            evo_data['method_alt1'] = evo_data['method_alt1'].split('>')[1].split('<')[0]
            evo_data['method_alt1'] = 'After ' + evo_data['method_alt1'] + ' learned'
        elif evo_data['method_alt1'].startswith('trade'):
            evo_data['method_alt1'] = evo_data['method_alt1'].split('>')[1].split('<')[0]
            evo_data['method_alt1'] = 'Trade holding ' + evo_data['method_alt1']    
    
    if (':' in evo_data['name']) or (':' in evo_data['evo_from']):
        evo_data['evo_from'] = evo_data['evo_from'].replace(':', '')
        evo_data['name'] = evo_data['name'].replace(':', '')
    return evo_data
   
def getPkmnData(soup,pkmn_data):
    #pdb.set_trace()
    pkmn_data['timestamp'] = datetime.datetime.now()
    nameDex = soup.find_all('h1')
    pkmn_data['name'] = str(nameDex).split('<h1>')[1].split('</h1>')[0].replace('\xe9','e').replace('\u2640','F').replace('\u2642','M')
    pkmn_data['typeI'] = ''
    pkmn_data['typeII'] = ''    
    pkmn_data['genderM'] = '0'
    pkmn_data['genderF'] = '0'
    vitalsDex = soup.find_all(attrs={'class':'svtabs-panel'})# 6 tables from pkmn_data to pokedex enteries
    tds = vitalsDex[0].find_all('td')
    try:
        try:
            pkmn_data['species'] = str(tds[2]).replace('\xc3\xa9','e').split('<td>')[1].split('</td')[0]
            pkmn_data['height'] = str(tds[3]).split('(')[1].split(')')[0].split('m')[0]
            pkmn_data['weight'] = str(tds[4]).split('(')[1].split(')')[0].split(' kg')[0]
            pkmn_data['abilityI'] = ''
            pkmn_data['abilityII'] = '' 
            pkmn_data['abilityIII'] = ''
            pkmn_data['evyield'] = str(tds[8]).split('\n\t\t\t\t\t')[1].split('\t\t\t\t')[0]
            pkmn_data['capture'] = str(tds[9]).split('>')[1].split(' <')[0]
            pkmn_data['basehappy'] = str(tds[10]).split('>')[1].split(' <')[0]
            pkmn_data['basexp'] = str(tds[11]).split('>')[1].split('<')[0]
            pkmn_data['growth'] = str(tds[12]).split('>')[1].split('<')[0]
            pkmn_data['eggsteps'] = str(tds[15]).split('base ')[1].split(')')[0]
            
            genders = tds[14].find_all('span')
            if len(genders) > 0:
                pkmn_data['genderM'] = str(genders[0]).split('">')[1].split('%')[0]
                pkmn_data['genderF'] = str(genders[1]).split('">')[1].split('%')[0]
            else:
                pkmn_data['genderM'] = pkmn_data['genderF'] = '0'
                
        except IndexError: # fixed errors up until chestnaught, examine his values
            pkmn_data['evyield'] = str(tds[7]).split('\n\t\t\t\t\t')[1].split('\t\t\t\t')[0]
            pkmn_data['capture'] = str(tds[8]).split('>')[1].split(' <')[0]
            pkmn_data['basehappy'] = str(tds[9]).split('>')[1].split(' <')[0]
            pkmn_data['basexp'] = str(tds[10]).split('>')[1].split('<')[0]
            pkmn_data['growth'] = str(tds[11]).split('>')[1].split('<')[0]
            pkmn_data['eggsteps'] = str(tds[14]).split('(')[1].split(')')[0]
            
                
    except IndexError:
        #pdb.set_trace()  chesnaught fix
        pkmn_data['eggsteps'] = str(tds[15]).split('(')[1].split(')')[0]
        genders = tds[14].find_all('span')
        if len(genders) > 0:
            pkmn_data['genderM'] = str(genders[0]).split('">')[1].split('%')[0]
            pkmn_data['genderF'] = str(genders[1]).split('">')[1].split('%')[0]
        else:
            pkmn_data['genderM'] = pkmn_data['genderF'] = '0'
    alink = tds[5].find_all('a')
    try:
        pkmn_data['abilityI'] = str(alink[0]).split('">')[1].split('</a>')[0]
        pkmn_data['abilityII'] = str(alink[1]).split('">')[1].split('</a>')[0]
        pkmn_data['abilityIII'] = str(alink[2]).split('">')[1].split('</a>')[0]
    except IndexError:
        pass
    
    if pkmn_data['genderM'] == '':
        try:
            try:
                genders = tds[13].find_all('span')
                pkmn_data['genderM'] = str(genders[0]).split('">')[1].split('%')[0]
                pkmn_data['genderF'] = str(genders[1]).split('">')[1].split('%')[0]
            except IndexError:
                genders = tds[14].find_all('span')
                pkmn_data['genderM'] = str(genders[0]).split('">')[1].split('%')[0]
                pkmn_data['genderF'] = str(genders[1]).split('">')[1].split('%')[0]
        except IndexError:
            pkmn_data['genderM'] = pkmn_data['genderF'] = '0'
            
    types = tds[1].find_all('a')
    try:
        pkmn_data['typeI'] = str(types[0]).split('">')[1].split('</')[0]
        pkmn_data['typeII'] = str(types[1]).split('">')[1].split('</')[0]
    except IndexError:
        pkmn_data['typeI'] = str(types[0]).split('">')[1].split('</')[0]
        pkmn_data['typeII'] = ''
        
    return pkmn_data
  
def scanSerPages(page_path):
    print "Processing ---", page_path
    
    with open(page_path,'rb') as fle:      
        soup = BeautifulSoup(fle.read())
        
    base_data = OrderedDict()
    base_data['timestamp'] = datetime.datetime.now()
    base_data['number'] = os.path.basename(page_path).replace('.html','').split('_')[0]
    base_data['name'] = os.path.basename(page_path).replace('.html','').split('_')[1]
    
    stats_data = base_data.copy()
    damage_data = base_data.copy()
    ###################################################################################
    
    dextables = soup.find_all('table', attrs={'class': 'dextable'})
    for dextable in dextables:
        header = dextable.find('td').get_text()
        
        if header == 'Stats':
            trs = dextable.find_all('tr')
            
            stats_data['hp'] = trs[2].find_all('td')[1].get_text()
            stats_data['attack'] = trs[2].find_all('td')[2].get_text()
            stats_data['defense'] = trs[2].find_all('td')[3].get_text()
            stats_data['sp_attack'] = trs[2].find_all('td')[4].get_text()
            stats_data['sp_defense'] = trs[2].find_all('td')[5].get_text()
            stats_data['speed'] = trs[2].find_all('td')[6].get_text()
            stats_data['total'] = trs[2].find_all('td')[0].get_text().split(' ')[-1]
        
        elif header == '\n\t\tDamage Taken\n\t\t':
            trs = dextable.find_all('tr')
            
            damage_data['normal'] = trs[2].find_all('td')[0].get_text().replace('*', '')
            damage_data['fire'] = trs[2].find_all('td')[1].get_text().replace('*', '')
            damage_data['water'] = trs[2].find_all('td')[2].get_text().replace('*', '')
            damage_data['electric'] = trs[2].find_all('td')[3].get_text().replace('*', '')
            damage_data['grass'] = trs[2].find_all('td')[4].get_text().replace('*', '')
            damage_data['ice'] = trs[2].find_all('td')[5].get_text().replace('*', '')
            damage_data['fight'] = trs[2].find_all('td')[6].get_text().replace('*', '')
            damage_data['poison'] = trs[2].find_all('td')[7].get_text().replace('*', '')
            damage_data['ground'] = trs[2].find_all('td')[8].get_text().replace('*', '')
            damage_data['flying'] = trs[2].find_all('td')[9].get_text().replace('*', '')
            damage_data['psychic'] = trs[2].find_all('td')[10].get_text().replace('*', '')
            damage_data['bug'] = trs[2].find_all('td')[11].get_text().replace('*', '')
            damage_data['rock'] = trs[2].find_all('td')[12].get_text().replace('*', '')
            damage_data['ghost'] = trs[2].find_all('td')[13].get_text().replace('*', '')
            damage_data['dragon'] = trs[2].find_all('td')[14].get_text().replace('*', '')
            damage_data['dark'] = trs[2].find_all('td')[15].get_text().replace('*', '')
            damage_data['steel'] = trs[2].find_all('td')[16].get_text().replace('*', '')
            damage_data['fairy'] = trs[2].find_all('td')[17].get_text().replace('*', '')
    
    return (stats_data, damage_data)
     
if __name__=="__main__":
    main()

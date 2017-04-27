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

#pkmoves.html was downloaded from, https://pokemondb.net/move/all
POKEMON_DIRPATH_ATK = r'C:\Users\jcarr\Desktop\pokemon_scripts\pkmoves.html'
POKEMON_MOVES_PATH = r'C:\Users\jcarr\Desktop\pokemon_scripts\pokemon_atk'

def main():
    start = datetime.datetime.now()
      
    #cleanTables()
    
    end = datetime.datetime.now()
    print "Completed: {0}".format(end-start)

def cleanTables(): 
    
    ##################################################################################
    # Clean pkmn_atk.csv
    ##################################################################################
    df_atk = pd.read_csv('pkmn_atk.csv')
    numeric_atk_fields = ['power', 'acc', 'pp', 'prob']
    for fld in numeric_atk_fields:
        df_atk[fld] = pd.to_numeric(df_atk[fld])        
        df_atk[fld] = df_atk[fld].fillna(value=0)
    #pdb.set_trace()
    #it's like DF[column] = DF[column].as_type(int)
    df_atk['tm'] = df_atk['tm'].fillna(value=' ')
    df_atk['effect'] = df_atk['effect'].fillna(value=' ')
    
    df_atk['mid'] = df_atk['mid'].astype(int)
    df_atk['name'] = df_atk['name'].astype(str)
    df_atk['type'] = df_atk['type'].astype(str)
    df_atk['cat'] = df_atk['cat'].astype(str)
    df_atk['tm'] = df_atk['tm'].astype(str)
    df_atk['effect'] = df_atk['effect'].astype(str)
        
    df_atk['power'] = df_atk['power'].astype(int)
    df_atk['acc'] = df_atk['acc'].astype(int)
    df_atk['pp'] = df_atk['pp'].astype(int)
    df_atk['prob'] = df_atk['prob'].astype(int)
    
    df_atk.to_csv('pkmn_atk_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
    ##################################################################################
    # Clean pkmn_damage.csv
    ##################################################################################
    df_dmg = pd.read_csv('pkmn_damage.csv')
    numeric_dmg_fields = ['normal', 'fire', 'water', 'electric', 'grass', 'ice', 'fight', 'poison', 'ground', 'flying', 'psychic', 'bug', 'rock', 'ghost', 'dragon', 'dark', 'steel', 'fairy']
    for fld in numeric_dmg_fields:
        df_dmg[fld] = df_dmg[fld].astype(float)
        df_dmg[fld] = df_dmg[fld].fillna(value=0)
    #pdb.set_trace()    
    df_dmg['typeII'] = df_dmg['typeII'].fillna(value=' ')
    df_dmg['number'] = df_dmg['number'].astype(int)
    df_dmg['typeI'] = df_dmg['typeI'].astype(str)
    df_dmg['typeII'] = df_dmg['typeII'].astype(str)
    df_dmg.to_csv('pkmn_dmg_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
    
    ##################################################################################
    # Clean pkmn_data.csv
    ##################################################################################
    df_data = pd.read_csv('pkmn_df.csv')
    df_data['typeII'] = df_data['typeII'].fillna(value=' ')
    df_data['abilityII'] = df_data['abilityII'].fillna(value=' ')
    df_data['abilityIII'] = df_data['abilityIII'].fillna(value=' ')
    df_data['genderM'] = df_data['genderM'].fillna(value=0)
    df_data['genderF'] = df_data['genderF'].fillna(value=0)
    df_data['capture'] = df_data['capture'].fillna(value=0)
    df_data['basehappy'] = df_data['basehappy'].fillna(value=0)
    df_data['basexp'] = df_data['basexp'].fillna(value=0)
    df_data['eggsteps'] = df_data['eggsteps'].replace('base ', '')
    
    string_data_fields = ['name', 'typeI', 'typeII', 'species','abilityI', 'abilityII', 'abilityIII', 'evyield', 'growth', 'eggsteps']
    for fld in string_data_fields:
        df_data[fld] = df_data[fld].astype(str).fillna(value=' ')
    #pdb.set_trace()
    df_data['number'] = df_data['number'].astype(int)
    df_data['capture'] = df_data['capture'].astype(int)
    df_data['basehappy'] = df_data['basehappy'].astype(int)
    df_data['basexp'] = df_data['basexp'].astype(int)
    df_data.to_csv('pkmn_data_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
    ##################################################################################
    # Clean pkmn_moves.csv
    ##################################################################################
    df_moves = pd.read_csv('pkmn_moves.csv')   
    string_moveset_fields = ['name', 'move', 'method']
    for fld in string_moveset_fields:
        df_moves[fld] = df_moves[fld].astype(str).fillna(value=' ')
    #pdb.set_trace()
    df_moves['idx'] = df_moves['idx'].astype(int)
    df_moves['pid'] = df_moves['pid'].astype(int)
    df_moves['mid'] = df_moves['mid'].astype(int)
    df_moves.to_csv('pkmn_moveset_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
    ##################################################################################
    # Clean pkmn_stats.csv
    ##################################################################################
    df_stats = pd.read_csv('pkmn_stats.csv')
    numeric_stat_fields = ['number', 'hp', 'attack', 'defense', 'sp_attack', 'sp_defense', 'speed', 'total']
    for fld in numeric_stat_fields:
        df_stats[fld] = df_stats[fld].astype(int).fillna(value=0)
    #pdb.set_trace()
    df_stats.to_csv('pkmn_stats_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
    ##################################################################################
    # Clean pkmn_evo.csv
    ##################################################################################
    df_evo = pd.read_csv('pkmn_evo.csv')
    string_evo_fields = ['evo_from','method','evo_name','method_alt1','evo_name_alt1','method_alt2','evo_name_alt2','method_alt3','evo_name_alt3','method_alt4','evo_name_alt4','method_alt5','evo_name_alt5','method_alt6','evo_name_alt6','method_alt7']
    for fld in string_evo_fields:
        df_evo[fld] = df_evo[fld].fillna(value=' ')
    for fld in string_evo_fields:
            df_evo[fld] = df_evo[fld].astype(str)
    #pdb.set_trace()
    df_evo['number'] = df_evo['number'].astype(int)
    df_evo.to_csv('pkmn_evo_final.csv', index=False)
    #After to_numeric use df.loc[df[col].isnull(),:] = 0.0
    ##################################################################################
    
    
def scanAtk():
    with open(POKEMON_DIRPATH_ATK,'rb') as fle:
        pkmn_html = fle.read()
    soup = BeautifulSoup(pkmn_html)
    atk_data_list = []
    #pdb.set_trace()
    
    moveTable = soup.find_all('table', attrs={'id':'moves'})
    moveDex = moveTable[0].find_all('td')
    i=0
    try:
        while i < 6310:
            #pdb.set_trace()
            if ('Z-Move' in moveDex[i+7].get_text()): 
                pass
            else: 
                atk_data = OrderedDict()
                atk_data['name'] = moveDex[i].get_text()
                atk_data['type'] = moveDex[i+1].get_text()
                try:
                    atk_data['cat'] = str(moveDex[i+2]).split('title="')[1].split('"')[0]
                except IndexError:
                    atk_data['cat'] = moveDex[i+2].get_text()
                atk_data['power'] = moveDex[i+3].get_text()
                atk_data['acc'] = moveDex[i+4].get_text()
                atk_data['pp'] = moveDex[i+5].get_text()
                atk_data['tm'] = moveDex[i+6].get_text()
                atk_data['effect'] = moveDex[i+7].get_text()
                atk_data['prob'] = moveDex[i+8].get_text()
                
                #get rid of unicode chars
                try:
                    atk_data['name'] = str(atk_data['name'])
                except UnicodeEncodeError:
                    atk_data['name'] = ''
                try:
                    atk_data['type'] = str(atk_data['type'])
                except UnicodeEncodeError:
                    atk_data['type'] = ''
                try:
                    atk_data['cat'] = str(atk_data['cat'])
                except UnicodeEncodeError:
                    atk_data['cat'] = ''
                try:
                    atk_data['power'] = str(atk_data['power'])
                except UnicodeEncodeError:
                    atk_data['power'] = ''
                try:
                    atk_data['acc'] = str(atk_data['acc'])
                except UnicodeEncodeError:
                    atk_data['acc'] = ''
                try:
                    atk_data['pp'] = str(atk_data['pp'])
                except UnicodeEncodeError:
                    atk_data['pp'] = ''
                try:
                    atk_data['effect'] = str(atk_data['effect'])
                except UnicodeEncodeError:
                    atk_data['effect'] = ''
                try:
                    atk_data['prob'] = str(atk_data['prob'])
                except UnicodeEncodeError:
                    atk_data['prob'] = ''
                
                atk_data_list.append(atk_data)
            i+=9
            
    except IndexError:
            #pdb.set_trace()
            pass
    
    #pdb.set_trace()
      
    df_atk = pd.DataFrame(atk_data_list)
    df_moves = pd.read_csv('pkmn_moves.csv')
    
    df_atk = df_moves[['mid','move']].merge(df_atk, left_on='move', right_on='name' )
    df_atk = df_atk.rename(columns={'mid':'fk_mid'})
    df_atk = df_atk.drop_duplicates()
    df_atk = df_atk.drop(['move'],1)
    df_atk.to_pickle('atk.pkl')
    df_atk.to_csv('pkmn_atk.csv',index=False)
    
    
def scanMoves():
    dfs = []
    
    move_pages = glob.glob(os.path.join(POKEMON_MOVES_PATH, '*'))
    for idx, move_page in enumerate(move_pages):
        with open(move_page, 'rb') as fle:
            move = fle.read()
        soup = BeautifulSoup(move)     
        name = soup.find_all('table', attrs={'class':'dextab'})[0].find('font').get_text().strip()
        zcheck = soup.find_all('table', attrs={'class':'dextable'})[0].find_all('tr')[5]
        
        #pdb.set_trace()
       
        if('Z-Power' not in str(zcheck)):
            print idx, name
            
            pkmn_list = []
            for method in ['level', 'TM', 'egg']:
                try:  
                    tbl = soup.find('a', attrs={'name':'{0}'.format(method)}).find_next('table', attrs={'class':'dextable'})
                
                    for row in tbl.find_all('tr')[2:]:
                        try:
                            cells = row.find_all('td')
                            #pdb.set_trace()
                            pkmn = OrderedDict()
                            pkmn['pid'] = str(cells[0].get_text()).split('#')[1]
                            pkmn['mid'] = str(idx - 1)
                            pkmn['name'] = cells[3].get_text()
                            pkmn['move'] = name
                            
                            if method == 'level': pkmn['method'] = cells[-1].get_text()
                            elif method == 'TM': pkmn['method'] = 'TM'
                            elif method == 'egg': pkmn['method'] = 'Egg'
                            
                            pkmn_list.append(pkmn)

                        except IndexError:
                            pass
                    
                except AttributeError:
                    pass
                df = pd.DataFrame(pkmn_list)    
                dfs.append(df)    
            else:
                pass
            
    
    df_moves = pd.concat(dfs)
    df_moves = df_moves.applymap(lambda s: "".join(i for i in s if 31 < ord(i) < 127))
    df_moves.to_pickle('df_moves.pkl')
    df_moves.to_csv('pkmn_moves.csv')
    pdb.set_trace()
if __name__=="__main__":
    main()

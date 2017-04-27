import os
import time
import pdb

from collections import OrderedDict

import requests
import pandas as pd

from bs4 import BeautifulSoup


POKEMON_DIRPATH_SER = r'C:\Users\jcarr\Desktop\pokemon_scripts\pokemon_ser'
POKEMON_DIRPATH_PDB = r'C:\Users\jcarr\Desktop\pokemon_scripts\pokemon_pdb'
POKEMON_DIRPATH_ATK = r'C:\Users\jcarr\Desktop\pokemon_scripts\pokemon_atk'

# stripps all non-ASCII chars usinf function stripped
# stripped = lambda s: " ".join(i for i in s if 31 < ord(i) < 127)    
# print(stripped("\ba\x00b\n\rc\fd\xc3"))

def main():
    #getSerDex()
    #getPDBDex()
    getATKDex()
    pass

def getSerDex(): 
    root_url = "http://www.serebii.net/pokedex-sm/"    
    numbers = [str(i).zfill(3) for i in xrange(1,803)]
   
    errors = []
    for number in numbers:
        try:
            page = root_url + number + '.shtml'
            
            result = requests.get(page).content
            soup = BeautifulSoup(result)
            
            name = soup.find('table', attrs={'class': 'dextab'}).find('table').find_all('td')[1]
            name = ''.join(name.get_text().split(' ')[1:])
        
            filename = number + '_' + name + '.html'
            filepath = os.path.join(POKEMON_DIRPATH_SER, filename)
            
            with open(filepath, 'wb') as fle:
                fle.write(result)
            
            print 'Processed {0} ---- {1}'.format(number, name)
            time.sleep(1)
            
        except UnicodeEncodeError:
            errors.append(number)
            
    print ("Completed\n\n")    
    print ("Errors:")
    for error in errors:
        print '\t', (error)
    

    
def getPDBDex():   
    root_url = "http://pokemondb.net/pokedex/"    
   
    data = pandas.read_csv('df.csv',header=0)
    names = list(data.name)
    
    errors = []
    for idx,name in enumerate(names):
        #pdb.set_trace()
        try:
            page = root_url + name
            
            result = requests.get(page).content
            soup = BeautifulSoup(result,"html.parser")
            
            num = idx+1
            filename = str(num) + '_' + name + '.html'
            filepath = os.path.join(POKEMON_DIRPATH_PDB, filename)
            
            with open(filepath, 'wb') as fle:
                fle.write(result)
            
            print 'Processed {0} ---- {1}'.format(num, name)
            time.sleep(1)
            
            #pdb.set_trace() # comment this
        
        except UnicodeEncodeError:
            errors.append(number)
            
    print ("Completed\n\n")    
    print ("Errors:")
    for error in errors:
        print (error)    
    
def getATKDex():
    #pdb.set_trace()
    main_page = 'http://www.serebii.net/attackdex-sm/'
    result = requests.get(main_page).content
    soup = BeautifulSoup(result)

    root_url = 'http://www.serebii.net'
    move_urls_list = []
    filenames=[]
    
    drop_downs = soup.find_all('select', attrs={'name':'SelectURL'})
    for drop_down in drop_downs:
        for option in drop_down.find_all('option')[1:]:
            move_url = root_url + option['value']
            move_urls_list.append(move_url) 
            filenames.append(move_url.split('/')[-1].replace('.shtml','.html'))
    
    errors = []
    for idx,filename in enumerate(filenames):
        #pdb.set_trace()
        try:
            page = move_urls_list[idx]
            
            result = requests.get(page).content
            soup = BeautifulSoup(result,"html.parser")
            
            
            filepath = os.path.join(POKEMON_DIRPATH_ATK, filename)
            
            with open(filepath, 'wb') as fle:
                fle.write(result)
            
            print 'Processed {0} ---- {1}'.format(idx+1, filename)
            time.sleep(1)
            
            #pdb.set_trace() # comment this
        
        except UnicodeEncodeError:
            errors.append(number)
            
    print ("Completed\n\n")    
    print ("Errors:")
    for error in errors:
        print (error)    
        
        
if __name__=="__main__":
    main()

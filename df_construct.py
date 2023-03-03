import requests
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import os
from requests.auth import HTTPBasicAuth
import json
import time


##TEST
text_file = None

##Zur Erfassung von zu großen Tagen gedacht
toobig = False


def construct_day(date_select,region_select,day):
    
    df_day = DataFrame()
    df_day_frac = None
    
    ###Iteriere profession class 
    for prof in range(0,25):
        try:
            time.sleep(3)
            ##muss str sein
            prof = str(prof)
            print(prof)
            response = requests.get('https://www.jobfeed.de/api/v3/search?region='+region_select+'&date='+date_select+'-'+str(day)+'&profession_class='+prof+'&_limit=10000&_labels=0', auth=HTTPBasicAuth('bibb', 'bibbJobfeed2017'))
            response_json = response.json()
    
            ##Wird bei einem Fehler nicht gesetzt und ist None 
            df_day_frac = DataFrame(response_json["results"])
        except Exception:
            #print("Fehler",file=text_file) 
            print("Fehler!")
      
        df_day = df_day.append(df_day_frac)
        
        ##Damit das gleich nicht nochmal angehängt wird
        df_day_frac = None
    
    print(df_day)
    return df_day
	
def construct_day_2(date_select,region_select,day):


    #print("construct_day_2 in df_construnct!")
    
    df_day = DataFrame()
    df_day_frac = None
    
    ###Iteriere profession class 
    for prof in range(0,25):
        try:
            time.sleep(3)
            ##muss str sein
            prof = str(prof)
            #print(prof)
            response = requests.get('https://www.jobfeed.de/api/v3/search?region='+region_select+'&date='+date_select+'-'+str(day)+'&profession_class='+prof+'&_limit=10000&_labels=0', auth=HTTPBasicAuth('bibb', 'bibbJobfeed2017'))
            response_json = response.json()
    
            ##Wird bei einem Fehler nicht gesetzt und ist None 
            df_day_frac = DataFrame(response_json["results"])
        except Exception:
            print("Fehler beim Download von TxtKernel Daten",file=text_file) 
            #print("Fehler!")
      
        df_day = df_day.append(df_day_frac)
        
        ##Damit das gleich nicht nochmal angehängt wird
        df_day_frac = None
		
	##Entferne Duplicate
	#df_day = df_day[df_day[„duplicate“]=False]
	
	##Entferne Duplicate Auskommentiert das so nicht in der Schleife und durch "unique" Funktion ersetzt
    #df_day = df_day[df_day['duplicate']==False]
    
    #print(df_day)
    return df_day
	

def aggregate_month_region(region_select,date_select):
    
    try:
        time.sleep(3)
        print("Bilde Aggregatwert für: "+date_select,file=text_file)
        print("in der Region: "+region_select,file=text_file)
        
        #date_select = '2020-08'
        url = 'https://www.jobfeed.de/api/v3/aggregate?date__range='+date_select+'-01__' +date_select+'-01%7C%7C%2B1M-1d%0A%0A&region='+region_select
        #print ("GESUCHT: "+url)
        
        response = requests.get(url, auth=HTTPBasicAuth('bibb', 'bibbJobfeed2017'))
   
        Kontrollwert = response.json()[1][1]
        print(Kontrollwert,file=text_file)
        return Kontrollwert
    except Exception:
        print("Fehler bei der Aggregatbildung",file=text_file)
        return None
		
def aggregate_month(date_select):
    
    try:
        time.sleep(3)
        print("Bilde Aggregatwert für: "+date_select,file=text_file)
        
        response = requests.get('https://www.jobfeed.de/api/v3/aggregate?date__range='+date_select+'-01__' +date_select+'-01%7C%7C%2B1M-1d%0A%0A', auth=HTTPBasicAuth('bibb', 'bibbJobfeed2017'))
        
        Kontrollwert = response.json()[1][1]
        print(Kontrollwert,file=text_file)
        return Kontrollwert
    except Exception:
        print("Fehler bei der Aggregatbildung",file=text_file)
        return None
	
def get_month_region_retry(region_select,month_select):
    
    
    ##Bilde Aggregat für Monat 
    Kontrollwert = aggregate_month_region(region_select,month_select)
    
    retrys = 5
    
    data_ok = False
    while (data_ok != True and retrys != 0):
        
        ##Fordere Daten als Ergebniswert einer anderen Funktion an 
        ergebnis_r = get_month_region(region_select,month_select)
        
        ##Geht nicht
        #ergebnis_r = ergebnis_r.job_id.unique()
        
        #ergebnis_unique = len(ergebnis_r)
        
        ergebnis_unique = len(ergebnis_r.job_id.unique())
        
        ###NEU verwendet jetzt ergebnis unique
        if ergebnis_unique == Kontrollwert:
            print("Alles prima",file=text_file)
            
            ###NEU !! 
            ergebnis_r = ergebnis_r.drop_duplicates(subset="job_id")
           
            #print(len(ergebnis_r))
            
            ##Beende While Schleife
            data_ok = True
        else:
            print("Doof, da musst du wohl noch mal ran!",file=text_file)
            print("Also nochmal! ",file=text_file)
            
            ##Direkter Output im Falle von Problemen
            #print("Doof, da musst du wohl noch mal ran!")
            #print("Also nochmal! ")
            retrys = retrys-1
            
            #NEU!
            ergebnis_r = None
            
    ##Rückgabe eine überprüften Ergebnis
    return ergebnis_r
	
def get_month_region(region_select,date_select):
    
    df_day = None
    
    df_month = DataFrame()
    
    
    ## Über 10000 Einträge für einen Tag in einem Monat
    too_large = False
    
    for day in range(1,32):
        try:
            time.sleep(3)
            
            ##NEU day muss neu definiert werden da das Format 1-09-2020 nicht mehr aktzeptiert wird! 
            
            ## Einfache Anhaengung von einer fuehrenden Null falls day <10 
            if day < 10:
                day = str(day)
                day = '0'+day
            
            
            print("Anfrage an TXT Kernel für den Tag: "+str(date_select)+'-'+str(day),file=text_file)
            print("Für die Region: "+region_select,file=text_file)
           
            response = requests.get('https://www.jobfeed.de/api/v3/search?region='+region_select+'&date='+date_select+'-'+str(day)+'&_limit=10000&_labels=0', auth=HTTPBasicAuth('bibb', 'bibbJobfeed2017'))
            response_json = response.json()
        
    
            
            df_day = DataFrame(response_json["results"])
            
            print("Daten erhalten",file=text_file)
            
            ##NEU!
            
            response = None
            response_json = None
            
            
            
            print(str(len(df_day))+" Zeilen in DF",file=text_file)
            
            if (len(df_day) == 0):
                print("Anfrage zu einem Tag enthielt keine Einträge !",file=text_file)
            
            ####!! ist von >= 10000 auf 1 gesetzt um zu TESTEN
            if (len(df_day) >= 10000):
                print("Anfrage zu einem Tag enthielt über 10000 Einträge !",file=text_file)
                #print("Anfrage zu einem Tag enthielt über 10000 Einträge !")
                
                global toobig
                
                toobig = True
                
                
                ##Anmerkung: Es wäre auch gut diese Information in der Ausgabe json (nicht die Fehler json)
                ## zu speichern
                
                ##Neue FUnktion um Tag mit vielen Abfragen zu zerlegen 
                df_day = construct_day_2(date_select,region_select,day)
                
                #def construct_day(date_select,region_select,day):
                
                #print("Tag mit über 10000 Einträgen konstruirt !")
                print("Tag mit über 10000 Einträgen konstruirt !",file=text_file)
                
            
        ### Kann im Falle eine kürzeren Monats auftreten in dem Fall wird das Rückgabe Ergebnis nicht verändert    
        except Exception:
            print("Fehler beim Download von TxtKernel Daten",file=text_file)  
      
        df_month = df_month.append(df_day)
        
        ##Damit das gleich nicht nochmal angehängt wird
        df_day = None
        
    ##Anmerkung: Hier soll sollen nur die Einträge mit unique job id vorhanden sein, Funktion aus tx_kernel_combine
    #if too_large == True:
        ### Verwefe Duplikate
        #df_month = df_month.drop_duplicates(subset='job_id')
    
    return df_month
	

	

    

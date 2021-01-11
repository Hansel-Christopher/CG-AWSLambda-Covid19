import requests
import json 
import boto3
import csv  
from datetime import datetime


class covid19apiaccess:  
    def upload_file(self, fn):
        s3 = boto3.resource('s3')
        date = datetime.today().strftime('%Y-%m-%d')
        datest = datetime.today().strftime('%Y%m%d%H%M%S')
        s3.meta.client.upload_file(fn, 'coda-christopher-covid-data-s3-58563', date+'/casescount.csv')
        return {
            'status': 'True',
            'statusCode': 200,
            'body': 'CSV Uploaded'
        }
        
    def totalcases(self):
        response = requests.get("https://api.covid19api.com/summary")    
        data=[]
        parsed_data = json.loads(response.text)        
        z = parsed_data["Countries"]    
        for i in range(0, len(z)):
            s = z[i]
        data.append([s['Country'],s['TotalConfirmed']])   
        f1 = "/tmp/casescount.csv"     
        fields = ['Country', 'Cases']  
        with open(f1, 'w') as csvfile:      
            csvwriter = csv.writer(csvfile)      
            csvwriter.writerow(fields)              
            csvwriter.writerows(data)         
        fname = "casecount"
        self.upload_file(f1)

        return

    def country_wise(self, country, fromdate, todate):
        response2 = requests.get("https://api.covid19api.com/country/"+country+"/status/confirmed?from="+fromdate+"&to="+todate)
        sumi = 0
        ps = json.loads(response2.text)
        
        for j in range(0, len(ps)):
            a = ps[j]
            sumi+= a["Cases"]
        data2=[]
        data2.append(["India",sumi])
            

    
    pass


x = covid19apiaccess()
x.totalcases()


    Accessing Covid19API summary endpoint




    response = requests.get("https://api.covid19api.com/summary")
    
    data=[]
    data3=[]
    countries = []
    
    #Parsing Summary data for countrywise data
    

    parsed_data = json.loads(response.text)        
    z = parsed_data["Countries"]    

    for i in range(0, len(z)):
        s = z[i]
        data.append([s['Country'],s['TotalConfirmed']])    
    
    

    #Using CSV writer class to perform creation of CSV files
    
    
    f2 = "/tmp/indiacases.csv"
    f3 = "/tmp/top3.csv"
        
    fields = ['Country', 'Cases']  
    fields3 = ['Country', 'TotalDeaths']              
        
    with open(f2, 'w') as csvfile:      
        csvwriter = csv.writer(csvfile)      
        csvwriter.writerow(fields)              
        csvwriter.writerows(data2) 
    
    x = sorted(z, key = lambda i: i['TotalDeaths'], reverse=True)
    y = x[0:3]
    
    for i in range(0,len(y)):
        aa = y[i]
        data3.append([aa['Country'],aa['TotalDeaths']])
    
    with open(f3, 'w') as csvfile:      
        csvwriter = csv.writer(csvfile)      
        csvwriter.writerow(fields3)              
        csvwriter.writerows(data3) 


    # Using boto3 to perform uploading of csv files to s3

    s3 = boto3.resource('s3')
    date = datetime.today().strftime('%Y-%m-%d')
    datest = datetime.today().strftime('%Y%m%d%H%M%S')
    s3.meta.client.upload_file(f1, 'coda-christopher-covid-data-s3-58563', date+'/casescount.csv')
    s3.meta.client.upload_file(f2, 'coda-christopher-covid-data-s3-58563', date+'/indiacases.csv')
    s3.meta.client.upload_file(f3, 'coda-christopher-covid-data-s3-58563', date+'/covid19_top3_affected_'+datest+'.csv')
    return {
        'status': 'True',
        'statusCode': 200,
        'body': 'CSV Uploaded'
    }
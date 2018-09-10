import requests
import lxml.html
import sys
import codecs
#import xlsxwriter
#import openpyxl
#from openpyxl import load_workbook
import time
import datetime
import os
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler
#import boto3
#import csv


'''
# input : UU:MM => YYYY/MM/DD UU:00:00
def convert_time_to_date_time(etime):
    etime_array = etime.split(":")
    print(etime_array)
    #print(etime_array[1])
    etime = etime_array[0] + ":" + "00"
    print(etime)
    etime += ':00'
    print(etime)
    current_datetime = str(datetime.datetime.now())[0:10]
    print(current_datetime)
    current_datetime = current_datetime + " " + etime
    print(current_datetime)
    return current_datetime
def csv_writer(output, path):
    """
    Write output to a CSV file path
    """
    with open(path, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        #print(writer)
        #writer.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])
        for line in output:
            writer.writerow(line)
            
'''

def write_bel20_to_amazon_s3():


    #print('=> begin write_bel20_to_amazon_s3')   

    # zorgt ervoor dat speciale karakters geprint worden : vb. Chinese karakters
    #sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)

    html = requests.get('https://www.beursduivel.be/Koersen/Aandelen.aspx')
    #print(type(html))
    doc = lxml.html.fromstring(html.content)
    #print(type(doc))
    Issue_Default = doc.xpath('//div[@class="IssueDefault"]')[0]
    #print(type(Issue_Default))
    titles = Issue_Default.xpath('.//td[@class="TitleCell DateTimeCell"]/a/text()')
    #print(type(titles))
    lastprices = Issue_Default.xpath('.//td[@class="ValueCell"][1]/span/text()')
    highprices = Issue_Default.xpath('.//td[@class="ValueCell"][4]/span/text()')
    lowprices = Issue_Default.xpath('.//td[@class="ValueCell"][5]/span/text()')
    finals = Issue_Default.xpath('.//td[@class="ValueCell"][6]/span/text()')
    times = Issue_Default.xpath('.//td[@class="ValueCell"][7]/span/text()')

    #print(times)
    #print(finals)


    localtime = time.localtime(time.time())
    #print("Local current time :", localtime.tm_year)

    row = 0

    HOST = "pemotec.be"
    PORT = 3306
    USER = "most_pemotec"
    PASSWORD = "z9H5%R^pjPxM&ePA"
    DB = "pemotec_test"


    index = 0
    connection = pymysql.Connect(host=HOST, port=PORT,
                               user=USER, passwd=PASSWORD, db=DB)
    dbhandler = connection.cursor()    
    dbhandler.execute("SELECT count(*) from koersen")    
    result = dbhandler.fetchall()
    for i in result:
        for j in i:
            index = j                
    print(index)
    #for info in zip(titles, lastprices, highprices, lowprices,finals,times):
    #csvfile = open('koersen.csv', 'w')
    #with csvfile:                 
    #    myFields = ['title','price','high','low','final','time']
    #    writer = csv.DictWriter(csvfile,fieldnames=myFields)
    #    writer.writeheader()
    for info in zip(titles[1:], lastprices[1:], highprices[1:], lowprices[1:],finals[1:],times[1:]):        
            #print(str(datetime.datetime.now())[0:17]+'00')
            resp = {}
            resp['title'] = info[0]
            resp['price'] = float(info[1].replace(',','.'))            
            resp['high']  = float(info[2].replace(',','.'))            
            resp['low']   = float(info[3].replace(',','.'))            
            resp['final'] = float(info[4].replace(',','.'))            
            #print(localtime.tm_mon)




            datum = datetime.datetime.strptime(str(datetime.datetime.now())[0:17]+'00', "%Y-%m-%d %H:%M:%S")
            print(datum)
            index += 1  
            print(index)  
            dbhandler.execute("INSERT INTO koersen values (%s,%s,%s,%s,%s,%s,%s)",(resp['title'],resp['price'],resp['high'],resp['low'],resp['final'],datum,index))
    connection.commit()
    dbhandler.close()            

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(write_bel20_to_amazon_s3, 'interval', seconds=10)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
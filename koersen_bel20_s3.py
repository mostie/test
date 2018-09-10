import requests
import lxml.html
import sys
import time
import datetime
import os
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler

def write_bel20():


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

    for info in zip(titles[1:], lastprices[1:], highprices[1:], lowprices[1:],finals[1:],times[1:]):        
            resp = {}
            resp['title'] = info[0]
            resp['price'] = float(info[1].replace(',','.'))            
            resp['high']  = float(info[2].replace(',','.'))            
            resp['low']   = float(info[3].replace(',','.'))            
            resp['final'] = float(info[4].replace(',','.'))            
            datum = datetime.datetime.strptime(str(datetime.datetime.now())[0:17]+'00', "%Y-%m-%d %H:%M:%S")
            print(datum)
            index += 1  
            print(index)  
            dbhandler.execute("INSERT INTO koersen values (%s,%s,%s,%s,%s,%s,%s)",(resp['title'],resp['price'],resp['high'],resp['low'],resp['final'],datum,index))
    connection.commit()
    dbhandler.close()            

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(write_bel20, 'interval', seconds=10)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
pass
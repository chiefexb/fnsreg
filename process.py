#!/usr/bin/python
#coding: utf8
from lxml import etree
from datetime import *
import timeit
import time
import fdb
import sys
from os import *
import logging
fields=['packet_id','packet_date','request_id','request_date','debitor_name','debitor_birthday','debitor_inn']
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".forma$
        st="Elapsed time:"+str(time.time() - self._startTime) # {:.3f} sec".for$
        logging.info(st)
def getgenerator(cur,gen):
 sq="SELECT GEN_ID("+gen+", 1) FROM RDB$DATABASE"
 try:
  cur.execute(sq)
 except:
  print "err"
 cur.execute(sq)
 r=cur.fetchall()
 try:
  g=r[0][0]
 except:
  g=-1
 return g
def quoted(a):
 st="'"+a+"'"
 return st
def main():
#Обработка параметров
 #print len (sys.argv)
 if len(sys.argv)<=1:
  print ("getfromint: нехватает параметров")
  sys.exit(2)
 print sys.argv[1]
#Открытие файла конфигурации
 try:
  f=file('./fns.xml')
 except Exception,e:
  print e
  sys.exit(2)
#Парсим xml конфигурации
 cfg = etree.parse(f)
 cfgroot=cfg.getroot()
 systemcodepage=cfgroot.find('codepage').text
#Ищем параметры базы
 dbparams=cfgroot.find('database_params')
 username=dbparams.find('username').text
 password=dbparams.find('password').text
 hostname=dbparams.find('hostname').text
 concodepage=dbparams.find('connection_codepage').text
 codepage=dbparams.find('codepage').text
 database=dbparams.find('database').text
 #log_file=logpar.find('log_file').text
 #log_file2=logpar.find('log_file2').text
#Определяем тип и путь файла
 filepar=cfgroot.find('file')
 output_path=filepar.find('output_path').text
 input_path=filepar.find('input_path').text
 input_arc_path=filepar.find('input_path_arc').text
 input_spo_path=filepar.find('input_path_spo').text
 input_spo_arc_path=filepar.find('input_path_spo_arc').text
 input_spo_err_path=filepar.find('input_path_spo_err').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filescheme=filepar.findall('scheme')
 zaproses=filescheme[0][0]
 if sys.argv[1]=='upload':
  print zaproses.tag
  zapros=zaproses.getchildren()[0]
  zaprostag=zapros.tag
  fld={}
  for ch in zapros.getchildren():
   if ch.text in fields:
    fld[ch.text]=ch.tag
  print fld
  ff=listdir(input_spo_path)
  print ff[0]
  fff=ff[0]
  xmlfile=file(input_spo_path+fff) #'rr4.xml')
  xml=etree.parse(xmlfile)
  xmlroot=xml.getroot()
  print xmlroot.tag
  zaproses=xmlroot.findall(zaprostag)
  print len (zaproses)
  sqlreq='INSERT INTO REQUESTS (ID, DATE_LOAD, PACKET_ID, PACKET_DATE, REQUEST_ID, REQUEST_DATE, DEBITOR_NAME, DEBITOR_BIRTHDAY, DEBITOR_INN, PROCESSED, DATE_PROCESSED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
  now=datetime.now()
  date_load=datetime.strftime(now,'%d.%m.%Y')
  print date_load
  chh= zaproses[0]
  rr={}
   #Соединяемся с базой ОСП
  try:
   con = fdb.connect (host=hostname, database=database, user=username, password=password,charset=concodepage)
  except  Exception, e:
   print("Ошибка при открытии базы данных:\n"+str(e))
   sys.exit(2)
  cur = con.cursor()

  for ffff in fld.keys():
   rr[ffff]=chh.find(fld[ffff]).text
  print rr
  #Проверка на дубликаты, если в данном файле packet_id совпадает с тем что в базе
  #Прекратить загрузку
  packet_id=rr['packet_id']
  sq='select * from requests where requests.packet_id='+(packet_id)
  print sq
  cur.execute(sq)
  rec=cur.fetchall()
  print len(rec)
  if len(rec)==0:
   id=getgenerator(cur,'GENREQ')
  else:
   st=u'Загрузка файла '+ff+u' невозможна, так как он уже загружен в базу, packet_id='+(packet_id)+u'есьб  в базе.'
   logging.error( st ) #logging.error
   rename(input_path+ff, input_err_path+ff)
 #EndIF
 return
if __name__ == "__main__":
    main()

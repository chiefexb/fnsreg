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
class Profiler(object):
    def __enter__(self):
        self._startTime = time.time()

    def __exit__(self, type, value, traceback):
        print "Elapsed time:",time.time() - self._startTime # {:.3f} sec".forma$
        st="Elapsed time:"+str(time.time() - self._startTime) # {:.3f} sec".for$
        logging.info(st)
const={}
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
 input_spo__arc_path=filepar.find('input_path_spo_arc').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filescheme=filepar.findall('scheme')
 zaproses=filescheme[0][0]
 print zaproses.tag
 zapros=zaproses.getchildren()[0]
 print zapros.tag
 return
if __name__ == "__main__":
    main()

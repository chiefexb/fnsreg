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
def main():
#Обработка параметров
 #print len (sys.argv)
 if len(sys.argv)<=1:
  print ("getfromint: нехватает параметров\nИспользование: getfromint ФАЙЛ_КОНФ$
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
logpar=cfgroot.find('logging')
 log_path=logpar.find('log_path').text
 log_file=logpar.find('log_file').text
 log_file2=logpar.find('log_file2').text
#Определяем тип и путь файла
 filepar=cfgroot.find('file')
 filecodepage=filepar.find('codepage').text
 output_path=filepar.find('output_path').text
 input_path=filepar.find('input_path').text
 input_arc_path=filepar.find('input_path_arc').text
 input_err_path=filepar.find('input_path_err').text
 filetype=filepar.find('type').text
 filenum=filepar.find('numeric').text
 #Определение схемы файла должна быть ветка для типов файлов пока разбираем xml
 filescheme=filepar.findall('scheme')
 return
if __name__ == "__main__":
    main()

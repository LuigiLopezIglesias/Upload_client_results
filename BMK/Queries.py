#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Tools to be called in principal script
'''

#######################################################################################################
#                                                                                                     
#######################################################################################################

## Modules needed
import sqlalchemy as sa
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker

#--# Conexion with Database---------------------------------------------------------------------------
def DBconexion():
  return sa.create_engine("postgresql://"+os.environ['DB_USER']+":"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_NAME'])

#--# Query against Database for Fastq files information-----------------------------------------------
def DBWsFastqQuery(PoolName):
  return pd.read_sql_query('select rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, rs.chain_type_id '
                              'from  run_samples rs'
                              ' join runs          r on rs.run_id         = r.id'
                              ' join muestra       m on m.id              = rs.sample_id'
                              ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                              ' join cliente       c on c.id              = m. id_cliente '
                              'where r.name = \''+PoolName+'\''
                              ' and processed'
                              ' and processed_history = 1'
                              ' and m.c_tipo_muestra in (1,2,3)'
                              ' and m.c_estado = 3 '
                              'order by m.c_muestra_wineseq;', DBconexion())

#--# Query Against Database for BMK Abundances origin-----------------------------------------------------
def DBAbundancesQuery(PoolName):
  return pd.read_sql_query('select rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, ct.name, c.ftp_path'
                              ' from run_samples   rs'
                              ' join runs           r on rs.run_id = r.id'
                              ' join muestra        m on m.id = rs.sample_id'
                              ' join tipo_muestra  tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                              ' join cliente        c on c.id = m.id_cliente'
                              ' join chain_types   ct on ct.id = rs.chain_type_id'
                              ' where rs.sample_id IN (SELECT rs.sample_id'
                                                            ' FROM run_samples rs'
                                                            ' join runs        r on rs.run_id = r.id'
                                                            ' join muestra     m on m.id = rs.sample_id'
                                                            ' WHERE r.name = \''+PoolName+'\''
                                                            ' and processed'
                                                            ' and processed_history = 1'
                                                            ' and m.c_estado = 3 '
                                                            ' and m.c_tipo_muestra != 1 and m.c_tipo_muestra != 2 and m.c_tipo_muestra != 3'
                                                            ' order by rs.sample_id)'
                              ' and processed and processed_history = 1'
                              ' and c.ftp_path != \'upload_errors\''
                              ' and r.name !=  \'upload_errors\'',DBconexion())

#--# Query against Database for metadata information--------------------------------------------------
def DBMetadataQuery(PoolName):
  return DBconexion().execute('select distinct tm.d_tipo_muestra, c.ftp_path, m.id_cliente '
                              'from  run_samples rs'
                              ' join runs          r on rs.run_id         = r.id'
                              ' join muestra       m on m.id              = rs.sample_id'
                              ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                              ' join cliente       c on c.id              = m. id_cliente '
                              'where r.name = \''+PoolName+'\''
                              ' and processed_history = 1'
                              ' and m.c_tipo_muestra != 1 and m.c_tipo_muestra != 2 and m.c_tipo_muestra != 3'
                              ' and m.c_estado = 3 '
                              ' order by m.id_cliente ')

#--# Query to know finished samples for client and sampletype----------------------------------------- 
def DBClientQuery(client, SampleType):
  return pd.read_sql_query('select m.c_muestra_wineseq, m.client_reference '
                                'from muestra m'
                                ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                                ' join cliente       c on c.id              = m. id_cliente '
                                'where m.c_estado = 3'
                                ' and m.id_cliente = '+str(client)+''
                                ' and tm.d_tipo_muestra = \''+SampleType+'\'',DBconexion())

#--# Query to insert results of pipeline--------------------------------------------------------------

def DBCompleateSamplesQuery(sampleList):
  DBconexion().execute('insert into ftp_notifications(sample_id) '
                       ' select id from muestra m' 
                       '    where c_muestra_wineseq in ('+sampleList+')'
                       '    and not exists (select sample_id from ftp_notifications'
                                            '   where m.id = sample_id);')


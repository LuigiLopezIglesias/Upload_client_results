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
  return pd.read_sql_query('SELECT rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, rs.chain_type_id '
                           '  FROM run_samples rs'
                           '  JOIN runs          r ON rs.run_id         = r.id'
                           '  JOIN muestra       m ON m.id              = rs.sample_id'
                           '  JOIN tipo_muestra tm ON tm.c_tipo_muestra = m.c_tipo_muestra'
                           '  JOIN cliente       c ON c.id              = m. id_cliente '
                           ' WHERE rs.sample_id IN (SELECT rs.sample_id'
                                                '     FROM run_samples rs'
                                                '     JOIN runs        r ON rs.run_id = r.id'
                                                '     JOIN muestra     m ON m.id      = rs.sample_id'
                                                '    WHERE r.name = \''+PoolName+'\''
                                                '      AND processed'
                                                '      AND processed_history = 1'
                                                '      AND m.c_estado = 3 '
                                                '      AND m.c_tipo_muestra IN (1,2,3)'
                                                ' ORDER BY rs.sample_id)'
                           '   AND processed'
                           '   AND processed_history = 1'
                           ' ORDER BY m.c_muestra_wineseq;', DBconexion())
                           
#--# Query Against Database for BMK Abundances origin-----------------------------------------------------
def DBAbundancesQuery(PoolName):
  return pd.read_sql_query('SELECT rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, ct.name, c.ftp_path'
                           '  FROM run_samples   rs'
                           '  JOIN runs           r ON rs.run_id         = r.id'
                           '  JOIN muestra        m ON m.id              = rs.sample_id'
                           '  JOIN tipo_muestra  tm ON tm.c_tipo_muestra = m.c_tipo_muestra'
                           '  JOIN cliente        c ON c.id              = m.id_cliente'
                           '  JOIN chain_types   ct ON ct.id             = rs.chain_type_id'
                           ' WHERE rs.sample_id IN (SELECT rs.sample_id'
                                                '     FROM run_samples rs'
                                                '     JOIN runs        r ON rs.run_id = r.id'
                                                '     JOIN muestra     m ON m.id      = rs.sample_id'
                                                '    WHERE r.name = \''+PoolName+'\''
                                                '      AND processed'
                                                '      AND processed_history = 1'
                                                '      AND m.c_estado = 3 '
                                                '      AND m.c_tipo_muestra NOT IN (1,2,3)'
                                                ' ORDER BY rs.sample_id)'
                           '   AND processed'
                           '   AND processed_history = 1'
                           '   AND c.ftp_path != \'upload_errors\''
                           '   AND r.name !=  \'upload_errors\'',DBconexion())

#--# Query against Database for metadata information--------------------------------------------------
def DBMetadataQuery(PoolName):
  return pd.read_sql_query('SELECT distinct tm.d_tipo_muestra, c.ftp_path, m.id_cliente '
                        '     FROM run_samples rs'
                        '     JOIN runs          r ON rs.run_id         = r.id'
                        '     JOIN muestra       m ON m.id              = rs.sample_id'
                        '     JOIN tipo_muestra tm ON tm.c_tipo_muestra = m.c_tipo_muestra'
                        '     JOIN cliente       c ON c.id              = m. id_cliente '
                        '    WHERE r.name = \''+PoolName+'\''
                        '      AND processed_history = 1'
                        '      AND m.c_tipo_muestra NOT IN (1,2,3)'
                        '      AND m.c_estado = 3 '
                        ' ORDER BY m.id_cliente ',DBconexion())

#--# Query to know finished samples for client and sampletype----------------------------------------- 
def DBClientQuery(client, SampleType):
  return pd.read_sql_query('SELECT m.c_muestra_wineseq, m.client_reference '
                           '  FROM muestra m'
                           '  JOIN tipo_muestra tm ON tm.c_tipo_muestra = m.c_tipo_muestra'
                           '  JOIN cliente       c ON c.id              = m. id_cliente '
                           ' WHERE m.c_estado = 3'
                           '   AND m.id_cliente = '+str(client)+''
                           '   AND tm.d_tipo_muestra = \''+SampleType+'\'',DBconexion())

#--# Query to insert results of pipeline--------------------------------------------------------------

def DBCompleateSamplesQuery(sampleList):
  DBconexion().execute('INSERT INTO ftp_notifications(sample_id) '
                       '     SELECT id '
                       '       FROM muestra m' 
                       '      WHERE c_muestra_wineseq in ('+sampleList+')'
                       '    AND NOT EXISTS (SELECT sample_id '
                                           '  FROM ftp_notifications'
                                           ' WHERE m.id = sample_id);')


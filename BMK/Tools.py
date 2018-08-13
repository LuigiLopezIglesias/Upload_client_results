#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Tools to be called in principal script
'''

#######################################################################################################
#                                                                                                     
#######################################################################################################

## Modules needed
import boto3
import sqlalchemy as sa
import os

#--# Conexion with Database---------------------------------------------------------------------------
def DBconexion():
  return sa.create_engine("postgresql://"+os.environ['DB_USER']+":"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_NAME'])

#--# Query against Database---------------------------------------------------------------------------
def DBQuery(PoolName):
  return DBconexion().execute('select rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, rs.chain_type_id '
                         'from  run_samples rs'
                         ' join runs          r on rs.run_id         = r.id'
                         ' join muestra       m on m.id              = rs.sample_id'
                         ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                         ' join cliente       c on c.id              = m. id_cliente '
                         'where r.name = \''+RunDate+'\''
                         ' and processed'
                         ' and processed_history = 1'
                         ' and m.c_estado = 3 '
                         'order by m.c_muestra_wineseq;')

#--# Fastq Moving fastq from Run S3 to General or Clients FTP-----------------------------------------
def FastqMovment(PoolName, Asignment):
  for i in DBQuery(PoolName):
    DbName = i[1]
    SampleType = i[2]
    RunName = i[3]
    PoolmixName = i[4]
    ChainType = i[5]

    if Asignment == 'FTP':
      print('Uploading file \x1b[1;31;10m'+RunName+'\x1b[0m --> \x1b[1;33;10m'+DbName +'\x1b[0m with type \x1b[1;36;10m'+SampleType+'\x1b[0m of \x1b[1;35;10m'+ClientFtpPath+'\x1b[0m client of date \x1b[1;31;10m'+PoolmixName[0:8]+'\x1b[0m')
    elif Asignment == 'General':
      print('Uploading file \x1b[1;31;10m'+str(RunName)+'\x1b[0m to good samples bucket folder '+PoolmixName[:3]'1231')
    else:
      print('Unknown Destination of use of this function')
      break

    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')

    if Asignment == 'FTP':
      operation_parameters = {'Bucket': 'raw-illumina-runs',
                              'Prefix': 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+RunName+'/'}
    else:
      operation_parameters = {'Bucket': 'raw-illumina-runs',
                              'Prefix': 'RUN_'+PoolmixName[:8]+'/FASTQ/'+PoolmixName[:8]+'_'+RunName+'/'}

    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
      forward = page['Contents'][0]['Key']
      reverse = page['Contents'][1]['Key']
      for sense in (forward,reverse):
        s3 = boto3.resource('s3')
        copy_source = {
          'Bucket': 'raw-illumina-runs',
          'Key': sense
        }
        if Asignment == 'FTP':
          if ChainType == 2:
            s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/Fungus/Fastq/'+DbName+'_S'+sense.split(RunName+'_S',1)[1])
          elif ChainType == 1:
            s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/Bacteria/Fastq/'+DbName+'b_S'+sense.split(RunName+'_S',1)[1])
          else:
            print('The sample '+DbName+' have a chain type different of Fungus or Bacteria')
        else:
          s3.meta.client.copy(copy_source, 'raw-illumina-runs', 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+RunName+'/'+RunName+'_S'+sense.split(RunName+'_S',1)[1])

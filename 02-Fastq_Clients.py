import optparse
import sqlalchemy as sa
import os
import boto3
import pandas as pd

parser = optparse.OptionParser()

parser.add_option('-P', '--PoolMix', action="store", dest="Run", help="Run name of poolmix", default=False)
options, args = parser.parse_args()

# Connection with database and query
db = sa.create_engine("postgresql://"+os.environ['DB_USER']+":"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_NAME'])

result_set = db.execute('select rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, c.ftp_path, rs.chain_type_id'
                              ' from  run_samples rs'
                              ' join runs r on rs.run_id = r.id'
                              ' join muestra m on m.id = rs.sample_id'
                              ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                              ' join cliente c on c.id = m. id_cliente'
                              ' where rs.sample_id IN (SELECT rs.sample_id'
                                                            ' FROM run_samples rs'
                                                            ' join runs r on rs.run_id = r.id'
                                                            ' join muestra m on m.id = rs.sample_id'
                                                            ' WHERE r.name = \''+options.Run+'\''
                                                            ' and processed and processed_history = 1'
                                                            ' and m.c_estado = 3 '
                                                            ' and m.c_tipo_muestra != 1 and m.c_tipo_muestra != 2 and m.c_tipo_muestra != 3'
                                                            ' order by rs.sample_id)'
                              ' and processed and processed_history = 1')

# Move and rename Fastq Files
for i in result_set:
  print(i)
  DbName = i[1]
  SampleType = i[2]
  RunName = i[3]
  PoolmixName = i[4]
  ClientFtpPath = i[5]
  ChainType = i[6]
  print('Uploading file \x1b[1;31;10m'+RunName+'\x1b[0m --> \x1b[1;33;10m'+DbName +'\x1b[0m with type \x1b[1;36;10m'+SampleType+'\x1b[0m of \x1b[1;35;10m'+ClientFtpPath+'\x1b[0m client of date \x1b[1;31;10m'+PoolmixName[0:8]+'\x1b[0m')
  if ChainType != 1:
    MO = "Bacteria"
  else:
    MO = "Fungus"
  client = boto3.client('s3')
  paginator = client.get_paginator('list_objects')
  operation_parameters = {'Bucket': 'raw-illumina-runs',
                          'Prefix': 'RUN_'+PoolmixName[0:8]+'/FASTQ/'+PoolmixName[0:8]+'_'+RunName+'/'}
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
      if MO == "Fungus":
       s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/'+MO+'/Fastq/'+DbName+'_S'+sense.split(RunName+'_S',1)[1])
      else:
       s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/'+MO+'/Fastq/'+DbName+'b_S'+sense.split(RunName+'_S',1)[1])


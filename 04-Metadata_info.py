import optparse
import sqlalchemy as sa
import os
import boto3
import pandas as pd
import xlsxwriter
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None


parser = optparse.OptionParser()

parser.add_option('-P', '--PoolMix', action="store", dest="Run", help="Run name of poolmix", default=False)
options, args = parser.parse_args()

# Connection with database and query
db = sa.create_engine("postgresql://"+os.environ['DB_USER']+":"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_NAME'])

result_set = db.execute('select distinct tm.d_tipo_muestra, c.ftp_path, m.id_cliente '
                         'from  run_samples rs'
                         ' join runs          r on rs.run_id         = r.id'
                         ' join muestra       m on m.id              = rs.sample_id'
                         ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                         ' join cliente       c on c.id              = m. id_cliente '
                         'where r.name = \''+options.Run+'\''
                         ' and processed_history = 1'
                         ' and m.c_tipo_muestra != 1 and m.c_tipo_muestra != 2 and m.c_tipo_muestra != 3'
                         ' and m.c_estado = 3 '
                         ' order by m.id_cliente ')

#Selection of client and sample type
for i in result_set:
  print('File with information of client \x1b[1;31;10m'+i[1]+'\x1b[0m and sample type \x1b[1;33;10m'+i[0]+'\x1b[0m' )
  sampleTypeClient = pd.read_sql_query('select m.c_muestra_wineseq, m.client_reference '
                                'from muestra m'
                                ' join tipo_muestra tm on tm.c_tipo_muestra = m.c_tipo_muestra'
                                ' join cliente       c on c.id              = m. id_cliente '
                                'where m.c_estado = 3'
                                ' and m.id_cliente = '+str(i[2])+''
                                ' and tm.d_tipo_muestra = \''+i[0]+'\'',db)
  
  writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Clients_references/'+i[1]+'_'+i[0].replace(" ", "_")+'.xlsx', engine='xlsxwriter')
  sampleTypeClient.to_excel(writer, sheet_name='Sheet1', index=False, startrow=1)
  workbook  = writer.book
  worksheet = writer.sheets['Sheet1']
  worksheet.write('A2', 'Sample Name')
  worksheet.write('B2', 'Client Reference')
  fmt = writer.book.add_format({"font_name": "Liberation Sans",
                                "bold": False,
                                "font_size": 10})
  worksheet.set_column('A:A', 10, fmt)
  worksheet.set_column('B:B', 20, fmt)
  worksheet.set_row(0, 70)
  worksheet.merge_range('A1:B1', '')
  worksheet.insert_image('A1', '/home/yamishakka/Documentos/Biomemakers/Logo-BM_DL_negro-1024x323-e1521645909584.png', {'x_offset': 15, 'y_offset': 10})
  writer.save()
  
  ## Upload files to S3
  client = boto3.client('s3')
  s3 = boto3.resource('s3')
  data = open('/home/yamishakka/Escritorio/Clients_references/'+i[1]+'_'+i[0].replace(" ", "_")+'.xlsx', 'rb')
  s3.Bucket('clientresultsftps3').put_object(Key=i[1]+'/'+i[0].replace(" ", "_")+'/'+i[1]+'_'+i[0].replace(" ", "")+'.xlsx', Body=data) 

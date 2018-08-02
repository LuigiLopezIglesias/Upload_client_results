import pandas as pd
import biom
import xlsxwriter
import optparse
import sqlalchemy as sa
import boto3
import os
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None

parser = optparse.OptionParser()

parser.add_option('-P', '--PoolMix', action="store", dest="Run", help="Run name of poolmix", default=False)
options, args = parser.parse_args()

# Connection with database and query
db = sa.create_engine("postgresql://"+os.environ['DB_USER']+":"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_NAME'])

RunDates = pd.read_sql_query('select rs.sample_id, m.c_muestra_wineseq, tm.d_tipo_muestra, rs.name, r.name, c.ftp_path, ct.name'
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
                                                            ' WHERE r.name = \''+options.Run+'\''
                                                            ' and processed and processed_history = 1'
                                                            ' and m.c_estado = 3 '
                                                            ' and m.c_tipo_muestra != 1 and m.c_tipo_muestra != 2 and m.c_tipo_muestra != 3'
                                                            ' order by rs.sample_id)'
                              ' and processed and processed_history = 1',db)
RunDates.columns = ['rs.sample_id', 'm.c_muestra_wineseq', 'tm.d_tipo_muestra', 'rs.name', 'r.name', 'c.ftp_path', 'ct.name']
for i in RunDates['r.name'].unique():
  aaa = RunDates[RunDates['r.name'].str.startswith(i)]
  for j in aaa['ct.name'].unique():
    bbb = aaa[aaa['ct.name'].str.startswith(j)]
    ### Descarga archivo biom
    ##client = boto3.client('s3')
    ##s3 = boto3.resource('s3')
    ##s3.Bucket('pipeline-analysis-results').download_file(options.Run+'/'+j+'_'+i[0:8]+'_mapped_reads_tax.biom', '/home/yamishakka/Escritorio/Biomemakers/00-Oldtown_storage/Runs/'+j+'_'+i[0:8]+'_mapped_reads_tax.biom')

    ## Analisis archivo biom
    table = biom.load_table('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+i[0:8]+'/'+j+'_'+i[0:8]+'_mapped_reads_tax.biom')
    abunInfo = table.to_dataframe()
    metadataInfo = table.metadata_to_dataframe('observation')
    fullTable = pd.concat([abunInfo, metadataInfo], axis=1)
      
    goodCV = fullTable[fullTable['confidence']>= 0.7].copy()
    goodCV.loc[:,'Species'] = goodCV.loc[:, 'taxonomy_6'].str.replace('s:', '', case=False)
      
    goodCV.taxonomy_6 = goodCV.taxonomy_6.str[2:]
    goodCV = goodCV.drop(['confidence', 'taxonomy_0', 'taxonomy_1', 'taxonomy_2', 'taxonomy_3', 'taxonomy_4', 'taxonomy_5', 'taxonomy_6'], axis=1)
    goodCV = goodCV.rename(columns={'taxonomy_6': 'Species'})
    goodCV = goodCV.sort_values(by=['Species'])
    for index ,row in bbb.iterrows(): 
      DbName = row[1]
      SampleType = row[2]
      RunName = row[3]
      PoolmixName = row[4]
      ClientFtpPath = row[5]
      ChainType = row[6]
      print('Uploading sample \x1b[1;31;10m'+RunName+'\x1b[0m to client \x1b[1;31;10m'+ClientFtpPath+'\x1b[0m of sample type \x1b[1;31;10m'+SampleType+'\x1b[0m from Run \x1b[1;31;10m'+PoolmixName+'\x1b[0m')
   
      ## Si el usuario tiene OTU_Abundance se hace aqui
      
      ## Abundancias para cliente (siempre)
      groupedSpecies = goodCV.groupby(['Species']).sum()
        
      # csv valido para ANITA asi que valdria como sustituto
      ##    groupedSpecies.to_csv('/home/yamishakka/Escritorio/Biomemakers/00-Oldtown_storage/Runs/Abundance_test.csv', sep=',',float_format='%0g', encoding='utf-8')

      sampIndividual = groupedSpecies.loc[:,[RunName]].copy()
      sampIndividual = sampIndividual*100/sampIndividual[RunName].sum()
      sampInd = sampIndividual[sampIndividual[RunName]>0]
      sampInd = sampInd.sort_values(by=RunName, ascending=False)
      if not os.path.exists('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+j+'_Individuales'):
        os.makedirs('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+j+'_Individuales')

      if j == '16S':
        writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+j+'_Individuales/'+DbName+'b_Bacteria.xlsx', engine='xlsxwriter')
      else:
        writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+options.Run[0:8]+'/'+j+'_Individuales/'+DbName+'_Fungus.xlsx', engine='xlsxwriter')

      sampInd.to_excel(writer, sheet_name='Sheet1', startrow=1)
      workbook  = writer.book
      worksheet = writer.sheets['Sheet1']
      if j == '16S':
        worksheet.write('B2', DbName+'b')
      else:
        worksheet.write('B2', DbName)
      fmt = writer.book.add_format({"font_name": "Liberation Sans",
                                    "bold": False,
                                    "font_size": 10})
      worksheet.set_column('A:A', 20, fmt)
      worksheet.set_column('B:B', 10, fmt)
      worksheet.set_row(0, 70)
      worksheet.merge_range('A1:B1', '')
      worksheet.insert_image('A1', '/home/yamishakka/Documentos/Biomemakers/Logo-BM_DL_negro-1024x323-e1521645909584.png', {'x_offset': 15, 'y_offset': 10})
      writer.save()
      
      ## Upload files to S3
      client = boto3.client('s3')
      s3 = boto3.resource('s3')
      if j == '16S':
        data = open('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+j+'_Individuales/'+DbName+'b_Bacteria.xlsx', 'rb')
        s3.Bucket('clientresultsftps3').put_object(Key=ClientFtpPath+'/'+SampleType.replace(" ", "_")+'/Bacteria/Abundances/'+DbName+'b_Bacteria.xlsx', Body=data)
      else:
        data = open('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+j+'_Individuales/'+DbName+'_Fungus.xlsx', 'rb') 
        s3.Bucket('clientresultsftps3').put_object(Key=ClientFtpPath+'/'+SampleType.replace(" ", "_")+'/Fungus/Abundances/'+DbName+'_Fungus.xlsx', Body=data)
      

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
import os
import BMK.Queries as Qr
import pandas      as pd
import biom
import xlsxwriter
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None

#--# Fastq Moving fastq from Run S3 and Clients FTP when is finished --------------------------------
def WsFastqMovement(PoolName):
  Query = Qr.DBWsFastqQuery(PoolName)
  print(Query)
  for index, row in Query.iterrows():
    DbName = row[1] 
    SampleType = row[2] 
    RunName = row[3]
    PoolmixName = row[4]
    ChainType = row[5]
  
    print('Uploading file \x1b[1;31;10m'+RunName+'\x1b[0m --> \x1b[1;33;10m'+DbName +'/b\x1b[0m with type \x1b[1;36;10m'+SampleType+'\x1b[0m of run \x1b[1;31;10m'+PoolmixName[0:8]+'\x1b[0m and storage in \x1b[1;33;10m'+PoolmixName[:4]+'1231\x1b[0m')

    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
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
        if ChainType == 1:
          s3.meta.client.copy(copy_source, 'raw-illumina-runs', 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+DbName+'/'+DbName+'_S'+sense.split(RunName+'_S',1)[1])
        elif ChainType == 2:
          s3.meta.client.copy(copy_source, 'raw-illumina-runs', 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+DbName+'b/'+DbName+'b_S'+sense.split(RunName+'_S',1)[1])
        else:
          print('The sample '+DbName+' have a chain type different of Fungus or Bacteria')
       

#--# Creation of abundances excels to client FTP -----------------------------------------------------  

def ClientsAbundances(PoolName):
  Query = Qr.DBAbundancesQuery(PoolName)
  Query.columns = ['rs.sample_id', 'm.c_muestra_wineseq', 'tm.d_tipo_muestra', 'rs.name', 'r.name', 'ct.name', 'c.ftp_path']
  print(Query)
  for RunDate in Query['r.name'].unique():
    RunList = Query[Query['r.name'].str.startswith(RunDate)]
    for Chain in RunList['ct.name'].unique():
      SampleGroup = RunList[RunList['ct.name'].str.startswith(Chain)]
      ### Descarga archivo biom
      ##client = boto3.client('s3')
      ##s3 = boto3.resource('s3')
      ##s3.Bucket('pipeline-analysis-results').download_file(options.Run+'/'+Chain+'_'+RunDate[0:8]+'_mapped_reads_tax.biom', '/home/yamishakka/Escritorio/Biomemakers/00-Oldtown_storage/Runs/'+Chain+'_'+RunDate[0:8]+'_mapped_reads_tax.biom')

      ## Analisis archivo biom
      table = biom.load_table('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+RunDate[0:8]+'/'+Chain+'_'+RunDate[0:8]+'_mapped_reads_tax.biom')
      abunInfo = table.to_dataframe()
      metadataInfo = table.metadata_to_dataframe('observation')
      fullTable = pd.concat([abunInfo, metadataInfo], axis=1)

      goodCV = fullTable[fullTable['confidence']>= 0.7].copy()
      goodCV.loc[:,'Species'] = goodCV.loc[:, 'taxonomy_6'].str.replace('s:', '', case=False)
      goodCV.taxonomy_6 = goodCV.taxonomy_6.str[2:]
      goodCV = goodCV.drop(['confidence', 'taxonomy_0', 'taxonomy_1', 'taxonomy_2', 'taxonomy_3', 'taxonomy_4', 'taxonomy_5', 'taxonomy_6'], axis=1)
      goodCV = goodCV.rename(columns={'taxonomy_6': 'Species'})
      goodCV = goodCV.sort_values(by=['Species'])

      for index ,row in SampleGroup.iterrows():
        DbName = row[1]
        SampleType = row[2]
        RunName = row[3]
        PoolmixName = row[4]
        ChainType = row[5]
        ClientFtpPath = row[6]
        print('Uploading sample \x1b[1;31;10m'+RunName+'\x1b[0m to client \x1b[1;31;10m'+ClientFtpPath+'\x1b[0m of sample type \x1b[1;31;10m'+SampleType+'\x1b[0m from Run \x1b[1;31;10m'+PoolmixName+'\x1b[0m')

        ## Si el usuario tiene OTU_Abundance se hace aqui

        ## Abundancias para cliente (siempre)
        groupedSpecies = goodCV.groupby(['Species']).sum()

        sampIndividual = groupedSpecies.loc[:,[RunName]].copy()
        sampIndividual = sampIndividual*100/sampIndividual[RunName].sum()
        sampInd = sampIndividual[sampIndividual[RunName]>0]
        sampInd = sampInd.sort_values(by=RunName, ascending=False)
        if not os.path.exists('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales'):
          os.makedirs('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales')

        if Chain == '16S':
          writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales/'+DbName+'b_Bacteria.xlsx', engine='xlsxwriter')
        else:
          writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales/'+DbName+'_Fungus.xlsx', engine='xlsxwriter')

        sampInd.to_excel(writer, sheet_name='Sheet1', startrow=1)
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']
        if Chain == '16S':
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

        ## Upload excel files to FTP
        client = boto3.client('s3')
        s3 = boto3.resource('s3')
        if Chain == '16S':
          data = open('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales/'+DbName+'b_Bacteria.xlsx', 'rb')
          s3.Bucket('clientresultsftps3').put_object(Key=ClientFtpPath+'/'+SampleType.replace(" ", "_")+'/Bacteria/Abundances/'+DbName+'b_Bacteria.xlsx', Body=data)
        else:
          data = open('/home/yamishakka/Escritorio/Biomemakers/00-NP_Abundances/'+PoolmixName[0:8]+'/'+Chain+'_Individuales/'+DbName+'_Fungus.xlsx', 'rb')
          s3.Bucket('clientresultsftps3').put_object(Key=ClientFtpPath+'/'+SampleType.replace(" ", "_")+'/Fungus/Abundances/'+DbName+'_Fungus.xlsx', Body=data)

        ## Upload Fastq to S3 and FTP
        print('Uploading file \x1b[1;31;10m'+RunName+'\x1b[0m --> \x1b[1;33;10m'+DbName +'\x1b[0m with type \x1b[1;36;10m'+SampleType+'\x1b[0m to \x1b[1;35;10m'+ClientFtpPath+'\x1b[0m client of run \x1b[1;31;10m'+PoolmixName[0:8]+'\x1b[0m and storage in \x1b[1;33;10m'+PoolmixName[:4]+'1231\x1b[0m')

        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects')
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
            if ChainType == 'ITS':
              s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/Fungus/Fastq/'+DbName+'_S'+sense.split(RunName+'_S',1)[1])
              s3.meta.client.copy(copy_source, 'raw-illumina-runs', 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+DbName+'/'+DbName+'_S'+sense.split(RunName+'_S',1)[1])

            elif ChainType == '16S':
              s3.meta.client.copy(copy_source, 'clientresultsftps3', ClientFtpPath +'/'+SampleType.replace(" ", "_")+'/Bacteria/Fastq/'+DbName+'b_S'+sense.split(RunName+'_S',1)[1])
              s3.meta.client.copy(copy_source, 'raw-illumina-runs', 'RUN_'+PoolmixName[:4]+'1231/FASTQ/'+PoolmixName[:4]+'1231_'+DbName+'b/'+DbName+'b_S'+sense.split(RunName+'_S',1)[1])
            else:
              print('The sample '+DbName+' have a chain type different of Fungus or Bacteria')

#--# Upload Metadata of finished samples by sample type and client

def ClientsMetadataCreation(PoolName):
  Query = Qr.DBMetadataQuery(PoolName)
  for row in Query:
    SampleType = row[0]
    ClientName = row[1]
    ClientID   = row[2]
    print('File with information of client \x1b[1;31;10m'+ClientName+'\x1b[0m and sample type \x1b[1;33;10m'+SampleType+'\x1b[0m' )
    sampleTypeClient = Qr.DBClientQuery(ClientID, SampleType)
    ## Excel creation
    writer = pd.ExcelWriter('/home/yamishakka/Escritorio/Clients_references/'+ClientName+'_'+SampleType.replace(" ", "_")+'.xlsx', engine='xlsxwriter')
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
    data = open('/home/yamishakka/Escritorio/Clients_references/'+ClientName+'_'+SampleType.replace(" ", "_")+'.xlsx', 'rb')
    s3.Bucket('clientresultsftps3').put_object(Key=ClientName+'/'+SampleType.replace(" ", "_")+'/'+ClientName+'_'+SampleType.replace(" ", "")+'.xlsx', Body=data)

#--# Upload Finished samples to comunicate to owners -------------------------------------------------

def ClientsToOwner(PoolName):
  result_set = Qr.DBAbundancesQuery(PoolName)
  myString = ", ".join(map(lambda x: "'" + x + "'", result_set['c_muestra_wineseq'].unique()))
  Qr.DBCompleateSamplesQuery(myString)

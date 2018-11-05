import apache_beam as beam
import datetime, os, hashlib, shutil,  subprocess, argparse



#csv_header,project_bq,dataset_bq,tab_bq,esq_bq,project,dir_rep,bucket,region,name_rep,name_job
def ejecutar_pipeline(project_bq,dataset_bq,tab_bq,project,bucket,dir_rep,region,name_rep,name_job,csv_header,field_company,periodo):
	argv = [
	'--project={0}'.format(project),
	'--job_name={}'.format(name_job),
    '--save_main_session',
    '--staging_location=gs://{}/{}'.format(bucket,'WRK'),
    '--temp_location=gs://{}/{}'.format(bucket,'WRK'),
    '--region={}'.format(region),
    #'--setup_file=./setup.py',
    '--num_workers=17',
    '--max_num_workers=100',
    '--disk_size_gb=300' ,
    '--work_disk_type=SSD',
    '--autoscaling_algorithm=THROUGHPUT_BASED',
    '--runner=DataflowRunner',
    '--teardown_policy=TEARDOWN_ALWAYS'
    ]
    
    #job_name = name_job+'-' + TMS
	
	def to_csv(field):  
		CSV_COLUMNS = '{}'.format(csv_header).split(',')
		for result in [field]:
			#data = ';'.join([str(result[k]) if k in result else '' for k in CSV_COLUMNS])
			data= ';'.join([( '' if str(result[k])=='None' else str(result[k])) if k in result else 'None' for k in CSV_COLUMNS])
    		yield str('{}'.format(data))
	
	class FilteringDoFn(beam.DoFn):
		def process(self, element,categoria):
			for row in [element]:
				if row['{}'.format(field_company)]==categoria: #3995466
					yield element
				else:
					return 
	query = """
        SELECT 
          {}
        FROM 
          `{}.{}.{}` 
        """.format(csv_header,project_bq,dataset_bq,tab_bq)
	
	print("Query a ejecutar:" + query)
	
	TMS=datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

  	print('Ejecutando dataflow job: {} ...'.format(name_job))
  	periodo=str(periodo[0:4]+periodo[4:6].zfill(2)) if len(periodo)==5 else periodo
  	OUTPUT_REPORTES='gs://{}/{}/{}_{}_'.format(bucket,dir_rep,name_rep,periodo)
  	
  	p= beam.Pipeline(argv=argv)
  	
  	DATA_BQ=(p| '{}'.format('BIGQUERY') >> beam.io.Read(beam.io.BigQuerySource(query = query, use_standard_sql = True))
  			)
  	
  	CIA_1=(DATA_BQ| '{}_filter'.format('045') >>beam.ParDo(FilteringDoFn(),categoria='045')
  						| '{}_tocsv'.format('045') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('045') >> beam.io.WriteToText(OUTPUT_REPORTES+'045'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)
   	CIA_2=(DATA_BQ| '{}_filter'.format('957') >>beam.ParDo(FilteringDoFn(),categoria='957')
  						| '{}_tocsv'.format('957') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('957') >> beam.io.WriteToText(OUTPUT_REPORTES+'957'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)
   
   	CIA_3=(DATA_BQ| '{}_filter'.format('544') >>beam.ParDo(FilteringDoFn(),categoria='544')
  						| '{}_tocsv'.format('544') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('544') >> beam.io.WriteToText(OUTPUT_REPORTES+'544'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)		
   	CIA_4=(DATA_BQ| '{}_filter'.format('469') >>beam.ParDo(FilteringDoFn(),categoria='469')
  						| '{}_tocsv'.format('469') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('469') >> beam.io.WriteToText(OUTPUT_REPORTES+'469'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)		   														
   	CIA_5=(DATA_BQ| '{}_filter'.format('692') >>beam.ParDo(FilteringDoFn(),categoria='692')
  						| '{}_tocsv'.format('692') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('692') >> beam.io.WriteToText(OUTPUT_REPORTES+'692'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)	   														

   	CIA_6=(DATA_BQ| '{}_filter'.format('462') >>beam.ParDo(FilteringDoFn(),categoria='462')
  						| '{}_tocsv'.format('462') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('462') >> beam.io.WriteToText(OUTPUT_REPORTES+'462'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)	

   	CIA_7=(DATA_BQ| '{}_filter'.format('035') >>beam.ParDo(FilteringDoFn(),categoria='035')
  						| '{}_tocsv'.format('035') >> beam.FlatMap(to_csv)
  						| '{}_write_csv'.format('035') >> beam.io.WriteToText(OUTPUT_REPORTES+'035'+'_'+TMS,
   														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';'))
   														)   		
  	p.run()  

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
	parser.add_argument('-pq','--project_bq', help='Bigquery s Project ID', required=True)
	parser.add_argument('-dq','--dataset_bq', help='Bigquery s dataset', required=True)
	parser.add_argument('-tq','--tab_bq', help='Bigquery s name table', required=True)
	parser.add_argument('-p','--project', help='Unique project ID donde se depositara el csv', required=True)
	parser.add_argument('-b','--bucket', help='Bucket where your data were ingested in Chapter 2', required=True)
	parser.add_argument('-dr','--dir_rep', help='Direccion donde se alojara el reporte', required=True)
	parser.add_argument('-hcsv','--csv_header', help='Header csv', required=True)
	parser.add_argument('-r','--region', help='Region', default='us-central1')
	parser.add_argument('-nr','--name_rep', help='Nombre reporte', default='Reporte'+'_')
	parser.add_argument('-nj','--name_job', help='Nombre job', default='reporte19891891')#field_company
	parser.add_argument('-fc','--field_company', help='Campo que hace referencia a la compania', default='CIA')
	parser.add_argument('-periodo','--periodo', help='Periodo', default='201810')
	args = vars(parser.parse_args())
	  
	#print "Correcting timestamps and writing to BigQuery dataset {}".format(dataset)

ejecutar_pipeline(project_bq=args['project_bq'],dataset_bq=args['dataset_bq'],tab_bq=args['tab_bq'],project=args['project'],bucket=args['bucket'],dir_rep=args['dir_rep'],csv_header=args['csv_header'],region=args['region'],name_rep=args['name_rep'],field_company=args['field_company'],periodo=args['periodo'],name_job=args['name_job'])

#sudo pip install apache_beam[gcp
#!python BQ_CSV_PAR_DOPAR.py -pq cpb100-216314 -dq malensbucket -tq tablaprueba1 -p cpb100-216314 -dr latam/file_test -b datosprueba -hcsv a,b,c,e -nj testpuntocoma
#!python BQ_CSV_DOPAR.py -pq cpb100-216314 -dq malensbucket -tq tablaprueba1 -p sp-os-rca-dev-01 -b testslatam -dr file_test_resp -hcsv a,b,c,e -nj testpardo1
#python BQ_CSV_DOPAR.py -pq sp-os-rca-dev-01 -dq PRUEBAS -tq tablaprueba1 -p sp-os-rca-dev-01 -b testslatam -dr file_test_resp -hcsv a,b,c,e -nj testpardo1

#testslatam/file_test/
#python BQ_CSV_DOPAR.py -pq sp-os-rca-dev-01 -dq EXSTAGE -tq AUDITORIA_USOS_ORI -p sp-os-rca-dev-01 -b testslatam -dr file_test_resp/auditoria_uso -hcsv COMPANIA,FORMATO_SERIE,CUPON,STT_CTBLE_USO,FECHA_CONTABLE,FCH_EMISION,FECHA_USO,GRUPO_CONTABLE,SIST_USO,TRNC_USO,RFIC_RFISC,AEROPUERTO_ORIGEN,AEROPUERTO_DESTINO,CIUDAD_ORIGEN,CIUDAD_DESTINO,MNT_USO_PAGO,MNT_USO_USD,MNT_USO_FUNC,MNT_OVER_USD,MNT_STD_USD,MNT_DSC_USD,MNT_COPET_USD,MNT_OVER_PAGO,MNT_STD_PAGO,MNT_DSC_PAGO,MNT_COPET_PAGO,MNT_OVER_FUNC,MNT_STD_FUNC,MNT_DSC_FUNC,MNT_COPET_FUNC,TAX_O1_USD,TAX_O1_PAGO,TAX_O1_FUNC,TAX_YQ_USD,TAX_YQ_PAGO,TAX_YQ_FUNC,JURO_USD_AR,JURO_PAGO_AR,JURO_FUNC_AR,JURO_USD_BR,JURO_PAGO_BR,JURO_FUNC_BR,TAXES_USD,TAXES_PAGO,TAXES_FUNC -nj testpardo4
"""
python Run_genera_csv2.py -pq sp-os-rca-dev-01 \
 -dq EXSTAGE \
 -tq AUDITORIA_USOS_ORI \
 -p sp-os-rca-dev-01 \
 -b testslatam \
 -dr file_test_resp/auditoria_uso2/allcompany \
 -hcsv COMPANIA,FORMATO_SERIE,CUPON,STT_CTBLE_USO,FECHA_CONTABLE,FCH_EMISION,FECHA_USO,GRUPO_CONTABLE,SIST_USO,TRNC_USO,RFIC_RFISC,AEROPUERTO_ORIGEN,AEROPUERTO_DESTINO,CIUDAD_ORIGEN,CIUDAD_DESTINO,MNT_USO_PAGO,MNT_USO_USD,MNT_USO_FUNC,MNT_OVER_USD,MNT_STD_USD,MNT_DSC_USD,MNT_COPET_USD,MNT_OVER_PAGO,MNT_STD_PAGO,MNT_DSC_PAGO,MNT_COPET_PAGO,MNT_OVER_FUNC,MNT_STD_FUNC,MNT_DSC_FUNC,MNT_COPET_FUNC,TAX_O1_USD,TAX_O1_PAGO,TAX_O1_FUNC,TAX_YQ_USD,TAX_YQ_PAGO,TAX_YQ_FUNC,JURO_USD_AR,JURO_PAGO_AR,JURO_FUNC_AR,JURO_USD_BR,JURO_PAGO_BR,JURO_FUNC_BR,TAXES_USD,TAXES_PAGO,TAXES_FUNC \
 -nr AUDITORIA_USO \
 -fc COMPANIA \
 -periodo 20181019 \
 -nj auditoriuso-compania
 """


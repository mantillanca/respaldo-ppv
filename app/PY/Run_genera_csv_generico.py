import apache_beam as beam
import datetime, os, hashlib, shutil,  subprocess, argparse
import datetime as dt
import dateutil.relativedelta as dtl


#csv_header,project_bq,dataset_bq,tab_bq,esq_bq,project,dir_rep,bucket,region,name_rep,name_job
def ejecutar_pipeline(project_bq,dataset_bq,tab_bq,project,bucket,dir_rep,region,name_rep,name_job,csv_header,field_company,company,field_date,tramo,periodo):
	argv = [
	'--project={0}'.format(project),
	'--job_name={}'.format(name_job),
    '--save_main_session',
    '--staging_location=gs://{}/{}'.format(bucket,'WRK'),
    '--temp_location=gs://{}/{}'.format(bucket,'WRK'),
    '--region={}'.format(region),
    #'--setup_file=./setup.py',
    #'--num_workers=17',
    '--max_num_workers=100',
    '--disk_size_gb=50' ,
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
				if str(row['{}'.format(field_company)]).zfill(3)==str(categoria): #3995466
					yield element
				else:
					return 
					
	class FilterDateDoFn(beam.DoFn):
		def process(self, element, date):
			for row in [element]:
				if str(row['{}'.format(field_date)]).replace('-','')[0:6]==date.strftime('%Y%m'): #3995466
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
		
	print('Ejecutando dataflow job: {} ...'.format(name_job))
  	
  	
  	TMS=datetime.datetime.now().strftime('%y%m%d-%H%M%S')
  	
  	#periodo=str(periodo[0:4]+periodo[4:6].zfill(2)) if len(periodo)==5 else periodo
  	
  	dt_periodo=dt.date(int(str(periodo)[0:4]),int(str(periodo)[4:6]),1)
  	
  	  		
  	OUTPUT_REPORTES='gs://{}/{}/{}_{}_'.format(bucket,dir_rep,name_rep,dt_periodo.strftime('%Y%m'))
  	
  	p= beam.Pipeline(argv=argv)
  	
  	DATA_BQ=(p| '{}'.format('BIGQUERY') >> beam.io.Read(beam.io.BigQuerySource(query = query, use_standard_sql = True))
  			)
  	
  	#company='045,957,544,469,692,462,035'
  	COMPANIA = '{}'.format(company).split(',')
  	#field_date='FCH_EMISION'
  	#periodo=201807
  	#tramo=13
  	if int(tramo) > 1: 
  		dt_tramo=[]
  		for i in range(int(tramo)):
  			dt_tramo.append(dt_periodo - dtl.relativedelta(months=+i))
  			
  		for i in COMPANIA:
  			CIA=(DATA_BQ| '{}_filter'.format(i) >>beam.ParDo(FilteringDoFn(),categoria=i))
  			for j in dt_tramo:
  				FILTRO2=(CIA|'{}_{}_filter'.format(i,j.strftime('%Y%m')) >>beam.ParDo(FilterDateDoFn(),date=j)
  							|'{}_{}_tocsv'.format(i,j.strftime('%Y%m')) >> beam.FlatMap(to_csv)
  							|'{}_{}_write_csv'.format(i,j.strftime('%Y%m')) >> beam.io.WriteToText(OUTPUT_REPORTES+i+'_'+j.strftime('%Y%m')+'_'+TMS,
  														file_name_suffix='.csv',
   														append_trailing_newlines=True,
   														num_shards=1,
   														shard_name_template='',
   														header='{}'.format(csv_header).replace(',',';')))
   	else:
   		for i in COMPANIA:
  			ONLY_CIA=(DATA_BQ| '{}_filter'.format(i) >>beam.ParDo(FilteringDoFn(),categoria=i)
  							 | '{}_tocsv'.format(i) >> beam.FlatMap(to_csv)
  							 | '{}_write_csv'.format(i) >> beam.io.WriteToText(OUTPUT_REPORTES+i+'_'+TMS,
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
	parser.add_argument('-company','--company', help='Companias que se ejecutaran', default='045,957,544,469,692,462,035')
	parser.add_argument('-periodo','--periodo', help='Periodo', default='2018-10-19')
	parser.add_argument('-fd','--field_date', help='Campo que hace referencia a la fecha de los meses que comprende el informe', default='FCH_EMISION')
	parser.add_argument('-tramo','--tramo', help='Cantidad de meses comprendidos en el informe', default='1')
	
	args = vars(parser.parse_args())
	  
	#print "Correcting timestamps and writing to BigQuery dataset {}".format(dataset)

ejecutar_pipeline(project_bq=args['project_bq'],dataset_bq=args['dataset_bq'],tab_bq=args['tab_bq'],project=args['project'],bucket=args['bucket'],dir_rep=args['dir_rep'],csv_header=args['csv_header'],region=args['region'],name_rep=args['name_rep'],field_company=args['field_company'],company=args['company'],field_date=args['field_date'],tramo=args['tramo'],periodo=args['periodo'],name_job=args['name_job'])



######################################################################################################
"""                                    FORMATO FECHA DEL INPUT
	     				        %Y%m                                 		   """
######################################################################################################


########################### PAQUETES QUE SE DEBEN INSTALAR EN GCP ####################################
"""
sudo pip install apache_beam[gcp]
sudo pip install python-dateutil
"""
######################################################################################################

################################## EJECUCION COMPANIA Y FECHA ########################################
# Este ejemplo realiza la ejecucion considerando como variable la compania y la fecha por lo cual el #
# resultado final son n_company*n_fechas cantidad de csv, en este caso particular eran 7 companias y #
# un tramo de 13 meses a considerar lo que nos entrega un total de 91 .CSV a generar                 #
"""
python BQ_CSV_DOPAR_RECUR_CD_F2.py -pq sp-os-rca-dev-01 \
 -dq PRUEBAS \
 -tq AUDITORIA_RNU_FORMATOPRUEBA \
 -p sp-os-rca-dev-01 \
 -b reportes_auditorias\
 -dr Ejecuciones/7company13date-f2 \
 -hcsv CIA,DCTO,CPN,EVENTO,GRUPO_CONTABLE,EST_CONTABLE,FCH_SAP,FCH_CONTABLE,FCH_EMISION,FCH_REGISTRO,ESPECIE,TRNC,MONEDA,MNT_USD,MNT_PAGO,MNT_LOCAL,OWNER_CONTABLE,FCH_VUELO,FCH_USO,CUIDAD_ORIGEN,CUIDAD_DESTINO,AEROPUERTO_ORIGEN,AEROPUERTO_DESTINO,BSAR,PAIS,RFIC_RFISC,CARRIER_OPERADOR,CARRIER_MARKETING,NMR_COMPROBANTE,NMR_VUELO,TPO_VTA_ESPECIAL,TPO_VTA_ESTANDARAGO,MNT_OVER_USD,MNT_STD_USD,MNT_DSC_USD,MNT_COPET_USD,MNT_OVER_PAGO,MNT_STD_PAGO,MNT_DSC_PAGO,MNT_COPET_PAGO,MNT_OVER_LOCAL,MNT_STD_LOCAL,MNT_DSC_LOCAL,MNT_COPET_LOCAL,TAX_O1_USD,TAX_O1_PAGO,TAX_O1_LOCAL,TAX_YQ_USD,TAX_YQ_PAGO,TAX_YQ_LOCAL,JURO_USD_AR,JURO_PAGO_AR,JURO_LOCAL_AR,JURO_USD_BR,JURO_PAGO_BR,JURO_LOCAL_BR,TAXES_USD_BR,TAXES_TAXES_PAGO,TAXES_TAXES_LOCAL,FECHA_TRAMO \
 -nr AUDITORIA_RNU \
 -fc CIA\
 -company 045,957,544,469,692,462,035 \
 -fd FCH_EMISION \
 -tramo 13 \
 -periodo 20189 \
 -nj rnu-generico-7company13date-f2
 """


#################################### EJECUCION SOLO COMPANIA #########################################
# Este ejemplo realiza la ejecucion considerando como variable solo la compania, por lo tanto el re- #
# sultado final son n_company cantidad de csv, en este caso particular eran 7 companias lo que nos   #
# entrega un total de 7 .CSV a generar                                                               #

"""
python BQ_CSV_DOPAR_RECUR_CD_F2.py -pq sp-os-rca-dev-01 \
 -dq PRUEBAS \
 -tq AUDITORIA_RNU_FORMATOPRUEBA \
 -p sp-os-rca-dev-01 \
 -b reportes_auditorias\
 -dr Ejecuciones/7company\
 -hcsv CIA,DCTO,CPN,EVENTO,GRUPO_CONTABLE,EST_CONTABLE,FCH_SAP,FCH_CONTABLE,FCH_EMISION,FCH_REGISTRO,ESPECIE,TRNC,MONEDA,MNT_USD,MNT_PAGO,MNT_LOCAL,OWNER_CONTABLE,FCH_VUELO,FCH_USO,CUIDAD_ORIGEN,CUIDAD_DESTINO,AEROPUERTO_ORIGEN,AEROPUERTO_DESTINO,BSAR,PAIS,RFIC_RFISC,CARRIER_OPERADOR,CARRIER_MARKETING,NMR_COMPROBANTE,NMR_VUELO,TPO_VTA_ESPECIAL,TPO_VTA_ESTANDARAGO,MNT_OVER_USD,MNT_STD_USD,MNT_DSC_USD,MNT_COPET_USD,MNT_OVER_PAGO,MNT_STD_PAGO,MNT_DSC_PAGO,MNT_COPET_PAGO,MNT_OVER_LOCAL,MNT_STD_LOCAL,MNT_DSC_LOCAL,MNT_COPET_LOCAL,TAX_O1_USD,TAX_O1_PAGO,TAX_O1_LOCAL,TAX_YQ_USD,TAX_YQ_PAGO,TAX_YQ_LOCAL,JURO_USD_AR,JURO_PAGO_AR,JURO_LOCAL_AR,JURO_USD_BR,JURO_PAGO_BR,JURO_LOCAL_BR,TAXES_USD_BR,TAXES_TAXES_PAGO,TAXES_TAXES_LOCAL,FECHA_TRAMO \
 -nr AUDITORIA_RNU \
 -fc CIA\
 -company 045,957,544,469,692,462,035 \
 -periodo 20189 \
 -nj rnu-generico-7company-f2
 """

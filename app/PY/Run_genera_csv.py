#

""" #====================================================================================================#
#  Nombre               : Run_genera_csv.py                                                              #
#  Fecha creacion       : 16-10-2018                                                                     #
#  Fecha modificacion   : 16-10-2018                                                                     #
#  Descripcion          :                                                                                #
#  Desarrollado         : everis                                                                         #
#  Forma de Ejecutar    : python                                                                         #
#  Variables de entrada : -pq -dq -tq -p -dr -b -hcsv -nj                                                #
#  



                                                                                                       #
# #===================================================================================================="""

import apache_beam as beam
import datetime, os, hashlib, shutil,  subprocess, argparse


#csv_header,project_bq,dataset_bq,tab_bq,esq_bq,project,dir_rep,bucket,region,name_rep,name_job
def ejecutar_pipeline(csv_header,project_bq,dataset_bq,tab_bq,project,dir_rep,bucket,region,name_rep,name_job):
   
    
	
	def to_csv(field):  
		CSV_COLUMNS = '{}'.format(csv_header).split(',')
		for result in [field]:
			data = ';'.join([str(result[k]) if k in result else '' for k in CSV_COLUMNS])
    		yield str('{}'.format(data))
	
	query = """
        SELECT 
          {}
        FROM 
          `{}.{}.{}` 
          LIMIT 10
        """.format(csv_header,project_bq,dataset_bq,tab_bq)
	
	print("Query a ejecutar:" + query)
	
	TMS=datetime.datetime.now().strftime('%y%m%d-%H%M%S')
	name_job = name_job+'-' + TMS
  	print('Ejecutando dataflow job: {} ...'.format(name_job))
  	
  	OUTPUT_REPORTES='gs://{}/{}/{}_{}'.format(bucket,dir_rep,name_rep,TMS)
	
	
	argv = [
	'--project={0}'.format(project),
	'--job_name={}'.format(name_job),
    '--save_main_session',
    '--staging_location=gs://{}/{}'.format(bucket,'WRK'),
    '--temp_location=gs://{}/{}'.format(bucket,'WRK'),
    '--region={}'.format(region),
    #'--setup_file=./setup.py',
    '--max_num_workers=10',
    '--disk_size_gb=30' ,
    '--work_disk_type=HDD',
    '--autoscaling_algorithm=THROUGHPUT_BASED',
    '--runner=DataflowRunner',
    '--teardown_policy=TEARDOWN_ALWAYS'
    ]
	
	
	
  	
  	p= beam.Pipeline(argv=argv)
  	
  	Trabajo=(p
  		| '{}_read'.format('csv') >> beam.io.Read(beam.io.BigQuerySource(query = query, use_standard_sql = True))
   		| '{}_csv'.format('csv') >> beam.FlatMap(to_csv)
   		| '{}_out'.format('csv') >> beam.io.WriteToText(OUTPUT_REPORTES,
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
	parser.add_argument('-dr','--dir_rep', help='Direccion donde se alojara el reporte', required=True)
	parser.add_argument('-b','--bucket', help='Bucket where your data were ingested in Chapter 2', required=True)
	parser.add_argument('-hcsv','--csv_header', help='Header csv', required=True)
	parser.add_argument('-r','--region', help='Region', default='us-central1')
	parser.add_argument('-nr','--name_rep', help='Nombre reporte', default='reporte'+'_'+datetime.datetime.now().strftime('%y%m%d-%H%M%S'))
	parser.add_argument('-nj','--name_job', help='Nombre job', default='reporte19891891')
	args = vars(parser.parse_args())
	  
	#print "Correcting timestamps and writing to BigQuery dataset {}".format(dataset)

ejecutar_pipeline(project_bq=args['project_bq'],dataset_bq=args['dataset_bq'],tab_bq=args['tab_bq'],project=args['project'],dir_rep=args['dir_rep'],bucket=args['bucket'],csv_header=args['csv_header'],region=args['region'],name_rep=args['name_rep'],name_job=args['name_job'])




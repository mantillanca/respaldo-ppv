#!/bin/bash  

# #======================================================================================================#
#  Nombre               : Run_Extrae_Reporteria.sh                                                       #
#  Fecha creacion       : 16-10-2018                                                                     #
#  Fecha modificacion   : 16-10-2018                                                                     #
#  Descripcion          :                                                                                #
#  Desarrollado         : everis                                                                         #
#  Forma de Ejecutar    : sh Run_Extrae_Reporteria.sh UNR 2018 07 auditoria_unr                          #
#  Variables de entrada :                                                                                #
# #======================================================================================================#

#========================================================================================================#
# DECLARACION DE GENERALES                                                                               #
# llamada del archivo de configuracion general del proyecto                                              #
#========================================================================================================#
#. /home/malenantillanca_everis/PPV/app/CNF/config.sh

_PATH=`cd ..; cd CNF; pwd`
. $_PATH/config.sh

#========================================================================================================#
# DECLARACION DE PARAMETROS                                                                              #
# Seccion donde se declaran los parametros globales a ocupar por la shell                                #
#========================================================================================================#

_FECHA_PROC=`date +'%Y%m%d'`
_PROCESO='Run_Extrae_Reporteria'                                # Nombre del Proceso
_FECHA_HORA=`date +'%Y%m%d_%H:%M:%S'`                           # Fecha y Hora de la ejecucion
_ARCHIVO_LOG=${PATH_LOG}'SHELL_'$_PROCESO'_'$_FECHA_HORA'.log'  # Nombre de Archivo LOG
_ERROR_PROCESO=${PATH_TMP}'Error_'$_PROCESO'.txt'               # Nombre de Archivo de Error Temporal
_NRO_PROCESO=23                                                 # Numero de Proceso MCP
_ESTADOJOB=${PATH_TMP}'EstadoSequence_'$_PROCESO'.txt'          # Estado de la secuencia de HOT
_REPORTE=$1                                                     # Reporte a generar
_TABLA_DESTINO='DETALLE_'$1
_TMS_JOB=`date +'%y%m%d%H%M%S'`


# ---------------------------------------------------------------------
echo 'parametro _FECHA_PROC :    |'${_FECHA_PROC}'|'
echo 'parametro _PROCESO :       |'${_PROCESO}'|'
echo 'parametro _FECHA_HORA :    |'${_FECHA_HORA}'|'
echo 'parametro _ARCHIVO_LOG :   |'${_ARCHIVO_LOG}'|'
echo 'parametro _ERROR_PROCESO : |'${_ERROR_PROCESO}'|'
echo 'parametro _NRO_PROCESO :   |'${_NRO_PROCESO}'|'
echo 'parametro _ESTADOJOB :     |'${_ESTADOJOB}'|'

#========================================================================================================#
#  1.0 Arcmado de Log del archivo                                                                        #
#========================================================================================================#

echo "*******************************************************************" > $_ARCHIVO_LOG
echo " Nombre Proyecto: ${PRM_GCP_PROJECT}                               " >>$_ARCHIVO_LOG
echo " Nombre Bucket  : ${PRM_GCP_BUCKET}                                " >>$_ARCHIVO_LOG
echo " Nombre Proyecto: ${PRM_RUTA_GCP_PROJECT}                          " >>$_ARCHIVO_LOG
echo " Nombre Esquema origen : ${PRM_GCP_ESQUEMA_TABLAS}                 " >>$_ARCHIVO_LOG
echo " Nombre Esquema Reportes: ${PRM_GCP_ESQUEMA_REPORTES}              " >>$_ARCHIVO_LOG
echo " Directorio GCS Reportes: ${PRM_RUTA_GCP_CSV_EJECUCIONES}          " >>$_ARCHIVO_LOG
echo " Fecha Ejecucion: ${_FECHA_PROC}                                   " >>$_ARCHIVO_LOG
echo "*******************************************************************" >>$_ARCHIVO_LOG

#========================================================================================================#
#  2.0 Ejecucion de la creacion de la tabla                                                              #
#========================================================================================================#
case "$1" in
  "VENTA")
_IN_ANIO=$2
_IN_MES=$3
_NMB_REPORTE=$4
  
sh ${PATH_SHL}Run_Carga_BQ_detalle_venta.sh $PRM_GCP_PROJECT_BQ $PRM_GCP_PROJECT_BQ $PRM_GCP_ESQUEMA_TABLAS  \
				$PRM_GCP_ESQUEMA_REPORTES $_TABLA_DESTINO $_IN_ANIO $_IN_MES $PRM_GCP_ESQUEMA_MAESTRO $PRM_GCP_ESQ_PARAMETROS
_RETURN=$?
if [ $_RETURN -ne 0 ]
then
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
	exit 1
else
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
#========================================================================================================#
#bq query --use_legacy_sql=false 'SELECT meses FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_RNU`'

#========================================================================================================#
#  2.1 ejecucion del archivo Python                                                                      #
#========================================================================================================#
	python ${PATH_PY}Run_genera_csv_generico.py  \
	 -pq $PRM_GCP_PROJECT \
	 -dq $PRM_GCP_ESQUEMA_REPORTES \
	 -tq $_TABLA_DESTINO \
	 -p  $PRM_GCP_PROJECT  \
	 -b  $PRM_GCP_BUCKET \
	 -dr $PRM_RUTA_GCP_CSV_EJECUCIONES  \
	 -hcsv $PRM_HCSV_VENTAS \
	 -nr  $_NMB_REPORTE \
     -fc $PRM_CIA_VENTAS \
     -periodo $_IN_ANIO$_IN_MES \
	 -nj genera-reporte-ventas`date +'%Y%m%d-%H%M%S'`
	 
 _RETURN=$?
	if [ $_RETURN -ne 0 ]
		then
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
			exit 1
		else
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
			exit 0
		fi
fi

;;


#========================================================================================================#
#  3.0 Ejecucion de la creacion de la tabla                                                              #
#========================================================================================================#
"USOS")
#sh Run_Extrae_Reporteria.sh USOS  2018 7 1 C [3,4,7,8,9,12] COMPANIA

_IN_ANIO=$2
_IN_MES=$3
_NMB_REPORTE=$4

sh ${PATH_SHL}Run_Carga_BQ_detalle_usos.sh $PRM_GCP_PROJECT_BQ $PRM_GCP_PROJECT_BQ $PRM_GCP_ESQUEMA_TABLAS  \
				$PRM_GCP_ESQUEMA_REPORTES $_TABLA_DESTINO $_IN_ANIO $_IN_MES $PRM_GCP_ESQUEMA_MAESTRO $PRM_GCP_ESQ_PARAMETROS 
_RETURN=$?
if [ $_RETURN -ne 0 ]
then
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
	exit 1
else
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
#========================================================================================================#
#  3.1 ejecucion del archivo Python                                                                      #
#========================================================================================================#
	python ${PATH_PY}Run_genera_csv_generico.py  \
	 -pq $PRM_GCP_PROJECT \
	 -dq $PRM_GCP_ESQUEMA_REPORTES \
	 -tq $_TABLA_DESTINO \
	 -p  $PRM_GCP_PROJECT  \
	 -b  $PRM_GCP_BUCKET \
	 -dr $PRM_RUTA_GCP_CSV_EJECUCIONES  \
	 -hcsv $PRM_HCSV_USOS \
	 -nr  $_NMB_REPORTE \
     -fc $PRM_CIA_USOS \
     -periodo $_IN_ANIO$_IN_MES \
	 -nj genera-reporte-usos`date +'%Y%m%d-%H%M%S'`

	 
	_RETURN=$?
	if [ $_RETURN -ne 0 ]
		then
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
			exit 1
		else
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
			exit 0
		fi
fi
;;

#========================================================================================================#
#  4.0 Ejecucion de la creacion de la tabla                                                              #
#  Forma de Ejecutar    : sh Run_Extrae_Reporteria.sh RNU 2018 07 auditoria_rnu                     #
#========================================================================================================#
"RNU")
_IN_ANIO=$2
_IN_MES=$3
_NMB_REPORTE=$4

_TRAMO=13
_FCH_CORTE=$_IN_ANIO`printf %02d $_IN_MES`

#sh Run_Carga_BQ_detalle_rnu.sh sp-os-rca-dev-01 sp-os-rca-dev-01 EXODSTB VW_EXODSTB DETALLE_RNU 201807 REPADMIN PRM_EXODSTB
sh ${PATH_SHL}Run_Carga_BQ_detalle_rnu.sh $PRM_GCP_PROJECT_BQ $PRM_GCP_PROJECT_BQ $PRM_GCP_ESQUEMA_TABLAS  \
				$PRM_GCP_ESQUEMA_REPORTES $_TABLA_DESTINO $_FCH_CORTE\
				$PRM_GCP_ESQUEMA_MAESTRO $PRM_GCP_ESQ_PARAMETROS
_RETURN=$?

if [ $_RETURN -ne 0 ]
then
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
	exit 1
else
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
#========================================================================================================#
Q_TRAMO_RNU='SELECT meses   FROM `'${PRM_GCP_PROJECT_BQ}'.'${PRM_GCP_ESQ_PARAMETROS}'.PRM_RNU`;'
json=`echo $Q_TRAMO_RNU | bq query --use_legacy_sql=false --format sparse`
TRAMO_RNU=$(echo $json |grep -Eo "[[:digit:]]+")
echo $TRAMO_RNU
#========================================================================================================#
#  4.1 ejecucion del archivo Python                                                                      #
#========================================================================================================#
	python ${PATH_PY}Run_genera_csv_generico.py  \
	 -pq $PRM_GCP_PROJECT \
	 -dq $PRM_GCP_ESQUEMA_REPORTES \
	 -tq $_TABLA_DESTINO \
	 -p  $PRM_GCP_PROJECT  \
	 -b  $PRM_GCP_BUCKET \
	 -dr $PRM_RUTA_GCP_CSV_EJECUCIONES  \
	 -hcsv $PRM_HCSV_RNU \
	 -nr $_NMB_REPORTE \
     -fc $PRM_CIA_RNU \
     -fd $PRM_FCH_RNU \
     -tramo $TRAMO_RNU \
     -periodo $_IN_ANIO$_IN_MES \
	 -nj genera-reporte-rnu`date +'%Y%m%d-%H%M%S'`
	 
 _RETURN=$?
	if [ $_RETURN -ne 0 ]
		then
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
			exit 1
		else
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
			exit 0
		fi
fi

;;

#========================================================================================================#
#  5.0 Ejecucion de la creacion de la tabla                                                              #
#  Forma de Ejecutar    : sh Run_Extrae_Reporteria.sh UNR 2018 07 auditoria_unr                     #
#========================================================================================================#
"UNR")
_IN_ANIO=$2
_IN_MES=$3
_NMB_REPORTE=$4


_FCH_CORTE=$_IN_ANIO`printf %02d $_IN_MES`

#sh Run_Carga_BQ_detalle_unr.sh sp-os-rca-dev-01 sp-os-rca-dev-01 EXODSTB VW_EXODSTB DETALLE_RNU 201807 REPADMIN PRM_EXODSTB
sh ${PATH_SHL}Run_Carga_BQ_detalle_unr.sh $PRM_GCP_PROJECT_BQ $PRM_GCP_PROJECT_BQ $PRM_GCP_ESQUEMA_TABLAS  \
				$PRM_GCP_ESQUEMA_REPORTES $_TABLA_DESTINO $_FCH_CORTE\
				$PRM_GCP_ESQUEMA_MAESTRO $PRM_GCP_ESQ_PARAMETROS
_RETURN=$?

if [ $_RETURN -ne 0 ]
then
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
	exit 1
else
	echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
#========================================================================================================#
Q_TRAMO_UNR='SELECT meses   FROM `'${PRM_GCP_PROJECT_BQ}'.'${PRM_GCP_ESQ_PARAMETROS}'.PRM_UNR`;'
json=`echo $Q_TRAMO_UNR | bq query --use_legacy_sql=false --format sparse`
TRAMO_UNR=$(echo $json |grep -Eo "[[:digit:]]+")
echo $TRAMO_UNR
#========================================================================================================#
#  4.1 ejecucion del archivo Python                                                                      #
#========================================================================================================#
	python ${PATH_PY}Run_genera_csv_generico.py  \
	 -pq $PRM_GCP_PROJECT \
	 -dq $PRM_GCP_ESQUEMA_REPORTES \
	 -tq $_TABLA_DESTINO \
	 -p  $PRM_GCP_PROJECT  \
	 -b  $PRM_GCP_BUCKET \
	 -dr $PRM_RUTA_GCP_CSV_EJECUCIONES  \
	 -hcsv $PRM_HCSV_UNR \
	 -nr $_NMB_REPORTE \
     -fc $PRM_CIA_UNR \
     -fd $PRM_FCH_UNR \
     -tramo $TRAMO_UNR \
     -periodo $_IN_ANIO$_IN_MES \
	 -nj genera-reporte-unr`date +'%Y%m%d-%H%M%S'`
	 
 _RETURN=$?
	if [ $_RETURN -ne 0 ]
		then
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} con error. Reintento" >>$_ARCHIVO_LOG	
			exit 1
		else
			echo "Ejecucion de sh ${_PROCESO} con fecha ${_FECHA_PROC} Correcto" >>$_ARCHIVO_LOG
			exit 0
		fi
fi

;;

  *)
    echo "No entro en ningun caso de reporte."
    exit 1
    ;;
esac


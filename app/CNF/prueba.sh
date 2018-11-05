#!/bin/bash
PRM_GCP_PROJECT_BQ=sp-os-rca-dev-01;					
PRM_GCP_ESQ_PARAMETROS=PRM_EXODSTB;	
Q_TRAMO_RNU='SELECT cnta  FROM `'${PRM_GCP_PROJECT_BQ}'.'${PRM_GCP_ESQ_PARAMETROS}'.PRM_UNR`;'
json=`echo $Q_TRAMO_RNU | bq query --use_legacy_sql=false --format json`
echo $json
#TRAMO_RNU=$(echo $json |grep -Eo "[[:digits:]]+")
echo grep "${json[@]}" #$TRAMO_RNU
#_FCH_CORTE=$_IN_ANIO`printf %02d $_IN_MES`
#echo printf "${!TRAMO[@]}"
#-s -X $json 1

exit 1
    ;;
esac 
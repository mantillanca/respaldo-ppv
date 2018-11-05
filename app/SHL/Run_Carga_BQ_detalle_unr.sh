#!/bin/bash



# #======================================================================================================#
#  Nombre               : Run_Carga_BQ_UNR.sh                                                            #
#  Fecha creacion       : 25-10-2018                                                                     #
#  Fecha modificacion   : 25-10-2018                                                                     #
#  Descripcion          :                                                                                #
#  Desarrollado         : everis                                                                         #
#  Variables de entrada :PROYECTO_BQ_DEST PROYECTO_BQ_ORI ESQ_ODSBQ ESQ_BQ_DEST TABLA_DEST_BQ            #
#                        FCH_ANIO FCH_MES CNT_MESES EST_CONTABLE  ESQ_MAESTRO                            #
#  Forma de Ejecutar    :sh Run_Carga_BQ_UNR.sh stately-diagram-198119 sp-os-rca-dev-6smm  \             #
#                        '${ESQ_ODSBQ}' SQ_TST_PRM REP_AUDITORIA 201803 13 C [3,4,7,12] 1 [1,2,4,24,28]  #
#                        REPADMIN                                                                        #
# #======================================================================================================#

PROYECTO_BQ_DEST=$1 #proyecto de destino de la consulta 
PROYECTO_BQ_ORI=$2 #proyecto de donde provienen las fuentes de BQ que alimentaran la query 
ESQ_ODSBQ=$3 #database donde se encuentran las diferentes tablas de donde se extrae la información
ESQ_BQ_DEST=$4 #database de destino de la consult
TABLA_BQ_DEST=$5 #nombre tabla de destino de la consulta
FCH_CORTE=$6 #fecha a la cual corresponde el reporte
ESQ_MAESTRO=$7 #database de donde viene la tabla maestra TAXES_ONE
ESQ_PARAMETROS=$8 #database que contiene las tablas maestras con los parametros necesarios para cada consulta


#CNT_MESES=$7
#EST_CONTABLE_C=$8
#TPEV_CDG_34712=$9
#TPEV_CDG_1=$10
#GRCN_CDG_1242428=$11

echo $PROYECTO_BQ_DEST
echo $PROYECTO_BQ_ORI
echo $ESQ_ODSBQ
echo $ESQ_BQ_DEST
echo $TABLA_BQ_DEST
echo $FCH_CORTE
echo $ESQ_MAESTRO
echo $ESQ_PARAMETROS


Query_unr='with prm as (
  SELECT meses, estatus, cnta, tpev_cdg_1, reporte
  FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_UNR`),
    grupo as (
  SELECT grp_cnt
  FROM  `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_UNR`) 

SELECT DTEV.DTEV_CDG_CUPON AS ID_CUPON,
  DTEV.CMHL_CDG AS OWNER_CONTABLE,
  FORMAT_DATE("%Y-%m-%d", DATE(TCKT.PRSR_FCH_EMISION)) AS FECHA_EMISION_VTA,
  DTVT.EVNT_EST_CONTABLE AS ESTATUS_VTA,
  FORMAT_DATE("%Y-%m-%d", DATE(DTVT.PRSR_FCH_EMISION)) AS FECHA_REVERA_VTA,
  FORMAT_DATE("%Y-%m-%d", DATE(DTVT.DTVT_FCH_SAP)) AS FECHA_SAP_VTA,
  DTVT.EVNT_MNT_USD AS MNT_USD_VTA,
  DTVT.EVNT_MNT_PAGO AS MNT_PAGO_VTA,
  DTEV.DTEV_MNT_LOCAL AS MNT_FUNC_VTA,
  SUBSTR(CONCAT("000", CAST(TCKT.LNAR_NMR_IATA_EMISION AS STRING)),-3) AS COMPANIA,
  TCKT.PRSR_NMR_DOCUMENTO AS FORMATO_SERIE,
  DTEV.DTPR_NMR_CUPON AS CUPON,
  DTEV.EVNT_EST_CONTABLE AS STT_CTBLE_USO,
  FORMAT_DATE("%Y-%m-%d", DATE(DTEV.EVNT_FCH_CONTABLE)) AS FECHA_CONTABLE_USO,
  FORMAT_DATE("%Y-%m-%d", DATE(DTEV.EVNT_FCH_USO)) AS FECHA_USO,
  DTEV.DTEV_CDG_GRCN AS GRUPO_CONTABLE,
  DTEV.TPEV_CDG AS SIST_USO,
  DTEV.EVNT_TRNS_CDG AS TRNC_USO,
  IAVT.IAVT_CDG_RFIC AS RFIC_RFISC,
  DTVT.CDDS_CDG_ORIGEN AS CIUDAD_ORIGEN,
  DTVT.CDDS_CDG_DESTINO AS CIUDAD_DESTINO,
  IAEV.ARPR_CDG_ORIGEN AS AEROPUETO_ORIGEN,
  IAEV.ARPR_CDG_DESTINO AS AEROPUETO_DESTINO,
  DTEV.EVNT_MNT_PAGO AS MNT_USO_PAGO,
  DTEV.EVNT_MNT_USD AS MNT_USO_USD,
  DTEV.DTEV_MNT_LOCAL AS MNT_USO_FUN,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CDPR_MNT_USD       ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_USD,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CDPR_MNT_PAGO      ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_PAGO,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CMPC_MNT_LOCAL     ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_FUNC,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CDPR_MNT_USD       ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_USD,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CDPR_MNT_PAGO      ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_PAGO,
  REPLACE(CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CMPC_MNT_LOCAL     ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_FUNC,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_USD,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_PAGO,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_FUNC,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_USD,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_PAGO,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_FUNC,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_USD,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_PAGO,
  REPLACE(CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_FUNC
FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_EVENTO` iaev 
  ON dtev.dtev_cdg_cupon = iaev.dtev_cdg_cupon
LEFT JOIN (select tpev_cdg_1 ,reporte FROM prm) PRM2 ON reporte="UNR"  
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_VENTA` dtvt
  ON dtev.tckt_cdg_ticket = dtvt.tckt_cdg_ticket
  AND dtev.dtpr_nmr_cupon = dtvt.dtpr_nmr_cupon
  AND dtev.prsr_fch_emision = dtvt.prsr_fch_emision
  AND dtvt.tpev_cdg IN UNNEST((PRM2.tpev_cdg_1))   
  AND dtev.cmhl_cdg = dtvt.cmhl_cdg  
INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_VENTA` iavt
  ON dtvt.dtvt_cdg_cupon = iavt.dtvt_cdg_cupon
INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TICKET` tckt
  ON tckt.tckt_cdg_ticket = dtev.tckt_cdg_ticket
  AND tckt.prsr_fch_emision = dtev.prsr_fch_emision
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.COMISION_CUPON` COMISION_CUPON
  ON dtev.dtev_cdg_cupon = CMPC_CDG_CUPON
  AND CMPC_CDG_CONCEPTO IN ("UPL","EXC","RFN","INW")
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TAX_CUPON` TAX_CUPON
  ON dtev.dtev_cdg_cupon = TXCN_CDG_CUPON
  AND TAX_CUPON.TXCN_CDG_CONCEPTO IN ("UPL","EXC","RFN","INW")
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_MAESTRO}'.TAXES_ONE` TAXES_ONE
  ON TAX_CUPON.TPTX_CDG = TAXES_ONE.TXES_CDG_TAX
  WHERE CAST(dtev.dtev_fch_sap AS DATE) 
  BETWEEN DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL - (select meses from prm) + 1 MONTH)
  AND DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
  AND dtev.evnt_est_contable IN (select estatus from prm)
  AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
  AND dtev.dtev_cdg_grcn IN UNNEST((select grp_cnt from grupo))
  AND((dtvt.evnt_est_contable IN (select estatus from prm) AND CAST(dtvt.dtvt_fch_sap as DATE) >= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)) 
    OR dtvt.dtvt_cdg_cupon IS NULL
    )
  AND ( (dtev.lnar_nmr_iata_emision = "957" AND dtev.cmhl_cdg = 200000201 )
     OR (dtev.lnar_nmr_iata_emision = "045" AND dtev.cmhl_cdg = 89862200  )
     OR (dtev.lnar_nmr_iata_emision = "462" AND dtev.cmhl_cdg = 200303785 )
     OR (dtev.lnar_nmr_iata_emision = "469" AND dtev.cmhl_cdg = 200505172 ) 
     OR (dtev.lnar_nmr_iata_emision = "544" AND dtev.cmhl_cdg = 199999999 )
     OR (dtev.lnar_nmr_iata_emision = "692" AND dtev.cmhl_cdg = 203164089 )
     OR (dtev.lnar_nmr_iata_emision = "035" AND dtev.cmhl_cdg = 202602698 ))
  GROUP BY DTEV.DTEV_CDG_CUPON,
  DTEV.CMHL_CDG,
  TCKT.PRSR_FCH_EMISION,
  DTVT.EVNT_EST_CONTABLE,
  DTVT.PRSR_FCH_EMISION,
  DTVT.DTVT_FCH_SAP,
  DTVT.EVNT_MNT_USD,
  DTVT.EVNT_MNT_PAGO,
  DTEV.DTEV_MNT_LOCAL,
  TCKT.LNAR_NMR_IATA_EMISION,
  TCKT.PRSR_NMR_DOCUMENTO,
  DTEV.DTPR_NMR_CUPON,
  DTEV.EVNT_EST_CONTABLE,
  DTEV.EVNT_FCH_CONTABLE,
  DTEV.EVNT_FCH_USO,
  DTEV.DTEV_CDG_GRCN,
  DTEV.TPEV_CDG,
  DTEV.EVNT_TRNS_CDG,
  IAVT.IAVT_CDG_RFIC,
  DTVT.CDDS_CDG_ORIGEN,
  DTVT.CDDS_CDG_DESTINO,
  IAEV.ARPR_CDG_ORIGEN,
  IAEV.ARPR_CDG_DESTINO,
  DTEV.EVNT_MNT_PAGO,
  DTEV.EVNT_MNT_USD,
  DTEV.DTEV_MNT_LOCAL;'


echo $Query_unr 
_ARCHIVO_LOG_BQ=/home/malenantillanca_everis/PPV/data/'SHELL_BQ_UNR'.log;
_FECHA_PROC=`date +'%Y%m%d'`;

echo "*******************************************************************" >$_ARCHIVO_LOG_BQ
echo " Query: ${Query_unr}                                               " >>$_ARCHIVO_LOG_BQ
echo " Fecha Ejecucion: ${_FECHA_PROC}                                   " >>$_ARCHIVO_LOG_BQ
echo " @FCH_MES_CORTE: '${FCH_CORTE}'                                    " >>$_ARCHIVO_LOG_BQ
echo "*******************************************************************" >>$_ARCHIVO_LOG_BQ

echo $Query_unr | bq query \
  --use_legacy_sql=False \
  --destination_table $PROYECTO_BQ_DEST:$ESQ_BQ_DEST.$TABLA_BQ_DEST \
  --replace \
  --parameter=FCH_MES_CORTE::$FCH_CORTE


_RETURN=$?
echo $_RETURN
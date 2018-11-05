#!/bin/bash



# #======================================================================================================#
#  Nombre               : Run_Carga_BQ_detalle_usos.sh                                                   #
#  Fecha creacion       : 26-10-2018                                                                     #
#  Fecha modificacion   : 26-10-2018                                                                     #
#  Descripcion          :                                                                                #
#  Desarrollado         : everis                                                                         #
#  Forma de Ejecutar    : sh                                                                             #
#  Variables de entrada :PROYECTO_BQ_DEST PROYECTO_BQ_ORI ESQ_ODSBQ ESQ_BQ_DEST TABLA_DEST_BQ            #
#                        FCH_ANIO FCH_MES ESQ_MAESTRO                                                    #
#                                                                                                        #
#                        sh Run_Carga_BQ_detalle_usos.sh sp-os-rca-dev-01 sp-os-rca-dev-01 EXODSTB \     #
#                          VW_EXODSTB DETALLE_USOS 2018 07 REPADMIN PRM_EXODSTB                          #
# #======================================================================================================#

PROYECTO_BQ_DEST=$1
PROYECTO_BQ_ORI=$2
ESQ_ODSBQ=$3
ESQ_BQ_DEST=$4
TABLA_BQ_DEST=$5
FCH_ANIO=$6
FCH_MES=$7
ESQ_MAESTRO=$8
ESQ_PARAMETROS=$9

echo $PROYECTO_BQ_DEST
echo $PROYECTO_BQ_ORI
echo $ESQ_ODSBQ
echo $ESQ_BQ_DEST
echo $TABLA_BQ_DEST
echo $FCH_ANIO
echo $FCH_MES
echo $ESQ_MAESTRO
echo $ESQ_PARAMETROS


Query_1=' with owner_cnt as (
                  SELECT owner,SUBSTR(CONCAT("000", CAST(iata AS STRING)),-3) as iata
                  FROM  `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_USOS` ,unnest(owner_contable)
                  ),
     prm as       (SELECT vlr_cupon,cnta,estatus 
                   FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_USOS`
                   ),
     grupo as     (SELECT grp_cnt
                  FROM  `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_USOS` ) 

SELECT  SUBSTR(CONCAT("000", CAST(TCKT.LNAR_NMR_IATA_EMISION AS STRING)),-3) COMPANIA
       ,TCKT.PRSR_NMR_DOCUMENTO	FORMATO_SERIE
       ,DTEV.DTPR_NMR_CUPON	CUPON
       ,DTEV.EVNT_EST_CONTABLE	STT_CTBLE_USO	
       ,FORMAT_DATE("%Y-%m-%d", DATE(DTEV.DTEV_FCH_SAP))FECHA_CONTABLE
       ,FORMAT_DATE("%Y-%m-%d", DATE(DTEV.PRSR_FCH_EMISION))FCH_EMISION
       ,FORMAT_DATE("%Y-%m-%d", DATE(DTEV.EVNT_FCH_USO))FECHA_USO
       ,DTEV.DTEV_CDG_GRCN	GRUPO_CONTABLE	
       ,DTEV.TPEV_CDG	SIST_USO	
       ,DTEV.EVNT_TRNS_CDG	TRNC_USO
       ,IAVT.IAVT_CDG_RFIC	RFIC_RFISC	
       ,IAEV.ARPR_CDG_ORIGEN	AEROPUERTO_ORIGEN	
       ,IAEV.ARPR_CDG_DESTINO	AEROPUERTO_DESTINO	
       ,DTVT.CDDS_CDG_ORIGEN	CIUDAD_ORIGEN	
       ,DTVT.CDDS_CDG_DESTINO	CIUDAD_DESTINO	
       ,REPLACE (CAST(DTEV.EVNT_MNT_PAGO AS STRING), ".", ",")	MNT_USO_PAGO
       ,REPLACE (CAST(DTEV.EVNT_MNT_USD AS STRING), ".", ",")	MNT_USO_USD	
       ,REPLACE (CAST(DTEV.DTEV_MNT_LOCAL AS STRING), ".", ",")	MNT_USO_FUNC
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD", "OVER", "COPET") THEN CDPR_MNT_USD ELSE 0 END)     AS STRING), ".", ",")  AS MNT_COMISIONES_USD
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD", "OVER", "COPET") THEN CDPR_MNT_PAGO ELSE 0 END)    AS STRING), ".", ",")  AS MNT_COMISIONES_PAGO
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD", "OVER", "COPET") THEN CMPC_MNT_LOCAL ELSE 0 END)   AS STRING), ".", ",")  AS MNT_COMISIONES_FUNC
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC") THEN CDPR_MNT_USD ELSE 0 END)                      AS STRING), ".", ",")  AS MNT_DSC_USD
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC") THEN CDPR_MNT_PAGO ELSE 0 END)                     AS STRING), ".", ",")  AS MNT_DSC_PAGO
       ,REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC") THEN CMPC_MNT_LOCAL ELSE 0 END)                    AS STRING), ".", ",")  AS MNT_DSC_FUNC
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4 THEN TXCN_MNT_TAX_USD ELSE 0 END)          AS STRING), ".", ",")  AS MNT_TAX_YQ_USD
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4 THEN TXCN_MNT_TAX_PAGO ELSE 0 END)         AS STRING), ".", ",")  AS MNT_TAX_YQ_PAGO
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4 THEN TXCN_MNT_TAX_LOCAL ELSE 0 END)        AS STRING), ".", ",")  AS MNT_TAX_YQ_FUNC
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5 THEN TXCN_MNT_TAX_USD ELSE 0 END)          AS STRING), ".", ",")  AS MNT_TAX_JURO_USD
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5 THEN TXCN_MNT_TAX_PAGO ELSE 0 END)         AS STRING), ".", ",")  AS MNT_TAX_JURO_PAGO
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5 THEN TXCN_MNT_TAX_LOCAL ELSE 0 END)        AS STRING), ".", ",")  AS MNT_TAX_JURO_FUNC
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1 THEN TXCN_MNT_TAX_USD ELSE 0 END)          AS STRING), ".", ",")  AS MNT_BOARDINGFEE_USD
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1 THEN TXCN_MNT_TAX_PAGO ELSE 0 END)         AS STRING), ".", ",")  AS MNT_BOARDINGFEE_PAGO
       ,REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1 THEN TXCN_MNT_TAX_LOCAL ELSE 0 END)        AS STRING), ".", ",")  AS MNT_BOARDINGFEE_FUNC
  FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev  
       INNER JOIN 
    owner_cnt
    ON dtev.lnar_nmr_iata_emision= owner_cnt.iata AND dtev.cmhl_cdg = owner_cnt.owner
       

        LEFT JOIN  `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_EVENTO` iaev ON dtev.dtev_cdg_cupon = iaev.dtev_cdg_cupon 
        LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_VENTA` dtvt  ON dtvt.tckt_cdg_ticket = dtev.tckt_cdg_ticket
                                                       AND dtvt.dtpr_nmr_cupon = dtev.dtpr_nmr_cupon
                                                       AND dtvt.prsr_fch_emision = dtev.prsr_fch_emision
                                                       AND dtvt.tpev_cdg = 1
        LEFT JOIN  `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_VENTA` iavt ON dtvt.dtvt_cdg_cupon = iavt.dtvt_cdg_cupon
        INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TICKET` tckt ON tckt.tckt_cdg_ticket = dtev.tckt_cdg_ticket
                                                   AND tckt.prsr_fch_emision = dtev.prsr_fch_emision
        INNER JOIN owner_cnt oc
            ON tckt.lnar_nmr_iata_emision= oc.iata
	      LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.COMISION_CUPON` COMISION_CUPON ON dtev.dtev_cdg_cupon = CMPC_CDG_CUPON AND CMPC_CDG_CONCEPTO IN ("UPL","EXC","RFN","INW") 
	      LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TAX_CUPON` TAX_CUPON ON dtev.dtev_cdg_cupon = TXCN_CDG_CUPON AND TAX_CUPON.TXCN_CDG_CONCEPTO IN ("UPL","EXC","RFN","INW") 
	      LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_MAESTRO}'.TAXES_ONE` TAXES_ONE ON TAX_CUPON.TPTX_CDG = TAXES_ONE.TXES_CDG_TAX
   WHERE 
      EXTRACT(YEAR from DATE(dtev.dtev_fch_sap))= @FCH_SAP_ANIO AND
      EXTRACT(MONTH from DATE(dtev.dtev_fch_sap))= @FCH_SAP_MES
      AND dtev.tpev_cdg IN UNNEST((SELECT CNTA FROM PRM))
      AND dtev.evnt_est_contable = (select estatus from prm)
      AND dtev_cdg_grcn IN UNNEST ((SELECT grp_cnt FROM GRUPO))
 GROUP BY dtev.dtev_cdg_cupon,
        TCKT.LNAR_NMR_IATA_EMISION,
        TCKT.PRSR_NMR_DOCUMENTO,
        DTEV.DTPR_NMR_CUPON,
        DTEV.EVNT_EST_CONTABLE,
        DTEV.DTEV_FCH_SAP,
        DTEV.PRSR_FCH_EMISION,
        DTEV.EVNT_FCH_USO,
        DTEV.DTEV_CDG_GRCN,
        DTEV.TPEV_CDG,
        DTEV.EVNT_TRNS_CDG,
        IAVT.IAVT_CDG_RFIC,
        IAEV.ARPR_CDG_ORIGEN,
        IAEV.ARPR_CDG_DESTINO,
        DTVT.CDDS_CDG_ORIGEN,
        DTVT.CDDS_CDG_DESTINO,
        DTEV.EVNT_MNT_PAGO,
        DTEV.EVNT_MNT_USD,
        DTEV.DTEV_MNT_LOCAL'
		
		echo $Query_1
 bq query \
 --use_legacy_sql=False  \
 --destination_table $PROYECTO_BQ_DEST:$ESQ_BQ_DEST.$TABLA_BQ_DEST \
 --replace \
 --parameter=FCH_SAP_ANIO:INT64:$FCH_ANIO \
 --parameter=FCH_SAP_MES:INT64:$FCH_MES \
 $Query_1

 _RETURN=$?
 echo $_RETURN	
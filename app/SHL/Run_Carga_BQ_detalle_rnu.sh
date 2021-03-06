#!/bin/bash



# #======================================================================================================#
#  Nombre               : Run_Carga_BQ_RNU.sh                                                            #
#  Fecha creacion       : 24-10-2018                                                                     #
#  Fecha modificacion   : 25-10-2018                                                                     #
#  Descripcion          :                                                                                #
#  Desarrollado         : everis                                                                         #
#  Variables de entrada :PROYECTO_BQ_DEST PROYECTO_BQ_ORI ESQ_ODSBQ ESQ_BQ_DEST TABLA_DEST_BQ            #
#                        FCH_ANIO FCH_MES CNT_MESES EST_CONTABLE  ESQ_MAESTRO                            #
#  Forma de Ejecutar    :sh Run_Carga_BQ_detalle_rnu.sh sp-os-rca-dev-01 sp-os-rca-dev-01 EXODSTB \      #
#                        VW_EXODSTB DETALLE_RNU 201807 REPADMIN PRM_EXODSTB                              #
# #======================================================================================================#

PROYECTO_BQ_DEST=$1 #proyecto de destino de la consulta 
PROYECTO_BQ_ORI=$2 #proyecto de donde provienen las fuentes de BQ que alimentaran la query 
ESQ_ODSBQ=$3 #database donde se encuentran las diferentes tablas de donde se extrae la información
ESQ_BQ_DEST=$4 #database de destino de la consult
TABLA_BQ_DEST=$5 #nombre tabla de destino de la consulta
FCH_CORTE=$6 #fecha a la cual corresponde el reporte
ESQ_MAESTRO=$7 #database de donde viene la tabla maestra TAXES_ONE
ESQ_PARAMETROS=$8 #database que contiene las tablas maestras con los parametros necesarios para cada consulta

echo $PROYECTO_BQ_DEST
echo $PROYECTO_BQ_ORI
echo $ESQ_ODSBQ
echo $ESQ_BQ_DEST
echo $TABLA_BQ_DEST
echo $FCH_CORTE
echo $ESQ_MAESTRO
echo $ESQ_PARAMETROS


Query_rnu='with prm as ( 
		SELECT meses, estatus, cnta, tpev_cdg_412, tpev_cdg_22 
  FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_RNU`),
		grupo as (
  SELECT grp_cnt
  FROM  `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_RNU`) 

SELECT
  DTVT_CDG_CUPON,
  SUBSTR(CONCAT("000", CAST(CIA AS STRING)),-3)        AS CIA,
  DCTO,CPN,EVENTO,GRUPO_CONTABLE,EST_CONTABLE,FCH_SAP,
  FORMAT_DATE("%Y-%m-%d", DATE(FCH_CONTABLE))          AS FCH_CONTABLE,
  FORMAT_DATE("%Y-%m-%d", DATE(FCH_EMISION))           AS FCH_EMISION,
  FORMAT_DATE("%Y-%m-%d", DATE(FCH_REGISTRO))          AS FCH_REGISTRO,
  ESPECIE,TRNC,MONEDA,MNT_USD,MNT_PAGO,MNT_LOCAL,OWNER_CONTABLE,
  FORMAT_DATE("%Y-%m-%d", DATE(FCH_VUELO))             AS FCH_VUELO,
  FORMAT_DATE("%Y-%m-%d", DATE(FCH_USO))               AS FCH_USO,
  CUIDAD_ORIGEN,CUIDAD_DESTINO,AEROPUERTO_ORIGEN,
  AEROPUERTO_DESTINO,BSAR,
  PAIS,RFIC_RFISC,CARRIER_OPERADOR,CARRIER_MARKETING,
  NMR_COMPROBANTE,NMR_VUELO,TPO_VTA_ESPECIAL,TPO_VTA_ESTANDAR,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CDPR_MNT_USD       ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_USD,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CDPR_MNT_PAGO      ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_PAGO,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("STD","OVER","COPET") THEN CMPC_MNT_LOCAL     ELSE 0 END) AS STRING), ".", ",") AS MNT_COMISIONES_FUNC,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CDPR_MNT_USD       ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_USD,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CDPR_MNT_PAGO      ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_PAGO,
  REPLACE (CAST(SUM(CASE WHEN TPCM_CDG IN ("DSC")                THEN CMPC_MNT_LOCAL     ELSE 0 END) AS STRING), ".", ",") AS MNT_DSC_FUNC,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_USD,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_PAGO,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 4        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_YQ_FUNC,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_USD,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_PAGO,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 5        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_TAX_JURO_FUNC,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_USD   ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_USD,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_PAGO  ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_PAGO,
  REPLACE (CAST(SUM(CASE WHEN TAXES_ONE.TPTA_TPO_TASA = 1        THEN TXCN_MNT_TAX_LOCAL ELSE 0 END) AS STRING), ".", ",") AS MNT_BOARDINGFEE_FUNC,
  SIST_USO,GRUPO_CONTABLE_USO,FECHA_CONTABLE_USO
  FROM (
    SELECT
      DTVT.DTVT_CDG_CUPON              AS DTVT_CDG_CUPON,
      TCKT.LNAR_NMR_IATA_EMISION       AS CIA,
      TCKT.PRSR_NMR_DOCUMENTO          AS DCTO,
      DTVT.DTPR_NMR_CUPON              AS CPN,
      DTVT.TPEV_CDG                    AS EVENTO,
      DTVT.DTVT_GRCN_CDG               AS GRUPO_CONTABLE,
      DTVT.EVNT_EST_CONTABLE           AS EST_CONTABLE,
      DTVT.DTVT_FCH_SAP                AS FCH_SAP,
      DTVT.EVNT_FCH_CONTABLE           AS FCH_CONTABLE,
      TCKT.PRSR_FCH_EMISION            AS FCH_EMISION,
      DTVT.EVNT_FCH_REGISTRO           AS FCH_REGISTRO,
      TCKT.TPPR_CDG                    AS ESPECIE,
      DTVT.EVNT_TRNS_CDG               AS TRNC,
      DTVT.MNDS_CDG_ISO_CHAR_EVENTO    AS MONEDA,
      DTVT.EVNT_MNT_USD                AS MNT_USD,
      DTVT.EVNT_MNT_PAGO               AS MNT_PAGO,
      DTVT.DTVT_MNT_LOCAL              AS MNT_LOCAL,
      DTVT.CMHL_CDG                    AS OWNER_CONTABLE,
      DTVT.EVNT_FCH_VUELO              AS FCH_VUELO,
      DTVT.EVNT_FCH_USO                AS FCH_USO,
      DTVT.CDDS_CDG_ORIGEN             AS CUIDAD_ORIGEN,
      DTVT.CDDS_CDG_DESTINO            AS CUIDAD_DESTINO,
      DTVT.ARPR_CDG_ORIGEN             AS AEROPUERTO_ORIGEN,
      DTVT.ARPR_CDG_DESTINO            AS AEROPUERTO_DESTINO,
      TRNS.BSAR_ORG                    AS BSAR,
      TRNS.BSAR_PSES_CDG_ISO           AS PAIS,
      IAVT.IAVT_CDG_RFIC               AS RFIC_RFISC,
      DTVT.LNAR_NMR_IATA_CARRIER       AS CARRIER_OPERADOR,
      DTVT.LNAR_NMR_IATA_MKT_CARRIER   AS CARRIER_MARKETING,
      DTVT.DTVT_NMR_COMPROBANTE        AS NMR_COMPROBANTE,
      DTVT.EVNT_NMR_VUELO              AS NMR_VUELO,
      IAVT.IAVT_FLG_VTA_SPECIAL        AS TPO_VTA_ESPECIAL,
      TCKT.PRSR_TPO_VTA                AS TPO_VTA_ESTANDAR,
      (SELECT 
          tpev_cdg
        FROM (
          SELECT tpev_cdg, ORDEN
          FROM 
          (SELECT 
              dtev.tpev_cdg,
              dtev.tckt_cdg_ticket id_ticket,
              dtev.dtpr_nmr_cupon cpn,
              dtev.cmhl_cdg owner,
              ROW_NUMBER() OVER (PARTITION BY dtev.tckt_cdg_ticket, dtev.dtpr_nmr_cupon, dtev.cmhl_cdg ) AS ORDEN
            FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
            INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_EVENTO` iaev
            ON iaev.dtev_cdg_cupon = dtev.dtev_cdg_cupon AND iaev.iaev_flg_doble_uso != "Y"
            WHERE CAST( dtev.prsr_fch_emision AS DATE) 
		          BETWEEN DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL - (select meses from prm) + 1 MONTH) 
              AND DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
              AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
              AND evnt_est_contable IN ("C","D")
              )
            WHERE id_ticket = dtvt.tckt_cdg_ticket
              AND cpn = dtvt.dtpr_nmr_cupon
              AND owner = dtvt.cmhl_cdg
              AND ORDEN=1
              )
      ) AS SIST_USO,
    (SELECT DTEV_CDG_GRCN
      FROM 
      (SELECT DTEV_CDG_GRCN, ORDEN
        FROM 
        (SELECT DTEV.DTEV_CDG_GRCN, dtev.tckt_cdg_ticket id_ticket,dtev.dtpr_nmr_cupon cpn,dtev.cmhl_cdg owner,
          ROW_NUMBER() OVER (PARTITION BY dtev.tckt_cdg_ticket, dtev.dtpr_nmr_cupon, dtev.cmhl_cdg ) AS ORDEN
          FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
          INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_EVENTO` iaev
          ON iaev.dtev_cdg_cupon = dtev.dtev_cdg_cupon AND iaev.iaev_flg_doble_uso != "Y"
                WHERE
                  CAST(dtev.prsr_fch_emision AS DATE) 
		              BETWEEN DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL - (select meses from prm) + 1 MONTH) 
		              AND DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
                  AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
                  AND evnt_est_contable IN ("C","D") 
                )
              WHERE id_ticket = dtvt.tckt_cdg_ticket
              AND cpn = dtvt.dtpr_nmr_cupon 
              AND owner = dtvt.cmhl_cdg 
              AND ORDEN=1
              )
            ) AS GRUPO_CONTABLE_USO,
          (SELECT DTEV_FCH_SAP
            FROM 
            (SELECT DTEV_FCH_SAP,ORDEN
              FROM 
              (SELECT DTEV.DTEV_FCH_SAP, dtev.tckt_cdg_ticket id_ticket, dtev.dtpr_nmr_cupon cpn, dtev.cmhl_cdg owner,
                  ROW_NUMBER() OVER (PARTITION BY dtev.tckt_cdg_ticket, dtev.dtpr_nmr_cupon, dtev.cmhl_cdg ) AS ORDEN
                FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
                INNER JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_EVENTO` iaev
                ON iaev.dtev_cdg_cupon = dtev.dtev_cdg_cupon AND iaev.iaev_flg_doble_uso != "Y"
                WHERE CAST(dtev.prsr_fch_emision AS DATE) 
                BETWEEN DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL - (select meses from prm) + 1 MONTH)
              	AND DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
                AND dtev.tpev_cdg IN UNNEST((select cnta from prm)) AND evnt_est_contable IN ("C","D") 
                )
              WHERE id_ticket = dtvt.tckt_cdg_ticket AND cpn = dtvt.dtpr_nmr_cupon AND owner = dtvt.cmhl_cdg AND ORDEN=1)
            ) AS FECHA_CONTABLE_USO
          FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_VENTA` dtvt
          LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.INFORMACION_ADICIONAL_VENTA` iavt 
          ON dtvt.dtvt_cdg_cupon = iavt.dtvt_cdg_cupon
          LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TICKET` tckt
          ON tckt.tckt_cdg_ticket = dtvt.tckt_cdg_ticket AND tckt.prsr_fch_emision = dtvt.prsr_fch_emision
          LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TRANSACCION` trns 
          ON trns.trns_seq = tckt.trns_seq 
          WHERE CAST(dtvt.prsr_fch_emision AS DATE) 
          BETWEEN DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL - (select meses from prm) + 1 MONTH) 
          AND DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
          AND dtvt.tpev_cdg = 1
          AND dtvt.cmhl_cdg IN (89862200, 200303785,200505172,199999999,203164089,200000201,202602698)
          AND dtvt.evnt_est_contable = (select estatus from prm) AND dtvt.dtpr_nmr_cupon > 0
          AND dtvt.dtvt_grcn_cdg IN UNNEST((select grp_cnt from grupo))
          AND 
          (NOT EXISTS
            (SELECT 1 FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
              WHERE dtvt.tckt_cdg_ticket = dtev.tckt_cdg_ticket
              AND dtvt.dtpr_nmr_cupon = dtev.dtpr_nmr_cupon AND dtvt.cmhl_cdg = dtev.cmhl_cdg
              AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
              AND dtev.evnt_est_contable = "C"
              AND CAST(dtev.dtev_fch_sap AS DATE) <= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
              ) 
            OR EXISTS 
            (SELECT 1
              FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
              WHERE dtvt.tckt_cdg_ticket = dtev.tckt_cdg_ticket AND dtvt.dtpr_nmr_cupon = dtev.dtpr_nmr_cupon
              AND dtvt.cmhl_cdg = dtev.cmhl_cdg
              AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
              AND dtev.evnt_est_contable = "C"
              AND CAST(dtev.dtev_fch_sap AS DATE) > DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
              )
            )
          AND NOT EXISTS
          (SELECT 1
            FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_CADUCO` dtcd
            WHERE dtvt.tckt_cdg_ticket = dtcd.tckt_cdg_ticket
            AND dtvt.dtpr_nmr_cupon = dtcd.dtpr_nmr_cupon AND dtvt.cmhl_cdg = dtcd.cmhl_cdg
            AND dtcd.tpev_cdg in UNNEST ((select tpev_cdg_22 from prm))
            AND dtcd.evnt_est_contable IN ("C","D")
            AND (
              CAST(dtcd.dtcd_fch_carga AS DATE) <= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
              OR dtcd.dtcd_fch_carga IS NULL 
              )
            )AND NOT EXISTS 
          (SELECT 1 FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
            WHERE dtvt.tckt_cdg_ticket = dtev.tckt_cdg_ticket
            AND dtvt.dtpr_nmr_cupon = dtev.dtpr_nmr_cupon AND dtvt.cmhl_cdg = dtev.cmhl_cdg
            AND dtev.tpev_cdg IN UNNEST((select tpev_cdg_412 from prm))
            AND dtev.evnt_est_contable = "D"
            AND 
            (CAST(dtev.dtev_fch_carga AS DATE) <= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
              OR dtev.dtev_fch_carga IS NULL
              )
            )AND NOT EXISTS 
          (SELECT 1 
            FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_EVENTO` dtev
            WHERE dtvt.tckt_cdg_ticket = dtev.tckt_cdg_ticket AND dtvt.dtpr_nmr_cupon = dtev.dtpr_nmr_cupon
            AND dtvt.cmhl_cdg = dtev.cmhl_cdg
            AND dtev.tpev_cdg IN UNNEST((select cnta from prm))
            AND CAST(dtev.dtev_fch_carga AS DATE) <= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
            AND EXISTS 
            (SELECT 1 
              FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.DETALLE_CADUCO` dtcd
              WHERE dtev.tckt_cdg_ticket = dtcd.tckt_cdg_ticket
              AND dtev.dtpr_nmr_cupon = dtcd.dtpr_nmr_cupon AND dtev.cmhl_cdg = dtcd.cmhl_cdg
              AND dtcd.tpev_cdg in UNNEST((select tpev_cdg_22 from prm))
              AND dtcd.evnt_est_contable = "D"
              )
            )
          AND CAST(dtvt.dtvt_fch_sap AS DATE) <= DATE_SUB(DATE_TRUNC(DATE_ADD(PARSE_DATE("%Y%m",@FCH_MES_CORTE), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)
    			) uncc
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.COMISION_CUPON` COMISION_CUPON ON uncc.dtvt_cdg_cupon = CMPC_CDG_CUPON AND CMPC_CDG_CONCEPTO = "SAL"
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_ODSBQ}'.TAX_CUPON` TAX_CUPON ON uncc.dtvt_cdg_cupon = TXCN_CDG_CUPON AND TAX_CUPON.TXCN_CDG_CONCEPTO = "SAL"
LEFT JOIN `'${PROYECTO_BQ_ORI}'.'${ESQ_MAESTRO}'.TAXES_ONE` TAXES_ONE  ON TAX_CUPON.TPTX_CDG = TAXES_ONE.TXES_CDG_TAX
GROUP BY DTVT_CDG_CUPON,CIA,DCTO,CPN,EVENTO,GRUPO_CONTABLE,EST_CONTABLE,FCH_SAP,FCH_CONTABLE,FCH_EMISION,FCH_REGISTRO,ESPECIE,TRNC,
MONEDA,MNT_USD,MNT_PAGO,MNT_LOCAL,OWNER_CONTABLE,FCH_VUELO,FCH_USO,CUIDAD_ORIGEN,CUIDAD_DESTINO,AEROPUERTO_ORIGEN,
AEROPUERTO_DESTINO,BSAR,PAIS,RFIC_RFISC,CARRIER_OPERADOR,CARRIER_MARKETING,NMR_COMPROBANTE,NMR_VUELO,TPO_VTA_ESPECIAL, 
TPO_VTA_ESTANDAR,SIST_USO,GRUPO_CONTABLE_USO,FECHA_CONTABLE_USO;'

echo $Query_rnu 
_ARCHIVO_LOG_BQ=/home/malenantillanca_everis/PPV/data/'SHELL_BQ_RNU'.log;
_FECHA_PROC=`date +'%Y%m%d'`;

echo "*******************************************************************" >$_ARCHIVO_LOG_BQ
echo " Query: ${Query_rnu}                                               " >>$_ARCHIVO_LOG_BQ
echo " Fecha Ejecucion: ${_FECHA_PROC}                                   " >>$_ARCHIVO_LOG_BQ
echo " @FCH_MES_CORTE: '${FCH_CORTE}'                                    " >>$_ARCHIVO_LOG_BQ
echo "*******************************************************************" >>$_ARCHIVO_LOG_BQ

#=====================================================================================#
#_TRAMO_RNU='SELECT meses   FROM `'${PROYECTO_BQ_ORI}'.'${ESQ_PARAMETROS}'.PRM_RNU`;'
#json=`echo $Q_TRAMO_RNU | bq query --use_legacy_sql=false --format sparse`
#TRAMO_RNU=$(echo $json |grep -Eo "[[:digit:]]+"); export TRAMO_RNU;
#=====================================================================================#

echo $Query_rnu | bq query \
	--use_legacy_sql=False \
	--destination_table $PROYECTO_BQ_DEST:$ESQ_BQ_DEST.$TABLA_BQ_DEST \
	--replace \
	--parameter=FCH_MES_CORTE::$FCH_CORTE


_RETURN=$?
echo $_RETURN
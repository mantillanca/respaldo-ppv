bq --location=US load --replace --autodetect --source_format=NEWLINE_DELIMITED_JSON --project_id sp-os-rca-dev-01  PRM_EXODSTB.PRM_UNR gs://sp-os-rca-bkt-01/prm_ppv/PRM_UNR.json
bq --location=US load --replace --autodetect --source_format=NEWLINE_DELIMITED_JSON --project_id sp-os-rca-dev-01  PRM_EXODSTB.PRM_RNU gs://sp-os-rca-bkt-01/prm_ppv/PRM_RNU.json
bq --location=US load --replace --autodetect --source_format=NEWLINE_DELIMITED_JSON --project_id sp-os-rca-dev-01   PRM_EXODSTB.PRM_USOS gs://sp-os-rca-bkt-01/prm_ppv/PRM_USOS.json 
bq --location=US load --replace --autodetect --source_format=NEWLINE_DELIMITED_JSON --project_id sp-os-rca-dev-01  PRM_EXODSTB.PRM_VENTA gs://sp-os-rca-bkt-01/prm_ppv/PRM_VENTA.json

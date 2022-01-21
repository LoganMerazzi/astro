FROM quay.io/astronomer/ap-airflow:2.2.3-onbuild

ENV AIRFLOW_VAR_PESSOA='{"nome":"Logan", "idade":"40", "api_secret":"MySecret"}'
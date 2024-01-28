POETRY						=$(shell command -v poetry 2> /dev/null)
PY3							=$(shell command -v python3 2> /dev/null)

DBT_MODEL_PATH				=$(shell pwd)/dbt_fivetran
CLONE_DBT_MODEL_SCRIPT_PATH	=$(shell pwd)/miscellaneous/dbt/clone_dbt_models.py

PY_RUNNER					=poetry run
DBT_CMD						=poetry run dbt
@if [ -z $(POETRY) ]; then \
	echo "poetry is not installed, try to use python3 in current env."; \
	PY_RUNNER=python3; \
	DBT_CMD=python3 -m dbt.cli.main \
elif [ -z $(PY3) ] \
	echo "both poetry and python3 could not be found."; \
	exit 2; \
fi


clone-dbt-local: ${DBT_MODEL_PATH} ${CLONE_DBT_MODEL_SCRIPT_PATH}
	cd ${DBT_MODEL_PATH}	\
		&& export INPUT=$$(${DBT_CMD} --quiet ls --models $(DBT_MODELS) --output json --output-keys "database schema name depends_on unique_id alias")	\
		&& ${PY_RUNNER} ${CLONE_DBT_MODEL_SCRIPT_PATH} $$INPUT

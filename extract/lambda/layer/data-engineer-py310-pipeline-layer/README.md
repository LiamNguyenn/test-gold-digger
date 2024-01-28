docker ps
docker exec -it 0bd76bb2fb43 /bin/bash
cd ~/layer_setup
mkdir -p layer/python/lib/python3.10/site-packages
pip install -r requirements.txt -t layer/python/lib/python3.10/site-packages/
cd layer
zip -r layer.zip *
take the zip and upload layer

TURN ON: Use Rosetta for x86/amd64 emulation on Apple Silicon
source /Users/liam.nguyen/Documents/2.Work/1.Coding/data-plumbers/env/mnt/lambda_linux/lambda/keypay/lambda-keypay-venv/bin/activate
chalice deploy --profile 418054751921_DataScientist --stage dev

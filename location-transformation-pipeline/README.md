Build command  
`docker image build -t us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/location-transformation-pipeline:latest .`

Push command  
`docker push us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/location-transformation-pipeline:latest`

Build DataFlow template  
```
gcloud dataflow flex-template build gs://cis9440-kr-dataflow/dataflow/templates/location-transformation-pipeline.json --image-gcr-path "us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/location-transformation-pipeline:latest" --sdk-language "PYTHON" --flex-template-base-image "gcr.io/dataflow-templates-base/python39-template-launcher-base" --metadata-file "metadata.json" --py-path "." --env "FLEX_TEMPLATE_PYTHON_PY_FILE=location-transformation-pipeline.py" --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```

Run locally
```
python location-transformation-pipeline.py --temp_location gs://cis9440-kr-dataflow/tmp/ --mode increment --project avid-garage-399623 --dataset cis9440_project
```
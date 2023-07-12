api:
	uvicorn api:app --port 6000 --reload

hpc-es:
	nohup /hpctmp/e0960100/Elastic-Search/elasticsearch-8.8.2/bin/elasticsearch > es.log 2>&1 &
	echo $! > es_pid.txt

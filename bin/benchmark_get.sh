#/bin/bash

wrk -c 100 -t 8 -d 10s  http://localhost:8000/queue 

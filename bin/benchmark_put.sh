#/bin/bash

wrk -c 100 -t 8 -d 10s -s bin/write.lua  http://localhost:8000/queue 

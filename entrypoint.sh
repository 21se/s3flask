#!/bin/sh

redis-server &
python /app/ksu_s3plus.py
rem set MINIO_ACCESS_KEY=minioaccess
rem set MINIO_SECRET_KEY=miniosecret
cd ./minio
minio.exe server ./data/
pause
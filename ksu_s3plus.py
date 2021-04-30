import requests
from redis import Redis
from minio import Minio
from flask import Flask, Response, request, render_template
import datetime
import os


# init 
config = {}
config['redis.server']       = os.getenv('REDIS_SERVER', 'localhost')
config['redis.port']         = os.getenv('REDIS_PORT', '6479')
config['minio.server']       = os.getenv('MINIO_SERVER', 'localhost')
config['minio.port']         = os.getenv('MINIO_PORT', '9000')
config['minio.access_key']   = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
config['minio.secret_key']   = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
config['flask.server']       = os.getenv('FLASK_SERVER', '0.0.0.0')
config['flask.port']         = os.getenv('FLASK_PORT', '5000')
app = Flask(__name__, template_folder = 'templates')
redis = Redis(config['redis.server'], config['redis.port'], 0)


@app.route('/', methods = ['GET'])
def route_welcome():
    return render_template('welcome.html'), 200


@app.route('/', methods = ['POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods = ['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


def proxy():
    # ***FIND*** перехват запроса для поиска по метаданным
    if 'list-type' in request.args and ':' in request.args['prefix']:
        # аутентификация запроса

        bucket_name = request.path[1:]

        r = requests.request(request.method,
                             'http://' + config['minio.server'] + ':' + config['minio.port'] + request.path,
                             params = request.args, stream = True, headers = request.headers,
                             allow_redirects = False, data = request.data)
        if r.status_code != 200:
            return r.text, r.status_code

        try:
            redis.ping()
        except Exception as exp:
            return render_template('error.html', text = str.format('Redis: {}', exp), bucket_name = bucket_name,
                                   request_path = request.path), 403

        files = []

        postfix = request.args['prefix'][request.args['prefix'].find(':'):]
        prefix = request.args['prefix'][:request.args['prefix'].find(':')]
        # пример запроса "mc find cred/bucket/:mytag:myvalue"
        for key in redis.keys('*' + postfix + '*'):
            key = key.decode()
            name = key[:key.find(':')]

            # возврат файлов из указанного каталога
            if name.startswith(bucket_name + prefix):
                name = name.replace(bucket_name, '')
                files.append(name)

        return render_template('find.html', files = files, bucket_name = bucket_name,
                               delimiter = request.args['delimiter'])

    path = request.path
    if ':' in path:
        path = path[:path.find(':')]

    # проксирование запроса к серверу MinIO
    r = requests.request(request.method, 'http://' + config['minio.server'] + ':' + config['minio.port'] + path,
                         params = request.args, stream = True, headers = request.headers,
                         allow_redirects = False, data = request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content = False):
            yield chunk

    out = Response(generate(), headers = headers)
    out.status_code = r.status_code

    if out.status_code == 200:
        # ***CP***
        if request.method == 'PUT':
            # проверка доступности хранилища Redis
            try:
                redis.ping()
            except Exception as exp:
                return render_template('error.html', text = str.format('Redis: {}', exp),
                                       bucket_name = request.path.replace('/', ''), request_path = request.path), 403

            # добавление метаданных в Redis при добавлении файла
            for key, value in request.headers.environ.items():
                if key.startswith('HTTP_X_AMZ_META_') and key != 'HTTP_X_AMZ_META_MC_ATTRS':
                    key = key.replace('HTTP_X_AMZ_META_', '').lower()
                    path = request.path
                    if path.startswith('/'):
                        path = path[1:]

                    for rkey in redis.keys(path + ':' + key + ':*'):
                        redis.delete(rkey)

                    redis.set(path + ':' + key + ':' + value, '1')

            redis.save()

        # ***DELETE***
        elif request.query_string.decode() == 'delete=':
            # проверка доступности хранилища Redis
            try:
                redis.ping()
            except Exception as exp:
                return render_template('error.html', text = str.format('Redis: {}', exp),
                                       bucket_name = request.path.replace('/', ''), request_path = request.path), 403

            if request.data:
                # удаление метаданных в Redis при удалении файла
                deleted = False
                data = parse(request.data)
                objects = data.get('Delete', {}).get('Object', {})

                rpath = request.path
                if rpath.startswith('/'):
                    rpath = rpath[1:]

                for obj in objects:
                    if type(obj) != str:
                        path = rpath + obj.get('Key', '')
                    else:
                        path = rpath + objects.get('Key', '')

                    for key in redis.keys(path + ':*'):
                        if redis.delete(key):
                            deleted = True

                if deleted:
                    redis.save()

    return out


def init_redis():
    # Инициализация базы redis по данным метатегов s3-minio
    minio = Minio(config['minio.server'] + ':' + config['minio.port'],
                  access_key = config['minio.access_key'],
                  secret_key = config['minio.secret_key'],
                  secure = False)
    buckets = minio.list_buckets()
    redis.flushdb()
    start = datetime.datetime.now()
    for bucket in buckets:
        for obj in minio.list_objects(bucket.name, recursive = True):
            if obj.is_dir: 
                continue

            for key, value in minio.stat_object(bucket.name, obj.object_name).metadata.items():
                if 'x-amz-meta-' in key:
                    if key == 'x-amz-meta-mc-attrs':
                        continue
                    key = key.replace('x-amz-meta-', '')
                    redis.set(bucket.name + '/' + obj.object_name + ':' + key + ':' + value, '1')
    redis.save()
    end = datetime.datetime.now()

    return str.format('Количество значений мета-тегов: {}. Времени прошло: {}.',
                      redis.dbsize(), str(end - start))


if __name__ == '__main__':
    init_redis()
    app.run(host = config['flask.server'], port = config['flask.port'])

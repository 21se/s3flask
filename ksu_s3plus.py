import datetime
import os

import requests
from flask import Flask, Response, request, render_template
from xmltodict import parse

from minio import Minio
from redis import Redis

# init
config = {}
config['redis.server'] = os.getenv('REDIS_SERVER', 'localhost')
config['redis.port'] = os.getenv('REDIS_PORT', '6379')
config['minio.server'] = os.getenv('MINIO_SERVER', 'localhost')
config['minio.port'] = os.getenv('MINIO_PORT', '9000')
config['minio.access_key'] = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
config['minio.secret_key'] = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
config['flask.server'] = os.getenv('FLASK_SERVER', '0.0.0.0')
config['flask.port'] = os.getenv('FLASK_PORT', '5000')

app = Flask(__name__, template_folder='templates')
redis = Redis(config['redis.server'], config['redis.port'], 0, charset='utf-8', decode_responses=True)
minio = Minio(config['minio.server'] + ':' + config['minio.port'],
              access_key=config['minio.access_key'],
              secret_key=config['minio.secret_key'],
              secure=False)


@app.route('/', methods=['GET'])
def route_welcome():
    return render_template('welcome.html'), 200


@app.route('/', methods=['POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


def proxy():
    # ***LS*** перехват запроса для поиска по метаданным
    redis_save = False

    if 'list-type' in request.args and ':' in request.args['prefix']:
        bucket_name = request.path[1:]

        # аутентификация запроса
        r = requests.request(request.method,
                             'http://' + config['minio.server'] + ':' + config['minio.port'] + request.path,
                             params=request.args, stream=True, headers=request.headers,
                             allow_redirects=False, data=request.data)
        if r.status_code != 200:
            return r.text, r.status_code

        try:
            redis.ping()
        except Exception as exp:
            return render_template('error.html', text=str.format('Redis: {}', exp), bucket_name=bucket_name,
                                   request_path=request.path), 403

        files = []

        postfix = request.args['prefix'][request.args['prefix'].find(':'):]
        prefix = request.args['prefix'][:request.args['prefix'].find(':')]

        # пример запроса "mc ls cred/bucket/:my_tag:my_value"
        for key in redis.keys('*' + postfix + '*'):
            name = key[:key.find(':')]

            if not name.startswith(bucket_name + prefix):
                continue

            # возврат файлов из указанного каталога
            name = name.replace(bucket_name, '')
            if name not in files:
                files.append(name)

        return render_template('find.html', files=files, bucket_name=bucket_name,
                               delimiter=request.args['delimiter']), 200

    path = request.path
    if ':' in path:
        path = path[:path.find(':')]

    # проксирование запроса к серверу MinIO
    r = requests.request(request.method, 'http://' + config['minio.server'] + ':' + config['minio.port'] + path,
                         params=request.args, stream=True, headers=request.headers,
                         allow_redirects=False, data=request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk

    out = Response(generate(), headers=headers)
    out.status_code = r.status_code

    if out.status_code == 200:
        # ***CP***
        if request.method == 'PUT':
            # проверка доступности хранилища Redis
            try:
                redis.ping()
            except Exception as exp:
                return render_template('error.html', text=str.format('Redis: {}', exp),
                                       bucket_name=request.path.replace('/', ''), request_path=request.path), 403

            path = request.path
            if path.startswith('/'):
                path = path[1:]

            # удаление существующих метаданных
            for key in redis.keys(path + ':*:*'):
                redis.delete(key)

            # добавление метаданных
            for key, value in request.headers:
                key = key.lower()
                if key.startswith('x-amz-meta-'):
                    key = requests.utils.unquote(key)
                    value = requests.utils.unquote(value)
                    key = key.replace('x-amz-meta-', '').lower()

                    redis.set(path + ':' + key + ':' + value, '1')
                    if not redis_save:
                        redis_save = True

            # если при добавлении отсутствуют метаданные, то проверяем на копирование файла
            if not redis_save:
                source_path = request.headers.get('X-Amz-Copy-Source')
                if source_path:
                    source_path = requests.utils.unquote(source_path)
                    for key in redis.keys(source_path + ':*:*'):
                        redis.set(key.replace(source_path, path), '1')
                        if not redis_save:
                            redis_save = True

            if redis_save:
                redis.save()

        # ***DELETE***
        elif request.method == 'POST' and 'delete' in request.args:
            # проверка доступности хранилища Redis
            try:
                redis.ping()
            except Exception as exp:
                return render_template('error.html', text=str.format('Redis: {}', exp),
                                       bucket_name=request.path.replace('/', ''), request_path=request.path), 403

            if request.data:
                # удаление метаданных в Redis при удалении файла
                data = parse(request.data)
                objects = data.get('Delete', {}).get('Object', {})

                rpath = request.path
                if rpath.startswith('/'):
                    rpath = rpath[1:]

                if not rpath.endswith('/'):
                    rpath += '/'

                for obj in objects:
                    if type(obj) != str:
                        path = rpath + obj.get('Key', '')
                    else:
                        path = rpath + objects.get('Key', '')

                    for key in redis.keys(path + ':*:*'):
                        redis.delete(key)
                        if not redis_save:
                            redis_save = True

                if redis_save:
                    redis.save()

    return out


def init_redis():
    # Инициализация базы redis по данным метатегов s3-minio
    save_redis = False
    buckets = minio.list_buckets()
    redis.flushdb()
    start = datetime.datetime.now()

    for bucket in buckets:
        for obj in minio.list_objects(bucket.name, recursive=True):
            if obj.is_dir:
                continue

            for key, value in minio.stat_object(bucket.name, obj.object_name).metadata.items():
                key = key.lower()
                if 'x-amz-meta-' in key:
                    key = key.replace('x-amz-meta-', '')

                    key = requests.utils.unquote(key)
                    value = requests.utils.unquote(value)

                    redis.set(bucket.name + '/' + obj.object_name + ':' + key + ':' + value, '1')
                    if not save_redis:
                        save_redis = True

    if save_redis:
        redis.save()

    end = datetime.datetime.now()

    return str.format('Загружено значений мета-тегов: {}. Времени прошло: {}.',
                      redis.dbsize(), end - start)


if __name__ == '__main__':
    print(' *', init_redis())
    app.run(host=config['flask.server'], port=config['flask.port'])

import requests
from redis import Redis
from minio import Minio
from flask import Flask, Response, request, render_template
from timeit import default_timer as timer
from datetime import timedelta
from xmltodict import parse
from os.path import exists
from configparser import ConfigParser


def create_config(path):
    cfg = ConfigParser()

    cfg.add_section('Redis')
    cfg.set('Redis', 'ip', 'localhost')
    cfg.set('Redis', 'port', '6379')
    cfg.add_section('Minio')
    cfg.set('Minio', 'ip', 'localhost')
    cfg.set('Minio', 'port', '9000')
    cfg.set('Minio', 'access_key', 'minioadmin')
    cfg.set('Minio', 'secret_key', 'minioadmin')
    cfg.add_section('Flask')
    cfg.set('Flask', 'ip', '0.0.0.0')
    cfg.set('Flask', 'port', '5000')

    with open(path, 'w') as config_file:
        cfg.write(config_file)


def get_config(path):
    if not exists(path):
        create_config(path)

    cfg = ConfigParser()
    cfg.read(path)

    return cfg


config = get_config('config.ini')
app = Flask(__name__, template_folder = 'templates')
redis = Redis(config['Redis']['ip'], config['Redis']['port'], 0)


@app.route('/', methods = ['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods = ['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


@app.route('/init', methods = ['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_init():
    minio = Minio(config['Minio']['ip'] + ':' + config['Minio']['port'],
                  access_key = config['Minio']['access_key'],
                  secret_key = config['Minio']['secret_key'],
                  secure = False)

    # аутентификация minIO
    try:
        buckets = minio.list_buckets()
    except Exception as exp:
        return str.format('MinIO: {}', exp), 403

    try:
        redis.flushdb()
    except Exception as exp:
        return str.format('Redis: {}', exp), 403

    start = timer()

    for bucket in buckets:
        for obj in minio.list_objects_v2(bucket.name, recursive = True):
            if obj.is_dir:
                continue

            for key, value in minio.stat_object(bucket.name, obj.object_name).metadata.items():
                if 'x-amz-meta-' in key:
                    if key == 'x-amz-meta-mc-attrs':
                        continue
                    key = key.replace('x-amz-meta-', '')
                    redis.set(bucket.name + '/' + obj.object_name + ':' + key + ':' + value, '1')

    redis.save()

    end = timer()

    return str.format('Количество значений мета-тегов: {}. Времени прошло: {}.',
                      redis.dbsize(), timedelta(seconds = end - start))


def proxy():
    # перехват запроса для поиска по метаданным

    if 'list-type' in request.args and ':' in request.args['prefix']:
        # аутентификация запроса

        bucket_name = request.path[1:]

        r = requests.request(request.method,
                             'http://' + config['Minio']['ip'] + ':' + config['Minio']['port'] + request.path,
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
    r = requests.request(request.method, 'http://' + config['Minio']['ip'] + ':' + config['Minio']['port'] + path,
                         params = request.args, stream = True, headers = request.headers,
                         allow_redirects = False, data = request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content = False):
            yield chunk

    out = Response(generate(), headers = headers)
    out.status_code = r.status_code

    if out.status_code == 200:
        if request.method == 'PUT':
            # TODO: отмена записи/удаления при недоступности Redis?
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


if __name__ == '__main__':
    app.run(host = config['Flask']['ip'], port = config['Flask']['port'])

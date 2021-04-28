import requests
from redis import Redis
from minio import Minio
from flask import Flask, Response, request, render_template
from timeit import default_timer as timer
from datetime import timedelta
from xmltodict import parse
from utils import get_config


class FlaskProxy(Flask):
    def run(self, host=None, port=None, debug=None, load_dotenv=True, **options):
        print(' *', init())
        Flask.run(self, host, port)


app = FlaskProxy(__name__, template_folder='templates')
config = get_config('config.ini')
redis = Redis(config['Redis']['ip'], config['Redis']['port'], 0, charset='utf-8', decode_responses=True)
minio = Minio(config['Minio']['ip'] + ':' + config['Minio']['port'],
              access_key=config['Minio']['access_key'],
              secret_key=config['Minio']['secret_key'],
              secure=False)


@app.route('/', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


@app.route('/init', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def init():

    save_redis = False

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
        for obj in minio.list_objects(bucket.name, recursive=True):
            if obj.is_dir:
                continue

            for key, value in minio.stat_object(bucket.name, obj.object_name).metadata.items():
                if key.startswith('x-amz-meta-'):
                    key = key.replace('x-amz-meta-', '')
                    key = requests.utils.unquote(key)
                    value = requests.utils.unquote(value)
                    redis.set(bucket.name + '/' + obj.object_name + ':' + key + ':' + value, '1')
                    print('redis.set', bucket.name + '/' + obj.object_name + ':' + key + ':' + value)
                    if not save_redis:
                        save_redis = True

    if save_redis:
        redis.save()
        print('redis.save')

    end = timer()

    return str.format('Загружено значений мета-тегов: {}. Времени прошло: {}.',
                      redis.dbsize(), timedelta(seconds=end - start))


def proxy():
    # перехват запроса для поиска по метаданным

    redis_save = False

    if 'list-type' in request.args and ':' in request.args['prefix']:
        bucket_name = request.path[1:]

        # аутентификация запроса
        r = requests.request(request.method,
                             'http://' + config['Minio']['ip'] + ':' + config['Minio']['port'] + request.path,
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
    r = requests.request(request.method, 'http://' + config['Minio']['ip'] + ':' + config['Minio']['port'] + path,
                         params=request.args, stream=True, headers=request.headers,
                         allow_redirects=False, data=request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk

    out = Response(generate(), headers=headers)
    out.status_code = r.status_code

    if out.status_code == 200:
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
            for rkey in redis.keys(path + ':*:*'):
                redis.delete(rkey)
                print('redis.delete', rkey)

            # добавление метаданных
            for key, value in request.headers.environ.items():
                if key.startswith('HTTP_X_AMZ_META_'):
                    key = requests.utils.unquote(key)
                    value = requests.utils.unquote(value)
                    key = key.replace('HTTP_X_AMZ_META_', '').lower()

                    redis.set(path + ':' + key + ':' + value, '1')
                    print('redis.set', path + ':' + key + ':' + value)
                    if not redis_save:
                        redis_save = True

            # если при добавлении отсутствуют метаданные, то проверяем на копирование файла
            if not redis_save:
                source_path = request.headers.environ.get('HTTP_X_AMZ_COPY_SOURCE')
                if source_path:
                    source_path = requests.utils.unquote(source_path)
                    for key in redis.keys(source_path + ':*:*'):
                        redis.set(key.replace(source_path, path), '1')
                        print('redis.set', key.replace(source_path, path))
                        if not redis_save:
                            redis_save = True

            if redis_save:
                redis.save()
                print('redis.save')

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

                    for key in redis.keys(path + ':*'):
                        redis.delete(key)
                        redis_save = True
                        print('redis.delete', key)

                if redis_save:
                    redis.save()
                    print('redis.save')

    return out


if __name__ == '__main__':
    print(app.run(host=config['Flask']['ip'], port=config['Flask']['port']))

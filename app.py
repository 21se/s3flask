from redis import Redis
from minio import Minio
from flask import Flask, Response, request, render_template
import xmltodict
import requests
import os
import configparser


def create_config(path):
    config = configparser.ConfigParser()

    config.add_section('Settings')
    config.set('Settings', 'dir', '/root/ksu_s3plus/')
    config.add_section('Redis')
    config.set('Redis', 'ip', 'localhost')
    config.set('Redis', 'port', '6379')
    config.add_section('Minio')
    config.set('Minio', 'ip', 'localhost')
    config.set('Minio', 'port', '9000')
    config.set('Minio', 'access_key', 'minioadmin')
    config.set('Minio', 'secret_key', 'minioadmin')
    config.add_section('Flask')
    config.set('Flask', 'ip', '0.0.0.0')
    config.set('Flask', 'port', '5000')

    with open(path, 'w') as config_file:
        config.write(config_file)


def get_config(path):
    if not os.path.exists(path):
        create_config(path)

    config = configparser.ConfigParser()
    config.read(path)

    return config


config = get_config('config.ini')
app = Flask(__name__, template_folder='templates')
redis = Redis(config['Redis']['ip'], config['Redis']['port'], 0)

@app.route('/', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


@app.route('/init', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_init():

    minio = Minio(config['Minio']['ip'] + ':' + config['Minio']['port'],
                  access_key=config['Minio']['access_key'],
                  secret_key=config['Minio']['secret_key'],
                  secure=False)

    redis.flushdb()

    for bucket in minio.list_buckets():
        for obj in minio.list_objects_v2(bucket.name, recursive=True):
            if obj.is_dir:
                continue

            for key, value in minio.stat_object(bucket.name, obj.object_name).metadata.items():
                if 'x-amz-meta-' in key and key != 'x-amz-meta-mc-attrs':
                    key = key.replace('x-amz-meta-', '')
                    redis.set(bucket.name + '/' + obj.object_name + ':' + key + ':' + value, '1')

    redis.save()

    return 'DB size: ' + str(redis.dbsize())


def proxy():

    # поиск по метаданным
    if 'list-type' in request.args and ':' in request.args['prefix']:
        files = []

        for key in redis.keys('*' + request.args['prefix'] + '*'):
            name = key.decode()[:key.decode().find(':')]
            files.append(name)

        return render_template('find.html', files=files)

    # удаление метаданных из redis при удалении файла
    if request.query_string.decode() == 'delete=':
        if request.data:
            deleted = False
            data = xmltodict.parse(request.data)
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

    path = request.path

    if ':' in path:
        path = path[:path.find(':')]

    r = requests.request(request.method, 'http://' + config['Minio']['ip'] + ':' + config['Minio']['port'] + path,
                         params=request.args, stream=True, headers=request.headers,
                         allow_redirects=False, data=request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk

    out = Response(generate(), headers=headers)
    out.status_code = r.status_code

    # добавление метаданных в redis при добавлении файла
    if out.status_code == 200:
        if request.method == 'PUT':
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

    return out


if __name__ == '__main__':
    app.run(host=config['Flask']['ip'], port=config['Flask']['port'])

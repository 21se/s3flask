from redis import Redis
from minio import Minio
from flask import Flask, Response, request, render_template
import xmltodict
import requests

app = Flask(__name__)
redis = Redis('localhost', 6379, 0)
minio = Minio('localhost:5000',
              access_key='minioadmin',
              secret_key='minioadmin',
              secure=False)


@app.route('/', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


@app.route('/init', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_init():

    redis.flushdb()

    for obj in minio.list_objects_v2('adminbucket', recursive=True):  # TODO: привязка к adminbucket?
        if obj.is_dir:
            continue

        for key, value in minio.stat_object('adminbucket', obj.object_name).metadata.items():
            if 'x-amz-meta-' in key and key != 'x-amz-meta-mc-attrs':
                key = key.replace('x-amz-meta-', '')
                redis.set('adminbucket/' + obj.object_name + ":" + key + ":" + value, '1')

    redis.save()

    return 'DB size: ' + str(redis.dbsize())


def proxy():

    # поиск по метаданным
    if "list-type" in request.args and ":" in request.args["prefix"]:
        files = []

        for key in redis.keys("*" + request.args["prefix"] + "*"):
            name = key.decode()[:key.decode().find(":")]
            files.append(name)

        return render_template('find.html', files=files)

    # удаление метаданных из redis при удалении файла
    # TODO: обработка удаления со страницы minio через webrpc?
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

                for key in redis.keys(path + ":*"):
                    if redis.delete(key):
                        deleted = True

            if deleted:
                redis.save()

    path = request.path

    if ':' in path:
        path = path[:path.find(':')]

    r = requests.request(request.method, 'http://localhost:9000' + path,
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
            save = False

            for key, value in request.headers.environ.items():
                if key.startswith('HTTP_X_AMZ_META_') and key != 'HTTP_X_AMZ_META_MC_ATTRS':
                    key = key.replace('HTTP_X_AMZ_META_', '').lower()
                    if request.path.startswith('/'):
                        request.path = request.path[1:]
                    if redis.set(request.path + ':' + key + ':' + value, '1'):
                        save = True

            if save:
                redis.save()

    return out


if __name__ == '__main__':
    app.run()

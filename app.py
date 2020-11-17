import redis as redis_client
from flask import Flask, Response, request, render_template
import requests
import xmltodict

app = Flask(__name__)
redis = redis_client.Redis('localhost', 6379, 0)


@app.route('/', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_home():
    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):
    return proxy()


@app.route('/find_guid/<path:guid>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_find_guid(guid):  # TODO: вернуть ListBucketResult?
    return render_template('find.html')


def proxy():
    r = requests.request(request.method, 'http://localhost:9000' + request.path,
                         params=request.args, stream=True, headers=request.headers,
                         allow_redirects=False, data=request.data)

    headers = dict(r.raw.headers)

    def generate():
        for chunk in r.raw.stream(decode_content=False):
            yield chunk

    out = Response(generate(), headers=headers)
    out.status_code = r.status_code

    if out.data:
        try:
            new_items = []
            datadict = xmltodict.parse(out.data)
            list_bucket_result = datadict.get('ListBucketResult')
            if list_bucket_result:
                contents = list_bucket_result.get('Contents', {})
                for content in contents:
                    new_items.append(content)
                for item in new_items:
                    contents.append(item)
                out.data = xmltodict.unparse(datadict)
        except Exception as error:
            print(error)

    if out.status_code == 200:
        if request.method == 'PUT':  # TODO: внутри сервера/на сервер?
            save = False
            for key, value in request.headers.environ.items():
                if key.startswith('HTTP_X_AMZ_META_') and key != 'HTTP_X_AMZ_META_MC_ATTRS':
                    key = key.replace('HTTP_X_AMZ_META_', '')
                    if redis.set(request.path + ':' + key + ':' + value, '1'):
                        save = True
            if save:
                redis.save()

    return out


if __name__ == '__main__':
    app.run()

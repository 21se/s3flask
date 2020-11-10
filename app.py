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


# @app.route('/copy', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
# def route_copy():


@app.route('/find_ksu', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route_find():
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
        if request.method == 'PUT':
            guid = request.headers.environ.get('HTTP_X_AMZ_META_GUID1C')
            if guid:
                if redis.set(request.path + ':guid1c:' + guid, '1'):
                    redis.save()

    return out


if __name__ == '__main__':
    app.run()

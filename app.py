from flask import Flask, Response, request, render_template
from minio import Minio
from minio.error import ResponseError
import requests

app = Flask(__name__)

minioClient = Minio('localhost:9000',
                  access_key='minioadmin',
                  secret_key='minioadmin',
                  secure=True)


@app.route('/', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def routehome():

    return proxy()


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def route(path):

    return proxy()


# @app.route('/copy', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
# def routecopy():




@app.route('/findksu', methods=['GET', 'POST', 'PUT', 'HEAD', 'DELETE', 'PATCH', 'OPTIONS'])
def routefind():

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


    return out


if __name__ == '__main__':

    app.run()

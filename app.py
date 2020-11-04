from flask import Flask, Response, request, render_template
import requests
import csv
import os

app = Flask(__name__)

def readGuids(filename):
    dictionary = {}

    try:
        with open(os.getcwd() + '\\' + filename, mode='r', newline='') as file:
            reader = csv.reader(file, delimiter=';')
            for rows in reader:
                key = rows[0]
                value = rows[1]

                dictionary[key] = value
    except:
        return dictionary

    return dictionary


def writeGuids(filename):
    with open(os.getcwd() + '\\' + filename, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows(objectsGuids.items())


objectsGuids = readGuids('objectsGUID.csv')

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

    if out.status_code == 200:
        if request.method == 'PUT':
            guid = request.headers.environ.get('HTTP_X_AMZ_META_GUID1C')
            if guid:
                objectsGuids[request.path] = guid
                writeGuids('objectsGUID.csv')

    return out


if __name__ == '__main__':
    app.run()

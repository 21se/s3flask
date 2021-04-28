from configparser import ConfigParser
from os.path import exists


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

    return cfg


def get_config(path):
    if not exists(path):
        return create_config(path)

    cfg = ConfigParser()
    cfg.read(path)

    return cfg

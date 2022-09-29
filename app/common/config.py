import json
import os
from dataclasses import dataclass
from os import path, environ

base_dir = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
config_local_file = path.join(base_dir, '.config_secret/settings_local.json')
config_proj_file = path.join(base_dir, '.config_secret/settings_proj.json')


config_secret_proj = json.loads(open(config_proj_file).read())

@dataclass
class Config:
    BASE_DIR = base_dir
    DB_POOL_RECYCLE: int = 900
    DB_ECHO: bool = True


@dataclass
class LocalConfig(Config):
    PROJ_RELOAD: bool = True
    CONFIG_SETTING_FILE: json = config_local_file


@dataclass
class ProdConfig(Config):
    PROJ_RELOAD: bool = False
    CONFIG_SETTING_FILE: json = config_proj_file


def conf():
    config = dict(prod=ProdConfig, local=LocalConfig)
    return config[environ.get("API_ENV", "local")]()

# .config_secret폴더 및 하위 파일 경로
# CONFIG_SECRET_DIR = os.path.join(ROOT_DIR, '.config_secret')
# CONFIG_SECRET_COMMON_FILE = os.path.join(CONFIG_SECRET_DIR, 'settings_common.json')
# CONFIG_SECRET_DEBUG_FILE = os.path.join(CONFIG_SECRET_DIR, 'settings_debug.json')
# CONFIG_SECRET_DEPLOY_FILE = os.path.join(CONFIG_SECRET_DIR, 'settings_deploy.json')
#
# config_secret_common = json.loads(open(CONFIG_SECRET_COMMON_FILE).read())
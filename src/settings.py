"""
認証情報などの環境変数を外部ファイルから取得するための関数
"""
import os
import pathlib
from dotenv import load_dotenv


dirname = pathlib.Path(__file__).parents[0]
dotenv_path = dirname / ".env"
load_dotenv(str(dotenv_path))

CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_SECRET = os.environ.get("ACCESS_SECRET")

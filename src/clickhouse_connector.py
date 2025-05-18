# clickhouse_connector.py
from clickhouse_driver import Client
import yaml
from pathlib import Path
from contextlib import contextmanager


class ClickHouseConnector:
    def __init__(self):
        self.config = self._load_config()

    def _load_config(self):
        """加载YAML配置文件"""
        config_path = Path(__file__).parent / "settings.yml"
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def __enter__(self):
        """进入上下文时创建连接"""
        self.client = Client(
            host=self.config['clickhouse']['host'],
            port=self.config['clickhouse']['port'],
            user=self.config['clickhouse']['user'],
            password=self.config['clickhouse']['password'],
            database=self.config['clickhouse']['database'],
            settings=self.config['clickhouse'].get('settings', {})
        )
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时关闭连接"""
        if hasattr(self, 'client'):
            self.client.disconnect()

    def execute(self, query, params=None):
        """执行查询并返回结果"""
        with self as client:
            return client.execute(query, params)
from clickhouse_driver import Client
import yaml
from pathlib import Path
from clickhouse_connector import ClickHouseConnector

def load_config():
    """加载YAML配置文件"""
    config_path = Path(__file__).parent / "settings.yml"
    with open(config_path, 'r',encoding='utf-8') as f:
        return yaml.safe_load(f)

def get_client():
    """创建ClickHouse客户端连接"""
    config = load_config()
    return Client(
        host=config['clickhouse']['host'],
        port=config['clickhouse']['port'],
        user=config['clickhouse']['user'],
        password=config['clickhouse']['password'],
        database=config['clickhouse']['database'],
        settings=config['clickhouse'].get('settings', {})
    )

# def test_connection(client):
#     """执行不依赖具体表的连接测试"""
#     try:
#         # 使用系统表或简单数学查询验证连接
#         result = client.execute('SELECT 1 AS connection_test')
#         if result and result[0][0] == 1:
#             return True, "Connection successful (ping test passed)"
#     except Exception as e:
#         return False, f"Connection failed: {str(e).split(':')[0]}"
#     return False, "Unexpected test result"


def test_connection(client):
    """查询业务表并返回前3条记录"""
    try:
        result = client.execute('SELECT * FROM products LIMIT 3')
        if result:
            return True, f"Connection successful. Sample data:\n{result}"
        else:
            return False, "No data in products table"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

if __name__ == "__main__":
    # client = get_client()
    client=ClickHouseConnector()
    success, message = test_connection(client)
    print(message)
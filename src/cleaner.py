from pathlib import Path
import pandas as pd
from datetime import datetime
import chardet
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def detect_encoding(filepath):
    """自动检测文件编码"""
    with open(filepath, 'rb') as f:
        result = chardet.detect(f.read(10000))  # 读取前10KB检测编码
    return result['encoding'] if result['confidence'] > 0.7 else 'gb18030'


def safe_read_csv(filepath, dtypes):
    """安全读取CSV文件，自动处理编码问题"""
    try:
        encoding = detect_encoding(filepath)
        logger.info(f"读取文件 {filepath}，检测到编码: {encoding}")
        return pd.read_csv(filepath, dtype=dtypes, encoding=encoding)
    except Exception as e:
        logger.error(f"读取文件 {filepath} 失败: {str(e)}")
        return None


def clean_price_data(df):
    """清洗价格数据"""
    if df is None or df.empty:
        return None

    # 统一日期格式
    df['change_date'] = pd.to_datetime(df['change_date'], errors='coerce')
    df = df.dropna(subset=['change_date'])

    # 保留所需字段
    df = df[['product_id', 'category_id', 'name', 'price', 'change_date']].copy()
    df.rename(columns={'change_date': 'date'}, inplace=True)

    # 数据清洗
    df = df[(df['price'] > 0) &
            (df['product_id'].notna()) &
            (df['category_id'].notna())]

    # 统一日期格式
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    return df


def main():
    # 设置路径（使用pathlib更安全）
    base_path = Path(__file__).resolve().parent.parent.parent / 'data'
    daily_price_path = base_path / 'utf'
    products_path = base_path / 'products.csv'
    categories_path = base_path / 'categories.csv'
    output_path = base_path / 'price.csv'

    # 读取基础数据
    logger.info("开始读取产品和类别数据")
    df_products = safe_read_csv(products_path, {'product_id': str, 'category_id': str})
    df_categories = safe_read_csv(categories_path, {'category_id': str})

    if df_products is None or df_categories is None:
        raise ValueError("基础数据读取失败")

    # 处理每日价格数据
    all_price_data = []
    processed_files = 0

    for file in daily_price_path.glob('*.csv'):
        logger.info(f"正在处理文件: {file.name}")
        df_daily = safe_read_csv(file, {'product_id': str, 'category_id': str})
        df_clean = clean_price_data(df_daily)

        if df_clean is not None:
            all_price_data.append(df_clean)
            processed_files += 1

    # 合并数据
    if not all_price_data:
        raise ValueError("没有有效的价格数据被处理")

    df_all = pd.concat(all_price_data, ignore_index=True)
    logger.info(f"成功合并 {processed_files} 个文件，总计 {len(df_all)} 条记录")

    # 保存结果
    df_all.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"结果已保存到 {output_path}")

    # 处理类别数据（原代码中的补充部分）
    if categories_path.exists():
        df_cat = pd.read_csv(categories_path)
        df_cat.fillna(-1, inplace=True)
        df_cat.to_csv(categories_path, index=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"程序运行失败: {str(e)}")
        raise
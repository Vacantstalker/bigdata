# cpi_calculator.py
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import logging
from pathlib import Path
from clickhouse_connector import ClickHouseConnector

# 配置详细日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cpi_calculator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def calculate_cpi(granularity='month'):
    """计算CPI（支持按月和按天计算）"""
    try:
        logger.info(f"开始计算{granularity}粒度CPI...")

        # 使用上下文管理器自动处理连接
        with ClickHouseConnector() as client:
            # 1. 获取基期数据
            base_query = """
            SELECT 
                category_id,
                avg(price) as base_price
            FROM commodity_prices
            WHERE date BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY category_id
            """
            logger.debug(f"执行基期查询: {base_query}")
            base_prices = client.execute(
                base_query,
                {'start_date': '2025-05-17', 'end_date': '2026-05-17'}
            )
            base_df = pd.DataFrame(base_prices, columns=['category_id', 'base_price'])
            logger.info(f"获取到 {len(base_df)} 个品类的基期价格")

            # 2. 获取时间粒度数据
            if granularity == 'month':
                time_query = """
                SELECT 
                    toStartOfMonth(toDate(date)) as time_period,
                    category_id,
                    avg(price) as avg_price
                FROM commodity_prices
                WHERE date BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY time_period, category_id
                ORDER BY time_period
                """
            else:  # day
                time_query = """
                SELECT 
                    toDate(date) as time_period,
                    category_id,
                    avg(price) as avg_price
                FROM commodity_prices
                WHERE date BETWEEN %(start_date)s AND %(end_date)s
                GROUP BY time_period, category_id
                ORDER BY time_period
                """

            logger.debug(f"执行{granularity}粒度查询: {time_query}")
            time_data = client.execute(
                time_query,
                {'start_date': '2025-05-17', 'end_date': '2028-05-15'}
            )
            time_df = pd.DataFrame(time_data, columns=['time_period', 'category_id', 'avg_price'])
            logger.info(f"获取到 {len(time_df)} 条{granularity}粒度价格记录")

            # 3. 获取品类权重
            weight_query = "SELECT category_id, weight FROM categories WHERE hierarchy = '3'"
            logger.debug(f"执行权重查询: {weight_query}")
            weights = client.execute(weight_query)
            weight_df = pd.DataFrame(weights, columns=['category_id', 'weight'])
            logger.info(f"获取到 {len(weight_df)} 个品类权重")

        # 4. 合并计算 (连接已自动关闭)
        logger.info(f"开始合并计算{granularity}粒度CPI...")
        merged = time_df.merge(base_df, on='category_id', how='inner') \
            .merge(weight_df, on='category_id', how='inner')

        # 强制转换为数值类型，非数字转为NaN
        merged['avg_price'] = pd.to_numeric(merged['avg_price'], errors='coerce')
        merged['base_price'] = pd.to_numeric(merged['base_price'], errors='coerce')
        merged['weight'] = pd.to_numeric(merged['weight'], errors='coerce')

        if merged.empty:
            raise ValueError("数据合并后为空，请检查品类ID匹配")

        if merged['base_price'].isna().any():
            logger.warning(f"发现 {merged['base_price'].isna().sum()} 条无效基期价格记录")

        merged['price_index'] = (merged['avg_price'] / merged['base_price']) * 100
        logger.debug(f"合并后数据样例:\n{merged.head()}")

        # 5. 计算加权CPI
        merged['weighted'] = merged['price_index'] * merged['weight']
        cpi_result = merged.groupby('time_period').agg(
            total_weight=('weight', 'sum'),
            total_weighted=('weighted', 'sum')
        )
        cpi_result['cpi'] = cpi_result['total_weighted'] / cpi_result['total_weight']
        cpi_result = cpi_result.reset_index()[['time_period', 'cpi']]

        # 动态确定基期（使用数据中的最早时间点）
        if len(cpi_result) == 0:
            raise ValueError("CPI计算结果为空，请检查输入数据")

        base_date = cpi_result['time_period'].min()
        base_cpi = cpi_result.loc[cpi_result['time_period'] == base_date, 'cpi'].values[0]

        # 标准化计算
        cpi_result['cpi_index'] = (cpi_result['cpi'] / base_cpi) * 100
        logger.info(f"使用动态基期: {base_date}")

        # 重命名列以区分粒度
        cpi_result = cpi_result.rename(columns={
            'time_period': f'time_period_{granularity}',
            'cpi_index': f'cpi_index_{granularity}'
        })

        return cpi_result

    except Exception as e:
        logger.error(f"{granularity}粒度CPI计算失败: {str(e)}", exc_info=True)
        raise


def visualize_combined_cpi(monthly_data, daily_data):
    """生成组合可视化图表"""
    try:
        plt.figure(figsize=(14, 7))

        # 绘制月粒度数据
        plt.plot(monthly_data['time_period_month'],
                 monthly_data['cpi_index_month'],
                 marker='o', linestyle='-', color='#1f77b4',
                 label='Monthly CPI')

        # 绘制日粒度数据（更细的线条）
        plt.plot(daily_data['time_period_day'],
                 daily_data['cpi_index_day'],
                 linestyle='-', color='#ff7f0e', alpha=0.5,
                 linewidth=1, label='Daily CPI')

        plt.title('Combined CPI Trend (2025-2028)\nBase: May 2025=100', pad=20)
        plt.xlabel('Date', labelpad=10)
        plt.ylabel('CPI Index', labelpad=10)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()

        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        plot_path = output_dir / 'combined_cpi_trend.png'
        plt.savefig(plot_path, dpi=300)
        plt.close()

        logger.info(f"组合CPI趋势图已保存到 {plot_path}")
        return plot_path
    except Exception as e:
        logger.error("组合图表生成失败", exc_info=True)
        raise


def save_combined_results(monthly_data, daily_data):
    """保存合并结果到CSV"""
    try:
        # 合并两个数据集
        daily_data['time_period_day'] = pd.to_datetime(daily_data['time_period_day'])
        monthly_data['time_period_month'] = pd.to_datetime(monthly_data['time_period_month'])

        # 合并两个数据集
        combined = pd.merge_asof(
            daily_data.sort_values('time_period_day'),
            monthly_data.sort_values('time_period_month'),
            left_on='time_period_day',
            right_on='time_period_month',
            direction='nearest'
        )

        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        csv_path = output_dir / 'combined_cpi_results.csv'

        combined.to_csv(csv_path, index=False)
        logger.info(f"组合CPI结果已保存到 {csv_path}")
        logger.debug(f"数据样例:\n{combined.head()}")
        return csv_path
    except Exception as e:
        logger.error("组合结果保存失败", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        logger.info("====== CPI计算程序开始 ======")

        # 计算两种粒度的CPI
        monthly_data = calculate_cpi(granularity='month')
        daily_data = calculate_cpi(granularity='day')

        # 保存和可视化结果
        save_combined_results(monthly_data, daily_data)
        visualize_combined_cpi(monthly_data, daily_data)

        logger.info("====== 程序执行成功 ======")
    except Exception as e:
        logger.critical("程序执行失败", exc_info=True)
        raise
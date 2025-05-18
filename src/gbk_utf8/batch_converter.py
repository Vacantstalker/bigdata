import os
from chardet import detect
from tqdm import tqdm  # 进度条可视化
from pathlib import Path

def batch_convert(input_dir, output_dir, target_encoding="utf-8", buffer_size=65536):
    """批量转换文件夹内所有CSV文件到目标编码（优化版）"""
    os.makedirs(output_dir, exist_ok=True)
    csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]

    for filename in tqdm(csv_files, desc="转换进度"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)

        # 优化1：仅读取文件前1KB检测编码（大文件时显著提速）
        with open(input_path, "rb") as f:
            raw_data = f.read(1024)
            encoding = detect(raw_data)["encoding"] or "gb2312"

        # 优化2：流式读写（避免内存爆炸）
        with open(input_path, "r", encoding=encoding, errors="ignore") as f_in, \
                open(output_path, "w", encoding=target_encoding) as f_out:
            while True:
                chunk = f_in.read(buffer_size)
                if not chunk:
                    break
                f_out.write(chunk)

        print(f"\n{filename}: {encoding} → {target_encoding}")


# 调用示例
root_dir=Path(__file__).parent.parent.parent
input_dir=root_dir/"raw"/"daily_price"
output_dir=root_dir/"raw"/"utf"
batch_convert(input_dir, output_dir, target_encoding="utf-8")
import os

# 创建项目持久化存储目录
os.makedirs('/workspace/delta', exist_ok=True)

# 创建向量数据库持久化存储目录
os.makedirs('/workspace/qdrant_data', exist_ok=True)
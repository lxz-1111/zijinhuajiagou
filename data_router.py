# data_router.py （云端优化）
def __init__(self):
    self.spark = SparkSession.builder \
        .config("spark.driver.memory", "1024m") \  # 适配2C4G配置
        .config("spark.sql.warehouse.dir", "/workspace/delta") \
        .getOrCreate()

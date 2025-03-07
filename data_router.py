from pyspark.sql import SparkSession
from qdrant_client import QdrantClient, models
import os
import logging

logging.basicConfig(level=logging.INFO)

class DataRouter:
    def __init__(self):
        """初始化内存数据库并创建集合"""
        try:
            # 使用嵌入式Qdrant代替本地服务
            self.client = QdrantClient(":memory:")
            # 定义集合参数（向量维度需与实际数据对齐）
            self._init_collection()
            logging.info("VectorDB初始化完成 (内存模式)")
        except Exception as e:
            logging.error(f"数据库初始化失败: {str(e)}")
            raise

    def _init_collection(self):
        """创建/重置向量集合"""
        self.client.recreate_collection(
            collection_name="windows_data",
            vectors_config=models.VectorParams(
                size=2,  # 向量维度设为2（对应示例数据）
                distance=models.Distance.COSINE
            )
        )

    def _write_vector_db(self, data):
        """
        写入向量数据库核心方法（增强兼容性）
        :param data: 列表格式 [ (id:int, vector:list, path:str), ... ]
        """
        points = []
        for item in data:
            # 自动进行路径规范化处理（适配Windows反斜杠）
            normalized_path = os.path.normpath(item[2](@ref)
            points.append(
                models.PointStruct(
                    id=item[0],
                    vector=item[1],
                    payload={
                        "source": normalized_path,
                        "desc": f"示例数据{item[0]}"
                    }
                )
            )
        
        try:
            # 执行批量插入操作
            self.client.upsert(
                collection_name="windows_data",
                points=points
            )
            logging.info(f"成功写入{len(points)}条向量数据")
        except Exception as e:
            logging.error(f"数据库写入异常: {repr(e)}")
            raise

    def process_spark_job(self, spark):
        """主处理流程（无文件依赖的模拟模式）"""
        # 生成内存测试数据
        sample_data = [
            (1, [0.1, 0.2], "C:\\Users\\Test\\data\\file1.txt"),
            (2, [0.3, 0.4], "D:\\Data\\file2.csv")
        ]
        
        # 调用数据库写入
        self._write_vector_db(sample_data)

if __name__ == "__main__":
    # 配置Spark环境（适配受限环境）
    spark = SparkSession.builder \
        .appName("WindowsDataRouter") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    
    try:
        router = DataRouter()
        router.process_spark_job(spark)
    except Exception as e:
        logging.critical(f"主流程错误: {str(e)}")
        exit(1)
    finally:
        spark.stop()
        logging.info("Spark会话已终止")

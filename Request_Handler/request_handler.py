from sqlalchemy import create_engine, text
from datetime import datetime
from database_config import get_sql_server_connection_string

def query_process_data_by_date_range(query_date):
    print(query_date)
    
    start_date = query_date[0]
    end_date = query_date[1]
    
    # 确保日期格式正确

        # 如果只提供了日期，添加时间部分
    if len(start_date) == 10:  # 只有YYYY-MM-DD
        start_date = f"{start_date} 00:00:00"
    if len(end_date) == 10:  # 只有YYYY-MM-DD
        end_date = f"{end_date} 23:59:59"

    try:
        # 连接到SQL Server数据库
        connection_string = get_sql_server_connection_string()
        engine = create_engine(connection_string)
        
        # 构建SQL查询
        query = text("""
        SELECT * FROM ods_voc_script_Processdata_realtime
        WHERE 记录时间 BETWEEN :start_date AND :end_date
        ORDER BY 记录时间
        """)
        
        # 执行查询
        with engine.connect() as conn:
            result = conn.execute(query, {'start_date': start_date, 'end_date': end_date})
            
            # 获取列名
            column_names = result.keys()
            
            # 获取所有结果并转换为字典列表
            results = []
            for row in result:
                # 将每行数据转换为字典
                record = {}
                for i, column in enumerate(column_names):
                    record[column] = row[i]  # 使用索引访问而不是字符串索引
                results.append(record)
        
        print(f"查询成功：在{start_date}到{end_date}之间找到{len(results)}条记录")
        return results
        
    except Exception as e:
        print(f"查询数据时发生错误：{e}")
        return None

# 使用示例
if __name__ == "__main__":
    # 示例1：使用完整的日期时间
    date_range1 = ['2023-10-01 08:00:00', '2023-10-02 17:30:00']
    result1 = query_process_data_by_date_range(date_range1)
    if result1 is not None and len(result1) > 0:
        print("第一条记录示例:")
        print(result1[0])
    
    # 示例2：只使用日期（将自动添加时间部分）
    date_range2 = ['2025-05-01', '2025-05-16']
    result2 = query_process_data_by_date_range(date_range2)
    if result2 is not None and len(result2) > 0:
        print("第一条记录示例:")
        print(result2[0])

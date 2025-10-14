from snap7.util import *
import snap7
import datetime
from sqlalchemy import create_engine, text


def get_process_data(plc):
        try:
            # 读取PLC数据，从DB块编号为2，起始地址为326的位置读取88个字节的数据         
            ByteArray_process=plc.read_area(snap7.types.Areas.MK, 0, 4,1)
            data=get_byte(ByteArray_process, 0)
            print("数据已成功保存到SQL Server数据库",data)
        except Exception as e:
            print(f"读取PLC数据时发生错误：{e}")
        finally:
            print("断开连接")
            plc.disconnect()

# 调用函数开始获取数据
#get_process_data()

# 测试程序
if __name__ == "__main__":
        # 创建客户端实例
    plc = snap7.client.Client()
    # 连接到PLC
    plc.connect('10.15.161.230', 0, 1)  # 替换为你的PLC IP地址、机架号和插槽号
    # 设置发送和接收超时时间（单位：毫秒）
    plc.send_timeout = 2000
    plc.recv_timeout = 2000
    # 检验连接
    if  plc.get_connected():
        print("连接到PLC成功！！！！")
    # 调用程序获取数据
    get_process_data(plc=plc)
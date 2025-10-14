from snap7.util import *
import snap7
def get_log_data(plc):
        try:
            # 读取PLC数据，例如从DB块编号为311，起始地址为0的位置读取2个字节的数据
            ByteArray_log = plc.db_read(311, 0, 2)
            log1=get_bool(ByteArray_log,0,0)
            log2=get_bool(ByteArray_log,0,1)
            log3=get_bool(ByteArray_log,0,2)
            log4=get_bool(ByteArray_log,0,3)
            log5=get_bool(ByteArray_log,0,4)
            log6=get_bool(ByteArray_log,0,5)
            log7=get_bool(ByteArray_log,0,6)
            log8=get_bool(ByteArray_log,0,7)
            log9=get_bool(ByteArray_log,1,0)
            log10=get_bool(ByteArray_log,1,1)
            log11=get_bool(ByteArray_log,1,2)
            log12=get_bool(ByteArray_log,1,3)
            log13=get_bool(ByteArray_log,1,4)
            log14=get_bool(ByteArray_log,1,5)
            log15=get_bool(ByteArray_log,1,6)
            log16=get_bool(ByteArray_log,1,7)
            #...the rest of the logs
            print(f"从PLC读取的数据：{log1,log2,log3,log4,log5,log6,log7,log8,log9,log10,log11,log12,log13,log14,log15,log16}")
        except Exception as e:
            print(f"读取PLC数据时发生错误：{e}")
        finally:
            # 断开连接
            plc.disconnect()

# 调用函数开始获取数据
#get_log_data()

# 测试程序
if __name__ == "__main__":
    # 创建客户端实例
    plc = snap7.client.Client()
    # 连接到PLC
    plc.connect('10.15.161.18', 0, 1)  # 替换为你的PLC IP地址、机架号和插槽号
    # 设置发送和接收超时时间（单位：毫秒）
    plc.send_timeout = 2000
    plc.recv_timeout = 2000
    # 检验连接
    if  plc.get_connected():
        print("连接到PLC成功！！！！")
    # 调用程序获取数据
    get_log_data(plc=plc)
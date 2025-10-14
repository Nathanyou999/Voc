from snap7.util import *
import snap7
def get_alarm_data(plc):
        try:
            # 读取PLC数据，例如从DB块编号为5，起始地址为0的位置读取5个字节的数据
            ByteArray_alarm = plc.db_read(5, 0, 5)
            Emergency_alarm=get_bool(ByteArray_alarm,0,0)
            Compressed_Air_alarm=get_bool(ByteArray_alarm,0,1)
            Controlling_Powersupplier_alarm=get_bool(ByteArray_alarm,0,2)
            RTO_Overload_Colling_Motor_alarm=get_bool(ByteArray_alarm,0,3)
            RTO_Motor_frequency_inverter_alarm=get_bool(ByteArray_alarm,0,4)
            RTO_Motor_starter_alarm=get_bool(ByteArray_alarm,0,5)
            RTO_Overload_Noise_Reduction_Fan_alarm=get_bool(ByteArray_alarm,0,6)
            Emergency_alarm=get_bool(ByteArray_alarm,0,7)
            #...the rest of the alarms
            print(f"从PLC读取的数据：{Emergency_alarm,Compressed_Air_alarm,Controlling_Powersupplier_alarm,RTO_Overload_Colling_Motor_alarm,RTO_Motor_frequency_inverter_alarm,RTO_Motor_starter_alarm,RTO_Overload_Noise_Reduction_Fan_alarm,Emergency_alarm}")
        except Exception as e:
            print(f"读取PLC数据时发生错误：{e}")
        finally:
            # 断开连接
            plc.disconnect()

# 调用函数开始获取数据
#get_alarm_data()

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
    get_alarm_data(plc=plc)
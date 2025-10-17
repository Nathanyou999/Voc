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


       # ===================== Alarm/Alert：直接采集（无开关、一次性读块） =====================
        # —— 报警位（按 PDF 列表顺序自行扩充；这里给常见示例位）——
        # buf_db5 = plc.db_read(5, 0, 6)   # DB5.DBX0.0~5.7（如你的PDF长度不同请改）
        # buf_db6 = plc.db_read(6, 0, 5)   # DB6.DBX0.0~4.7
        #
        # ALARM_RTO_burner_fault         = int(get_bool(buf_db5, 0, 0))  # [ADDED-ALARM] RTO燃烧机故障 | DB5.DBX0.0
        # ALARM_RTO_fan_fault            = int(get_bool(buf_db5, 0, 1))  # [ADDED-ALARM] RTO风机故障   | DB5.DBX0.1
        # ALARM_assist_fan_fault         = int(get_bool(buf_db5, 0, 2))  # [ADDED-ALARM] 助燃风机故障 | DB5.DBX0.2
        # ALARM_backflush_fan_fault      = int(get_bool(buf_db5, 0, 3))  # [ADDED-ALARM] 反吹风机故障 | DB5.DBX0.3
        # ALARM_rotor1_fan_fault         = int(get_bool(buf_db5, 0, 4))  # [ADDED-ALARM] 1#转轮风机故障 | DB5.DBX0.4
        # ALARM_rotor1_drive_fault       = int(get_bool(buf_db5, 0, 5))  # [ADDED-ALARM] 1#转轮驱动故障 | DB5.DBX0.5
        # ALARM_rotor1_desorp_fan_fault  = int(get_bool(buf_db5, 0, 6))  # [ADDED-ALARM] 1#转轮脱附风机故障 | DB5.DBX0.6
        # ALARM_LEL_high                 = int(get_bool(buf_db6, 1, 5))  # [ADDED-ALARM] LEL高报警       | DB6.DBX1.5

        # —— 告警/设定（数值类；示例地址，按你的 PDF 如实修改/补充）——
        # 若你的 Alert 数值在 DB2/DB3，可直接在上面的 ByteArray 上按偏移取值；这里演示从 DB4 连续读取。
        try:
            buf_db4 = plc.db_read(4, 200, 32)  # 示例：DB4 起始200，长度32（按 PDF 修改）
            ALERT_LEL_threshold            = round(get_real(buf_db4, 0), 3)   # [ADDED-ALERT] LEL报警阈值     | DB4.DBD200
            ALERT_RTO_furnace_high_set    = round(get_real(buf_db4, 4), 3)   # [ADDED-ALERT] RTO炉膛高温设定 | DB4.DBD204
            ALERT_alarm_code              = int(get_int(buf_db4, 8))         # [ADDED-ALERT] 报警代码/计数   | DB4.DBW208
        except Exception:
            ALERT_LEL_threshold = None
            ALERT_RTO_furnace_high_set = None
            ALERT_alarm_code = None
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
from snap7.util import *
import snap7
import datetime
from sqlalchemy import create_engine, text
from database_config import get_sql_server_connection_string

def get_process_data(plc):
        try:
            # 读取PLC数据，从DB块编号为2，起始地址为326的位置读取88个字节的数据
            ByteArray_process = plc.db_read(2, 326, 88)
            PT3010_RTO_inlet_pressure=round(get_real(ByteArray_process,8),3)
            PT3010_RTO_furnace_pressure=round(get_real(ByteArray_process,12),3)
            TE3018_furnace_tempA = round(get_real(ByteArray_process,40),3)
            TE3019_furnace_tempB = round(get_real(ByteArray_process,44),3)
            TE3020_furnace_tempC = round(get_real(ByteArray_process,48),3)
            TE3021_furnace_tempD = round(get_real(ByteArray_process,52),3)
            TE3010_RTO_inlet_temp = round(get_real(ByteArray_process,80),3)
            TE3011_RTO_exhaust_temp = round(get_real(ByteArray_process,84),3)

            ByteArray_process_2 = plc.db_read(3, 1610, 164)
            PT2010_1_rotor_exhaust_inlet_pressure = round(get_real(ByteArray_process_2,0),3)
            PT2011_1_rotor_desorption_fan_inlet_pressure = round(get_real(ByteArray_process_2,4),3)         
            TRH1011_1_rotor_inlet_temp = round(get_real(ByteArray_process_2,8),3)
            TRH1012_1_rotor_inlet_humidity = round(get_real(ByteArray_process_2,12),3)
            TE1010_1_rotor_filterbox_inlet_temp = round(get_real(ByteArray_process_2,136),3)    
            TE2010_1_rotor_cooling_outlet_temp = round(get_real(ByteArray_process_2,140),3)
            TE2011_1_rotor_desorption_inlet_temp = round(get_real(ByteArray_process_2,144),3)    
            TE2012_1_rotor_desorption_outlet_temp = round(get_real(ByteArray_process_2,148),3)
            TE2014_1_heatexchanger_hotside_outlet_temp = round(get_real(ByteArray_process_2,156),3)
            TE2016_1_heatexchanger_coldside_outlet_desorption_temp = round(get_real(ByteArray_process_2,160),3)
            #...the rest of the alarms
            print(f"从PLC读取的数据：{PT3010_RTO_inlet_pressure}, {PT3010_RTO_furnace_pressure}, {TE3018_furnace_tempA}, "
                  f"{TE3019_furnace_tempB}, {TE3020_furnace_tempC}, {TE3021_furnace_tempD}, {TE3010_RTO_inlet_temp}, {TE3011_RTO_exhaust_temp}, "
                  f"{PT2010_1_rotor_exhaust_inlet_pressure}, {PT2011_1_rotor_desorption_fan_inlet_pressure}, {TRH1011_1_rotor_inlet_temp}, "
                  f"{TRH1012_1_rotor_inlet_humidity}, {TE1010_1_rotor_filterbox_inlet_temp}, {TE2010_1_rotor_cooling_outlet_temp}, "
                  f"{TE2011_1_rotor_desorption_inlet_temp}, {TE2012_1_rotor_desorption_outlet_temp}, {TE2014_1_heatexchanger_hotside_outlet_temp}, "
                  f"{TE2016_1_heatexchanger_coldside_outlet_desorption_temp}")
            
            # 连接到SQL Server数据库
            connection_string = get_sql_server_connection_string()
            engine = create_engine(connection_string)
            
            # 获取当前时间
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 插入数据
            insert_query = text('''
            INSERT INTO ods_voc_script_Processdata_realtime (
                记录时间,
                PT3010_RTO进口压力,
                PT3011_RTO炉膛压力,
                TE3018_炉膛温度A,
                TE3019_炉膛温度B,
                TE3020_炉膛温度C,
                TE3021_炉膛温度D,
                TE3010_RTO进口温度,
                TE3011_RTO排烟温度,
                PT2010_1_转轮废气进口压力,
                PT2011_1_转轮脱附风机进口压力,
                TRH1011_1_转轮进口温度检测,
                TRH1011_1_转轮进口湿度检测,
                TE1010_1_转轮过滤箱进口温度,
                TE2010_1_转轮冷却出口温度,
                TE2011_1_转轮脱附进口温度,
                TE2012_1_转轮脱附出口温度,
                TE2014_1_换热器热侧出口温度,
                TE2016_1_换热器冷侧出口脱附温度
            ) VALUES (:current_time, :PT3010_RTO_inlet_pressure, :PT3010_RTO_furnace_pressure, :TE3018_furnace_tempA, :TE3019_furnace_tempB, :TE3020_furnace_tempC, :TE3021_furnace_tempD, :TE3010_RTO_inlet_temp, :TE3011_RTO_exhaust_temp, :PT2010_1_rotor_exhaust_inlet_pressure, :PT2011_1_rotor_desorption_fan_inlet_pressure, :TRH1011_1_rotor_inlet_temp, :TRH1012_1_rotor_inlet_humidity, :TE1010_1_rotor_filterbox_inlet_temp, :TE2010_1_rotor_cooling_outlet_temp, :TE2011_1_rotor_desorption_inlet_temp, :TE2012_1_rotor_desorption_outlet_temp, :TE2014_1_heatexchanger_hotside_outlet_temp, :TE2016_1_heatexchanger_coldside_outlet_desorption_temp)
            ''')
            
            with engine.connect() as conn:
                conn.execute(insert_query, {
                    'current_time': current_time,
                    'PT3010_RTO_inlet_pressure': PT3010_RTO_inlet_pressure,
                    'PT3010_RTO_furnace_pressure': PT3010_RTO_furnace_pressure,
                    'TE3018_furnace_tempA': TE3018_furnace_tempA,
                    'TE3019_furnace_tempB': TE3019_furnace_tempB,
                    'TE3020_furnace_tempC': TE3020_furnace_tempC,
                    'TE3021_furnace_tempD': TE3021_furnace_tempD,
                    'TE3010_RTO_inlet_temp': TE3010_RTO_inlet_temp,
                    'TE3011_RTO_exhaust_temp': TE3011_RTO_exhaust_temp,
                    'PT2010_1_rotor_exhaust_inlet_pressure': PT2010_1_rotor_exhaust_inlet_pressure,
                    'PT2011_1_rotor_desorption_fan_inlet_pressure': PT2011_1_rotor_desorption_fan_inlet_pressure,
                    'TRH1011_1_rotor_inlet_temp': TRH1011_1_rotor_inlet_temp,
                    'TRH1012_1_rotor_inlet_humidity': TRH1012_1_rotor_inlet_humidity,
                    'TE1010_1_rotor_filterbox_inlet_temp': TE1010_1_rotor_filterbox_inlet_temp,
                    'TE2010_1_rotor_cooling_outlet_temp': TE2010_1_rotor_cooling_outlet_temp,
                    'TE2011_1_rotor_desorption_inlet_temp': TE2011_1_rotor_desorption_inlet_temp,
                    'TE2012_1_rotor_desorption_outlet_temp': TE2012_1_rotor_desorption_outlet_temp,
                    'TE2014_1_heatexchanger_hotside_outlet_temp': TE2014_1_heatexchanger_hotside_outlet_temp,
                    'TE2016_1_heatexchanger_coldside_outlet_desorption_temp': TE2016_1_heatexchanger_coldside_outlet_desorption_temp
                })
                conn.commit()
            
            print("数据已成功保存到SQL Server数据库")
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
    plc.connect('10.15.161.18', 0, 1)  # 替换为你的PLC IP地址、机架号和插槽号
    # 设置发送和接收超时时间（单位：毫秒）
    plc.send_timeout = 2000
    plc.recv_timeout = 2000
    # 检验连接
    if  plc.get_connected():
        print("连接到PLC成功！！！！")
    # 调用程序获取数据
    get_process_data(plc=plc)
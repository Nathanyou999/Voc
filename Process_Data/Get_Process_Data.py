from snap7.util import *
# -*- coding: utf-8 -*-

# ===== Imports（保留原有；仅补充 get_bool/get_int）=====
from snap7.util import get_real
from snap7.util import get_bool, get_int  # [ADDED] 报警位/整型数值
import snap7
import datetime
from sqlalchemy import create_engine, text
from database_config import get_sql_server_connection_string


def get_process_data(plc):
    try:
        # ===================== DB2：一次性读（保持你原来的方式） =====================
        ByteArray_process = plc.db_read(2, 326, 88)  # DB2 start=326, size=88

        # ---- DB2（按 PDF 地址升序）----
        # 说明：英文变量名 + 注释（中文名 | 绝对地址 | buf偏移）
        RTO_gas_flow                 = round(get_real(ByteArray_process, 0),  3)   # [ADDED] RTO燃烧机天然气流量计 | DB2.DBD326 | buf+0
        RTO_inlet_LEL                = round(get_real(ByteArray_process, 4),  3)   # [ADDED] RTO进口LEL浓度检测   | DB2.DBD330 | buf+4
        PT3010_RTO_inlet_pressure    = round(get_real(ByteArray_process, 8),  3)   #        PT3010 RTO进口压力     | DB2.DBD334 | buf+8
        PT3011_RTO_furnace_pressure  = round(get_real(ByteArray_process, 12), 3)   #        PT3011 RTO炉膛压力     | DB2.DBD338 | buf+12

        RTO_fan_freq_feedback        = round(get_real(ByteArray_process, 16), 3)   # [ADDED] RTO风机频率反馈       | DB2.DBD342 | buf+16
        RTO_fan_freq_setpoint        = round(get_real(ByteArray_process, 20), 3)   # [ADDED] RTO风机频率控制       | DB2.DBD346 | buf+20
        backflush_fan_freq_feedback  = round(get_real(ByteArray_process, 24), 3)   # [ADDED] 反吹风机频率反馈       | DB2.DBD350 | buf+24
        backflush_fan_freq_setpoint  = round(get_real(ByteArray_process, 28), 3)   # [ADDED] 反吹风机频率控制       | DB2.DBD354 | buf+28
        assist_fan_freq_feedback     = round(get_real(ByteArray_process, 32), 3)   # [ADDED] 助燃风机频率反馈       | DB2.DBD358 | buf+32
        assist_fan_freq_setpoint     = round(get_real(ByteArray_process, 36), 3)   # [ADDED] 助燃风机频率控制       | DB2.DBD362 | buf+36

        TE3018_furnace_temp_A        = round(get_real(ByteArray_process, 40), 3)   #        TE3018 炉膛温度A        | DB2.DBD366 | buf+40
        TE3019_furnace_temp_B        = round(get_real(ByteArray_process, 44), 3)   #        TE3019 炉膛温度B        | DB2.DBD370 | buf+44
        TE3020_furnace_temp_C        = round(get_real(ByteArray_process, 48), 3)   #        TE3020 炉膛温度C        | DB2.DBD374 | buf+48
        TE3021_furnace_temp_D        = round(get_real(ByteArray_process, 52), 3)   #        TE3021 炉膛温度D        | DB2.DBD378 | buf+52

        # （PDF顺序里 TE3010/TE3011 在末尾地址区）
        TE3010_RTO_inlet_temp        = round(get_real(ByteArray_process, 80), 3)   #        TE3010 RTO进口温度      | DB2.DBD406 | buf+80
        TE3011_RTO_exhaust_temp      = round(get_real(ByteArray_process, 84), 3)   #        TE3011 RTO排烟温度      | DB2.DBD410 | buf+84

        # ===================== DB3：一次性读（保持你原来的方式） =====================
        ByteArray_process_2 = plc.db_read(3, 1610, 164)  # DB3 start=1610, size=164

        # ---- DB3（按 PDF 地址升序）----
        PT2010_1_rotor_exhaust_inlet_pressure    = round(get_real(ByteArray_process_2, 0),   3)  #        PT2010_1 转轮废气进口压力           | DB3.DBD1610 | buf+0
        PT2011_1_rotor_desorp_fan_inlet_pressure = round(get_real(ByteArray_process_2, 4),   3)  #        PT2011_1 转轮脱附风机进口压力       | DB3.DBD1614 | buf+4
        TRH1011_1_rotor_inlet_temp               = round(get_real(ByteArray_process_2, 8),   3)  #        TRH1011_1 转轮进口温度              | DB3.DBD1618 | buf+8
        TRH1011_1_rotor_inlet_humidity           = round(get_real(ByteArray_process_2, 12),  3)  #        TRH1011_1 转轮进口湿度              | DB3.DBD1622 | buf+12

        FT2010_1_rotor_inlet_flow                = round(get_real(ByteArray_process_2, 16),  3)  # [ADDED] FT2010_1 转轮进口流量计          | DB3.DBD1626 | buf+16
        FT2011_1_rotor_desorp_outlet_flow        = round(get_real(ByteArray_process_2, 20),  3)  # [ADDED] FT2011_1 转轮脱附出口流量计      | DB3.DBD1630 | buf+20
        FT2012_1_rotor_desorp_burner_gas_flow_fb = round(get_real(ByteArray_process_2, 24),  3)  # [ADDED] FT2012_1 脱附燃烧机燃气流量反馈  | DB3.DBD1634 | buf+24

        rotor1_fan_freq_feedback                 = round(get_real(ByteArray_process_2, 28),  3)  # [ADDED] 1#转轮风机频率反馈              | DB3.DBD1638 | buf+28
        rotor1_fan_freq_setpoint                 = round(get_real(ByteArray_process_2, 32),  3)  # [ADDED] 1#转轮风机频率控制              | DB3.DBD1642 | buf+32
        rotor1_drive_freq_feedback               = round(get_real(ByteArray_process_2, 36),  3)  # [ADDED] 1#转轮驱动频率反馈              | DB3.DBD1646 | buf+36
        rotor1_drive_freq_setpoint               = round(get_real(ByteArray_process_2, 40),  3)  # [ADDED] 1#转轮驱动频率控制              | DB3.DBD1650 | buf+40
        rotor1_desorp_freq_feedback              = round(get_real(ByteArray_process_2, 44),  3)  # [ADDED] 1#转轮脱附频率反馈              | DB3.DBD1654 | buf+44
        rotor1_desorp_freq_setpoint              = round(get_real(ByteArray_process_2, 48),  3)  # [ADDED] 1#转轮脱附频率控制              | DB3.DBD1658 | buf+48

        TE1010_1_rotor_filterbox_inlet_temp      = round(get_real(ByteArray_process_2, 136), 3)  #        TE1010_1 转轮过滤箱进口温度        | DB3.DBD1746 | buf+136
        TE2010_1_rotor_cooling_outlet_temp       = round(get_real(ByteArray_process_2, 140), 3)  #        TE2010_1 转轮冷却出口温度          | DB3.DBD1750 | buf+140
        TE2011_1_rotor_desorp_inlet_temp         = round(get_real(ByteArray_process_2, 144), 3)  #        TE2011_1 转轮脱附进口温度          | DB3.DBD1754 | buf+144
        TE2012_1_rotor_desorp_outlet_temp        = round(get_real(ByteArray_process_2, 148), 3)  #        TE2012_1 转轮脱附出口温度          | DB3.DBD1758 | buf+148
        TE2013_1_rotor_adsorption_outlet_temp    = round(get_real(ByteArray_process_2, 152), 3)  # [ADDED] TE2013_1 转轮吸附出口温度      | DB3.DBD1762 | buf+152
        TE2014_1_hex_hot_outlet_temp             = round(get_real(ByteArray_process_2, 156), 3)  #        TE2014_1 换热器热侧出口温度        | DB3.DBD1766 | buf+156
        TE2016_1_hex_cold_outlet_desorp_temp     = round(get_real(ByteArray_process_2, 160), 3)  #        TE2016_1 换热器冷侧出口脱附温度    | DB3.DBD1770 | buf+160


        # ===================== 写库（一次 INSERT；列顺序按 PDF/上面顺序） =====================
        connection_string = get_sql_server_connection_string()
        engine = create_engine(connection_string)
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        insert_query = text("""
        INSERT INTO [ods_voc_script_Processdata_realtime_1m] (
            [记录时间],

            -- === DB2（按PDF顺序/地址升序）===
            [RTO燃烧机天然气流量计],             -- [ADDED]
            [RTO进口LEL浓度检测],                 -- [ADDED]
            [PT3010_RTO进口压力],
            [PT3011_RTO炉膛压力],
            [RTO风机频率反馈],                    -- [ADDED]
            [RTO风机频率控制],                    -- [ADDED]
            [反吹风机频率反馈],                    -- [ADDED]
            [反吹风机频率控制],                    -- [ADDED]
            [助燃风机频率反馈],                    -- [ADDED]
            [助燃风机频率控制],                    -- [ADDED]
            [TE3018_炉膛温度A],
            [TE3019_炉膛温度B],
            [TE3020_炉膛温度C],
            [TE3021_炉膛温度D],
            [TE3010_RTO进口温度],
            [TE3011_RTO排烟温度],

            -- === DB3（按PDF顺序/地址升序）===
            [PT2010_1转轮废气进口压力],
            [PT2011_1转轮脱附风机进口压力],
            [TRH1011_1转轮进口温湿度计温度检测],
            [TRH1011_1转轮进口温湿度计湿度检测],
            [FT2010_1转轮进口流量计],            -- [ADDED]
            [FT2011_1转轮脱附出口流量计],        -- [ADDED]
            [FT2012_1转轮脱附燃烧机燃气流量反馈],-- [ADDED]
            [1#转轮风机频率反馈],                 -- [ADDED]
            [1#转轮风机频率控制],                 -- [ADDED]
            [1#转轮驱动频率反馈],                 -- [ADDED]
            [1#转轮驱动频率控制],                 -- [ADDED]
            [1#转轮脱附频率反馈],                 -- [ADDED]
            [1#转轮脱附频率控制],                 -- [ADDED]
            [TE1010_1转轮过滤箱进口温度],
            [TE2010_1转轮冷却出口温度],
            [TE2011_1转轮脱附进口温度],
            [TE2012_1转轮脱附出口温度],
            [TE2013_1转轮吸附出口温度],          -- [ADDED]
            [TE2014_1换热器热侧出口温度],
            [TE2016_1换热器冷侧出口脱附温度]

        ) VALUES (
            :current_time,

            -- DB2
            :RTO_gas_flow,
            :RTO_inlet_LEL,
            :PT3010_RTO_inlet_pressure,
            :PT3011_RTO_furnace_pressure,
            :RTO_fan_freq_feedback,
            :RTO_fan_freq_setpoint,
            :backflush_fan_freq_feedback,
            :backflush_fan_freq_setpoint,
            :assist_fan_freq_feedback,
            :assist_fan_freq_setpoint,
            :TE3018_furnace_temp_A,
            :TE3019_furnace_temp_B,
            :TE3020_furnace_temp_C,
            :TE3021_furnace_temp_D,
            :TE3010_RTO_inlet_temp,
            :TE3011_RTO_exhaust_temp,

            -- DB3
            :PT2010_1_rotor_exhaust_inlet_pressure,
            :PT2011_1_rotor_desorp_fan_inlet_pressure,
            :TRH1011_1_rotor_inlet_temp,
            :TRH1011_1_rotor_inlet_humidity,
            :FT2010_1_rotor_inlet_flow,
            :FT2011_1_rotor_desorp_outlet_flow,
            :FT2012_1_rotor_desorp_burner_gas_flow_fb,
            :rotor1_fan_freq_feedback,
            :rotor1_fan_freq_setpoint,
            :rotor1_drive_freq_feedback,
            :rotor1_drive_freq_setpoint,
            :rotor1_desorp_freq_feedback,
            :rotor1_desorp_freq_setpoint,
            :TE1010_1_rotor_filterbox_inlet_temp,
            :TE2010_1_rotor_cooling_outlet_temp,
            :TE2011_1_rotor_desorp_inlet_temp,
            :TE2012_1_rotor_desorp_outlet_temp,
            :TE2013_1_rotor_adsorption_outlet_temp,
            :TE2014_1_hex_hot_outlet_temp,
            :TE2016_1_hex_cold_outlet_desorp_temp

        )
        """)

        params = {
            'current_time': current_time,

            # DB2
            'RTO_gas_flow': RTO_gas_flow,
            'RTO_inlet_LEL': RTO_inlet_LEL,
            'PT3010_RTO_inlet_pressure': PT3010_RTO_inlet_pressure,
            'PT3011_RTO_furnace_pressure': PT3011_RTO_furnace_pressure,
            'RTO_fan_freq_feedback': RTO_fan_freq_feedback,
            'RTO_fan_freq_setpoint': RTO_fan_freq_setpoint,
            'backflush_fan_freq_feedback': backflush_fan_freq_feedback,
            'backflush_fan_freq_setpoint': backflush_fan_freq_setpoint,
            'assist_fan_freq_feedback': assist_fan_freq_feedback,
            'assist_fan_freq_setpoint': assist_fan_freq_setpoint,
            'TE3018_furnace_temp_A': TE3018_furnace_temp_A,
            'TE3019_furnace_temp_B': TE3019_furnace_temp_B,
            'TE3020_furnace_temp_C': TE3020_furnace_temp_C,
            'TE3021_furnace_temp_D': TE3021_furnace_temp_D,
            'TE3010_RTO_inlet_temp': TE3010_RTO_inlet_temp,
            'TE3011_RTO_exhaust_temp': TE3011_RTO_exhaust_temp,

            # DB3
            'PT2010_1_rotor_exhaust_inlet_pressure': PT2010_1_rotor_exhaust_inlet_pressure,
            'PT2011_1_rotor_desorp_fan_inlet_pressure': PT2011_1_rotor_desorp_fan_inlet_pressure,
            'TRH1011_1_rotor_inlet_temp': TRH1011_1_rotor_inlet_temp,
            'TRH1011_1_rotor_inlet_humidity': TRH1011_1_rotor_inlet_humidity,
            'FT2010_1_rotor_inlet_flow': FT2010_1_rotor_inlet_flow,
            'FT2011_1_rotor_desorp_outlet_flow': FT2011_1_rotor_desorp_outlet_flow,
            'FT2012_1_rotor_desorp_burner_gas_flow_fb': FT2012_1_rotor_desorp_burner_gas_flow_fb,
            'rotor1_fan_freq_feedback': rotor1_fan_freq_feedback,
            'rotor1_fan_freq_setpoint': rotor1_fan_freq_setpoint,
            'rotor1_drive_freq_feedback': rotor1_drive_freq_feedback,
            'rotor1_drive_freq_setpoint': rotor1_drive_freq_setpoint,
            'rotor1_desorp_freq_feedback': rotor1_desorp_freq_feedback,
            'rotor1_desorp_freq_setpoint': rotor1_desorp_freq_setpoint,
            'TE1010_1_rotor_filterbox_inlet_temp': TE1010_1_rotor_filterbox_inlet_temp,
            'TE2010_1_rotor_cooling_outlet_temp': TE2010_1_rotor_cooling_outlet_temp,
            'TE2011_1_rotor_desorp_inlet_temp': TE2011_1_rotor_desorp_inlet_temp,
            'TE2012_1_rotor_desorp_outlet_temp': TE2012_1_rotor_desorp_outlet_temp,
            'TE2013_1_rotor_adsorption_outlet_temp': TE2013_1_rotor_adsorption_outlet_temp,
            'TE2014_1_hex_hot_outlet_temp': TE2014_1_hex_hot_outlet_temp,
            'TE2016_1_hex_cold_outlet_desorp_temp': TE2016_1_hex_cold_outlet_desorp_temp


        }

        with engine.connect() as conn:
            conn.execute(insert_query, params)
            conn.commit()

        print("数据已成功保存到SQL Server数据库")
    except Exception as e:
        print(f"读取PLC数据时发生错误：{e}")
    finally:
        print("断开连接")
        plc.disconnect()


# ===== 主入口（保留你原来的写法）=====
if __name__ == "__main__":
    plc = snap7.client.Client()
    plc.connect('10.15.161.18', 0, 1)  # 替换为你的PLC IP、机架、插槽
    plc.send_timeout = 2000
    plc.recv_timeout = 2000
    if plc.get_connected():
        print("连接到PLC成功！！！！")
    get_process_data(plc=plc)

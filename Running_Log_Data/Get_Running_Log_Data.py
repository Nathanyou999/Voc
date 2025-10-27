# -*- coding: utf-8 -*-
from snap7.util import get_bool
import snap7
import datetime
from sqlalchemy import create_engine, text
from database_config import get_sql_server_connection_string


# ============ 1) 读取：DB311 两字节（16 位） ============
def read_log_values(plc):
    """
    读取 DB311 的运行日志位，返回 dict：
    { 'LOG01_RTO_System_Run': 0/1, ... }
    """
    buf = plc.db_read(311, 0, 2)  # %DB311.DBX0.0 ~ %DB311.DBX1.7

    # 英文变量名；注释=中文列名 + 位地址
    LOG01_RTO_System_Run = int(get_bool(buf, 0, 0))  # RTO系统运行            | %DB311.DBX0.0
    LOG02_RTO_System_Stop = int(get_bool(buf, 0, 1))  # RTO系统停机            | %DB311.DBX0.1
    LOG03_Rotor1_System_Run = int(get_bool(buf, 0, 2))  # 1#转轮系统运行         | %DB311.DBX0.2
    LOG04_Rotor1_System_Stop = int(get_bool(buf, 0, 3))  # 1#转轮系统停机         | %DB311.DBX0.3
    LOG05_Rotor2_System_Run = int(get_bool(buf, 0, 4))  # 2#转轮系统运行         | %DB311.DBX0.4
    LOG06_Rotor2_System_Stop = int(get_bool(buf, 0, 5))  # 2#转轮系统停机         | %DB311.DBX0.5
    LOG07_RTO_Fan_Run = int(get_bool(buf, 0, 6))  # RTO风机运行            | %DB311.DBX0.6
    LOG08_Assist_Fan_Run = int(get_bool(buf, 0, 7))  # 助燃风机运行           | %DB311.DBX0.7
    LOG09_Backflush_Fan_Run = int(get_bool(buf, 1, 0))  # 反吹风机运行           | %DB311.DBX1.0
    LOG10_RTO_Burner_Run = int(get_bool(buf, 1, 1))  # RTO燃烧机运行          | %DB311.DBX1.1
    LOG11_Rotor1_Fan_Run = int(get_bool(buf, 1, 2))  # 1#转轮风机运行         | %DB311.DBX1.2
    LOG12_Rotor1_Drive_Run = int(get_bool(buf, 1, 3))  # 1#转轮驱动运行         | %DB311.DBX1.3
    LOG13_Rotor1_Desorp_Fan_Run = int(get_bool(buf, 1, 4))  # 1#转轮脱附风机运行     | %DB311.DBX1.4
    LOG14_Rotor2_Fan_Run = int(get_bool(buf, 1, 5))  # 2#转轮风机运行         | %DB311.DBX1.5
    LOG15_Rotor2_Drive_Run = int(get_bool(buf, 1, 6))  # 2#转轮驱动运行         | %DB311.DBX1.6
    LOG16_Rotor2_Desorp_Fan_Run = int(get_bool(buf, 1, 7))  # 2#转轮脱附风机运行     | %DB311.DBX1.7

    return {
        "LOG01_RTO_System_Run": LOG01_RTO_System_Run,
        "LOG02_RTO_System_Stop": LOG02_RTO_System_Stop,
        "LOG03_Rotor1_System_Run": LOG03_Rotor1_System_Run,
        "LOG04_Rotor1_System_Stop": LOG04_Rotor1_System_Stop,
        "LOG05_Rotor2_System_Run": LOG05_Rotor2_System_Run,
        "LOG06_Rotor2_System_Stop": LOG06_Rotor2_System_Stop,
        "LOG07_RTO_Fan_Run": LOG07_RTO_Fan_Run,
        "LOG08_Assist_Fan_Run": LOG08_Assist_Fan_Run,
        "LOG09_Backflush_Fan_Run": LOG09_Backflush_Fan_Run,
        "LOG10_RTO_Burner_Run": LOG10_RTO_Burner_Run,
        "LOG11_Rotor1_Fan_Run": LOG11_Rotor1_Fan_Run,
        "LOG12_Rotor1_Drive_Run": LOG12_Rotor1_Drive_Run,
        "LOG13_Rotor1_Desorp_Fan_Run": LOG13_Rotor1_Desorp_Fan_Run,
        "LOG14_Rotor2_Fan_Run": LOG14_Rotor2_Fan_Run,
        "LOG15_Rotor2_Drive_Run": LOG15_Rotor2_Drive_Run,
        "LOG16_Rotor2_Desorp_Fan_Run": LOG16_Rotor2_Desorp_Fan_Run,
    }


# ============ 2) INSERT 语句（与 alarm 的拼法一致） ============
def build_insert_log_query():
    return text("""
     INSERT INTO [ods_voc_script_Logdata_realtime] (
         [记录时间],
         [RTO系统运行], [RTO系统停机],
         [1#转轮系统运行], [1#转轮系统停机],
         [2#转轮系统运行], [2#转轮系统停机],
         [RTO风机运行], [助燃风机运行], [反吹风机运行], [RTO燃烧机运行],
         [1#转轮风机运行], [1#转轮驱动运行], [1#转轮脱附风机运行],
         [2#转轮风机运行], [2#转轮驱动运行], [2#转轮脱附风机运行]
     ) VALUES (
         :current_time,
         :LOG01_RTO_System_Run, :LOG02_RTO_System_Stop,
         :LOG03_Rotor1_System_Run, :LOG04_Rotor1_System_Stop,
         :LOG05_Rotor2_System_Run, :LOG06_Rotor2_System_Stop,
         :LOG07_RTO_Fan_Run, :LOG08_Assist_Fan_Run, :LOG09_Backflush_Fan_Run, :LOG10_RTO_Burner_Run,
         :LOG11_Rotor1_Fan_Run, :LOG12_Rotor1_Drive_Run, :LOG13_Rotor1_Desorp_Fan_Run,
         :LOG14_Rotor2_Fan_Run, :LOG15_Rotor2_Drive_Run, :LOG16_Rotor2_Desorp_Fan_Run
     )
     """)


# ============ 3) 参数构造（与 alarm 的 build_params 同风格） ============
def build_log_params(current_time, vals: dict):
    p = {"current_time": current_time}
    keys = [
        "LOG01_RTO_System_Run", "LOG02_RTO_System_Stop",
        "LOG03_Rotor1_System_Run", "LOG04_Rotor1_System_Stop",
        "LOG05_Rotor2_System_Run", "LOG06_Rotor2_System_Stop",
        "LOG07_RTO_Fan_Run", "LOG08_Assist_Fan_Run",
        "LOG09_Backflush_Fan_Run", "LOG10_RTO_Burner_Run",
        "LOG11_Rotor1_Fan_Run", "LOG12_Rotor1_Drive_Run",
        "LOG13_Rotor1_Desorp_Fan_Run", "LOG14_Rotor2_Fan_Run",
        "LOG15_Rotor2_Drive_Run", "LOG16_Rotor2_Desorp_Fan_Run",
    ]
    for k in keys:
        p[k] = int(vals.get(k, 0))
    return p


# ============ 4) 对外函数：与 get_alarm_data 同风格 ============
def get_log_data(plc):
    """
    读取 DB311 → 一次 INSERT 落库（不在此处断开连接）
    """
    try:
        plc = snap7.client.Client()
        plc.connect('10.15.161.18', 0, 1)
        plc.send_timeout = 2000
        plc.recv_timeout = 2000
        vals = read_log_values(plc)
        engine = create_engine(get_sql_server_connection_string())
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_query = build_insert_log_query()
        params = build_log_params(current_time, vals)

        with engine.connect() as conn:
            conn.execute(insert_query, params)
            conn.commit()

        print("日志数据已成功保存到 SQL Server。")
    except Exception as e:
        print(f"读取/写入日志数据时发生错误：{e}")


# ===== 可独立运行（谁创建谁销毁；保持和 alarm 的 main 一致）=====
if __name__ == "__main__":
    plc = snap7.client.Client()
    try:
        plc.send_timeout = 8000
        plc.recv_timeout = 8000
        plc.connect('10.15.161.18', 0, 1)
        if plc.get_connected():
            print("连接到PLC成功！！！！")
        get_log_data(plc)  # 内部不 disconnect，复用连接
    finally:
        try:
            plc.disconnect()
        except Exception:
            pass

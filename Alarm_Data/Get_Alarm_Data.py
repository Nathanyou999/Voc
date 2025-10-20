# -*- coding: utf-8 -*-
from snap7.util import *
from snap7.util import get_bool
import snap7
import datetime
from sqlalchemy import create_engine, text
from database_config import get_sql_server_connection_string


def read_alarm_values(plc):
    """
    读取 PDF 3 页全部报警位（DB5 + DB6），返回 dict: { 'Axx_Name': 0/1, ... }
    变量名为英文；每个变量行有中文+DB地址注释，顺序与 PDF 一致。
    """
    # 一次性读块
    buf5 = plc.db_read(5, 0, 6)   # DB5.DBX0.0 ~ DB5.DBX5.7
    buf6 = plc.db_read(6, 0, 5)   # DB6.DBX0.0 ~ DB6.DBX4.7
    b5 = lambda byte, bit: int(get_bool(buf5, byte, bit))
    b6 = lambda byte, bit: int(get_bool(buf6, byte, bit))

    # ===== DB5 (0.0 ~ 5.7) =====
    A01_EmergencyStopAlarm = b5(0,0)  # 急停报警 | DB5.DBX0.0
    A02_CompressedAirAlarm = b5(0,1)  # 压缩空气报警 | DB5.DBX0.1
    A03_ControlPowerAlarm = b5(0,2)  # 控制电源报警 | DB5.DBX0.2
    A04_RTOFanCoolingMotorOverload = b5(0,3)  # RTO风机冷却电机过载 | DB5.DBX0.3
    A05_RTOFanInverterFault = b5(0,4)  # RTO风机变频器故障 | DB5.DBX0.4
    A06_RTOFanStarterFault = b5(0,5)  # RTO风机启动故障 | DB5.DBX0.5
    A07_RTOFanAirPressureAlarm = b5(0,6)  # RTO风机风压报警 | DB5.DBX0.6
    A08_RTOSilencerRoomAxialFanOverload = b5(0,7)  # RTO消音房轴流风扇过载 | DB5.DBX0.7

    A09_RTOSilencerRoomAxialFanStarterFault = b5(1,0)  # RTO消音房轴流风扇启动故障 | DB5.DBX1.0
    A10_RTOAssistFanOverload = b5(1,1)  # RTO助燃风机过载 | DB5.DBX1.1
    A11_RTOAssistFanStarterFault = b5(1,2)  # RTO助燃风机启动故障 | DB5.DBX1.2
    A12_RTOBackflushFanOverload = b5(1,3)  # RTO反吹风机过载 | DB5.DBX1.3
    A13_RTOBackflushFanStarterFault = b5(1,4)  # RTO反吹风机启动故障 | DB5.DBX1.4
    A14_RTOSwitchValveServoMotorDriveFault = b5(1,5)  # RTO转阀伺服电机驱动故障 | DB5.DBX1.5
    A15_RTOSwitchValveServoMotorDriveAlarm = b5(1,6)  # RTO转阀伺服电机驱动报警 | DB5.DBX1.6
    A16_RTOSwitchValveServoMotorCoolingMotorFault = b5(1,7)  # RTO转阀伺服电机冷却电机故障 | DB5.DBX1.7

    A17_MV3012_RTO_OpenNotInPositionAlarm = b5(2,0)  # MV3012_RTO 处理阀开不到位报警 | DB5.DBX2.0
    A18_MV3012_RTO_CloseNotInPositionAlarm = b5(2,1)  # MV3012_RTO 处理阀关不到位报警 | DB5.DBX2.1
    A19_AV3011_RTO_OpenNotInPositionAlarm = b5(2,2)  # AV3011_RTO 直排阀开不到位报警 | DB5.DBX2.2
    A20_AV3011_RTO_CloseNotInPositionAlarm = b5(2,3)  # AV3011_RTO 直排阀关不到位报警 | DB5.DBX2.3
    A21_MV3010_RTO_FreshAir_OpenNotInPositionAlarm = b5(2,4)  # MV3010_RTO 新风阀开不到位报警 | DB5.DBX2.4
    A22_MV3010_RTO_FreshAir_CloseNotInPositionAlarm = b5(2,5)  # MV3010_RTO 新风阀关不到位报警 | DB5.DBX2.5
    A23_AV3017_AssistAirValve_OpenNotInPositionAlarm = b5(2,6)  # AV3017_ 助燃风阀开不到位报警 | DB5.DBX2.6
    A24_AV3017_AssistAirValve_CloseNotInPositionAlarm = b5(2,7)  # AV3017_ 助燃风阀关不到位报警 | DB5.DBX2.7

    A25_MV3015_RotaryRTO_BackflushValve_OpenNotInPositionAlarm = b5(3,0)  # MV3015_ 旋转RTO反吹阀开不到位报警 | DB5.DBX3.0
    A26_MV3015_RotaryRTO_BackflushValve_CloseNotInPositionAlarm = b5(3,1)  # MV3015_ 旋转RTO反吹阀关不到位报警 | DB5.DBX3.1
    A27_CV3016_RTO_HighTempProportionalValveAlarm = b5(3,2)  # CV3016_RTO 高温比例阀报警 | DB5.DBX3.2
    A28_RTOBurner_LocalModeOn = b5(3,3)  # RTO燃烧机本地模式开启 | DB5.DBX3.3
    A29_RTOBurner_FlameoutFaultAlarm = b5(3,4)  # RTO燃烧机熄火故障报警 | DB5.DBX3.4
    A30_RTOBurnerGas_HighPressureFaultAlarm = b5(3,5)  # RTO燃烧机燃气高压故障报警 | DB5.DBX3.5
    A31_RTOBurnerGas_LowPressureFaultAlarm = b5(3,6)  # RTO燃烧机燃气低压故障报警 | DB5.DBX3.6
    A32_RTOBurner_GasLeakTestFailedAlarm = b5(3,7)  # RTO燃烧机燃气检漏失败报警 | DB5.DBX3.7

    A33_RTOBurner_AssistAirPressureAbnormalFaultAlarm = b5(4,0)  # RTO燃烧机助燃风压异常故障报警 | DB5.DBX4.0
    A34_RTOBurner_GasLeakageAlarm = b5(4,1)  # RTO燃烧机燃气外漏报警 | DB5.DBX4.1
    A35_RTO_FurnacePressureHighHighAlarm = b5(4,2)  # RTO炉膛压力超高限报警 | DB5.DBX4.2
    A36_RTOFan_InletPressureAbnormalAlarm_FixedFrequencyRun = b5(4,3)  # RTO风机进口压力异常报警（连锁 RTO风机固定频率运行）| DB5.DBX4.3
    A37_RTO_FurnaceTempHighHighAlarm = b5(4,4)  # RTO炉膛温度超高限报警 | DB5.DBX4.4
    A38_RTO_LowerLayerTempOverTempAlarm = b5(4,5)  # RTO下层温度超温报警 | DB5.DBX4.5
    A39_RTO_OutletTempHighHighAlarm = b5(4,6)  # RTO出口温度超高限报警 | DB5.DBX4.6
    A40_RTO_FurnaceTempLowAlarm_WorkModeBelowTreatTemp = b5(4,7)  # RTO炉膛温度低报警(工作模式下炉膛温度低于废气处理温度) | DB5.DBX4.7

    A41_RTO_FurnaceThermocoupleAbnormalAlarm = b5(5,0)  # RTO炉膛热电偶异常报警 | DB5.DBX5.0
    A42_LEL_HighAlarm = b5(5,1)  # LEL浓度高报警 | DB5.DBX5.1
    # A43~A47 备用（Spare） DB5.DBX5.2~5.6 — 默认不入库
    A48_AlarmTest = b5(5,7)  # 报警测试 | DB5.DBX5.7

    # ===== DB6 (0.0 ~ 4.3) =====
    A50_RotorFan_CoolingMotorOverload = b6(0,0)  # 转轮风机冷却电机过载 | DB6.DBX0.0
    A51_RotorFan_InverterFault = b6(0,1)  # 转轮风机变频器故障 | DB6.DBX0.1
    A52_RotorFan_StarterFault = b6(0,2)  # 转轮风机启动故障 | DB6.DBX0.2
    A53_RotorFan_AirPressureAlarm = b6(0,3)  # 转轮风机风压报警 | DB6.DBX0.3
    A54_RotorDrive_InverterFault = b6(0,4)  # 转轮驱动变频器故障 | DB6.DBX0.4
    A55_RotorDrive_StarterFault = b6(0,5)  # 转轮驱动启动故障 | DB6.DBX0.5
    A56_DesorpFan_InverterFault = b6(0,6)  # 转轮脱附风机变频器故障 | DB6.DBX0.6
    A57_DesorpFan_StarterFault = b6(0,7)  # 转轮脱附风机启动故障 | DB6.DBX0.7

    A58_DesorpFan_AirPressureAlarm = b6(1,0)  # 转轮脱附风机风压报警 | DB6.DBX1.0
    A59_DesorpFan_CoolingMotorOverload = b6(1,1)  # 转轮脱附风机冷却电机过载 | DB6.DBX1.1
    A60_FilterBox_Stage1_DifferentialPressureAlarm = b6(1,2)  # 转轮过滤箱一级压差报警 | DB6.DBX1.2
    A61_FilterBox_Stage2_DifferentialPressureAlarm = b6(1,3)  # 转轮过滤箱二级压差报警 | DB6.DBX1.3
    A62_FilterBox_Stage3_DifferentialPressureAlarm = b6(1,4)  # 转轮过滤箱三级压差报警 | DB6.DBX1.4
    A63_FilterBox_Stage4_DifferentialPressureAlarm = b6(1,5)  # 转轮过滤箱四级压差报警 | DB6.DBX1.5
    # A64,A65 备用（Spare） DB6.DBX1.6~1.7 — 默认不入库

    A66_FilterBox_DiffPressureUnhandled4hAutoCooldownShutdown = b6(2,0)  # 压差未处理超4小时自动降温停机 | DB6.DBX2.0
    A67_MV1012_RotorInletValve_ActuatorA_OpenNotInPositionAlarm = b6(2,1)  # MV1012 A开不到位报警 | DB6.DBX2.1
    A68_MV1012_RotorInletValve_ActuatorA_CloseNotInPositionAlarm = b6(2,2)  # MV1012 A关不到位报警 | DB6.DBX2.2
    A69_MV1010_RotorFreshAirValve_ActuatorA_OpenNotInPositionAlarm = b6(2,3)  # MV1010 A开不到位报警 | DB6.DBX2.3
    A70_MV1010_RotorFreshAirValve_ActuatorA_CloseNotInPositionAlarm = b6(2,4)  # MV1010 A关不到位报警 | DB6.DBX2.4
    A71_MV2013_RotorFanOutletValve_ActuatorA_OpenNotInPositionAlarm = b6(2,5)  # MV2013 A开不到位报警 | DB6.DBX2.5
    A72_MV2013_RotorFanOutletValve_ActuatorA_CloseNotInPositionAlarm = b6(2,6)  # MV2013 A关不到位报警 | DB6.DBX2.6
    A73_MV2016_HEXColdSideOutletValve_OpenNotInPositionAlarm = b6(2,7)  # MV2016 冷侧出口阀开不到位报警 | DB6.DBX2.7

    A74_MV2016_HEXColdSideOutletValve_CloseNotInPositionAlarm = b6(3,0)  # MV2016 冷侧出口阀关不到位报警 | DB6.DBX3.0
    A75_CV2012_DesorpOutletValveA_Alarm = b6(3,1)  # CV2012 脱附出口阀A报警 | DB6.DBX3.1
    A76_CV2015_HEXColdSideInlet_ProportionalValveAlarm = b6(3,2)  # CV2015 冷侧进口比例阀报警 | DB6.DBX3.2
    A77_CV2014_HEX_TempControl_ProportionalValveAlarm = b6(3,3)  # CV2014 调温比例阀报警 | DB6.DBX3.3
    A78_CV2017_HEXHotSideOutletValve_Alarm = b6(3,4)  # CV2017 热侧出口阀报警 | DB6.DBX3.4
    A79_CV2010_RotorCoolingOutletValve_Alarm = b6(3,5)  # CV2010 冷却出口阀报警 | DB6.DBX3.5
    A80_CV2011_RotorDesorpInletValve_Alarm = b6(3,6)  # CV2011 脱附进口阀报警 | DB6.DBX3.6
    A81_RotorDesorpInletTempHighHighAlarm = b6(3,7)  # 脱附进口温度超高限报警 | DB6.DBX3.7

    A82_RotorInletTempHighAlarm = b6(4,0)  # 转轮进口温度超高报警 | DB6.DBX4.0
    A83_HEXColdSideOutletDesorpTempHighHighAlarm = b6(4,1)  # 冷侧出口脱附温度超高限报警 | DB6.DBX4.1
    A84_HEXHotSideOutletTempHighAlarm = b6(4,2)  # 热侧出口温度超高报警 | DB6.DBX4.2
    A85_RotorInletPressureAbnormalAlarm_RotorFanFixedFrequencyRun = b6(4,3)  # 转轮进口压力异常报警(连锁固定频率) | DB6.DBX4.3

    # 打包为 dict（英文键）
    return {
        # DB5
        "A01_EmergencyStopAlarm": A01_EmergencyStopAlarm,
        "A02_CompressedAirAlarm": A02_CompressedAirAlarm,
        "A03_ControlPowerAlarm": A03_ControlPowerAlarm,
        "A04_RTOFanCoolingMotorOverload": A04_RTOFanCoolingMotorOverload,
        "A05_RTOFanInverterFault": A05_RTOFanInverterFault,
        "A06_RTOFanStarterFault": A06_RTOFanStarterFault,
        "A07_RTOFanAirPressureAlarm": A07_RTOFanAirPressureAlarm,
        "A08_RTOSilencerRoomAxialFanOverload": A08_RTOSilencerRoomAxialFanOverload,
        "A09_RTOSilencerRoomAxialFanStarterFault": A09_RTOSilencerRoomAxialFanStarterFault,
        "A10_RTOAssistFanOverload": A10_RTOAssistFanOverload,
        "A11_RTOAssistFanStarterFault": A11_RTOAssistFanStarterFault,
        "A12_RTOBackflushFanOverload": A12_RTOBackflushFanOverload,
        "A13_RTOBackflushFanStarterFault": A13_RTOBackflushFanStarterFault,
        "A14_RTOSwitchValveServoMotorDriveFault": A14_RTOSwitchValveServoMotorDriveFault,
        "A15_RTOSwitchValveServoMotorDriveAlarm": A15_RTOSwitchValveServoMotorDriveAlarm,
        "A16_RTOSwitchValveServoMotorCoolingMotorFault": A16_RTOSwitchValveServoMotorCoolingMotorFault,
        "A17_MV3012_RTO_OpenNotInPositionAlarm": A17_MV3012_RTO_OpenNotInPositionAlarm,
        "A18_MV3012_RTO_CloseNotInPositionAlarm": A18_MV3012_RTO_CloseNotInPositionAlarm,
        "A19_AV3011_RTO_OpenNotInPositionAlarm": A19_AV3011_RTO_OpenNotInPositionAlarm,
        "A20_AV3011_RTO_CloseNotInPositionAlarm": A20_AV3011_RTO_CloseNotInPositionAlarm,
        "A21_MV3010_RTO_FreshAir_OpenNotInPositionAlarm": A21_MV3010_RTO_FreshAir_OpenNotInPositionAlarm,
        "A22_MV3010_RTO_FreshAir_CloseNotInPositionAlarm": A22_MV3010_RTO_FreshAir_CloseNotInPositionAlarm,
        "A23_AV3017_AssistAirValve_OpenNotInPositionAlarm": A23_AV3017_AssistAirValve_OpenNotInPositionAlarm,
        "A24_AV3017_AssistAirValve_CloseNotInPositionAlarm": A24_AV3017_AssistAirValve_CloseNotInPositionAlarm,
        "A25_MV3015_RotaryRTO_BackflushValve_OpenNotInPositionAlarm": A25_MV3015_RotaryRTO_BackflushValve_OpenNotInPositionAlarm,
        "A26_MV3015_RotaryRTO_BackflushValve_CloseNotInPositionAlarm": A26_MV3015_RotaryRTO_BackflushValve_CloseNotInPositionAlarm,
        "A27_CV3016_RTO_HighTempProportionalValveAlarm": A27_CV3016_RTO_HighTempProportionalValveAlarm,
        "A28_RTOBurner_LocalModeOn": A28_RTOBurner_LocalModeOn,
        "A29_RTOBurner_FlameoutFaultAlarm": A29_RTOBurner_FlameoutFaultAlarm,
        "A30_RTOBurnerGas_HighPressureFaultAlarm": A30_RTOBurnerGas_HighPressureFaultAlarm,
        "A31_RTOBurnerGas_LowPressureFaultAlarm": A31_RTOBurnerGas_LowPressureFaultAlarm,
        "A32_RTOBurner_GasLeakTestFailedAlarm": A32_RTOBurner_GasLeakTestFailedAlarm,
        "A33_RTOBurner_AssistAirPressureAbnormalFaultAlarm": A33_RTOBurner_AssistAirPressureAbnormalFaultAlarm,
        "A34_RTOBurner_GasLeakageAlarm": A34_RTOBurner_GasLeakageAlarm,
        "A35_RTO_FurnacePressureHighHighAlarm": A35_RTO_FurnacePressureHighHighAlarm,
        "A36_RTOFan_InletPressureAbnormalAlarm_FixedFrequencyRun": A36_RTOFan_InletPressureAbnormalAlarm_FixedFrequencyRun,
        "A37_RTO_FurnaceTempHighHighAlarm": A37_RTO_FurnaceTempHighHighAlarm,
        "A38_RTO_LowerLayerTempOverTempAlarm": A38_RTO_LowerLayerTempOverTempAlarm,
        "A39_RTO_OutletTempHighHighAlarm": A39_RTO_OutletTempHighHighAlarm,
        "A40_RTO_FurnaceTempLowAlarm_WorkModeBelowTreatTemp": A40_RTO_FurnaceTempLowAlarm_WorkModeBelowTreatTemp,
        "A41_RTO_FurnaceThermocoupleAbnormalAlarm": A41_RTO_FurnaceThermocoupleAbnormalAlarm,
        "A42_LEL_HighAlarm": A42_LEL_HighAlarm,
        "A48_AlarmTest": A48_AlarmTest,

        # DB6
        "A50_RotorFan_CoolingMotorOverload": A50_RotorFan_CoolingMotorOverload,
        "A51_RotorFan_InverterFault": A51_RotorFan_InverterFault,
        "A52_RotorFan_StarterFault": A52_RotorFan_StarterFault,
        "A53_RotorFan_AirPressureAlarm": A53_RotorFan_AirPressureAlarm,
        "A54_RotorDrive_InverterFault": A54_RotorDrive_InverterFault,
        "A55_RotorDrive_StarterFault": A55_RotorDrive_StarterFault,
        "A56_DesorpFan_InverterFault": A56_DesorpFan_InverterFault,
        "A57_DesorpFan_StarterFault": A57_DesorpFan_StarterFault,
        "A58_DesorpFan_AirPressureAlarm": A58_DesorpFan_AirPressureAlarm,
        "A59_DesorpFan_CoolingMotorOverload": A59_DesorpFan_CoolingMotorOverload,
        "A60_FilterBox_Stage1_DifferentialPressureAlarm": A60_FilterBox_Stage1_DifferentialPressureAlarm,
        "A61_FilterBox_Stage2_DifferentialPressureAlarm": A61_FilterBox_Stage2_DifferentialPressureAlarm,
        "A62_FilterBox_Stage3_DifferentialPressureAlarm": A62_FilterBox_Stage3_DifferentialPressureAlarm,
        "A63_FilterBox_Stage4_DifferentialPressureAlarm": A63_FilterBox_Stage4_DifferentialPressureAlarm,
        "A66_FilterBox_DiffPressureUnhandled4hAutoCooldownShutdown": A66_FilterBox_DiffPressureUnhandled4hAutoCooldownShutdown,
        "A67_MV1012_RotorInletValve_ActuatorA_OpenNotInPositionAlarm": A67_MV1012_RotorInletValve_ActuatorA_OpenNotInPositionAlarm,
        "A68_MV1012_RotorInletValve_ActuatorA_CloseNotInPositionAlarm": A68_MV1012_RotorInletValve_ActuatorA_CloseNotInPositionAlarm,
        "A69_MV1010_RotorFreshAirValve_ActuatorA_OpenNotInPositionAlarm": A69_MV1010_RotorFreshAirValve_ActuatorA_OpenNotInPositionAlarm,
        "A70_MV1010_RotorFreshAirValve_ActuatorA_CloseNotInPositionAlarm": A70_MV1010_RotorFreshAirValve_ActuatorA_CloseNotInPositionAlarm,
        "A71_MV2013_RotorFanOutletValve_ActuatorA_OpenNotInPositionAlarm": A71_MV2013_RotorFanOutletValve_ActuatorA_OpenNotInPositionAlarm,
        "A72_MV2013_RotorFanOutletValve_ActuatorA_CloseNotInPositionAlarm": A72_MV2013_RotorFanOutletValve_ActuatorA_CloseNotInPositionAlarm,
        "A73_MV2016_HEXColdSideOutletValve_OpenNotInPositionAlarm": A73_MV2016_HEXColdSideOutletValve_OpenNotInPositionAlarm,
        "A74_MV2016_HEXColdSideOutletValve_CloseNotInPositionAlarm": A74_MV2016_HEXColdSideOutletValve_CloseNotInPositionAlarm,
        "A75_CV2012_DesorpOutletValveA_Alarm": A75_CV2012_DesorpOutletValveA_Alarm,
        "A76_CV2015_HEXColdSideInlet_ProportionalValveAlarm": A76_CV2015_HEXColdSideInlet_ProportionalValveAlarm,
        "A77_CV2014_HEX_TempControl_ProportionalValveAlarm": A77_CV2014_HEX_TempControl_ProportionalValveAlarm,
        "A78_CV2017_HEXHotSideOutletValve_Alarm": A78_CV2017_HEXHotSideOutletValve_Alarm,
        "A79_CV2010_RotorCoolingOutletValve_Alarm": A79_CV2010_RotorCoolingOutletValve_Alarm,
        "A80_CV2011_RotorDesorpInletValve_Alarm": A80_CV2011_RotorDesorpInletValve_Alarm,
        "A81_RotorDesorpInletTempHighHighAlarm": A81_RotorDesorpInletTempHighHighAlarm,
        "A82_RotorInletTempHighAlarm": A82_RotorInletTempHighAlarm,
        "A83_HEXColdSideOutletDesorpTempHighHighAlarm": A83_HEXColdSideOutletDesorpTempHighHighAlarm,
        "A84_HEXHotSideOutletTempHighAlarm": A84_HEXHotSideOutletTempHighAlarm,
        "A85_RotorInletPressureAbnormalAlarm_RotorFanFixedFrequencyRun": A85_RotorInletPressureAbnormalAlarm_RotorFanFixedFrequencyRun,
    }


def build_insert_alarm_query():
    """
    生成和 processdata 风格一致的 INSERT（按 PDF 顺序，剔除“备用”）
    """
    return text("""
    INSERT INTO [ods_voc_script_Alarmdata_realtime] (
        [记录时间],
        -- === DB5 ===
        [急停报警],
        [压缩空气报警],
        [控制电源报警],
        [RTO风机冷却电机过载],
        [RTO风机变频器故障],
        [RTO风机启动故障],
        [RTO风机风压报警],
        [RTO消音房轴流风扇过载],
        [RTO消音房轴流风扇启动故障],
        [RTO助燃风机过载],
        [RTO助燃风机启动故障],
        [RTO反吹风机过载],
        [RTO反吹风机启动故障],
        [RTO转阀伺服电机驱动故障],
        [RTO转阀伺服电机驱动报警],
        [RTO转阀伺服电机冷却电机故障],
        [MV3012_RTO处理阀开不到位报警],
        [MV3012_RTO处理阀关不到位报警],
        [AV3011_RTO直排阀开不到位报警],
        [AV3011_RTO直排阀关不到位报警],
        [MV3010_RTO新风阀开不到位报警],
        [MV3010_RTO新风阀关不到位报警],
        [AV3017_助燃风阀开不到位报警],
        [AV3017_助燃风阀关不到位报警],
        [MV3015_旋转RTO反吹阀开不到位报警],
        [MV3015_旋转RTO反吹阀关不到位报警],
        [CV3016_RTO高温比例阀报警],
        [RTO燃烧机本地模式开启],
        [RTO燃烧机熄火故障报警],
        [RTO燃烧机燃气高压故障报警],
        [RTO燃烧机燃气低压故障报警],
        [RTO燃烧机燃气检漏失败报警],
        [RTO燃烧机助燃风压异常故障报警],
        [RTO燃烧机燃气外漏报警],
        [RTO炉膛压力超高限报警],
        [RTO风机进口压力异常报警(连锁RTO风机固定频率运行)],
        [RTO炉膛温度超高限报警],
        [RTO下层温度超温报警],
        [RTO出口温度超高限报警],
        [RTO炉膛温度低报警(工作模式下炉膛温度低于废气处理温度)],
        [RTO炉膛热电偶异常报警],
        [LEL浓度高报警],
        [报警测试],
        -- === DB6 ===
        [转轮风机冷却电机过载],
        [转轮风机变频器故障],
        [转轮风机启动故障],
        [转轮风机风压报警],
        [转轮驱动变频器故障],
        [转轮驱动启动故障],
        [转轮脱附风机变频器故障],
        [转轮脱附风机启动故障],
        [转轮脱附风机风压报警],
        [转轮脱附风机冷却电机过载],
        [转轮过滤箱一级压差报警],
        [转轮过滤箱二级压差报警],
        [转轮过滤箱三级压差报警],
        [转轮过滤箱四级压差报警],
        [转轮过滤箱压差报警未处理超4小时自动降温停机],
        [MV1012转轮进口阀执行器A开不到位报警],
        [MV1012转轮进口阀执行器A关不到位报警],
        [MV1010转轮新风阀执行器A开不到位报警],
        [MV1010转轮新风阀执行器A关不到位报警],
        [MV2013转轮风机出口阀执行器A开不到位报警],
        [MV2013转轮风机出口阀执行器A关不到位报警],
        [MV2016_换热器冷侧出口阀开不到位报警],
        [MV2016_换热器冷侧出口阀关不到位报警],
        [CV2012转轮脱附出口阀A报警],
        [CV2015_换热器冷侧进口比例阀报警],
        [CV2014_换热器调温比例阀报警],
        [CV2017换热器热侧出口阀报警],
        [CV2010转轮冷却出口阀报警],
        [CV2011转轮脱附进口阀报警],
        [转轮脱附进口温度超高限报警],
        [转轮进口温度超高报警],
        [换热器冷侧出口脱附温度超高限报警],
        [换热器热侧出口温度超高报警],
        [转轮进口压力异常报警(连锁转轮风机固定频率运行)]
    ) VALUES (
        :current_time,

        -- DB5
        :A01_EmergencyStopAlarm,
        :A02_CompressedAirAlarm,
        :A03_ControlPowerAlarm,
        :A04_RTOFanCoolingMotorOverload,
        :A05_RTOFanInverterFault,
        :A06_RTOFanStarterFault,
        :A07_RTOFanAirPressureAlarm,
        :A08_RTOSilencerRoomAxialFanOverload,
        :A09_RTOSilencerRoomAxialFanStarterFault,
        :A10_RTOAssistFanOverload,
        :A11_RTOAssistFanStarterFault,
        :A12_RTOBackflushFanOverload,
        :A13_RTOBackflushFanStarterFault,
        :A14_RTOSwitchValveServoMotorDriveFault,
        :A15_RTOSwitchValveServoMotorDriveAlarm,
        :A16_RTOSwitchValveServoMotorCoolingMotorFault,
        :A17_MV3012_RTO_OpenNotInPositionAlarm,
        :A18_MV3012_RTO_CloseNotInPositionAlarm,
        :A19_AV3011_RTO_OpenNotInPositionAlarm,
        :A20_AV3011_RTO_CloseNotInPositionAlarm,
        :A21_MV3010_RTO_FreshAir_OpenNotInPositionAlarm,
        :A22_MV3010_RTO_FreshAir_CloseNotInPositionAlarm,
        :A23_AV3017_AssistAirValve_OpenNotInPositionAlarm,
        :A24_AV3017_AssistAirValve_CloseNotInPositionAlarm,
        :A25_MV3015_RotaryRTO_BackflushValve_OpenNotInPositionAlarm,
        :A26_MV3015_RotaryRTO_BackflushValve_CloseNotInPositionAlarm,
        :A27_CV3016_RTO_HighTempProportionalValveAlarm,
        :A28_RTOBurner_LocalModeOn,
        :A29_RTOBurner_FlameoutFaultAlarm,
        :A30_RTOBurnerGas_HighPressureFaultAlarm,
        :A31_RTOBurnerGas_LowPressureFaultAlarm,
        :A32_RTOBurner_GasLeakTestFailedAlarm,
        :A33_RTOBurner_AssistAirPressureAbnormalFaultAlarm,
        :A34_RTOBurner_GasLeakageAlarm,
        :A35_RTO_FurnacePressureHighHighAlarm,
        :A36_RTOFan_InletPressureAbnormalAlarm_FixedFrequencyRun,
        :A37_RTO_FurnaceTempHighHighAlarm,
        :A38_RTO_LowerLayerTempOverTempAlarm,
        :A39_RTO_OutletTempHighHighAlarm,
        :A40_RTO_FurnaceTempLowAlarm_WorkModeBelowTreatTemp,
        :A41_RTO_FurnaceThermocoupleAbnormalAlarm,
        :A42_LEL_HighAlarm,
        :A48_AlarmTest,

        -- DB6
        :A50_RotorFan_CoolingMotorOverload,
        :A51_RotorFan_InverterFault,
        :A52_RotorFan_StarterFault,
        :A53_RotorFan_AirPressureAlarm,
        :A54_RotorDrive_InverterFault,
        :A55_RotorDrive_StarterFault,
        :A56_DesorpFan_InverterFault,
        :A57_DesorpFan_StarterFault,
        :A58_DesorpFan_AirPressureAlarm,
        :A59_DesorpFan_CoolingMotorOverload,
        :A60_FilterBox_Stage1_DifferentialPressureAlarm,
        :A61_FilterBox_Stage2_DifferentialPressureAlarm,
        :A62_FilterBox_Stage3_DifferentialPressureAlarm,
        :A63_FilterBox_Stage4_DifferentialPressureAlarm,
        :A66_FilterBox_DiffPressureUnhandled4hAutoCooldownShutdown,
        :A67_MV1012_RotorInletValve_ActuatorA_OpenNotInPositionAlarm,
        :A68_MV1012_RotorInletValve_ActuatorA_CloseNotInPositionAlarm,
        :A69_MV1010_RotorFreshAirValve_ActuatorA_OpenNotInPositionAlarm,
        :A70_MV1010_RotorFreshAirValve_ActuatorA_CloseNotInPositionAlarm,
        :A71_MV2013_RotorFanOutletValve_ActuatorA_OpenNotInPositionAlarm,
        :A72_MV2013_RotorFanOutletValve_ActuatorA_CloseNotInPositionAlarm,
        :A73_MV2016_HEXColdSideOutletValve_OpenNotInPositionAlarm,
        :A74_MV2016_HEXColdSideOutletValve_CloseNotInPositionAlarm,
        :A75_CV2012_DesorpOutletValveA_Alarm,
        :A76_CV2015_HEXColdSideInlet_ProportionalValveAlarm,
        :A77_CV2014_HEX_TempControl_ProportionalValveAlarm,
        :A78_CV2017_HEXHotSideOutletValve_Alarm,
        :A79_CV2010_RotorCoolingOutletValve_Alarm,
        :A80_CV2011_RotorDesorpInletValve_Alarm,
        :A81_RotorDesorpInletTempHighHighAlarm,
        :A82_RotorInletTempHighAlarm,
        :A83_HEXColdSideOutletDesorpTempHighHighAlarm,
        :A84_HEXHotSideOutletTempHighAlarm,
        :A85_RotorInletPressureAbnormalAlarm_RotorFanFixedFrequencyRun
    )
    """)


def build_params(current_time, A):
    """
    A: read_alarm_values(plc) 的返回 dict
    生成与 INSERT 对应的参数 dict
    """
    p = {"current_time": current_time}
    keys = [
        # DB5
        "A01_EmergencyStopAlarm","A02_CompressedAirAlarm","A03_ControlPowerAlarm",
        "A04_RTOFanCoolingMotorOverload","A05_RTOFanInverterFault","A06_RTOFanStarterFault",
        "A07_RTOFanAirPressureAlarm","A08_RTOSilencerRoomAxialFanOverload",
        "A09_RTOSilencerRoomAxialFanStarterFault","A10_RTOAssistFanOverload",
        "A11_RTOAssistFanStarterFault","A12_RTOBackflushFanOverload","A13_RTOBackflushFanStarterFault",
        "A14_RTOSwitchValveServoMotorDriveFault","A15_RTOSwitchValveServoMotorDriveAlarm",
        "A16_RTOSwitchValveServoMotorCoolingMotorFault","A17_MV3012_RTO_OpenNotInPositionAlarm",
        "A18_MV3012_RTO_CloseNotInPositionAlarm","A19_AV3011_RTO_OpenNotInPositionAlarm",
        "A20_AV3011_RTO_CloseNotInPositionAlarm","A21_MV3010_RTO_FreshAir_OpenNotInPositionAlarm",
        "A22_MV3010_RTO_FreshAir_CloseNotInPositionAlarm","A23_AV3017_AssistAirValve_OpenNotInPositionAlarm",
        "A24_AV3017_AssistAirValve_CloseNotInPositionAlarm","A25_MV3015_RotaryRTO_BackflushValve_OpenNotInPositionAlarm",
        "A26_MV3015_RotaryRTO_BackflushValve_CloseNotInPositionAlarm","A27_CV3016_RTO_HighTempProportionalValveAlarm",
        "A28_RTOBurner_LocalModeOn","A29_RTOBurner_FlameoutFaultAlarm","A30_RTOBurnerGas_HighPressureFaultAlarm",
        "A31_RTOBurnerGas_LowPressureFaultAlarm","A32_RTOBurner_GasLeakTestFailedAlarm",
        "A33_RTOBurner_AssistAirPressureAbnormalFaultAlarm","A34_RTOBurner_GasLeakageAlarm",
        "A35_RTO_FurnacePressureHighHighAlarm","A36_RTOFan_InletPressureAbnormalAlarm_FixedFrequencyRun",
        "A37_RTO_FurnaceTempHighHighAlarm","A38_RTO_LowerLayerTempOverTempAlarm","A39_RTO_OutletTempHighHighAlarm",
        "A40_RTO_FurnaceTempLowAlarm_WorkModeBelowTreatTemp","A41_RTO_FurnaceThermocoupleAbnormalAlarm",
        "A42_LEL_HighAlarm","A48_AlarmTest",
        # DB6
        "A50_RotorFan_CoolingMotorOverload","A51_RotorFan_InverterFault","A52_RotorFan_StarterFault",
        "A53_RotorFan_AirPressureAlarm","A54_RotorDrive_InverterFault","A55_RotorDrive_StarterFault",
        "A56_DesorpFan_InverterFault","A57_DesorpFan_StarterFault","A58_DesorpFan_AirPressureAlarm",
        "A59_DesorpFan_CoolingMotorOverload","A60_FilterBox_Stage1_DifferentialPressureAlarm",
        "A61_FilterBox_Stage2_DifferentialPressureAlarm","A62_FilterBox_Stage3_DifferentialPressureAlarm",
        "A63_FilterBox_Stage4_DifferentialPressureAlarm","A66_FilterBox_DiffPressureUnhandled4hAutoCooldownShutdown",
        "A67_MV1012_RotorInletValve_ActuatorA_OpenNotInPositionAlarm",
        "A68_MV1012_RotorInletValve_ActuatorA_CloseNotInPositionAlarm",
        "A69_MV1010_RotorFreshAirValve_ActuatorA_OpenNotInPositionAlarm",
        "A70_MV1010_RotorFreshAirValve_ActuatorA_CloseNotInPositionAlarm",
        "A71_MV2013_RotorFanOutletValve_ActuatorA_OpenNotInPositionAlarm",
        "A72_MV2013_RotorFanOutletValve_ActuatorA_CloseNotInPositionAlarm",
        "A73_MV2016_HEXColdSideOutletValve_OpenNotInPositionAlarm",
        "A74_MV2016_HEXColdSideOutletValve_CloseNotInPositionAlarm",
        "A75_CV2012_DesorpOutletValveA_Alarm","A76_CV2015_HEXColdSideInlet_ProportionalValveAlarm",
        "A77_CV2014_HEX_TempControl_ProportionalValveAlarm","A78_CV2017_HEXHotSideOutletValve_Alarm",
        "A79_CV2010_RotorCoolingOutletValve_Alarm","A80_CV2011_RotorDesorpInletValve_Alarm",
        "A81_RotorDesorpInletTempHighHighAlarm","A82_RotorInletTempHighAlarm",
        "A83_HEXColdSideOutletDesorpTempHighHighAlarm","A84_HEXHotSideOutletTempHighAlarm",
        "A85_RotorInletPressureAbnormalAlarm_RotorFanFixedFrequencyRun"
    ]
    for k in keys:
        p[k] = int(A.get(k, 0))
    return p


def main():
    plc = snap7.client.Client()
    try:
        plc.connect('10.15.161.18', 0, 1)   # 替换为你的PLC IP/机架/插槽
        plc.send_timeout = 2000
        plc.recv_timeout = 2000
        if plc.get_connected():
            print("连接到PLC成功！！！！")

        # 读取报警
        alarms = read_alarm_values(plc)
        # 组装 SQL & 参数
        insert_alarm_query = build_insert_alarm_query()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        params_alarm = build_params(current_time, alarms)

        # 写库
        engine = create_engine(get_sql_server_connection_string())
        with engine.connect() as conn:
            conn.execute(insert_alarm_query, params_alarm)
            conn.commit()
        print("报警数据已成功保存到 SQL Server 数据库。")

        # 小结
        active = [k for k, v in alarms.items() if v == 1]
        print(f"当前激活报警数量: {len(active)}")
        if active:
            print("激活报警(前20):", active[:20])

    except Exception as e:
        print(f"读取/写入报警数据时发生错误：{e}")
    finally:
        print("断开连接")
        try:
            plc.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()

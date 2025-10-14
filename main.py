from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
app = FastAPI()
from apscheduler.schedulers.background import BackgroundScheduler
scheduler = BackgroundScheduler()
from Alarm_Data import Get_Alarm_Data
from Process_Data.Get_Process_Data import get_process_data
from Running_Log_Data import Get_Running_Log_Data
from Request_Handler.request_handler import query_process_data_by_date_range
import snap7

# 配置CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，生产环境中应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有HTTP头
)

def json_format(stuff, code = 200):
    if code == 200 or code == 201:
        data1 = {
            "code": code,
            "data": stuff,
            "msg": "success",
        }
        return data1

@app.get("/")
async def read_root():
    return {"message": "Hello World"}

# 修改路由，将请求参数作为字典处理
@app.get("/process_data/")
async def process_data_query(request: Request):
    try:
        # 获取所有查询参数
        params = request.query_params
        print("所有参数:", params)
        
        # 获取所有的date[]参数
        date_list = request.query_params.getlist('date[]')
        print("提取的日期列表:", date_list)
       
        # 传递日期列表给处理函数
        result = query_process_data_by_date_range(date_list)
        return json_format(result)
    
    except Exception as e:
        return json_format({"error": f"查询失败: {str(e)}"}, 400)

@app.on_event("startup")
async def app_start():
    # 创建PLC客户端实例
    plc = snap7.client.Client()
    # 设置发送和接收超时时间（单位：毫秒）
    plc.send_timeout = 2000
    plc.recv_timeout = 2000
    
    # 定义一个包装函数，用于在定时任务中执行
    def process_data_job():
        try:
            # 连接到PLC
            plc.connect('10.15.161.18', 0, 1)  # 替换为你的PLC IP地址、机架号和插槽号
            if plc.get_connected():
                print("连接到PLC成功！开始获取数据...")
                # 调用函数获取数据
                get_process_data(plc=plc)
            else:
                print("PLC连接失败，无法获取数据")
        except Exception as e:
            print(f"执行定时任务时发生错误：{e}")
    
    # 添加定时任务，每隔60秒执行一次获取PLC数据的任务
    scheduler.add_job(process_data_job, 'interval', minutes=1)
    # scheduler.add_job(Get_Alarm_Data, 'interval', seconds=10)  # 每隔 10 秒执行一次获取 PLC 数据的任务
    # scheduler.add_job(Get_Running_Log_Data, 'interval', seconds=10)  # 每隔 10 秒执行一次获取 PLC 数据的任务
    
    # 启动调度器
    scheduler.start()
    print("后台任务调度器已启动，将每分钟执行一次数据采集")

@app.on_event("shutdown")
async def app_shutdown():
    # 关闭调度器
    scheduler.shutdown()
    print("应用关闭，调度器已停止")
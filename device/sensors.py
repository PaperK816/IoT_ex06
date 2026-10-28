import json
import time
import random
import os
import sys
import paho.mqtt.client as mqtt

# 将项目根目录加入环境变量，以便顺利导入 config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


class CampusEnergySensorDevice:
    def __init__(self):
        self.client_id = f'{config.CLIENT_ID}_{random.randint(1000, 9999)}'
        self.client = mqtt.Client(client_id=self.client_id)

        if config.USERNAME and config.PASSWORD:
            self.client.username_pw_set(config.USERNAME, config.PASSWORD)

        if os.path.exists(config.CA_CERTS):
            self.client.tls_set(ca_certs=config.CA_CERTS)

        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message

        # 定义三类监测对象（教学楼、宿舍楼、实验楼）
        self.buildings = [
            {"id": "building_edu", "name": "第一教学楼", "base_power": 120.0, "max_power": 180.0},
            {"id": "building_dorm", "name": "学生宿舍楼", "base_power": 150.0, "max_power": 220.0},
            {"id": "building_lab", "name": "综合实验楼", "base_power": 190.0, "max_power": 260.0}
        ]
        # 初始化各楼栋的初始累计电量 (kWh)
        self.cumulative_energy = {
            b["id"]: round(random.uniform(8000.0, 15000.0), 2) for b in self.buildings
        }

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f'[SensorDevice] 校园能源模拟设备连接 MQTT 代理成功! (rc={rc})')
            client.subscribe(config.TOPIC_COMMAND)
            print(f'[SensorDevice] 已成功订阅控制主题: {config.TOPIC_COMMAND}')
        else:
            print(f'[SensorDevice] 连接失败，返回码 {rc}')

    def on_publish(self, client, userdata, mid):
        pass

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            print(f'[SensorDevice] 收到来自 {msg.topic} 的控制/调度指令: {payload}')
            data = json.loads(payload)
            if 'action' in data:
                action = data['action']
                target = data.get('target', 'all')
                val = data.get('value', 'N/A')
                print(f'==> [能源联动执行] 目标: {target}, 动作: {action}, 参数值: {val}')
        except Exception as e:
            print(f'[SensorDevice] 解析下发消息出错: {e}')

    def generate_energy_data(self):
        """生成包含三类校园建筑的组合能耗遥测数据"""
        objects_data = []
        for b in self.buildings:
            # 模拟实时用电功率波动 (kW)
            power = round(b["base_power"] + random.uniform(-15.0, 40.0), 2)

            # 随时间推移累加电量 (kWh)，假设每5秒上报一次，换算成小时增量
            energy_increment = round(power * (5.0 / 3600.0), 4)
            self.cumulative_energy[b["id"]] = round(self.cumulative_energy[b["id"]] + energy_increment, 2)

            # 异常/功率超限告警判定逻辑
            # 当实时功率超过预设安全阈值（例如最大额定功率的 85%）时，触发告警
            warning_threshold = b["max_power"] * 0.85
            status = "normal"
            if power > warning_threshold:
                status = "power_overload_warning"

            objects_data.append({
                "device_id": b["id"],
                "building_name": b["name"],
                "power_kw": power,
                "energy_kwh": self.cumulative_energy[b["id"]],
                "threshold_kw": round(warning_threshold, 2),
                "status": status
            })

        return {
            "timestamp": int(time.time()),
            "objects": objects_data
        }

    def start(self):
        print(f'[SensorDevice] 正在连接至云端代理 {config.BROKER_HOST}:{config.BROKER_PORT}...')
        try:
            self.client.connect(config.BROKER_HOST, config.BROKER_PORT, keepalive=60)
            self.client.loop_start()

            print(f'[SensorDevice] 开始定时向主题 {config.TOPIC_TELEMETRY} 上报零碳校园能耗数据...')

            while True:
                data = self.generate_energy_data()
                payload = json.dumps(data)
                self.client.publish(config.TOPIC_TELEMETRY, payload, qos=1)
                print(f'[已上报能耗数据] 包含 {len(data["objects"])} 类建筑 | 时间戳: {data["timestamp"]}')
                time.sleep(5)
        except KeyboardInterrupt:
            print('[SensorDevice] 正在停止能源模拟设备...')
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            print(f'[SensorDevice] 连接发生错误: {e}')


if __name__ == '__main__':
    sensor_device = CampusEnergySensorDevice()
    sensor_device.start()
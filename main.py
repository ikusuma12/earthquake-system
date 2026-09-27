import asyncio
import json
import logging
import websockets
import asyncssh
import aiohttp  # APIリクエスト用に追加

# --- 設定値 ---

# 常時シャットダウン対象（メインPCやNASなど、固定IPの機器）

#本番URL
WS_URL = "wss://api.p2pquake.net/v2/ws"

#サンドボックス
# WS_URL = "wss://api-realtime-sandbox.p2pquake.net/v2/ws"
THRESHOLD_SCALE = 45 # 45 = 5弱

# 千葉市の自宅環境を想定した地域指定
TARGET_REGIONS = ["千葉県", "千葉県北西部", "千葉県北東部", "千葉県南部"]

# TARGET_REGIONS = []

# ミニPC上で稼働している自作APIのエンドポイント
API_URL_MACHINE_LIST = "http://192.168.0.240:9000/api/machine/list"

DEVICES_ALWAYS_KILL = [
    {
        "name": "UGREEN_NAS",
        "host": "192.168.0.45",
        "port": 22,
        "user": "msy2000wada",
        "pass": "Nmgw6990",
        "command": "echo Nmgw6990 | sudo -S shutdown -h now"
        # "command" : "echo SSH_Test_OK"
    }
]


processed_eews = set()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def get_active_machines_configs():
    """
    APIを1回だけ叩き、稼働中(power: on)のPCをすべて見つけて
    SSH接続設定のリストをまとめて返す。
    """
    configs = []
    # 今回APIで状態をチェックしたい対象マシンのリスト
    target_machines = ["torrent", "north", "terra"]
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL_MACHINE_LIST, timeout=1.0) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # 取得した配列データをループ処理
                    for machine in data:
                        m_id = machine.get("machine_id")
                        # 対象マシンかつ、電源ONの場合のみリストに追加
                        if m_id in target_machines and machine.get("power") == "on":
                            logger.info(f"[{m_id}] APIにて稼働(power: on)を確認しました。")
                            
                            configs.append({
                                "name": m_id,  # torrent, north, terra が正しく入る
                                "host": machine.get("ip_address"),
                                "port": machine.get("ssh_port", 22),
                                "user": "msy20",
                                "key_path": "/home/msy20minipc/.ssh/id_ed25519",
                                "command": "shutdown /h"
                                # "command" : "echo SSH_Test_OK"
                            })
                    return configs
        return configs
    except asyncio.TimeoutError:
        logger.warning("[API] タイムアウト。PC群はスキップします。")
        return []
    except Exception as e:
        logger.error(f"[API] 通信エラー: {e}")
        return []

async def execute_ssh_command(device):
    """各デバイスに対してSSHでコマンドを実行する"""
    try:
        # portの指定がない場合はデフォルトの22を使用
        port = device.get('port', 22)
        logger.info(f"[{device['name']}] へ接続中... ({device['host']}:{port})")
        
        # 共通の接続パラメーター
        connect_params = {
            "host": device['host'],
            "port": port,
            "username": device['user'],
            "known_hosts": None
        }
        
        # 辞書の中身を見て認証方式を切り替える
        if 'pass' in device:
            connect_params['password'] = device['pass']  # パスワード認証
        elif 'key_path' in device:
            connect_params['client_keys'] = [device['key_path']]  # 公開鍵認証
            
        # **connect_params で辞書を展開して引数として渡す
        async with asyncssh.connect(**connect_params) as conn:
            result = await conn.run(device['command'])
            logger.info(f"[{device['name']}] 実行完了: {result.stdout.strip() or 'No output'}")
            
    except Exception as e:
        logger.error(f"[{device['name']}] 実行失敗: {e}")



async def emergency_shutdown_sequence():
    """並列シャットダウンシーケンス"""
    logger.warning("!!! 緊急シャットダウンシーケンス開始 !!!")
    
    # 1. 常時シャットダウン対象（NAS等）をタスクに追加
    tasks = [execute_ssh_command(device) for device in DEVICES_ALWAYS_KILL]
    
    # 2. APIを1回だけ叩いて、稼働中のPCすべての設定を取得しタスクに追加
    active_pc_configs = await get_active_machines_configs()
    for pc_config in active_pc_configs:
        tasks.append(execute_ssh_command(pc_config))

    # 3. 全タスクを同時並行で発射
    await asyncio.gather(*tasks)
    logger.info("対象デバイスへの緊急コマンド送信が完了しました。")


async def process_eew(data):
    """緊急地震速報のペイロード評価"""
    eew_id = data.get("earthquake", {}).get("id")
    if not eew_id or eew_id in processed_eews:
        return

    areas = data.get("areas", [])
    should_trigger = False
    max_scale = 0

    for area in areas:
        area_name = area.get("name", "")
        scale_from = area.get("scaleFrom", 0) 
        
        if TARGET_REGIONS and not any(r in area_name for r in TARGET_REGIONS):
            continue

        max_scale = max(max_scale, scale_from)
        if scale_from >= THRESHOLD_SCALE:
            should_trigger = True
            break

    if should_trigger:
        logger.warning(f"【発火条件到達】対象地域で震度5弱以上を予測 (最大予測スケール: {max_scale})")
        processed_eews.add(eew_id)
        asyncio.create_task(emergency_shutdown_sequence())

async def ws_listener():
    """WebSocket常時接続ループ"""
    while True:
        try:
            # P2Pquake本番環境のWebSocketは ping/pong がなくても安定しやすいですが、
            # ゾンビ接続対策として ping_interval を設定しておくとより強固です。
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as websocket:
                logger.info("WebSocket接続成功。データ待機中...")
                async for message in websocket:
                    data = json.loads(message)
                    # code = data.get("code")
                    # logger.info(f"データを受信しました (Code: {code})")

                    # if code == 551:
                    #     logger.warning("【テスト発火】551を受信したため、強制的にシャットダウンシーケンスを起動します！")
                    #     asyncio.create_task(emergency_shutdown_sequence())
                    if data.get("code") == 556:
                        await process_eew(data)
        except websockets.ConnectionClosed:
            logger.warning("切断されました。5秒後に再接続します...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"予期せぬエラー: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(ws_listener())
    except KeyboardInterrupt:
        logger.info("システムを終了します。")
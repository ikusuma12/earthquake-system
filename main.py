import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
import aiohttp
import asyncssh

# --- 設定値 ---

# 強震モニタ EEW エンドポイント（※http必須）
KMONI_BASE_URL = "http://www.kmoni.bosai.go.jp/webservice/hypo/eew/"
JST = timezone(timedelta(hours=9))

# 発火対象の最大予測震度（5弱以上で発火）
TRIGGER_SCALES = ["5弱", "5強", "6弱", "6強", "7"]

# 震源地フィルタ（千葉市自宅への揺れを考慮し、近郊震源を対象に設定）
# 空リスト [] に設定した場合は、全国どこでも予測震度5弱以上で発火します
TARGET_REGIONS = ["千葉", "東京", "神奈川", "埼玉", "茨城", "房総", "東京湾"]

# ミニPC上で稼働している自作APIのエンドポイント
API_URL_MACHINE_LIST = "http://127.0.0.1:9000/api/machine/list"

# 常時シャットダウン対象（NAS等、固定設定の機器）
DEVICES_ALWAYS_KILL = [
    {
        "name": "UGREEN_NAS",
        "host": "192.168.0.45",
        "port": 22,
        "user": "msy2000wada",
        "pass": "Nmgw6990",
        "command": "echo Nmgw6990 | sudo -S shutdown -h now"
    }
]

# 処理済み地震IDのキャッシュ（同一地震での重複発火防止）
processed_eews = set()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


async def get_active_machines_configs():
    """
    自作APIを1回だけ叩き、稼働中(power: on)のPCをすべて検出して
    SSH接続設定のリストをまとめて返す。
    """
    configs = []
    target_machines = ["torrent", "north", "terra"]
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_URL_MACHINE_LIST, timeout=1.0) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for machine in data:
                        m_id = machine.get("machine_id")
                        if m_id in target_machines and machine.get("power") == "on":
                            logger.info(f"[{m_id}] APIにて稼働(power: on)を確認しました。")
                            
                            # ポタ電保護のないtorrentはメモリ書き込みを省いて最速強制シャットダウン
                            # ポタ電保護のあるnorth, terraは作業状態保持のため休止状態へ
                            if m_id == "torrent":
                                cmd = "shutdown /s /f /t 0"
                            else:
                                cmd = "shutdown /h"
                            
                            configs.append({
                                "name": m_id,
                                "host": machine.get("ip_address"),
                                "port": machine.get("ssh_port", 22),
                                "user": "msy20",
                                "key_path": "/home/msy20minipc/.ssh/id_ed25519",
                                "command": cmd
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
    """各デバイスに対してSSHでコマンドを並列実行する"""
    try:
        port = device.get('port', 22)
        logger.info(f"[{device['name']}] へ接続中... ({device['host']}:{port})")
        
        connect_params = {
            "host": device['host'],
            "port": port,
            "username": device['user'],
            "known_hosts": None
        }
        
        # 認証方式の切り替え（パスワード / 公開鍵）
        if 'pass' in device:
            connect_params['password'] = device['pass']
        elif 'key_path' in device:
            connect_params['client_keys'] = [device['key_path']]
            
        async with asyncssh.connect(**connect_params) as conn:
            result = await conn.run(device['command'])
            logger.info(f"[{device['name']}] 実行完了: {result.stdout.strip() or 'No output'}")
            
    except Exception as e:
        logger.error(f"[{device['name']}] 実行失敗: {e}")


async def emergency_shutdown_sequence():
    """全デバイス並列シャットダウンシーケンス"""
    logger.warning("!!! 緊急シャットダウンシーケンス開始 !!!")
    
    # 1. 常時シャットダウン対象（NAS等）
    tasks = [execute_ssh_command(device) for device in DEVICES_ALWAYS_KILL]
    
    # 2. 自作APIから稼働中PCを動的に取得してタスクに追加
    active_pc_configs = await get_active_machines_configs()
    for pc_config in active_pc_configs:
        tasks.append(execute_ssh_command(pc_config))

    # 3. 全タスクを同時並行で一斉送信
    await asyncio.gather(*tasks)
    logger.info("対象デバイスへの緊急コマンド送信が完了しました。")


async def process_eew(data):
    """強震モニタEEWデータの判定・トリガー処理"""
    report_id = data.get("report_id")
    is_cancel = data.get("is_cancel") == "true"
    
    # 地震ID欠落、キャンセル報、または既に発火済みの地震は無視
    if not report_id or is_cancel or report_id in processed_eews:
        return

    scale = data.get("calcintensity", "")      # 例: "5弱", "4"
    region = data.get("region_name", "")       # 例: "千葉県北西部"
    report_num = data.get("report_num", "1")
    alert_flg = data.get("alertflg", "")       # "警報" または "予報"

    logger.info(f"【EEW受信 第{report_num}報】震源: {region} | 最大予測震度: {scale} | 区分: {alert_flg}")

    # 1. 震度条件チェック
    if scale not in TRIGGER_SCALES:
        return

    # 2. 地域フィルタチェック
    if TARGET_REGIONS and not any(r in region for r in TARGET_REGIONS):
        logger.info(f"設定対象外の震源地のためスキップします: {region}")
        return

    # 発火条件達成
    logger.warning(f"🚨【発火条件到達】対象地域({region})で最大予測震度 {scale} を検知！")
    processed_eews.add(report_id)
    
    # 即座に非同期でシャットダウンタスクを発射
    asyncio.create_task(emergency_shutdown_sequence())


async def kmoni_poller():
    """強震モニタ 1秒周期非同期HTTPポーリングループ"""
    connector = aiohttp.TCPConnector(limit=5, keepalive_timeout=30)
    heartbeat_count = 0  # 生存確認カウンター
    
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("強震モニタ EEW リアルタイムポーリング監視を開始しました。")
        
        while True:
            target_dt = datetime.now(JST) - timedelta(seconds=2)
            timestamp_str = target_dt.strftime("%Y%m%d%H%M%S")
            url = f"{KMONI_BASE_URL}{timestamp_str}.json"

            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=0.9)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        
                        # EEW発令時
                        if data.get("result", {}).get("message") == "":
                            await process_eew(data)
                        else:
                            # 平常時：60秒に1回だけ生存確認ログを出力
                            heartbeat_count += 1
                            if heartbeat_count >= 60:
                                logger.info(f"[Heartbeat] 正常監視中 (直近取得: {timestamp_str})")
                                heartbeat_count = 0
                                
                    elif resp.status == 404:
                        pass

            except asyncio.TimeoutError:
                pass
            except aiohttp.ClientError as e:
                logger.warning(f"HTTP通信一時エラー: {e}")
            except Exception as e:
                logger.error(f"予期せぬエラー: {e}")

            await asyncio.sleep(1.0)


if __name__ == "__main__":
    try:
        asyncio.run(kmoni_poller())
    except KeyboardInterrupt:
        logger.info("システムを終了します。")
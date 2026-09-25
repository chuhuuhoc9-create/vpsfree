import paho.mqtt.client as mqtt
import json
import time
import threading
import uuid
import ssl
import pyfiglet
import os
from termcolor import colored
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

running = True
cookies = []
idbox = []
message = ""
delay = 0

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def banner():
    try:
        width = os.get_terminal_size().columns
    except OSError:
        width = 80

    info = [
        ("Dev:", "Chử Hữu Học"),
        ("Facebook:", "https://www.facebook.com/share/1C5aWHp3mu/"),
        ("Zalo:", "0972487211")
    ]

    max_label = max(len(label) for label, _ in info)
    info_lines = [f"{label.ljust(max_label + 2)}{value}" for label, value in info]
    block_width = max(len(line) for line in info_lines)
    left_pad = max(0, (width - block_width) // 2)
    pad = " " * left_pad
    print("\n" * 3)
    for line in info_lines:
        print(colored(pad + line, 'white'))
    print("\n" * 2)
    
def log(message):
    print(message)
    
def get_token(cookie):
    parts = cookie.split(';')
    c_user = None
    xs = None
    
    for part in parts:
        part = part.strip()
        if part.startswith('c_user='):
            c_user = part.split('=')[1]
        elif part.startswith('xs='):
            xs = part.split('=')[1]
    
    return f"{c_user}|{xs}" if c_user and xs else cookie

def create_mqtt(cookie):
    try:
        token = get_token(cookie)
        client_id = f"mqttwsclient_{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(
            client_id=client_id,
            transport="websockets",
            protocol=mqtt.MQTTv31
        )
        
        client.username_pw_set(
            username=json.dumps({
                "u": token.split('|')[0] if '|' in token else token,
                "s": 1,
                "chat_on": True,
                "fg": True,
                "d": str(uuid.uuid4()),
                "ct": "websocket",
                "mqtt_sid": "",
                "aid": 219994525426954,
                "st": [],
                "pm": [],
                "cp": 3,
                "ecp": 10,
                "pack": []
            }),
            password=""
        )
        
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
        
        client.ws_set_options(path="/chat", headers={
            "Cookie": cookie,
            "Origin": "https://www.facebook.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        
        client.connect("edge-chat.facebook.com", 443, 60)
        client.loop_start()
        
        time.sleep(2)
        
        return client, token
        
    except Exception as e:
        return None, None

def send_message(client, token, thread_id, message, cookie_list, idbox_list):
    while running:
        try:
            msg_id = str(int(time.time() * 1000))
            
            payload = {
                "body": message,
                "msgid": msg_id,
                "sender_fbid": token.split('|')[0] if '|' in token else token,
                "to": thread_id,
                "offline_threading_id": msg_id
            }
            
            topic = f"/send_message2"
            
            result = client.publish(
                topic,
                json.dumps(payload),
                qos=1
            )
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log(f"Gửi tin nhắn thành công tới: {idbox_list}")
                time.sleep(delay)
            else:
                log(f"Gửi tin nhắn thất bại tới: {idbox_list}")
                time.sleep(delay)
            
        except Exception as e:
            log(f"Lỗi: {e}")
            time.sleep(delay)

def worker(cookie_list, cookie):
    global running, idbox, message
    
    client, token = create_mqtt(cookie)
    if not client or not token:
        log(f"Cookie {cookie_list + 1}: Không thể kết nối tới MQTT")
        return
    
    threads = []
    for idbox_list in idbox:
        thread = threading.Thread(
            target=send_message,
            args=(client, token, idbox_list, message, cookie_list, idbox_list)
        )
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    try:
        while running:
            time.sleep(1)
    except:
        pass
    
    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass

def main():
    clear()
    banner()
    global running, cookies, idbox, message, delay
    
    log("Nhập Cookie (nhập 'done' để kết thúc)")
    count = 1
    while True:
        cookie = input(f"> ").strip()
        if cookie.lower() == 'done':
            break
        if cookie:
            cookies.append(cookie)
            count += 1
    
    if not cookies:
        log("Không có cookie nào được nhập")
        return
    log(f"Đã lưu {len(cookies)} cookie\n")
    
    log("Nhập ID Box (nhập 'done' để kết thúc)")
    count = 1
    while True:
        idbox_list = input(f"> ").strip()
        if idbox_list.lower() == 'done':
            break
        if idbox_list:
            idbox.append(idbox_list)
            count += 1
    
    if not idbox:
        log("Không có ID Box nào được nhập")
        return
    log(f"Đã lưu {len(idbox)} ID Box\n")
    
    file_path = input("Nhập file txt: ").strip()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            message = f.read().strip()
        if not message:
            log("File không có nội dung")
            return
        log(f"Đã lưu file txt\n")
    except Exception as e:
        log(f"Lỗi: {e}")
        return
    
    try:
        delay_input = float(input("Nhập delay: ").strip())
        delay = max(0.1, delay_input)
    except:
        delay = 15
    
    log("Đã bắt đầu gửi tin nhắn")
    
    try:
        threads = []
        for cookie_list, cookie in enumerate(cookies):
            thread = threading.Thread(target=worker, args=(cookie_list, cookie))
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        while running:
            time.sleep(1)
        
    except KeyboardInterrupt:
        running = False
        log("\n\nĐã dừng tool")

if __name__ == "__main__":
    main()

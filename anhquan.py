import paho.mqtt.client as mqtt
import json
import time
import threading
import uuid
import ssl
import os
from termcolor import colored
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

running = True
cookies = []
idbox = []
message = ""
delay = 0
active_worker = {}
lock = threading.Lock()

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def banner():
    try:
        width = os.get_terminal_size().columns
    except OSError:
        width = 80
    info = [
        ("Dev:", "Anh Quan"),
        ("Facebook:", "https://www.facebook.com/imnezha"),
        ("Zalo:", "0345095628")
    ]
    max_label = max(len(l) for l, _ in info)
    lines = [f"{l.ljust(max_label + 2)}{v}" for l, v in info]
    pad = " " * max(0, (width - max(len(x) for x in lines)) // 2)
    print("\n" * 2)
    for line in lines:
        print(colored(pad + line, "white"))
    print("\n")

def get_uid(cookie):
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("c_user="):
            return part.split("=", 1)[1]
    return None

def get_token(cookie):
    c_user, xs = None, None
    for part in cookie.split(";"):
        part = part.strip()
        if part.startswith("c_user="):
            c_user = part.split("=")[1]
        elif part.startswith("xs="):
            xs = part.split("=")[1]
    return f"{c_user}|{xs}" if c_user and xs else cookie

def connect_mqtt(cookie):
    try:
        token = get_token(cookie)
        uid = token.split("|")[0]
        client = mqtt.Client(
            client_id=f"mqttwsclient_{uuid.uuid4().hex[:8]}",
            transport="websockets",
            protocol=mqtt.MQTTv31
        )
        client.username_pw_set(
            username=json.dumps({
                "u": uid,
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
        client.ws_set_options(
            path="/chat",
            headers={
                "Cookie": cookie,
                "Origin": "https://www.facebook.com",
                "User-Agent": "Mozilla/5.0"
            }
        )
        client.connect("edge-chat.facebook.com", 443, 60)
        client.loop_start()
        time.sleep(2)
        return client, uid
    except:
        return None, None

def send_message(client, uid, box_id, msg, cookie_id, cookie):
    fail = 0
    while running and cookie_id in active_worker:
        try:
            payload = {
                "body": msg,
                "msgid": str(int(time.time() * 1000)),
                "sender_fbid": uid,
                "to": box_id,
                "offline_threading_id": str(int(time.time() * 1000))
            }
            res = client.publish("/send_message2", json.dumps(payload), qos=1)
            if res.rc != mqtt.MQTT_ERR_SUCCESS:
                fail += 1
            else:
                fail = 0
            if fail >= 3:
                with lock:
                    active_worker.pop(cookie_id, None)
                print(f"\nCookie {get_uid(cookie)} die, loại bỏ")
                break
            time.sleep(delay)
        except:
            fail += 1
            if fail >= 3:
                with lock:
                    active_worker.pop(cookie_id, None)
                print(f"\nCookie {get_uid(cookie)} die, loại bỏ")
                break
            time.sleep(delay)

def worker(cookie_id, cookie):
    client, uid = connect_mqtt(cookie)
    if not client:
        with lock:
            active_worker.pop(cookie_id, None)
        print(f"\nCookie {get_uid(cookie)} die, loại bỏ")
        return
    for box in idbox:
        threading.Thread(
            target=send_message,
            args=(client, uid, box, message, cookie_id, cookie),
            daemon=True
        ).start()
    while running and cookie_id in active_worker:
        time.sleep(1)
    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass

def validate_cookie(cookie):
    client, _ = connect_mqtt(cookie)
    if not client:
        return False
    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass
    return True

def monitor_cookie():
    while running:
        time.sleep(20)
        with lock:
            items = list(active_worker.items())
        for cid, ck in items:
            if not validate_cookie(ck):
                with lock:
                    active_worker.pop(cid, None)
                print(f"\nCookie {get_uid(ck)} die, loại bỏ")

def start_spam():
    print("\nĐã bắt đầu gửi tin nhắn")
    for i, ck in enumerate(cookies):
        cid = f"ck_{i}_{int(time.time())}"
        with lock:
            active_worker[cid] = ck
        threading.Thread(target=worker, args=(cid, ck), daemon=True).start()
        time.sleep(0.5)
    threading.Thread(target=monitor_cookie, daemon=True).start()

def add_cookie():
    print("\nNhập cookie mới (done để kết thúc):")
    new = []
    while True:
        ck = input("> ").strip()
        if ck.lower() == "done":
            break
        if ck:
            new.append(ck)
    added = 0
    for ck in new:
        if not validate_cookie(ck):
            print(f"Cookie {get_uid(ck)} die, loại bỏ")
            continue
        cid = f"ck_new_{int(time.time())}_{added}"
        with lock:
            active_worker[cid] = ck
            cookies.append(ck)
        threading.Thread(target=worker, args=(cid, ck), daemon=True).start()
        added += 1
        time.sleep(0.5)
    print(f"\nĐã thêm {added}/{len(new)} cookie")

def menu():
    global running
    while running:
        print("\n1. Thêm cookie\n2. Thoát\n")
        c = input("Chọn: ").strip()
        if c == "1":
            add_cookie()
        elif c == "2":
            running = False
            print("\nĐã dừng tool")
        else:
            print("Sai lựa chọn")

def main():
    global delay, message
    clear()
    banner()
    print("Nhập Cookie (done để kết thúc)")
    while True:
        ck = input("> ").strip()
        if ck.lower() == "done":
            break
        if ck:
            cookies.append(ck)
    if not cookies:
        return
    print("\nNhập ID Box (done để kết thúc)")
    while True:
        b = input("> ").strip()
        if b.lower() == "done":
            break
        if b:
            idbox.append(b)
    if not idbox:
        return
    path = input("Nhập file txt: ").strip()
    with open(path, "r", encoding="utf-8") as f:
        message = f.read().strip()
    if not message:
        return
    try:
        delay = max(0.1, float(input("Nhập delay: ").strip()))
    except:
        delay = 15
    start_spam()
    menu()

if __name__ == "__main__":
    main()
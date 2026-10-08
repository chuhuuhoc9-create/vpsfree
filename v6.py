import threading
import time
import re
import requests
import os
import sys
import json
import hashlib
import random
import string
import paho.mqtt.client as mqtt
import ssl
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse


def treo_generate_offline_threading_id() -> str:
    """Generate offline threading ID for MQTT messages"""
    ret = int(time.time() * 1000)
    value = random.randint(0, 4294967295)
    binary_str = format(value, "022b")[-22:]
    msgs = bin(ret)[2:] + binary_str
    return str(int(msgs, 2))

def treo_json_minimal(data):
    """Minimal JSON serialization"""
    return json.dumps(data, separators=(",", ":"))



def extract_uid_from_cookie(cookie):
    """Lấy UID từ cookie Facebook"""
    try:
        # Tìm c_user trong cookie
        match = re.search(r'c_user=(\d+)', cookie)
        if match:
            return match.group(1)
    except:
        pass
    return None

def check_cookie_basic(cookie):
    """Kiểm tra cookie cơ bản xem có đúng format không"""
    if not cookie:
        return False
    
    # Kiểm tra có chứa các thành phần cơ bản của cookie Facebook
    required_parts = ['c_user=', 'xs=']
    for part in required_parts:
        if part not in cookie:
            return False
    
    # Kiểm tra có lấy được UID không
    uid = extract_uid_from_cookie(cookie)
    if not uid:
        return False
        
    return True



class TreoMQTTSender:
    def __init__(self, fb_data):
        self.fb_data = fb_data
        self.mqtt = None
        self.ws_req_number = 0
        self.syncToken = None
        self.lastSeqID = fb_data.get("lastSeqID", "0")
        self.req_callbacks = {}
        self.cookie_hash = hashlib.md5(fb_data['cookie'].encode()).hexdigest()
        self.last_cleanup = time.time()
        self.success_count = 0
        self.is_connected = False
        self.connection_attempts = 0
        self.last_attempt = 0

    def cleanup_memory(self):
        """Dọn dẹp bộ nhớ"""
        current_time = time.time()
        if current_time - self.last_cleanup > 3600:
            self.req_callbacks.clear()
            self.last_cleanup = current_time

    def on_connect(self, client, userdata, flags, rc):
        """Callback khi kết nối MQTT thành công"""
        if rc == 0:
            self.is_connected = True
            self.connection_attempts = 0
            print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Connected")
            
            # Subscribe topics
            topics = [("/t_ms", 0)]
            client.subscribe(topics)

            # Publish queue
            queue = {
                "sync_api_version": 10,
                "max_deltas_able_to_process": 1000,
                "delta_batch_size": 500,
                "encoding": "JSON",
                "entity_fbid": self.fb_data['user_id']
            }

            if self.syncToken is None:
                topic = "/messenger_sync_create_queue"
                queue["initial_titan_sequence_id"] = self.lastSeqID
                queue["device_params"] = None
            else:
                topic = "/messenger_sync_get_diffs"
                queue["last_seq_id"] = self.lastSeqID
                queue["sync_token"] = self.syncToken

            client.publish(
                topic,
                treo_json_minimal(queue),
                qos=1,
                retain=False,
            )
        else:
            self.is_connected = False
            print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Connection failed")

    def on_disconnect(self, client, userdata, rc):
        """Callback khi mất kết nối MQTT"""
        self.is_connected = False
        print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Disconnected")
        
        if rc != 0:
            time.sleep(5)
            try:
                client.reconnect()
            except:
                pass

    def connect(self):
        """Kết nối MQTT"""
        if self.connection_attempts >= 3:
            print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Too many connection attempts")
            return False
            
        current_time = time.time()
        if current_time - self.last_attempt < 10:
            time.sleep(10 - (current_time - self.last_attempt))
            
        self.last_attempt = time.time()
        self.connection_attempts += 1
        
        try:
            session_id = random.randint(1, 2 ** 53)
            user = {
                "u": self.fb_data["user_id"],
                "s": session_id,
                "chat_on": treo_json_minimal(True),
                "fg": False,
                "d": ''.join(random.choices(string.ascii_lowercase + string.digits, k=8)) + '-' +
                     ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' +
                     ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' +
                     ''.join(random.choices(string.ascii_lowercase + string.digits, k=4)) + '-' +
                     ''.join(random.choices(string.ascii_lowercase + string.digits, k=12)),
                "ct": "websocket",
                "aid": 219994525426954,
                "mqtt_sid": "",
                "cp": 3,
                "ecp": 10,
                "st": ["/t_ms", "/messenger_sync_get_diffs", "/messenger_sync_create_queue"],
                "pm": [],
                "dc": "",
                "no_auto_fg": True,
                "gas": None,
                "pack": [],
            }

            host = f"wss://edge-chat.messenger.com/chat?region=eag&sid={session_id}"
            options = {
                "client_id": "mqttwsclient",
                "username": treo_json_minimal(user),
                "clean": True,
                "ws_options": {
                    "headers": {
                        "Cookie": self.fb_data['cookie'],
                        "Origin": "https://www.messenger.com",
                        "User-Agent": "Mozilla/5.0 (Linux; Android 9; SM-G973U Build/PPR1.180610.011) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Mobile Safari/537.36",
                        "Referer": "https://www.messenger.com/",
                        "Host": "edge-chat.messenger.com",
                    },
                },
                "keepalive": 10,
            }

            self.mqtt = mqtt.Client(
                client_id="mqttwsclient",
                clean_session=True,
                protocol=mqtt.MQTTv31,
                transport="websockets",
            )

            self.mqtt.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLSv1_2)
            self.mqtt.on_connect = self.on_connect
            self.mqtt.on_disconnect = self.on_disconnect
            self.mqtt.username_pw_set(username=options["username"])

            parsed_host = urlparse(host)
            self.mqtt.ws_set_options(
                path=f"{parsed_host.path}?{parsed_host.query}",
                headers=options["ws_options"]["headers"],
            )

            self.mqtt.connect(
                host=options["ws_options"]["headers"]["Host"],
                port=443,
                keepalive=options["keepalive"],
            )

            self.mqtt.loop_start()
            
            for _ in range(10):
                if self.is_connected:
                    return True
                time.sleep(0.5)
                
            return False
            
        except Exception as e:
            print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Connection error")
            return False

    def send_message(self, thread_id, message):
        """Gửi tin nhắn qua MQTT"""
        if not self.is_connected:
            if not self.connect():
                return False

        self.cleanup_memory()
        self.ws_req_number += 1

        task_payload = {
            "thread_id": thread_id,
            "otid": treo_generate_offline_threading_id(),
            "source": 0,
            "send_type": 1,
            "text": message,
            "initiating_source": 1
        }
        
        content = {
            "app_id": "2220391788200892",
            "payload": {
                "tasks": [{
                    "label": 46,
                    "payload": json.dumps(task_payload, separators=(",", ":")),
                    "queue_name": "send_message",
                    "task_id": self.ws_req_number,
                    "failure_count": None,
                }],
                "epoch_id": treo_generate_offline_threading_id(),
                "version_id": "7214102258676893",
            },
            "request_id": self.ws_req_number,
            "type": 3
        }

        content["payload"] = json.dumps(content["payload"], separators=(",", ":"))

        try:
            self.mqtt.publish(
                topic="/ls_req",
                payload=json.dumps(content, separators=(",", ":")),
                qos=1,
                retain=False,
            )
            self.success_count += 1
            return True
        except Exception as e:
            print(f"[MQTT] {self.fb_data.get('display_name', 'Unknown')}: Send error")
            self.is_connected = False
            return False

    def disconnect(self):
        """Ngắt kết nối MQTT"""
        if self.mqtt:
            try:
                self.mqtt.disconnect()
                self.mqtt.loop_stop()
            except:
                pass
        self.is_connected = False
        self.cleanup_memory()


class Messenger:
    def __init__(self, cookie, account_number=0):
        self.cookie = cookie
        self.account_number = account_number
        self.user_id = self.extract_uid()
        self.active = True
        self.message_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.mqtt_sender = None
        self.display_name = f"ACC{account_number}"

    def extract_uid(self):
        """Chỉ lấy UID từ cookie"""
        try:
            match = re.search(r'c_user=(\d+)', self.cookie)
            if match:
                return match.group(1)
        except:
            pass
        return "Unknown"

    def init_mqtt(self):
        """Khởi tạo MQTT sender"""
        if self.mqtt_sender is None:
            fb_data = {
                "user_id": self.user_id,
                "display_name": self.display_name,
                "cookie": self.cookie,
                "lastSeqID": "0"
            }
            self.mqtt_sender = TreoMQTTSender(fb_data)
            return self.mqtt_sender.connect()
        return self.mqtt_sender.is_connected

    def gui_tn(self, recipient_id, message):
        """Gửi tin nhắn - sử dụng MQTT"""
        if not self.active:
            return False
            
        self.message_count += 1
        
        if not self.init_mqtt():
            self.fail_count += 1
            return False
        
        success = self.mqtt_sender.send_message(recipient_id, message)
        
        if success:
            self.success_count += 1
        else:
            self.fail_count += 1
            
        return success

    def disconnect(self):
        """Ngắt kết nối"""
        if self.mqtt_sender:
            self.mqtt_sender.disconnect()
        self.active = False



class SpamManager:
    def __init__(self):
        self.accounts = []
        self.threads = []
        self.is_running = True
        self.recipient_ids = []
        self.message = ""
        self.delay = 0
        self.total_accounts = 0
        
    def clear_screen(self):
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def show_banner(self):
        banner = """
╔══════════════════════════════════════════════════════════════════╗
║              TOOL MESSENGER - BY ANHHOANG               ║
║              Phiên bản Premium- tiền by anhhoang           ║
╚══════════════════════════════════════════════════════════════════╝
        """
        print(banner)
    
    def show_accounts_list(self):
        """Hiển thị danh sách tài khoản dạng bảng"""
        if not self.accounts:
            print("  📭 Chưa có tài khoản nào")
            return
            
        print("\n👥 DANH SÁCH TÀI KHOẢN:")
        print("=" * 70)
        print(f"{'STT':<4} {'TÀI KHOẢN':<10} {'UID':<20} {'TRẠNG THÁI':<15} {'ĐÃ GỬI':<15}")
        print("=" * 70)
        
        for idx, acc in enumerate(self.accounts, 1):
            status = "✅ Hoạt động" if acc.active else "❌ Tạm dừng"
            stats = f"✓{acc.success_count} ✗{acc.fail_count}"
            print(f"{idx:<4} {acc.display_name:<10} {acc.user_id:<20} {status:<15} {stats:<15}")
        
        print("=" * 70)

    def load_message_from_file(self, filename="ngon.txt"):
        """Đọc nội dung tin nhắn từ file .txt"""
        try:
            # Kiểm tra file tồn tại
            if not os.path.exists(filename):
                print(f"❌ Không tìm thấy file {filename}")
                return False
            
            # Đọc nội dung file
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not content.strip():
                print(f"❌ File {filename} rỗng")
                return False
            
            self.message = content
            print(f"✅ Đã đọc {len(self.message)} ký tự từ file {filename}")
            
            # Hiển thị preview
            preview = self.message[:100] + "..." if len(self.message) > 100 else self.message
            print(f"📝 Preview: {preview}")
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi đọc file: {e}")
            return False

    def setup_spam(self):
        """Thiết lập spam ban đầu"""
        self.clear_screen()
        self.show_banner()
        
        print("\n" + "═" * 70)
        print("🚀 THIẾT LẬP SPAM")
        print("═" * 70)
        
        
        print("\n📝 BƯỚC 1: CHỌN FILE NỘI DUNG TIN NHẮN")
        print("-" * 50)
        print("Mặc định: ngon.txt (cùng thư mục)")
        print("Có thể nhập tên file khác (vd: noidung.txt, spam.txt, ...)")
        
        filename = input("\n📁 Tên file (Enter để dùng ngon.txt): ").strip()
        if not filename:
            filename = "ngon.txt"
        
        if not self.load_message_from_file(filename):
            print("\n❌ Không thể đọc nội dung từ file!")
            retry = input("Thử lại? (y/n): ").strip().lower()
            if retry == 'y':
                self.setup_spam()
                return
            else:
                return
        

        print("\n📋 BƯỚC 2: NHẬP ID NHẬN TIN")
        print("-" * 50)
        print("Nhập từng ID, gõ 'done' để kết thúc:")
        
        while True:
            uid = input(f"  ID #{len(self.recipient_ids) + 1}: ").strip()
            if uid.lower() in ['done', 'xong']:
                break
            if uid:
                self.recipient_ids.append(uid)
                print(f"  ✅ Đã thêm: {uid}")
        
        if not self.recipient_ids:
            print("❌ Phải có ít nhất 1 ID nhận tin!")
            return
        
        
        print("\n⏰ BƯỚC 3: NHẬP DELAY")
        print("-" * 50)
        while True:
            try:
                delay = int(input("Delay (giây, 0 = không delay): ").strip())
                if delay >= 0:
                    self.delay = delay
                    print(f"✅ Delay: {delay} giây")
                    break
            except:
                print("❌ Vui lòng nhập số!")
        
        # ===== BƯỚC 4: NHẬP COOKIE =====
        print("\n🔐 BƯỚC 4: NHẬP COOKIE")
        print("-" * 50)
        print("Nhập từng cookie, gõ 'done' để kết thúc:")
        
        while True:
            cookie = input(f"  Cookie #{self.total_accounts + 1}: ").strip()
            if cookie.lower() in ['done', 'xong']:
                break
                
            if cookie:
                self.total_accounts += 1
                
                # Lấy UID từ cookie
                uid = extract_uid_from_cookie(cookie)
                
                if uid:
                    # Tạo tài khoản mới
                    acc = Messenger(cookie, self.total_accounts)
                    acc.user_id = uid
                    self.accounts.append(acc)
                    print(f"  ✅ ACC{self.total_accounts}: UID {uid}")
                else:
                    print(f"  ❌ Cookie không hợp lệ (không tìm thấy UID)")
                    self.total_accounts -= 1
        
        if not self.accounts:
            print("❌ Không có tài khoản nào hợp lệ!")
            return
        
        
        self.start_spam()
        
        # Hiển thị thông tin
        self.clear_screen()
        self.show_banner()
        print("\n" + "═" * 70)
        print("✅ ĐANG CHẠY SPAM")
        print("═" * 70)
        print(f"├─ File nội dung: {filename}")
        print(f"├─ Số ký tự: {len(self.message)}")
        print(f"├─ Tổng tài khoản: {len(self.accounts)}")
        print(f"├─ ID nhận tin: {len(self.recipient_ids)}")
        print(f"└─ Delay: {self.delay} giây")
        print("═" * 70)
        
        self.show_accounts_list()
        print("\n⏳ Đang khởi động...")
        time.sleep(2)
        
        self.show_management_menu()
    
    def start_spam(self):
        """Bắt đầu spam"""
        self.is_running = True
        self.threads = []
        
        for acc in self.accounts:
            if acc.active:
                for recipient_id in self.recipient_ids:
                    thread = threading.Thread(
                        target=self.spam_worker,
                        args=(acc, recipient_id),
                        daemon=True
                    )
                    self.threads.append(thread)
                    thread.start()
        
        return True
    
    def spam_worker(self, account, recipient_id):
        """Worker gửi tin nhắn"""
        while self.is_running and account.active:
            try:
                success = account.gui_tn(recipient_id, self.message)
                
                if self.delay > 0:
                    time.sleep(self.delay)
                else:
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"[WORKER] {account.display_name}: Lỗi")
                time.sleep(1)
    
    def show_management_menu(self):
        """Menu quản lý chính"""
        while True:
            try:
                self.clear_screen()
                self.show_banner()
                
                # Thống kê
                total_sent = sum(acc.success_count for acc in self.accounts)
                total_failed = sum(acc.fail_count for acc in self.accounts)
                total_messages = sum(acc.message_count for acc in self.accounts)
                
                print("\n" + "═" * 70)
                print("📊 THỐNG KÊ")
                print("═" * 70)
                print(f"├─ Tài khoản: {len(self.accounts)}")
                print(f"├─ ID nhận: {len(self.recipient_ids)}")
                print(f"├─ Tin đã gửi: {total_messages}")
                print(f"├─ Thành công: {total_sent}")
                print(f"├─ Thất bại: {total_failed}")
                if total_messages > 0:
                    print(f"├─ Tỉ lệ: {total_sent/total_messages*100:.1f}%")
                print(f"└─ Delay: {self.delay} giây")
                print("═" * 70)
                
                # Hiển thị danh sách tài khoản
                self.show_accounts_list()
                
                # Hiển thị thông tin file nội dung
                print(f"\n📝 File nội dung: Đã tải ({len(self.message)} ký tự)")
                
                # Menu
                print("\n" + "═" * 70)
                print("🎮 MENU")
                print("═" * 70)
                print(" 1. Thêm tài khoản mới")
                print(" 2. Xóa tài khoản")
                print(" 3. Bật/Tắt tài khoản")
                print(" 4. Xem lại ID nhận tin")
                print(" 5. Thêm ID nhận tin")
                print(" 6. Xóa ID nhận tin")
                print(" 7. Xem nội dung tin nhắn")
                print(" 8. Tải lại nội dung từ file")
                print(" 9. Đổi file nội dung khác")
                print("10. Đổi delay")
                print("11. DỪNG TẤT CẢ")
                print(" 0. Refresh")
                print("═" * 70)
                
                choice = input("\n👉 Chọn: ").strip()
                
                if choice == "1":
                    self.add_account()
                elif choice == "2":
                    self.remove_account()
                elif choice == "3":
                    self.toggle_account()
                elif choice == "4":
                    self.show_recipients()
                elif choice == "5":
                    self.add_recipient()
                elif choice == "6":
                    self.remove_recipient()
                elif choice == "7":
                    self.show_message()
                elif choice == "8":
                    self.reload_message()
                elif choice == "9":
                    self.change_message_file()
                elif choice == "10":
                    self.change_delay()
                elif choice == "11":
                    self.stop_all()
                    print("\n🛑 Đã dừng!")
                    break
                elif choice == "0":
                    continue
                else:
                    print("❌ Sai lựa chọn!")
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n\n👋 Thoát...")
                self.stop_all()
                break
    
    def add_account(self):
        """Thêm tài khoản mới"""
        print("\n➕ THÊM TÀI KHOẢN")
        print("-" * 50)
        
        cookie = input("Cookie: ").strip()
        if cookie:
            uid = extract_uid_from_cookie(cookie)
            if uid:
                self.total_accounts += 1
                acc = Messenger(cookie, self.total_accounts)
                acc.user_id = uid
                self.accounts.append(acc)
                print(f"✅ Đã thêm ACC{self.total_accounts} - UID: {uid}")
                
                if self.is_running and acc.active:
                    for recipient_id in self.recipient_ids:
                        thread = threading.Thread(
                            target=self.spam_worker,
                            args=(acc, recipient_id),
                            daemon=True
                        )
                        self.threads.append(thread)
                        thread.start()
            else:
                print("❌ Cookie không hợp lệ!")
        else:
            print("❌ Cookie trống!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def remove_account(self):
        """Xóa tài khoản"""
        if not self.accounts:
            print("❌ Không có tài khoản!")
            input("Nhấn Enter...")
            return
        
        print("\n🗑️ XÓA TÀI KHOẢN")
        print("-" * 50)
        
        for idx, acc in enumerate(self.accounts, 1):
            print(f"{idx}. {acc.display_name} - {acc.user_id}")
        
        try:
            choice = int(input("\nSố tài khoản cần xóa (0 = hủy): "))
            if 1 <= choice <= len(self.accounts):
                removed = self.accounts.pop(choice - 1)
                removed.disconnect()
                print(f"✅ Đã xóa {removed.display_name}")
            elif choice != 0:
                print("❌ Sai số!")
        except:
            print("❌ Sai lựa chọn!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def toggle_account(self):
        """Bật/tắt tài khoản"""
        if not self.accounts:
            print("❌ Không có tài khoản!")
            input("Nhấn Enter...")
            return
        
        print("\n⚙️ BẬT/TẮT TÀI KHOẢN")
        print("-" * 50)
        
        for idx, acc in enumerate(self.accounts, 1):
            status = "✅" if acc.active else "❌"
            print(f"{idx}. {status} {acc.display_name} - {acc.user_id}")
        
        try:
            choice = int(input("\nSố tài khoản (0 = hủy): "))
            if 1 <= choice <= len(self.accounts):
                acc = self.accounts[choice - 1]
                acc.active = not acc.active
                state = "BẬT" if acc.active else "TẮT"
                print(f"✅ Đã {state} {acc.display_name}")
                
                if acc.active and self.is_running:
                    for recipient_id in self.recipient_ids:
                        thread = threading.Thread(
                            target=self.spam_worker,
                            args=(acc, recipient_id),
                            daemon=True
                        )
                        self.threads.append(thread)
                        thread.start()
            elif choice != 0:
                print("❌ Sai số!")
        except:
            print("❌ Sai lựa chọn!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def show_recipients(self):
        """Xem danh sách ID nhận"""
        print("\n📋 ID NHẬN TIN:")
        print("-" * 50)
        if not self.recipient_ids:
            print("  (Trống)")
        else:
            for idx, rid in enumerate(self.recipient_ids, 1):
                print(f"  {idx}. {rid}")
        print(f"\nTổng số: {len(self.recipient_ids)}")
        input("\nNhấn Enter để tiếp tục...")
    
    def add_recipient(self):
        """Thêm ID nhận tin"""
        print("\n📥 THÊM ID NHẬN TIN")
        print("-" * 50)
        
        rid = input("ID mới: ").strip()
        if rid:
            if rid not in self.recipient_ids:
                self.recipient_ids.append(rid)
                print(f"✅ Đã thêm: {rid}")
                
                if self.is_running:
                    for acc in self.accounts:
                        if acc.active:
                            thread = threading.Thread(
                                target=self.spam_worker,
                                args=(acc, rid),
                                daemon=True
                            )
                            self.threads.append(thread)
                            thread.start()
            else:
                print("⚠️ ID đã tồn tại!")
        else:
            print("❌ ID trống!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def remove_recipient(self):
        """Xóa ID nhận tin"""
        if not self.recipient_ids:
            print("❌ Không có ID nào!")
            input("Nhấn Enter...")
            return
        
        print("\n🗑️ XÓA ID NHẬN TIN")
        print("-" * 50)
        
        for idx, rid in enumerate(self.recipient_ids, 1):
            print(f"{idx}. {rid}")
        
        try:
            choice = int(input("\nSố ID cần xóa (0 = hủy): "))
            if 1 <= choice <= len(self.recipient_ids):
                removed = self.recipient_ids.pop(choice - 1)
                print(f"✅ Đã xóa: {removed}")
            elif choice != 0:
                print("❌ Sai số!")
        except:
            print("❌ Sai lựa chọn!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def show_message(self):
        """Xem nội dung tin nhắn"""
        print("\n📝 NỘI DUNG TIN NHẮN:")
        print("-" * 50)
        print(self.message)
        print("-" * 50)
        print(f"Tổng số ký tự: {len(self.message)}")
        input("\nNhấn Enter để tiếp tục...")
    
    def reload_message(self):
        """Tải lại nội dung từ file hiện tại"""
        print("\n🔄 TẢI LẠI NỘI DUNG")
        print("-" * 50)
        
        filename = input("Nhập tên file (Enter để dùng ngon.txt): ").strip()
        if not filename:
            filename = "ngon.txt"
        
        if self.load_message_from_file(filename):
            print("✅ Đã tải lại nội dung thành công!")
        else:
            print("❌ Không thể tải lại nội dung!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def change_message_file(self):
        """Đổi file nội dung khác"""
        print("\n📁 ĐỔI FILE NỘI DUNG")
        print("-" * 50)
        
        filename = input("Nhập tên file mới (vd: noidung.txt): ").strip()
        if not filename:
            print("❌ Tên file không được để trống!")
        else:
            if self.load_message_from_file(filename):
                print("✅ Đã đổi file nội dung thành công!")
            else:
                print("❌ Không thể đọc file mới!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def change_delay(self):
        """Đổi delay"""
        print("\n⏰ ĐỔI DELAY")
        print("-" * 50)
        print(f"Delay hiện tại: {self.delay} giây")
        
        try:
            new_delay = int(input("Delay mới: ").strip())
            if new_delay >= 0:
                self.delay = new_delay
                print(f"✅ Delay mới: {new_delay} giây")
            else:
                print("❌ Delay phải >= 0!")
        except:
            print("❌ Sai số!")
        
        input("\nNhấn Enter để tiếp tục...")
    
    def stop_all(self):
        """Dừng tất cả"""
        self.is_running = False
        
        for acc in self.accounts:
            acc.disconnect()
        
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1)
        
        self.threads = []

def main():
    manager = SpamManager()
    try:
        manager.setup_spam()
    except KeyboardInterrupt:
        print("\n\n👋 Thoát...")
        manager.stop_all()
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        input("\nNhấn Enter để thoát...")

if __name__ == "__main__":
    main()
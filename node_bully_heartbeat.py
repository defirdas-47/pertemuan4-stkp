import socket
import threading
import time
import sys

BASE_PORT = 5000

class BullyNode:

    def __init__(self, node_id, all_nodes_list):
        self.node_id = node_id
        self.all_nodes = sorted(all_nodes_list)         # Urutkan daftar semua node dalam cluster
        self.port = BASE_PORT + self.node_id
        self.coordinator = max(self.all_nodes)          # Awalnya node dengan ID tertinggi dianggap sebagai Koordinator
        self.is_electing = False                        # Penanda apakah node lagi sibuk menggelar pemilihan
        self.higher_responded = False                   # penanda apakah ada node lebih tinggi yang membalas pesan 'OK'

        # Failure Detector (Heartbeat)
        self.last_heartbeat_time = time.time()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Mengatasi erorr WINDOWS WinError 10054 (UDP Reset)
        if sys.platform == 'win32':
            try:
                SIO_UDP_CONNRESET = -1744830452
                self.sock.ioctl(SIO_UDP_CONNRESET, [0])
            except Exception:
                pass
            
        # Buat socket UDP (SOCK_DGRAM)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('127.0.0.1', self.port))        # Tempel socket ke IP localhost dan port milik node ini
        
        print(f"[NODE-{self.node_id}] Listening pada Port {self.port}. Current Coordinator: Node-{self.coordinator}")

    def send_to_node(self, target_id, message):
        target_port = BASE_PORT + target_id              # Hitung port node tujuan
        
        try:
            # Kirim pesan UDP menggunakan sendto()
            self.sock.sendto(message.encode('utf-8'), ('127.0.0.1', target_port))
            
        except Exception:
            pass                                         # Jika node tujuan mati/crash, error diabaikan

    def start_election(self):
        if self.is_electing:
            return                          # Jika proses pemilihan sedang berjalan, abaikan pemicu baru
        
        self.is_electing = True
        self.higher_responded = False
        print(f"\n[ELECTION INITIATED] Node-{self.node_id} memulai proses Leader Election!")

        # Cari daftar node yang ID-nya LEBIH TINGGI dari ID node ini
        higher_nodes = [nid for nid in self.all_nodes if nid > self.node_id]
        
        # Jika tidak ada node dengan ID lebih tinggi, deklarasikan diri sebagai Koordinator
        if not higher_nodes:
            self.declare_coordinator()          # Deklarasikan diri langsung sebagai Koordinator baru
            self.is_electing = False
            return

        # Kirim pesan ELECTION ke seluruh node yang bernilai lebih tinggi
        for target in higher_nodes:
            print(f"[NODE-{self.node_id}] Mengirim message 'ELECTION' ke Node-{target}")
            self.send_to_node(target, f"ELECTION:{self.node_id}")
            
            # Timeout window 2 detik menunggu respons 'OK'
            time.sleep(2)

            # Jika dalam 2 detik TIDAK ADA node lebih tinggi yang membalas 'OK'
            if not self.higher_responded:
                self.declare_coordinator()       # deklarasikan diri jadi koordinator baru
                self.is_electing = False

        # Mengumumkan Diri Sebagai Koordinator
    def declare_coordinator(self):
            self.coordinator = self.node_id      # Ubah koordinator lokal jadi ID sendiri
            print(f"\n>>> [ELECTION WON] Node-{self.node_id} mendeklarasikan diri sebagai NEW COORDINATOR! <<<")

            # Broadcast/Siar kan status ke SELURUH node lain
            for target in self.all_nodes:
                if target != self.node_id:
                    self.send_to_node(target, f"COORDINATOR:{self.node_id}")

    # Koordinator mengirim Heartbeat setiap 3 detik
    def heartbeat_broadcaster(self):
        while True:
            if self.coordinator == self.node_id:
                for target in self.all_nodes:
                    if target != self.node_id:
                        self.send_to_node(target, f"HEARTBEAT:{self.node_id}")
            time.sleep(3) # Dikirim setiap 3 detik

    # Node anggota memonitor apakah Koordinator masih hidup setiap 7 detik
    def failure_detector(self):
        while True:
            time.sleep(1)
            # Jika node ini bukan koordinator dan sedang tidak ada proses election yang berjalan
            if self.coordinator != self.node_id and not self.is_electing:
                selisih_waktu = time.time() - self.last_heartbeat_time
                if selisih_waktu > 7: # Timeout 7 detik tanpa heartbeat
                    print(f"\n[TIMEOUT] 7 detik tanpa Heartbeat dari Node-{self.coordinator}!")
                    self.start_election()


        # Loop Pendengar Pesan Masuk
    def listen_loop(self):
        while True:
            try:
                # Terima paket data UDP yang masuk maksimal 1024 byte
                data, _ = self.sock.recvfrom(1024)
                message = data.decode('utf-8').strip()

                # Memecah string pesan, misal "ELECTION:2" -> msg_type="ELECTION", sender_str="2"
                msg_type, _, sender_str = message.partition(':')
                sender_id = int(sender_str) if sender_str else 0

                if msg_type == "ELECTION":
                    print(f"[NODE-{self.node_id}] Menerima ELECTION dari Node-{sender_id}. Mengirim 'OK'.")
                    self.send_to_node(sender_id, f"OK:{self.node_id}")      
                    # Jalankan pemilihan mandiri di thread terpisah agar tidak menahan perulangan
                    threading.Thread(target=self.start_election, daemon=True).start()

                # Menerima OK dari node yang ID nya lebih tinggi
                elif msg_type == "OK":
                    print(f"[NODE-{self.node_id}] Menerima OK dari Node-{sender_id}. Menunggu deklarasi Coordinator.")
                    self.higher_responded = True        # Tandai bila ada node atas yang mengambil alih

                # Menerima pengumuman Koordinator baru
                elif msg_type == "COORDINATOR":
                    self.coordinator = sender_id        # Update siapa Koordinator aktif saat ini
                    print(f"[COORDINATOR UPDATE] Coordinator aktif saat ini: Node-{self.coordinator}")
                    self.is_electing = False

                elif msg_type == "HEARTBEAT":
                    if sender_id == self.coordinator:
                        self.last_heartbeat_time = time.time() # Perbarui timestamp heartbeat terakhir

            except ConnectionResetError:
                continue
            except Exception:
                break

def start_node(node_id):
        nodes_cluster = [1, 2, 3, 4] # Cluster 4 node
        node = BullyNode(node_id, nodes_cluster)

        # Thread pendengar pesan masuk
        listener = threading.Thread(target=node.listen_loop, daemon=True)
        listener.start()

        # Thread pengirim Heartbeat otomatis (aktif jika jadi koordinator)
        hb_thread = threading.Thread(target=node.heartbeat_broadcaster, daemon=True)
        hb_thread.start()
    
    # Thread detektor kegagalan koordinator
        fd_thread = threading.Thread(target=node.failure_detector, daemon=True)
        fd_thread.start()

        print("Sistem berjalan otomatis dengan Heartbeat & Failure Detector.")
        print("Ketik 'elect' untuk memicu Leader Election, atau 'status' untuk cek Coordinator:")
        
        while True:
            cmd = input()
            if cmd.lower() == 'elect':
                node.start_election()
                
            elif cmd.lower() == 'status':
                print(f"Current Coordinator: Node-{node.coordinator}")
                
if __name__ == '__main__':
        if len(sys.argv) < 2:
            print("Usage: python node_bully.py <NODE_ID (1-4)>")
            sys.exit(1)
        start_node(int(sys.argv[1]))





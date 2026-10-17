import socket
import threading
import time
import sys

BASE_PORT = 5000

class BullyNode:

    def __init__(self, node_id, all_nodes_list):
        self.node_id = node_id
        self.all_nodes = sorted(all_nodes_list)         # Urutan daftar semua node dalam cluster
        self.port = BASE_PORT + self.node_id
        self.coordinator = max(self.all_nodes)          # Awalnya node dengan id tertinggi dianggap sebagai koordinator
        self.is_electing = False                        # Penanda apakah node lagi sibuk menggelar pemilihan
        self.higher_responded = False                   # penanda apakah ada node lebih tinggi yang membalas pesan 'OK'


        # Mengatasi erorr WINDOWS WinError 10054 
        if sys.platform == 'win32':
            try:
                SIO_UDP_CONNRESET = -1744830452
                self.sock.ioctl(SIO_UDP_CONNRESET, [0])
            except Exception:
                pass
            
        # Buat socket UDP 
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('127.0.0.1', self.port))        # Tempel socket ke IP localhost dan port puna node ini
        
        print(f"[NODE-{self.node_id}] Listening pada Port {self.port}. Current Coordinator: Node-{self.coordinator}")

        # Berbeda dengan TCP yang butuh koneksi langgeng, UDP bersifat connectionless
        # Node cukup bind ke port miliknya untuk siap menerima data
        # bind =  mengikat suatu alamat IP atau nomor port tertentu ke sebuah aplikasi, layanan, atau antarmuka jaringan agar bisa dipakai untuk mengirim dan menerima data.

    def send_to_node(self, target_id, message):
        target_port = BASE_PORT + target_id              # Hitung port node tujuan
        
        try:
            # Kirim pesan UDP menggunakan sendto()
            self.sock.sendto(message.encode('utf-8'), ('127.0.0.1', target_port))
            
        except Exception:
            pass                                         # Jika node tujuan crash, error diabaikan

    def start_election(self):
        if self.is_electing:
            return                          # Jika proses pemilihan sedang berjalan, abaikan pemicu baru
        
        self.is_electing = True
        self.higher_responded = False
        print(f"\n[ELECTION INITIATED] Node-{self.node_id} memulai proses Leader Election!")

        # Cari daftar node yang id nya lebih tinggi dari id node ini
        higher_nodes = [nid for nid in self.all_nodes if nid > self.node_id]
        
        # Jika tidak ada node dengan id lebih tinggi, deklarasikan diri sebagai koodinator 
        if not higher_nodes:
            self.declare_coordinator()          # Deklarasi diri langsung sebagai koordinator baru
            self.is_electing = False
            return

        # Kirim pesan election ke seluruh node yang bernilai lebih tinggi
        for target in higher_nodes:
            print(f"[NODE-{self.node_id}] Mengirim message 'ELECTION' ke Node-{target}")
            self.send_to_node(target, f"ELECTION:{self.node_id}")
            
            # Tunggu respon selama 2 detik (Timeout)
            time.sleep(2)

            # Jika dalam 2 detik tidak ada node lebih tinggi yang membalas 'OK'
            if not self.higher_responded:
                self.declare_coordinator()       # Anggap node-node di atasnya mati, deklarasi diri jadi koordinator baru
                self.is_electing = False

        # Mengumumkan Diri Sebagai Koordinator
    def declare_coordinator(self):
            self.coordinator = self.node_id      # Ubah koordinator lokal jadi id sendiri
            print(f"\n>>> [ELECTION WON] Node-{self.node_id} mendeklarasikan diri sebagai NEW COORDINATOR! <<<")

            # Siar kan status ke seluruh node lain
            for target in self.all_nodes:
                if target != self.node_id:
                    self.send_to_node(target, f"COORDINATOR:{self.node_id}")

        # Loop Pendengar Pesan Masuk
    def listen_loop(self):
            while True:
                # Terima paket data UDP yang masuk maks 1024 byte
                data, _ = self.sock.recvfrom(1024)
                message = data.decode('utf-8').strip()

                # Memecah string pesan, misal "ELECTION:2" -> msg_type="ELECTION", sender_str="2"
                msg_type, _, sender_str = message.partition(':')
                sender_id = int(sender_str) if sender_str else 0

                if msg_type == "ELECTION":
                    print(f"[NODE-{self.node_id}] Menerima ELECTION dari Node-{sender_id}. Mengirim 'OK'.")
                    self.send_to_node(sender_id, f"OK:{self.node_id}")      # Balas OK 
                    # Jalankan pemilihan mandiri di thread terpisah agar tidak menahan perulangan
                    threading.Thread(target=self.start_election, daemon=True).start()

                # Menerima OK dari node yang id nya lebih tinggi
                elif msg_type == "OK":
                    print(f"[NODE-{self.node_id}] Menerima OK dari Node-{sender_id}. Menunggu deklarasi Coordinator.")
                    self.higher_responded = True        # Tandai bahwa ada node atas yang mengambil alih

                # Menerima pengumuman koordinator baru
                elif msg_type == "COORDINATOR":
                    self.coordinator = sender_id        # Update siapa koordinator aktif saat ini
                    print(f"[COORDINATOR UPDATE] Coordinator aktif saat ini: Node-{self.coordinator}")
                    self.is_electing = False

    
def start_node(node_id):
    nodes_cluster = [1, 2, 3, 4] # Cluster 4 node
    node = BullyNode(node_id, nodes_cluster)
    listener = threading.Thread(target=node.listen_loop, daemon=True)
    listener.start()
        
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




    
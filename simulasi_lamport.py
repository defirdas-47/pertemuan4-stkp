import time
import queue

class LamportNode:
    def __init__(self, node_id, total_nodes):
        self.node_id = node_id              # ID unik untuk node/komputer ini 
        self.total_nodes = total_nodes      # Total node dalam sistem terdistribusi
        self.clock = 0                      # Nilai jam logis awal, dimulai dari angka 0
        self.inbox = queue.Queue()          # Antrean pesan FIFO untuk terima pesan dari node lain

    # Fungsi ini cuma pembantu buat mencetak teks ke layar biar rapi, nampilin jam Komputer, ID Node, nilai Clock saat itu, dan aksi yang lagi dilakukan
    def log(self, action):
        waktu = time.strftime('%H:%M:%S')   # Mengambil jam fisik sistem saat ini misal 14:05:10
        print(f"[{waktu}] Node-{self.node_id} (Clock: {self.clock:02d}) ->{action}")
        
    def internal_event(self):
        self.clock += 1                        
        self.log("Eksekusi INTERNAL EVENT")
    # Digunakan saat node melakukan proses di dalam dirinya sendiri tanpa berhubungan sama node lain 
    # aturan Lamport: Setiap ada kejadian internal, clock = clock + 1

    def send_message(self, target_node, message_text):
        self.clock += 1                       # Sebelum kirim pesan naikkan clock +1
        timestamp_send = self.clock           # Mencatat angka clock saat ini sebagai timestamp pengiriman
        payload = {
            'sender': self.node_id,
            'timestamp': timestamp_send,
            'content': message_text
            }
        
        self.log(f"SEND ke Node-{target_node.node_id}: '{message_text}'[Timestamp Send: {timestamp_send}]")
        
        # Masukkan payload langsung ke inbox node tujuan
        target_node.inbox.put(payload)
    # Sebelum lempar pesan node penembak menaikkan clock-nya. Angka clock ini diselipkan ke dalam pesan (timestamp_send)
    # Lalu pesan dimasukkan ke inbox node penerima

    def process_inbox(self):
        while not self.inbox.empty():          # Selama masih ada pesan di kotak masuk/inbox
            msg = self.inbox.get()             # Ambil pesan terdepan
            sender = msg['sender']             # Timestamp bawaan dari si pengirim
            t_msg = msg['timestamp']
            content = msg['content']

            # Aturan Lamport: max(clock_lokal, timestamp_pesan) + 1
            clock_before = self.clock
            self.clock = max(self.clock, t_msg) + 1
            self.log(f"RECEIVE dari Node-{sender}: '{content}' [TimestampMsg: {t_msg} | Clock: {clock_before} -> {self.clock}]")

    # Baris self.clock = max(self.clock, t_msg) + 1 adalah tempat beraksinya Lamport Clock 
    # Node mengecek mana angka yang lebih besar antara clock lokalnya sendiri dan timestamp bawaan pesan
    # Diambil nilai terbesarnya lalu ditambah +1.   
    # Makanya kalau timestamp pesan lebih besar dari clock penerima, clock penerima akan melompat maju

def run_lamport_simulation():
        print("=== SIMULASI LAMPORT LOGICAL CLOCK ===\n")

        # 1. Buat 3 node (Node-1, Node-2, Node-3). Clock awal ketiganya = 0
        node1 = LamportNode(node_id=1, total_nodes=3)       
        node2 = LamportNode(node_id=2, total_nodes=3)
        node3 = LamportNode(node_id=3, total_nodes=3)
        
        # Rangkaian Event Skenario
        node1.internal_event()              # Node-1: clock (0 -> 1)
        node2.internal_event()              # Node-2: clock (0 -> 1)
        
        # Node 1 kirim message ke Node 2
        # Node-1 clock naik +1 (1 -> 2). Timestamp dikirim = 2
        node1.send_message(node2, "Halo Node 2 dari Node 1")

        # Node-2 terima pesan
        # Clock lokal Node-2 saat ini = 1, Timestamp dari Node-1 = 2
        # Node-2 hitung: max(1, 2) + 1 = 3 -> Clock Node-2 berubah jadi 3
        node2.process_inbox() 

        # Node 2 eksekusi internal event lalu forward message ke Node 3
        node2.internal_event()             # Node-2 clock (3 -> 4)
        # Node-2 clock naik (4 -> 5). Timestamp dikirim = 5
        node2.send_message(node3, "Update data dari Node 2 ke Node 3")
        
        # Node 3 melakukan internal event sebelum memproses inbox
        node3.internal_event()             # Node-3 clock (0 -> 1)
        node3.internal_event()             # Node-3 clock (1 -> 2)

        # Node-3 terima pesan dari Node-2
        # Clock lokal Node-3 saat ini = 2, Timestamp dari Node-2 = 5
        # Node-3 hitung: max(2, 5) + 1 = 6 -> Clock Node-3 melompat dari 2 jadi 6!
        node3.process_inbox()
        
        print("\n=== FINAL LOGICAL CLOCK STATE ===")
        print(f"Node-1 Clock: {node1.clock} | Node-2 Clock: {node2.clock} | Node-3 Clock: {node3.clock}")
        
if __name__ == '__main__':
    run_lamport_simulation()


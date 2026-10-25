import socket, sys
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1); s.bind(("127.0.0.1",8443)); s.listen(1)
c,_=s.accept(); data=b""; c.settimeout(3)
try:
    while len(data)<5 or len(data)<5+int.from_bytes(data[3:5],'big'):
        chunk=c.recv(65536)
        if not chunk: break
        data+=chunk
except Exception: pass
open(sys.argv[1],"wb").write(data); c.close()

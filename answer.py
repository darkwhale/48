import socket
from disk import get_status
from logs import make_log

answer_port = 12346


# 接受客户端的请求并返回系统状态；
def answer():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = socket.gethostname()

    sock.bind((host, answer_port))
    sock.listen(5)

    while True:
        connection, address = sock.accept()
        symbol = connection.recv(1024).decode()

        src_size = int(symbol[1:])

        computer_status = get_status(src_size)
        connection.send(computer_status.encode())

        make_log("INFO", "客户机请求，返回系统状态------")

        connection.close()

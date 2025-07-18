import socket
import os

HEAD_SERVER_HOST = 'localhost'
HEAD_SERVER_PORT = 9000

def send_command(command, file_content=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HEAD_SERVER_HOST, HEAD_SERVER_PORT))
        s.send(command.encode())
        if file_content:
            s.send(file_content)
        response = s.recv(1024 * 1024)
        return response


def list_files():
    response = send_command("LIST")
    print("Files on server:", response.decode())


def save_file(filepath):
    if not os.path.isfile(filepath):
        print("File not found.")
        return
    filename = os.path.basename(filepath)
    with open(filepath, 'rb') as f:
        content = f.read()
    filesize = len(content)
    response = send_command(f"SAVE {filename} {filesize}", content)
    print("Response:", response.decode())


def retrieve_file(filename):
    response = send_command(f"RETRIEVE {filename}")
    if response == b'NOT FOUND':
        print("File not found on server.")
    else:
        with open(filename, 'wb') as f:
            f.write(response)
        print(f"File '{filename}' downloaded.")


def delete_file(filename):
    response = send_command(f"DELETE {filename}")
    print("Response:", response.decode())


def main():
    print("Distributed File System Client")
    print("Commands: LIST, SAVE <filepath>, RETRIEVE <filename>, DELETE <filename>, EXIT")

    while True:
        try:
            cmd = input("DFS> ").strip()
            if not cmd:
                continue
            if cmd.upper() == "EXIT":
                break
            elif cmd.upper() == "LIST":
                list_files()
            elif cmd.upper().startswith("SAVE "):
                _, path = cmd.split(" ", 1)
                save_file(path)
            elif cmd.upper().startswith("RETRIEVE "):
                _, name = cmd.split(" ", 1)
                retrieve_file(name)
            elif cmd.upper().startswith("DELETE "):
                _, name = cmd.split(" ", 1)
                delete_file(name)
            else:
                print("Invalid command.")
        except Exception as e:
            print("Error:", e)


if __name__ == '__main__':
    main()
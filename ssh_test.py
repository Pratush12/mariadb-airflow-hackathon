import paramiko

def run_command_via_ssh(host, port, username, password, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, port=port, username=username, password=password)
    stdin, stdout, stderr = ssh.exec_command(command)
    out = stdout.read().decode()
    err = stderr.read().decode()
    ssh.close()
    return out, err

if __name__ == "__main__":
    host = "127.0.0.1"       # Use localhost on Mac
    port = 2222              # The host-mapped SSH port
    username = "root"
    password = "mariadb_root_pass"
    command = "ls -ltr"

    stdout, stderr = run_command_via_ssh(host, port, username, password, command)

    print("STDOUT:\n", stdout)
    print("STDERR:\n", stderr)

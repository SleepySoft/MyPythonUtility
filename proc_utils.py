import sys
import os
import time
import subprocess
import socket
import psutil
import platform

# Global variable to hold the socket lock for this process instance
_INSTANCE_LOCK_SOCKET = None


def _get_logical_name(proc):
    """
    Internal Helper: Resolves the 'logical' name of a process.
    If it's python, returns the script name (e.g., 'worker.py').
    If it's a binary, returns the binary name (e.g., 'nginx').
    """
    try:
        name = proc.name().lower()
        cmdline = proc.cmdline()

        # Check if it's a Python process
        if 'python' in name and cmdline:
            # 调试器包装（PyCharm pydevd 等）：真实脚本在 --file 参数之后
            for i, arg in enumerate(cmdline):
                if arg == '--file' and i + 1 < len(cmdline) and cmdline[i + 1].endswith('.py'):
                    return os.path.basename(cmdline[i + 1]).lower()
            # Look for the first argument ending in .py
            for arg in cmdline:
                if arg.endswith('.py'):
                    # Return just the filename, not full path, for easier matching
                    return os.path.basename(arg).lower()
        return name
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return ""


def find_processes(target_name: str) -> list[int]:
    """
    1. Detect if a program is running.
    Args:
        target_name: 'worker.py' or 'chrome.exe'
    Returns:
        List of PIDs (empty list if not found).
    """
    found_pids = []
    target = target_name.lower()

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        # Skip the current process to avoid detecting self
        if proc.info['pid'] == os.getpid():
            continue

        logical_name = _get_logical_name(proc)
        if logical_name == target:
            found_pids.append(proc.info['pid'])

    return found_pids


def start_program(command: str, background=True, no_window=False) -> int:
    """
    2. Start a program (Smart Python detection).
    Args:
        command: Full command string e.g., "worker.py -a 1" or "notepad.exe"
        background: If True, does not block (fire and forget).
        no_window: If True, hides the console window (Windows only).
    Returns:
        PID of the new process if successful, -1 if failed.
    """
    try:
        args = command.split()
        if not args: return -1

        # Smart Python handling: if starts with .py, prepend python executable
        if args[0].endswith('.py'):
            args.insert(0, sys.executable)

        # Setup startup flags
        kwargs = {
            'env': os.environ.copy(),
            'cwd': os.getcwd()
        }

        # Windows specific: Hide window
        if platform.system() == 'Windows' and no_window:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            kwargs['startupinfo'] = startupinfo
            # For pure background detachment on Windows
            if background:
                kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS

        # Linux specific: Detach session (Daemon-like)
        if platform.system() != 'Windows' and background:
            kwargs['start_new_session'] = True
            kwargs['stdout'] = subprocess.DEVNULL
            kwargs['stderr'] = subprocess.DEVNULL
            kwargs['stdin'] = subprocess.DEVNULL

        proc = subprocess.Popen(args, **kwargs)

        if not background:
            proc.wait()  # Block if not background

        return proc.pid
    except Exception as e:
        print(f"Error starting program: {e}", file=sys.stderr)
        return -1


def get_process_by_port(port: int) -> tuple[int, str]:
    """
    3. Find program occupying a port.
    Returns:
        (PID, LogicalName). Returns (-1, "") if not found.
    """
    try:
        connections = psutil.net_connections()
        for conn in connections:
            if conn.laddr.port == port and conn.status == 'LISTEN':
                pid = conn.pid
                if pid:
                    try:
                        proc = psutil.Process(pid)
                        name = _get_logical_name(proc)
                        return pid, name
                    except psutil.NoSuchProcess:
                        pass
    except Exception:
        pass  # Permission errors possible on Linux without root
    return -1, ""


def kill_processes(target) -> int:
    """
    4. Kill program(s).
    Args:
        target: Can be an int (PID), a list of ints (PIDs), or a str (name).
    Returns:
        Count of processes successfully killed.
    """
    pids_to_kill = []

    # Normalize input to a list of PIDs
    if isinstance(target, int):
        pids_to_kill = [target]
    elif isinstance(target, list):
        pids_to_kill = target
    elif isinstance(target, str):
        pids_to_kill = find_processes(target)

    count = 0
    for pid in pids_to_kill:
        if pid <= 0: continue
        try:
            p = psutil.Process(pid)
            p.terminate()  # Try graceful shutdown first
            try:
                p.wait(timeout=1)
            except psutil.TimeoutExpired:
                p.kill()  # Force kill if stubborn
            count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return count


def acquire_instance_lock(port: int = 65432) -> bool:
    """
    5. Single Instance Lock via Socket.
    Args:
        port: The port to bind to.
    Returns:
        True if lock acquired (you are the only instance).
        False if already running.
    """
    global _INSTANCE_LOCK_SOCKET
    try:
        _INSTANCE_LOCK_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Windows/Linux behavior: binding to localhost prevents external access
        _INSTANCE_LOCK_SOCKET.bind(('127.0.0.1', port))
        return True
    except socket.error:
        _INSTANCE_LOCK_SOCKET = None
        return False

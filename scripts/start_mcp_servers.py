# -*- coding: utf-8 -*-
"""
Скрипт для запуску всіх MCP серверів.
"""

import subprocess
import sys
import os
from pathlib import Path


def start_mcp_servers():
    """Запускає всі MCP сервери у фоновому режимі."""
    # Шлях до кореня проекту
    project_root = Path(__file__).parent.parent
    
    # Список MCP серверів
    mcp_servers = [
        'mcp_servers.schema_mcp_server',
        'mcp_servers.query_builder_mcp_server',
        'mcp_servers.analytics_mcp_server',
        'mcp_servers.report_mcp_server'
    ]
    
    processes = []
    
    print("Запуск MCP серверів...")
    
    for server_module in mcp_servers:
        try:
            # Запускаємо сервер у фоновому процесі
            process = subprocess.Popen(
                [sys.executable, '-m', server_module],
                cwd=str(project_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ.copy()
            )
            processes.append((server_module, process))
            print(f"✓ Запущено {server_module} (PID: {process.pid})")
        except Exception as e:
            print(f"✗ Помилка запуску {server_module}: {e}")
    
    if processes:
        print(f"\nЗапущено {len(processes)} MCP серверів.")
        print("Для зупинки натисніть Ctrl+C")
        
        try:
            # Чекаємо на завершення процесів
            for server_module, process in processes:
                process.wait()
        except KeyboardInterrupt:
            print("\nЗупинка MCP серверів...")
            for server_module, process in processes:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    print(f"✓ Зупинено {server_module}")
                except Exception as e:
                    print(f"✗ Помилка зупинки {server_module}: {e}")
                    process.kill()
    else:
        print("Не вдалося запустити жодного MCP сервера")


if __name__ == "__main__":
    start_mcp_servers()

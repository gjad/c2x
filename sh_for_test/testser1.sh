#!/bin/sh
# 管理一个仅用于测试 systemd 服务控制的空操作服务：testser1.service

set -eu

SERVICE_NAME="testser1"
UNIT_NAME="${SERVICE_NAME}.service"
UNIT_FILE="/etc/systemd/system/${UNIT_NAME}"

unit_content() {
    cat <<'EOF'
[Unit]
Description=Test service 1 that does nothing

[Service]
Type=simple
# 保持运行但不执行实际工作，便于测试 start/stop/status。
ExecStart=/usr/bin/sleep infinity
Restart=no

[Install]
WantedBy=multi-user.target
EOF
}

usage() {
    cat <<EOF
用法：$0 <命令>

命令：
  view       查看服务定义和当前状态
  install    安装服务定义（不会自动启动）
  uninstall  停止、禁用并卸载服务定义
  start      启动服务
  stop       停止服务
  restart    重启服务
  status     查看服务状态
  enable     设置开机自动启动
  disable    取消开机自动启动
  help       显示帮助

示例：
  sudo $0 install
  $0 start
  $0 status
  $0 stop
  sudo $0 uninstall
EOF
}

require_systemctl() {
    if ! command -v systemctl >/dev/null 2>&1; then
        echo "错误：找不到 systemctl，当前系统可能不是 systemd。" >&2
        exit 1
    fi
}

as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        echo "错误：此操作需要 root 权限，请使用 sudo 运行。" >&2
        exit 1
    fi
}

install_service() {
    require_systemctl
    unit_content | as_root tee "$UNIT_FILE" >/dev/null
    as_root systemctl daemon-reload
    echo "已安装 ${UNIT_NAME}：${UNIT_FILE}"
    echo "服务未自动启动；需要时执行：$0 start"
}

uninstall_service() {
    require_systemctl
    if [ -e "$UNIT_FILE" ]; then
        as_root systemctl stop "$UNIT_NAME" 2>/dev/null || true
        as_root systemctl disable "$UNIT_NAME" 2>/dev/null || true
        as_root rm -f "$UNIT_FILE"
        as_root systemctl daemon-reload
        echo "已卸载 ${UNIT_NAME}。"
    else
        echo "${UNIT_NAME} 未安装。"
    fi
}

view_service() {
    require_systemctl
    if [ -e "$UNIT_FILE" ]; then
        echo "===== 服务定义：${UNIT_FILE} ====="
        as_root cat "$UNIT_FILE"
        echo
        echo "===== 当前状态 ====="
        systemctl --no-pager status "$UNIT_NAME" || true
    else
        echo "${UNIT_NAME} 尚未安装，预览服务定义："
        unit_content
    fi
}

service_action() {
    require_systemctl
    action="$1"
    systemctl list-unit-files "$UNIT_NAME" 2>/dev/null | grep -q "^${UNIT_NAME}" || {
        echo "错误：${UNIT_NAME} 尚未安装，请先执行：sudo $0 install" >&2
        exit 1
    }
    as_root systemctl "$action" "$UNIT_NAME"
}

command_name="${1:-help}"
case "$command_name" in
    view)      view_service ;;
    install)   install_service ;;
    uninstall) uninstall_service ;;
    start|stop|restart|enable|disable)
        service_action "$command_name" ;;
    status)
        require_systemctl
        systemctl --no-pager status "$UNIT_NAME"
        ;;
    help|-h|--help) usage ;;
    *)
        echo "错误：未知命令：${command_name}" >&2
        usage >&2
        exit 2
        ;;
esac

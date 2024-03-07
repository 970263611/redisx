package com.dahuaboke.redisx.core;

/**
 * author: dahua
 * date: 2024/3/6 11:20
 */
public enum SlaveState {
    // 未开启主从复制功能
    // 当前服务器是普通的Redis实例
    REPL_STATE_NONE,
    // 待发起Socket连接主服务器
    REPL_STATE_CONNECT,
    // Socket连接成功
    REPL_STATE_CONNECTING,
    // 已经发送了PING请求包
    // 并等待接收主服务器PONG回复
    REPL_STATE_RECEIVE_PONG,
    // 待发起密码认证
    REPL_STATE_SEND_AUTH,
    // 已经发起了密码认证请求“AUTH<password>”
    // 等待接收主服务器回复
    REPL_STATE_RECEIVE_AUTH,
    // 待发送端口号
    REPL_STATE_SEND_PORT,
    // 已发送端口号“REPLCONF listening-port<port>”
    // 等待接收主服务器回复
    REPL_STATE_RECEIVE_PORT,
    //待发送IP地址
    REPL_STATE_SEND_IP,
    // 已发送IP地址“REPLCONF ip address<ip>”
    // 等待接收主服务器回复
    // 该IP地址与端口号用于主服务器主动建立Socket连接
    // 并向从服务器同步数据；
    REPL_STATE_RECEIVE_IP,
    // 主从复制功能进行过优化升级
    // 不同版本Redis服务器支持的能力可能不同
    // 因此从服务器需要告诉主服务器自己支持的主从复制能力
    // 通过命令“REPLCONF capa <capability>”实现
    REPL_STATE_SEND_CAPA,
    // 等待接收主服务器回复
    REPL_STATE_RECEIVE_CAPA,
    // 待发送PSYNC命令
    REPL_STATE_SEND_PSYNC,
    // 等待接收主服务器PSYNC命令的回复结果
    REPL_STATE_RECEIVE_PSYNC,
    // 正在接收RDB文件
    REPL_STATE_TRANSFER,
    // RDB文件接收并载入完毕
    // 主从复制连接建立成功
    // 此时从服务器只需要等待接收主服务器同步数据即可
    REPL_STATE_CONNECTED;
}

package com.ksyun.campus.client.cli;

import dfs_project.Metaserver;
import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;
import com.ksyun.campus.client.client.MinFSClient;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;

/**
 * MinFS 命令行客户端
 * 提供与Go客户端相同的命令行接口
 */
public class MinFSCLI {
    private EFileSystem fileSystem;
    private MinFSClient client;
    private Scanner scanner;

    public MinFSCLI() {
        this.scanner = new Scanner(System.in);
    }

    public static void main(String[] args) {
        System.out.println("=== MinFS Java客户端 ===");
        
        MinFSCLI cli = new MinFSCLI();
        
        // 初始化客户端连接
        if (!cli.initialize()) {
            System.err.println("❌ 客户端初始化失败");
            System.exit(1);
        }
        
        // 启动命令行界面
        cli.startCommandLine();
    }

    /**
     * 初始化客户端连接
     */
    private boolean initialize() {
        try {
            System.out.println("正在连接到MinFS集群...");
            
            // 使用etcd服务发现模式
            this.fileSystem = new EFileSystem("minfs");
            this.client = new MinFSClient("http://localhost:2379", "/minfs", true);
            
            System.out.println("✅ 连接成功！");
            System.out.println("输入 'help' 查看可用命令");
            return true;
        } catch (Exception e) {
            System.err.println("连接失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * 启动命令行界面
     */
    private void startCommandLine() {
        while (true) {
            System.out.print("minfs> ");
            String line = scanner.nextLine().trim();
            
            if (line.isEmpty()) {
                continue;
            }
            
            String[] parts = line.split("\\s+");
            String command = parts[0].toLowerCase();
            String[] args = new String[parts.length - 1];
            System.arraycopy(parts, 1, args, 0, parts.length - 1);
            
            if (!executeCommand(command, args)) {
                break;
            }
        }
    }

    /**
     * 执行命令
     */
    private boolean executeCommand(String command, String[] args) {
        try {
            switch (command) {
                case "help":
                case "h":
                    showHelp();
                    break;
                    
                case "exit":
                case "quit":
                case "q":
                    System.out.println("再见！");
                    return false;
                    
                case "cluster":
                    cmdCluster(args);
                    break;
                    
                case "replicas":
                case "repl":
                    cmdReplicas(args);
                    break;
                    
                case "mkdir":
                    cmdMkdir(args);
                    break;
                    
                case "create":
                    cmdCreate(args);
                    break;
                    
                case "ls":
                    cmdLs(args);
                    break;
                    
                case "stat":
                    cmdStat(args);
                    break;
                    
                case "rm":
                    cmdRm(args);
                    break;
                    
                case "write":
                    cmdWrite(args);
                    break;
                    
                case "read":
                    cmdRead(args);
                    break;
                    
                case "put":
                    cmdPut(args);
                    break;
                    
                case "get":
                    cmdGet(args);
                    break;
                    
                default:
                    System.out.println("未知命令: " + command + "。输入 'help' 查看可用命令。");
                    break;
            }
        } catch (Exception e) {
            System.err.println("命令执行失败: " + e.getMessage());
        }
        return true;
    }

    /**
     * 显示帮助信息
     */
    private void showHelp() {
        System.out.println("可用命令:");
        System.out.println("  help                      - 显示帮助信息");
        System.out.println("  exit/quit/q              - 退出客户端");
        System.out.println("");
        System.out.println("文件操作:");
        System.out.println("  create <path>            - 创建文件");
        System.out.println("  mkdir <path>             - 创建目录");
        System.out.println("  ls <path>                - 列出目录内容");
        System.out.println("  stat <path>              - 显示文件/目录状态");
        System.out.println("  rm <path>                - 删除文件/目录 (递归)");
        System.out.println("");
        System.out.println("数据操作:");
        System.out.println("  write <path> <text>      - 向文件写入文本");
        System.out.println("  read <path>              - 读取文件内容");
        System.out.println("  put <local_file> <remote_path> - 上传本地文件");
        System.out.println("  get <remote_path> <local_file> - 下载文件");
        System.out.println("");
        System.out.println("集群:");
        System.out.println("  cluster                  - 显示集群信息");
        System.out.println("  replicas [path]          - 显示文件副本状态 (不指定路径则显示所有文件)");
        System.out.println("");
        System.out.println("示例:");
        System.out.println("  mkdir /test");
        System.out.println("  create /test/hello.txt");
        System.out.println("  write /test/hello.txt \"Hello MinFS!\"");
        System.out.println("  read /test/hello.txt");
        System.out.println("  stat /test/hello.txt");
        System.out.println("  ls /test");
        System.out.println("  rm /test/hello.txt");
        System.out.println("  rm /test");
    }

    private void cmdCluster(String[] args) {
        fileSystem.getClusterInfo();
    }

    private void cmdReplicas(String[] args) {
        String path = (args.length > 0) ? args[0] : "";
        
        try {
            Metaserver.GetReplicationInfoResponse replicationInfo = 
                client.getReplicationInfo(path);
                
            if (replicationInfo != null) {
                System.out.println("=== 副本分布信息 ===");
                System.out.println("总文件数: " + replicationInfo.getTotalFiles());
                System.out.println("健康文件数: " + replicationInfo.getHealthyFiles());
                System.out.println("副本不足文件数: " + replicationInfo.getUnderReplicatedFiles());
                System.out.println("副本过多文件数: " + replicationInfo.getOverReplicatedFiles());
                
                if (!replicationInfo.getFilesList().isEmpty()) {
                    System.out.println("\n文件副本详情:");
                    for (Metaserver.ReplicationStatus file : replicationInfo.getFilesList()) {
                        System.out.println("  文件: " + file.getPath());
                        System.out.println("    期望副本数: " + file.getExpectedReplicas());
                        System.out.println("    实际副本数: " + file.getActualReplicas());
                        System.out.println("    健康状态: " + file.getStatus());
                        
                        if (!file.getBlocksList().isEmpty()) {
                            System.out.println("    数据块分布:");
                            for (Metaserver.BlockReplicationInfo block : file.getBlocksList()) {
                                System.out.println("      块ID " + block.getBlockId() + 
                                    " -> " + block.getLocationsList() + 
                                    " (副本数: " + block.getReplicaCount() + ")");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // 忽略异常
        }
    }

    private void cmdMkdir(String[] args) {
        if (args.length < 1) {
            return;
        }
        fileSystem.mkdir(args[0]);
    }

    private void cmdCreate(String[] args) {
        if (args.length < 1) {
            return;
        }
        
        FSOutputStream output = fileSystem.create(args[0]);
        if (output == null) {
            return;
        }
        
        try {
            output.close();
        } catch (Exception e) {
            // 忽略关闭异常
        }
    }

    private void cmdLs(String[] args) {
        if (args.length < 1) {
            return;
        }
        fileSystem.listFileStats(args[0]);
    }

    private void cmdStat(String[] args) {
        if (args.length < 1) {
            return;
        }
        fileSystem.getFileStats(args[0]);
    }

    private void cmdRm(String[] args) {
        if (args.length < 1) {
            return;
        }
        fileSystem.delete(args[0]);
    }

    private void cmdWrite(String[] args) {
        if (args.length < 2) {
            return;
        }
        
        String path = args[0];
        StringBuilder text = new StringBuilder();
        for (int i = 1; i < args.length; i++) {
            if (i > 1) text.append(" ");
            text.append(args[i]);
        }
        
        String content = text.toString();
        if (content.startsWith("\"") && content.endsWith("\"")) {
            content = content.substring(1, content.length() - 1);
        }
        
        // 简化的三步曲：create -> write -> close
        FSOutputStream output = fileSystem.create(path);
        if (output == null) {
            return; // SDK 已经打印了失败详情
        }
        
        try {
            output.write(content.getBytes());
            output.close(); // SDK 内部会打印写入成功信息
        } catch (Exception e) {
            // SDK 已经处理了错误信息，这里不需要额外打印
        }
    }

    private void cmdRead(String[] args) {
        if (args.length < 1) {
            return;
        }
        
        // 按照模板规范：getFileStats -> open -> FSInputStream.read -> close
        StatInfo statInfo = fileSystem.getFileStats(args[0]);
        if (statInfo == null) {
            return;
        }
        
        FSInputStream input = fileSystem.open(args[0]);
        if (input == null) {
            return;
        }
        
        try {
            byte[] data = new byte[(int) statInfo.getSize()];
            int totalRead = input.read(data); // 直接使用FSInputStream.read()
            
            String content = new String(data, 0, totalRead);
            System.out.println("--- 文件内容开始 ---");
            System.out.print(content);
            System.out.println("\n--- 文件内容结束 ---");
        } catch (Exception e) {
            // 忽略异常
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                // 忽略关闭异常
            }
        }
    }

    private void cmdPut(String[] args) {
        if (args.length < 2) {
            return;
        }
        
        String localFile = args[0];
        String remotePath = args[1];
        
        try {
            byte[] data = Files.readAllBytes(Paths.get(localFile));
            
            // 简化的三步曲：create -> write -> close
            FSOutputStream output = fileSystem.create(remotePath);
            if (output == null) {
                return; // SDK 已经打印了失败详情
            }
            
            output.write(data);
            output.close(); // SDK 内部会打印写入成功信息
        } catch (IOException e) {
            // 忽略异常
        } catch (Exception e) {
            // SDK 已经处理了错误信息，这里不需要额外打印
        }
    }

    private void cmdGet(String[] args) {
        if (args.length < 2) {
            return;
        }
        
        String remotePath = args[0];
        String localFile = args[1];
        
        StatInfo statInfo = fileSystem.getFileStats(remotePath);
        if (statInfo == null) {
            return;
        }
        
        FSInputStream input = fileSystem.open(remotePath);
        if (input == null) {
            return;
        }
        
        try {
            byte[] data = new byte[(int) statInfo.getSize()];
            int totalRead = 0;
            int bytesRead;
            
            while (totalRead < data.length && (bytesRead = input.read(data, totalRead, data.length - totalRead)) != -1) {
                totalRead += bytesRead;
            }
            
            Files.write(Paths.get(localFile), data);
            // 下载完成
        } catch (Exception e) {
            // 忽略异常
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                // 忽略关闭异常
            }
        }
    }
}
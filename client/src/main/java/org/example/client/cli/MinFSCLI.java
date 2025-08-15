package org.example.client.cli;

import dfs_project.Metaserver;
import org.example.client.EFileSystem;
import org.example.client.FSInputStream;
import org.example.client.FSOutputStream;
import org.example.client.client.MinFSClient;
import org.example.client.domain.ClusterInfo;
import org.example.client.domain.StatInfo;

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
            this.client = fileSystem.getClient();
            
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

    /**
     * 集群信息命令
     */
    private void cmdCluster(String[] args) {
        try {
            Metaserver.ClusterInfo clusterInfo = client.getClusterInfo();
            if (clusterInfo != null) {
                System.out.println("=== 集群信息 ===");
                
                // 主MetaServer信息
                if (clusterInfo.hasMasterMetaServer()) {
                    System.out.println("主MetaServer: " + 
                        clusterInfo.getMasterMetaServer().getHost() + ":" + 
                        clusterInfo.getMasterMetaServer().getPort());
                }
                
                // 从MetaServer信息
                System.out.println("从MetaServer数量: " + clusterInfo.getSlaveMetaServerCount());
                
                // DataServer信息
                System.out.println("DataServer数量: " + clusterInfo.getDataServerCount());
                
                if (clusterInfo.getDataServerCount() > 0) {
                    System.out.println("DataServer详情:");
                    for (int i = 0; i < clusterInfo.getDataServerCount(); i++) {
                        var ds = clusterInfo.getDataServer(i);
                        System.out.println("  " + (i+1) + ". " + ds.getHost() + ":" + ds.getPort() +
                            " (文件: " + ds.getFileTotal() + ", 容量: " + ds.getUseCapacity() + "/" + ds.getCapacity() + "MB)");
                    }
                }
                
                System.out.println("✅ 集群信息获取成功");
            } else {
                System.err.println("❌ 无法获取集群信息");
            }
        } catch (Exception e) {
            System.err.println("❌ 获取集群信息失败: " + e.getMessage());
        }
    }

    /**
     * 副本信息命令
     */
    private void cmdReplicas(String[] args) {
        try {
            String path = (args.length > 0) ? args[0] : "";
            
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
                
                System.out.println("✅ 副本信息获取成功");
            } else {
                System.err.println("❌ 无法获取副本信息");
            }
        } catch (Exception e) {
            System.err.println("❌ 获取副本信息失败: " + e.getMessage());
        }
    }

    /**
     * 创建目录命令
     */
    private void cmdMkdir(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: mkdir <path>");
            return;
        }
        
        try {
            boolean success = fileSystem.mkdir(args[0]);
            if (success) {
                System.out.println("✅ 目录创建成功: " + args[0]);
            } else {
                System.err.println("❌ 目录创建失败: " + args[0]);
            }
        } catch (Exception e) {
            System.err.println("❌ 创建目录失败: " + e.getMessage());
        }
    }

    /**
     * 创建文件命令
     */
    private void cmdCreate(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: create <path>");
            return;
        }
        
        try (FSOutputStream output = fileSystem.create(args[0])) {
            // 创建空文件
            output.flush();
            System.out.println("✅ 文件创建成功: " + args[0]);
        } catch (Exception e) {
            System.err.println("❌ 创建文件失败: " + e.getMessage());
        }
    }

    /**
     * 列出目录命令
     */
    private void cmdLs(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: ls <path>");
            return;
        }
        
        try {
            List<StatInfo> items = fileSystem.listFileStats(args[0]);
            if (items != null && !items.isEmpty()) {
                System.out.println("目录 " + args[0] + " 的内容:");
                for (StatInfo item : items) {
                    String type = (item.getType() == org.example.client.domain.FileType.Directory) ? "DIR " : "FILE";
                    System.out.println("  " + type + " " + item.getPath() + 
                        " (大小: " + item.getSize() + ", 修改时间: " + item.getMtime() + ")");
                }
                System.out.println("✅ 共 " + items.size() + " 个项目");
            } else {
                System.out.println("目录为空或不存在: " + args[0]);
            }
        } catch (Exception e) {
            System.err.println("❌ 列出目录失败: " + e.getMessage());
        }
    }

    /**
     * 查看文件状态命令
     */
    private void cmdStat(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: stat <path>");
            return;
        }
        
        try {
            StatInfo statInfo = fileSystem.getFileStats(args[0]);
            if (statInfo != null) {
                System.out.println("=== 文件状态信息 ===");
                System.out.println("路径: " + statInfo.getPath());
                System.out.println("类型: " + statInfo.getType());
                System.out.println("大小: " + statInfo.getSize() + " 字节");
                System.out.println("修改时间: " + statInfo.getMtime());
                System.out.println("MD5: " + statInfo.getMd5());
                
                if (statInfo.getReplicaData() != null && !statInfo.getReplicaData().isEmpty()) {
                    System.out.println("副本信息:");
                    for (int i = 0; i < statInfo.getReplicaData().size(); i++) {
                        var replica = statInfo.getReplicaData().get(i);
                        System.out.println("  副本" + (i+1) + ": " + replica.getDsNode() + " -> " + replica.getPath());
                    }
                }
                
                System.out.println("✅ 状态信息获取成功");
            } else {
                System.err.println("❌ 文件不存在: " + args[0]);
            }
        } catch (Exception e) {
            System.err.println("❌ 获取文件状态失败: " + e.getMessage());
        }
    }

    /**
     * 删除文件/目录命令
     */
    private void cmdRm(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: rm <path>");
            return;
        }
        
        try {
            boolean success = fileSystem.delete(args[0]);
            if (success) {
                System.out.println("✅ 删除成功: " + args[0]);
            } else {
                System.err.println("❌ 删除失败: " + args[0]);
            }
        } catch (Exception e) {
            System.err.println("❌ 删除失败: " + e.getMessage());
        }
    }

    /**
     * 写入文件命令
     */
    private void cmdWrite(String[] args) {
        if (args.length < 2) {
            System.out.println("用法: write <path> <text>");
            return;
        }
        
        try {
            String path = args[0];
            // 拼接所有文本参数
            StringBuilder text = new StringBuilder();
            for (int i = 1; i < args.length; i++) {
                if (i > 1) text.append(" ");
                text.append(args[i]);
            }
            
            // 去除引号
            String content = text.toString();
            if (content.startsWith("\"") && content.endsWith("\"")) {
                content = content.substring(1, content.length() - 1);
            }
            
            try (FSOutputStream output = fileSystem.create(path)) {
                output.write(content.getBytes());
                output.flush();
                System.out.println("✅ 写入成功: " + path + " (" + content.length() + " 字节)");
            }
        } catch (Exception e) {
            System.err.println("❌ 写入文件失败: " + e.getMessage());
        }
    }

    /**
     * 读取文件命令
     */
    private void cmdRead(String[] args) {
        if (args.length < 1) {
            System.out.println("用法: read <path>");
            return;
        }
        
        try {
            StatInfo statInfo = fileSystem.getFileStats(args[0]);
            if (statInfo == null) {
                System.err.println("❌ 文件不存在: " + args[0]);
                return;
            }
            
            try (FSInputStream input = fileSystem.open(args[0])) {
                byte[] data = new byte[(int) statInfo.getSize()];
                int totalRead = 0;
                int bytesRead;
                
                while (totalRead < data.length && (bytesRead = input.read(data, totalRead, data.length - totalRead)) != -1) {
                    totalRead += bytesRead;
                }
                
                String content = new String(data);
                System.out.println("文件内容 " + args[0] + ":");
                System.out.println("--- 开始 ---");
                System.out.print(content);
                System.out.println("\n--- 结束 ---");
                System.out.println("✅ 读取成功 (" + totalRead + " 字节)");
            }
        } catch (Exception e) {
            System.err.println("❌ 读取文件失败: " + e.getMessage());
        }
    }

    /**
     * 上传文件命令
     */
    private void cmdPut(String[] args) {
        if (args.length < 2) {
            System.out.println("用法: put <local_file> <remote_path>");
            return;
        }
        
        try {
            String localFile = args[0];
            String remotePath = args[1];
            
            // 读取本地文件
            byte[] data = Files.readAllBytes(Paths.get(localFile));
            
            // 上传到远程
            try (FSOutputStream output = fileSystem.create(remotePath)) {
                output.write(data);
                output.flush();
                System.out.println("✅ 上传成功: " + localFile + " -> " + remotePath + " (" + data.length + " 字节)");
            }
        } catch (IOException e) {
            System.err.println("❌ 读取本地文件失败: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("❌ 上传文件失败: " + e.getMessage());
        }
    }

    /**
     * 下载文件命令
     */
    private void cmdGet(String[] args) {
        if (args.length < 2) {
            System.out.println("用法: get <remote_path> <local_file>");
            return;
        }
        
        try {
            String remotePath = args[0];
            String localFile = args[1];
            
            // 获取文件信息
            StatInfo statInfo = fileSystem.getFileStats(remotePath);
            if (statInfo == null) {
                System.err.println("❌ 远程文件不存在: " + remotePath);
                return;
            }
            
            // 读取远程文件
            try (FSInputStream input = fileSystem.open(remotePath)) {
                byte[] data = new byte[(int) statInfo.getSize()];
                int totalRead = 0;
                int bytesRead;
                
                while (totalRead < data.length && (bytesRead = input.read(data, totalRead, data.length - totalRead)) != -1) {
                    totalRead += bytesRead;
                }
                
                // 写入本地文件
                Files.write(Paths.get(localFile), data);
                System.out.println("✅ 下载成功: " + remotePath + " -> " + localFile + " (" + totalRead + " 字节)");
            }
        } catch (Exception e) {
            System.err.println("❌ 下载文件失败: " + e.getMessage());
        }
    }
}
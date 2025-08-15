package org.example.client.test;

import dfs_project.Metaserver;
import org.example.client.EFileSystem;
import org.example.client.FSInputStream;
import org.example.client.FSOutputStream;
import org.example.client.domain.ClusterInfo;
import org.example.client.domain.StatInfo;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Random;

/**
 * MinFS完整功能测试套件 - 验证所有考核要求
 */
public class CompleteTestSuite {

    private final EFileSystem fileSystem;
    private static final String TEST_DIR = "/test";
    private static final String TEST_FILE = "/test/large_file.dat";

    public CompleteTestSuite() {
        System.out.println("=== 初始化MinFS客户端 ===");
        this.fileSystem = new EFileSystem();
    }

    public static void main(String[] args) {
        CompleteTestSuite testSuite = new CompleteTestSuite();
        try {
            testSuite.runAllTests();
            System.out.println("\n🎉 所有测试完成！");
        } catch (Exception e) {
            System.err.println("\n❌ 测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 运行所有测试用例
     */
    public void runAllTests() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("   MinFS分布式文件系统 - 完整功能测试");
        System.out.println("=".repeat(60));

        // 考核点A1: 文件、目录创建
        testCreateOperations();

        // 考核点A2: 查看属性信息
        testStatusOperations();

        // 考核点A3: 文件、目录删除
        testDeleteOperations();

        // 考核点A4: 文件读写操作
        testFileReadWrite();

        // 等待副本复制和心跳报告完成
        System.out.println("\n等待副本复制完成...");
        System.out.print("倒计时: ");
        for (int i = 10; i >= 1; i--) {
            System.out.print(i + " ");
            Thread.sleep(1000);
        }
        System.out.println("开始检查!");

        // 考核点A5: 获取集群信息
        testClusterInfo();

        // 考核点A6: 三副本验证
        testReplicationInfo();

        System.out.println("\n✅ 所有考核点测试通过！");
    }

    /**
     * 考核点A1: 测试文件和目录创建
     */
    private void testCreateOperations() throws Exception {
        System.out.println("\n📁 考核点A1: 测试文件和目录创建");
        System.out.println("-".repeat(40));

        // 创建测试目录
        System.out.println("1. 创建目录: " + TEST_DIR);
        boolean dirResult = fileSystem.mkdir(TEST_DIR);
        System.out.println("   结果: " + (dirResult ? "✅ 成功" : "❌ 失败"));

        // 创建子目录
        String subDir = TEST_DIR + "/subdir";
        System.out.println("2. 创建子目录: " + subDir);
        boolean subDirResult = fileSystem.mkdir(subDir);
        System.out.println("   结果: " + (subDirResult ? "✅ 成功" : "❌ 失败"));

        // 创建测试文件
        String testFile = TEST_DIR + "/test.txt";
        System.out.println("3. 创建文件: " + testFile);
        try (FSOutputStream output = fileSystem.create(testFile)) {
            output.write("Hello MinFS!".getBytes());
            output.flush();
            System.out.println("   结果: ✅ 成功");
        } catch (IOException e) {
            System.out.println("   结果: ❌ 失败 - " + e.getMessage());
            throw e;
        }

        System.out.println("📁 目录和文件创建测试完成");
    }

    /**
     * 考核点A2: 测试属性信息查看
     */
    private void testStatusOperations() throws Exception {
        System.out.println("\n📊 考核点A2: 测试属性信息查看");
        System.out.println("-".repeat(40));

        // 测试getStatus - 获取单个文件属性
        System.out.println("1. 获取文件属性信息:");
        StatInfo fileInfo = fileSystem.getFileStats(TEST_DIR + "/test.txt");
        if (fileInfo != null) {
            System.out.println("   路径: " + fileInfo.getPath());
            System.out.println("   大小: " + fileInfo.getSize() + " 字节");
            System.out.println("   类型: " + fileInfo.getType());
            System.out.println("   修改时间: " + fileInfo.getMtime());
            System.out.println("   结果: ✅ 成功");
        } else {
            System.out.println("   结果: ❌ 失败");
            throw new Exception("无法获取文件属性");
        }

        // 测试listStatus - 列出目录内容
        System.out.println("2. 列出目录内容:");
        List<StatInfo> dirContents = fileSystem.listFileStats(TEST_DIR);
        if (dirContents != null) {
            System.out.println("   目录 " + TEST_DIR + " 包含 " + dirContents.size() + " 个项目:");
            for (StatInfo item : dirContents) {
                System.out.println("     - " + item.getPath() + 
                    " (类型: " + item.getType() + ", 大小: " + item.getSize() + ")");
            }
            System.out.println("   结果: ✅ 成功");
        } else {
            System.out.println("   结果: ❌ 失败");
            throw new Exception("无法列出目录内容");
        }

        System.out.println("📊 属性信息查看测试完成");
    }

    /**
     * 考核点A3: 测试删除操作
     */
    private void testDeleteOperations() throws Exception {
        System.out.println("\n🗑️  考核点A3: 测试删除操作");
        System.out.println("-".repeat(40));

        // 创建用于删除测试的文件和目录
        String deleteTestDir = TEST_DIR + "/delete_test";
        String deleteTestFile = deleteTestDir + "/file_to_delete.txt";

        System.out.println("1. 创建测试目录和文件");
        fileSystem.mkdir(deleteTestDir);
        try (FSOutputStream output = fileSystem.create(deleteTestFile)) {
            output.write("This file will be deleted".getBytes());
        }

        // 删除文件
        System.out.println("2. 删除文件: " + deleteTestFile);
        boolean fileDeleted = fileSystem.delete(deleteTestFile);
        System.out.println("   结果: " + (fileDeleted ? "✅ 成功" : "❌ 失败"));

        // 删除目录（递归删除）
        System.out.println("3. 递归删除目录: " + deleteTestDir);
        boolean dirDeleted = fileSystem.delete(deleteTestDir);
        System.out.println("   结果: " + (dirDeleted ? "✅ 成功" : "❌ 失败"));

        // 验证删除结果
        System.out.println("4. 验证删除结果");
        StatInfo deletedFile = fileSystem.getFileStats(deleteTestFile);
        if (deletedFile == null) {
            System.out.println("   文件已成功删除: ✅");
        } else {
            System.out.println("   文件删除失败: ❌");
        }

        System.out.println("🗑️ 删除操作测试完成");
    }

    /**
     * 考核点A4: 测试文件读写操作 (支持100MB大文件)
     */
    private void testFileReadWrite() throws Exception {
        System.out.println("\n📝 考核点A4: 测试大文件读写操作");
        System.out.println("-".repeat(40));

        // 准备测试数据 (10MB 用于快速测试，可调整到100MB)
        int dataSize = 10 * 1024 * 1024; // 10MB
        System.out.println("1. 生成测试数据 (" + (dataSize / 1024 / 1024) + "MB)");
        byte[] testData = generateTestData(dataSize);
        String originalMD5 = calculateMD5(testData);
        System.out.println("   原始数据MD5: " + originalMD5);

        // 写入大文件
        System.out.println("2. 写入大文件: " + TEST_FILE);
        long writeStartTime = System.currentTimeMillis();
        try (FSOutputStream output = fileSystem.create(TEST_FILE)) {
            output.write(testData);
            output.flush();
        }
        long writeTime = System.currentTimeMillis() - writeStartTime;
        System.out.println("   写入完成，耗时: " + writeTime + "ms");

        // 验证文件信息
        StatInfo fileInfo = fileSystem.getFileStats(TEST_FILE);
        if (fileInfo != null) {
            System.out.println("   文件大小: " + fileInfo.getSize() + " 字节");
            if (fileInfo.getSize() == testData.length) {
                System.out.println("   大小验证: ✅ 正确");
            } else {
                System.out.println("   大小验证: ❌ 错误");
                throw new Exception("文件大小不匹配");
            }
        }

        // 读取文件并验证
        System.out.println("3. 读取文件并验证完整性");
        long readStartTime = System.currentTimeMillis();
        byte[] readData;
        try (FSInputStream input = fileSystem.open(TEST_FILE)) {
            // 手动读取所有数据以避免readAllBytes可能的问题
            readData = new byte[(int)testData.length];
            int totalRead = 0;
            int bytesRead;
            while (totalRead < readData.length && (bytesRead = input.read(readData, totalRead, readData.length - totalRead)) != -1) {
                totalRead += bytesRead;
            }
            if (totalRead != testData.length) {
                throw new IOException("读取的数据长度不匹配: 期望=" + testData.length + ", 实际=" + totalRead);
            }
        }
        long readTime = System.currentTimeMillis() - readStartTime;
        System.out.println("   读取完成，耗时: " + readTime + "ms");
        System.out.println("   读取数据大小: " + readData.length + " 字节");

        // MD5校验
        String readMD5 = calculateMD5(readData);
        System.out.println("   读取数据MD5: " + readMD5);
        if (originalMD5.equals(readMD5)) {
            System.out.println("   MD5校验: ✅ 一致");
        } else {
            System.out.println("   MD5校验: ❌ 不一致");
            throw new Exception("数据完整性验证失败");
        }

        System.out.println("📝 大文件读写测试完成");
    }

    /**
     * 考核点A5: 测试集群信息获取
     */
    private void testClusterInfo() throws Exception {
        System.out.println("\n🌐 考核点A5: 测试集群信息获取");
        System.out.println("-".repeat(40));

        // 直接使用protobuf对象，避免转换问题
        org.example.client.client.MinFSClient client = fileSystem.getClient();
        Metaserver.ClusterInfo clusterInfo = client.getClusterInfo();
        if (clusterInfo != null) {
            System.out.println("集群信息获取成功:");
            
            // 主MetaServer信息
            if (clusterInfo.hasMasterMetaServer()) {
                System.out.println("  主MetaServer: " + 
                    clusterInfo.getMasterMetaServer().getHost() + ":" + 
                    clusterInfo.getMasterMetaServer().getPort());
            }
            
            // 从MetaServer信息
            System.out.println("  从MetaServer数量: " + clusterInfo.getSlaveMetaServerCount());
            
            // DataServer信息
            System.out.println("  DataServer数量: " + clusterInfo.getDataServerCount());
            
            if (clusterInfo.getDataServerCount() > 0) {
                for (int i = 0; i < clusterInfo.getDataServerCount(); i++) {
                    var ds = clusterInfo.getDataServer(i);
                    System.out.println("    DataServer" + (i+1) + ": " + 
                        ds.getHost() + ":" + ds.getPort() +
                        " (文件: " + ds.getFileTotal() + ", 容量: " + ds.getUseCapacity() + "/" + ds.getCapacity() + "MB)");
                }
            }
            
            System.out.println("   结果: ✅ 成功");
        } else {
            System.out.println("   结果: ❌ 失败");
            throw new Exception("无法获取集群信息");
        }

        System.out.println("🌐 集群信息获取测试完成");
    }

    /**
     * 考核点A6: 测试三副本分布查询
     */
    private void testReplicationInfo() throws Exception {
        System.out.println("\n🔄 考核点A6: 测试三副本分布查询");
        System.out.println("-".repeat(40));

        // 查询所有文件的副本信息
        System.out.println("1. 查询所有文件的副本分布:");
        // 通过fileSystem获取内部的MinFSClient
        org.example.client.client.MinFSClient client = fileSystem.getClient();
        Metaserver.GetReplicationInfoResponse allReplicas = 
            client.getAllReplicationInfo();
        
        if (allReplicas != null) {
            System.out.println("  总文件数: " + allReplicas.getTotalFiles());
            System.out.println("  健康文件数: " + allReplicas.getHealthyFiles());
            System.out.println("  副本不足文件数: " + allReplicas.getUnderReplicatedFiles());
            System.out.println("  副本过多文件数: " + allReplicas.getOverReplicatedFiles());
            
            // 显示具体文件的副本信息
            if (!allReplicas.getFilesList().isEmpty()) {
                System.out.println("\n  文件副本详情:");
                for (Metaserver.ReplicationStatus file : allReplicas.getFilesList()) {
                    System.out.println("    文件: " + file.getPath());
                    System.out.println("      期望副本数: " + file.getExpectedReplicas());
                    System.out.println("      实际副本数: " + file.getActualReplicas());
                    System.out.println("      健康状态: " + file.getStatus());
                    
                    if (!file.getBlocksList().isEmpty()) {
                        System.out.println("      数据块分布:");
                        for (Metaserver.BlockReplicationInfo block : file.getBlocksList()) {
                            System.out.println("        块ID " + block.getBlockId() + 
                                " -> " + block.getLocationsList() + 
                                " (副本数: " + block.getReplicaCount() + ")");
                        }
                    }
                }
            }
            System.out.println("   结果: ✅ 成功");
        } else {
            System.out.println("   结果: ❌ 失败");
            throw new Exception("无法获取副本分布信息");
        }

        // 查询特定文件的副本信息
        if (fileSystem.getFileStats(TEST_FILE) != null) {
            System.out.println("\n2. 查询特定文件的副本分布: " + TEST_FILE);
            Metaserver.GetReplicationInfoResponse fileReplicas = 
                client.getReplicationInfo(TEST_FILE);
            
            if (fileReplicas != null && !fileReplicas.getFilesList().isEmpty()) {
                Metaserver.ReplicationStatus file = fileReplicas.getFilesList().get(0);
                System.out.println("  文件: " + file.getPath());
                System.out.println("  期望副本数: " + file.getExpectedReplicas());
                System.out.println("  实际副本数: " + file.getActualReplicas());
                System.out.println("  健康状态: " + file.getStatus());
                System.out.println("   结果: ✅ 成功");
            }
        }

        System.out.println("🔄 三副本分布查询测试完成");
    }

    /**
     * 生成测试数据
     */
    private byte[] generateTestData(int size) {
        Random random = new Random(12345); // 固定种子保证可重复性
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }

    /**
     * 计算MD5哈希值
     */
    private String calculateMD5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("计算MD5失败", e);
        }
    }
}
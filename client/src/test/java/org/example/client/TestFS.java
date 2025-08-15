package org.example.client;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
public class TestFS {

    @Test
    public void testAll()throws Exception {
        EFileSystem eFileSystem=new EFileSystem();
        //测试目录创建
        System.out.println(eFileSystem.mkdir("root/zxa"));
        //测试文件创建
        System.out.println(eFileSystem.create("root/zxa/a.txt"));
        System.out.println(eFileSystem.create("root/zxa/b.txt"));
        //测试删除文件
        System.out.println(eFileSystem.delete("root/zxa/a.txt"));
        //测试获取文件或目录的属性信息
        System.out.println(eFileSystem.getFileStats("root/zxa/b.txt"));
        //测试列出目录下的所有条目
        System.out.println(eFileSystem.listFileStats("root/zxa"));
        //测试获取集群信息
        System.out.println(eFileSystem.getClusterInfo());
    }
}

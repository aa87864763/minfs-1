package com.ksyun.campus.client;

/**
 * 基类，定义了通用的文件系统方法和变量
 * 整体的文件组织结构为以下形式
 * {namespace}/{dir}
 *                  /{subdir}
 *                  /{subdir}/file
 *                  /file
 */
public abstract class FileSystem {

    //文件系统名称，可理解成命名空间，可以存在多个命名空间，多个命名空间下的文件目录结构是独立的
    protected String defaultFileSystemName;

    //远程调用
    protected void callRemote(){
        // 使用gRPC进行远程调用
    }

}

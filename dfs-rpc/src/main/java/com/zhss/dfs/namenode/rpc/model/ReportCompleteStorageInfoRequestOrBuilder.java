// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NameNodeRpcModel.proto

package com.zhss.dfs.namenode.rpc.model;

public interface ReportCompleteStorageInfoRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:com.zhss.dfs.namenode.rpc.ReportCompleteStorageInfoRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string ip = 1;</code>
   */
  String getIp();
  /**
   * <code>optional string ip = 1;</code>
   */
  com.google.protobuf.ByteString
      getIpBytes();

  /**
   * <code>optional string hostname = 2;</code>
   */
  String getHostname();
  /**
   * <code>optional string hostname = 2;</code>
   */
  com.google.protobuf.ByteString
      getHostnameBytes();

  /**
   * <code>optional string filenames = 3;</code>
   */
  String getFilenames();
  /**
   * <code>optional string filenames = 3;</code>
   */
  com.google.protobuf.ByteString
      getFilenamesBytes();

  /**
   * <code>optional int64 storedDataSize = 4;</code>
   */
  long getStoredDataSize();
}
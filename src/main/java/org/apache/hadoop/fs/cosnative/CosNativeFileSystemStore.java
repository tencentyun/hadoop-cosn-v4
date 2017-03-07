/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.cosnative;

import static org.apache.hadoop.fs.cosnative.NativeCosFileSystem.PATH_DELIMITER;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.meta.InsertOnly;
import com.qcloud.cos.request.CopyFileRequest;
import com.qcloud.cos.request.CreateFolderRequest;
import com.qcloud.cos.request.DelFileRequest;
import com.qcloud.cos.request.DelFolderRequest;
import com.qcloud.cos.request.GetFileInputStreamRequest;
import com.qcloud.cos.request.GetFileLocalRequest;
import com.qcloud.cos.request.ListFolderRequest;
import com.qcloud.cos.request.MoveFileRequest;
import com.qcloud.cos.request.StatFileRequest;
import com.qcloud.cos.request.StatFolderRequest;
import com.qcloud.cos.request.UploadFileRequest;
import com.qcloud.cos.sign.Credentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CosNativeFileSystemStore implements NativeFileSystemStore {

    private COSClient cosClient;
    private String bucket;
    final private int MAX_RETRY_TIME = 5;

    public static final Logger LOG = LoggerFactory.getLogger(CosNativeFileSystemStore.class);

    private Credentials initCosCred(Configuration conf) throws IOException {
        String appidStr = conf.get("fs.cos.userinfo.appid");
        if (appidStr == null) {
            throw new IOException("config fs.cos.userinfo.appid miss!");
        }
        int appId = Integer.valueOf(appidStr);
        String secretId = conf.get("fs.cos.userinfo.secretId");
        if (secretId == null) {
            throw new IOException("config fs.cos.userinfo.secretId miss!");
        }
        String secretKey = conf.get("fs.cos.userinfo.secretKey");
        if (secretKey == null) {
            throw new IOException("config fs.cos.userinfo.secretKey miss!");
        }
        Credentials cosCred = new Credentials(appId, secretId, secretKey);
        return cosCred;
    }

    private ClientConfig initCosConfig(Configuration conf) throws IOException {
        String region = conf.get("fs.cos.userinfo.region", "gz");

        boolean useCDN = conf.getBoolean("fs.cos.userinfo.useCDN", false);
        boolean useHttps = conf.getBoolean("fs.cos.userinfo.usehttps", false);

        ClientConfig config = new ClientConfig();
        config.setRegion(region);
        config.setDownloadAction(useHttps, useCDN);
        return config;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        try {
            Credentials cred = initCosCred(conf);
            ClientConfig config = initCosConfig(conf);
            this.cosClient = new COSClient(config, cred);
            this.bucket = uri.getHost();
        } catch (Exception e) {
            handleException(e, "");
        }
    }

    @Override
    public void storeFile(String key, File file, byte[] md5Hash) throws IOException {
        String localPath = file.getCanonicalPath();
        LOG.debug("store file:, localPath:" + localPath + ", len:" + file.length());

        UploadFileRequest request = new UploadFileRequest(this.bucket, key, localPath);
        // 上层调用判断了没有设置覆盖写而文件存在的情况了时已经抛出异常，所以Store这层都是覆盖写
        request.setInsertOnly(InsertOnly.OVER_WRITE);
        String uploadFileRet = callCosSdkWithRetry(request);
        JSONObject uploadFileJson = new JSONObject(uploadFileRet);
        if (uploadFileJson.getInt("code") != 0) {
            String errMsg = String.format("store File faild, local: %s, cos key: %s, ret: %s",
                    localPath, key, uploadFileRet);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        } else {
            String debugMsg = String.format("store File SUCCESS, local: %s, cos key: %s, ret: %s",
                    localPath, key, uploadFileRet);
            LOG.debug(debugMsg);
        }
    }

    public void storeLargeFile(String key, File file, byte[] md5Hash) throws IOException {
        this.storeFile(key, file, md5Hash);
    }



    // for cos, storeEmptyFile means create a directory
    @Override
    public void storeEmptyFile(String key) throws IOException {
        if (!key.endsWith(PATH_DELIMITER)) {
            key = key + PATH_DELIMITER;
        }
        CreateFolderRequest request = new CreateFolderRequest(this.bucket, key);
        String createFolderRet = callCosSdkWithRetry(request);
        JSONObject createFolderJson = new JSONObject(createFolderRet);
        if (createFolderJson.getInt("code") != 0) {
            String debugMsg =
                    String.format("storeEmptyFile(create folder) faild, cos key: %s, ret: %s", key,
                            createFolderRet);
            LOG.debug(debugMsg); // 改成debug 是因为创建父目录的时候 常常会有路径冲突的现象
            handleException(new Exception(debugMsg), key);
        } else {
            String debugMsg =
                    String.format("storeEmptyFile(create folder) SUCCESS, cos key: %s, ret: %s",
                            key, createFolderRet);
            LOG.debug(debugMsg);
        }
    }

    @Override
    public FileMetadata retrieveMetadata(String key) throws IOException {
        if (key.endsWith(PATH_DELIMITER)) {
            key = key.substring(0, key.length() - 1);
        }

        if (!key.isEmpty()) {
            StatFileRequest statFileRequest = new StatFileRequest(this.bucket, key);
            String statFileRet = callCosSdkWithRetry(statFileRequest);
            JSONObject statFileJson = new JSONObject(statFileRet);
            if (statFileJson.getInt("code") == 0) {
                JSONObject statDataJson = statFileJson.getJSONObject("data");
                long mtime = statDataJson.getLong("mtime") * 1000; // convert to ms
                long fileSize = statDataJson.getLong("filesize");
                FileMetadata fileMetadata = new FileMetadata(key, fileSize, mtime, true);
                return fileMetadata;
            }
            LOG.debug("retive file MetaData key:{}, server ret:{}", key, statFileRet);
        }
        // judge if the key is directory
        key = key + PATH_DELIMITER;
        StatFolderRequest statFolderRequest = new StatFolderRequest(this.bucket, key);
        String statFolderRet = callCosSdkWithRetry(statFolderRequest);
        JSONObject statFolderJson = new JSONObject(statFolderRet);
        if (statFolderJson.getInt("code") == 0) {
            JSONObject statDataJson = statFolderJson.getJSONObject("data");
            long mtime = statDataJson.getLong("mtime") * 1000;
            long fileSize = 4096; // 目录没有文件长度为0
            FileMetadata fileMetadata = new FileMetadata(key, fileSize, mtime, false);
            return fileMetadata;
        }
        LOG.debug("retive dir MetaData key:{}, server ret:{}", key, statFolderRet);
        return null;
    }

    /**
     * @param key The key is the object name that is being retrieved from the cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key) throws IOException {
        LOG.debug("retrive key:{}", key);
        GetFileInputStreamRequest request = new GetFileInputStreamRequest(this.bucket, key);
        try {
            InputStream fileStream = cosClient.getFileInputStream(request);
            return fileStream;
        } catch (Exception e) {
            String errMsg =
                    String.format("retrive key %s occur a exception %s", key, e.getMessage());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    /**
     *
     * @param key The key is the object name that is being retrieved from the cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        LOG.debug("retrive key:{}, byteRangeStart:{}", key, byteRangeStart);
        GetFileInputStreamRequest request = new GetFileInputStreamRequest(this.bucket, key);
        request.setRangeStart(byteRangeStart);
        try {
            InputStream fileStream = cosClient.getFileInputStream(request);
            return fileStream;
        } catch (Exception e) {
            String errMsg =
                    String.format("retrive key %s with byteRangeStart %d occur a exception: %s",
                            key, byteRangeStart, e.getMessage());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength) throws IOException {
        return list(prefix, maxListingLength, null, false);
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength, String priorLastKey,
            boolean recurse) throws IOException {

        return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    /**
     * list objects
     *
     * @param prefix prefix
     * @param delimiter delimiter
     * @param maxListingLength max no. of entries
     * @param priorLastKey last key in any previous search
     * @return a list of matches
     * @throws IOException on any reported failure
     */

    private PartialListing list(String prefix, String delimiter, int maxListingLength,
            String priorLastKey) throws IOException {
        LOG.debug("list prefix:" + prefix);
        int lastDelimiterIndex = prefix.lastIndexOf(PATH_DELIMITER);
        String listFolderPath = prefix.substring(0, lastDelimiterIndex + 1);
        LOG.debug("folderPath:" + listFolderPath);
        String searchPrefix = prefix.substring(lastDelimiterIndex + 1);
        LOG.debug("searchPrefix:" + searchPrefix);
        ListFolderRequest request = new ListFolderRequest(this.bucket, listFolderPath);
        request.setNum(maxListingLength);
        if (priorLastKey != null) {
            request.setContext(priorLastKey);
        } else {
            request.setContext("");
        }
        request.setPrefix(searchPrefix);
        if (delimiter != null) {
            request.setDelimiter(delimiter);
            LOG.debug("list set delimiter sep " + delimiter);
        } else {
            request.setDelimiter("");
            LOG.debug("list set delimiter blank str");
        }

        String listResultStr = callCosSdkWithRetry(request);
        JSONObject listResultJson = new JSONObject(listResultStr);
        if (listResultJson.getInt("code") != 0) {
            String errMsg =
                    String.format("list folder faild, cos folder key: %s, ret: %s", prefix, listResultStr);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), prefix);
        }
        JSONObject listDataJson = listResultJson.getJSONObject("data");
        JSONArray infosArry = listDataJson.getJSONArray("infos");
        ArrayList<String> commonPrefixsArry = new ArrayList<>();
        ArrayList<FileMetadata> fileMetadataArry = new ArrayList<>();

        for (int i = 0; i < infosArry.length(); i++) {
            JSONObject infosMemberJson = infosArry.getJSONObject(i);
            if (infosMemberJson.has("access_url")) {
                String filePath = listFolderPath + infosMemberJson.getString("name");
                long fileLen = infosMemberJson.getLong("filelen");
                long mtime = infosMemberJson.getLong("mtime") * 1000;
                fileMetadataArry.add(new FileMetadata(filePath, fileLen, mtime, true));
            } else {
                String folderPath = listFolderPath + infosMemberJson.getString("name");
                commonPrefixsArry.add(folderPath);
            }
        }

        FileMetadata[] fileMetadata = new FileMetadata[fileMetadataArry.size()];
        for (int i = 0; i < fileMetadataArry.size(); ++i) {
            fileMetadata[i] = fileMetadataArry.get(i);
        }
        String[] commonPrefixs = new String[commonPrefixsArry.size()];
        for (int i = 0; i < commonPrefixsArry.size(); ++i) {
            commonPrefixs[i] = commonPrefixsArry.get(i);
        }
        // 如果list over为true, 则表明已经遍历完
        if (listDataJson.getBoolean("listover")) {
            return new PartialListing(null, fileMetadata, commonPrefixs);
        } else {
            return new PartialListing(listDataJson.getString("context"), fileMetadata,
                    commonPrefixs);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        LOG.debug("Deleting key: {} from bucket: {}", key, this.bucket);
        String delObjectStr = null;
        // 如果key以分隔符结束, 表明这是一个目录
        if (key.endsWith(PATH_DELIMITER)) {
            DelFolderRequest request = new DelFolderRequest(this.bucket, key);
            delObjectStr = callCosSdkWithRetry(request);
        } else {
            DelFileRequest request = new DelFileRequest(this.bucket, key);
            delObjectStr = callCosSdkWithRetry(request);
        }
        JSONObject delObjectJson = new JSONObject(delObjectStr);
        // -197 表示文件或目录不存在
        if (delObjectJson.getInt("code") != 0 && delObjectJson.getInt("code") != -197) {
            String errMsg =
                    String.format("del object faild, cos key: %s, ret: %s", key, delObjectStr);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        } else {
            String debugMsg =
                    String.format("del object SUCCESS, cos key: %s, ret: %s", key, delObjectStr);
            LOG.debug(debugMsg);
        }
    }

    public void rename(String srcKey, String dstKey) throws IOException {
        LOG.debug("store rename srcKey:{}, dstKey:{}", srcKey, dstKey);
        MoveFileRequest moveRequest = new MoveFileRequest(this.bucket, srcKey, dstKey);
        String moveFileRet = callCosSdkWithRetry(moveRequest);
        JSONObject moveFileJson = new JSONObject(moveFileRet);
        if (moveFileJson.getInt("code") != 0) {
            String errMsg =
                    String.format("move object faild, src cos key: %s, dst cos key: %s, ret: %s",
                            srcKey, dstKey, moveFileRet);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), srcKey);
        } else {
            String debugMsg =
                    String.format("move object SUCCESS, src cos key: %s, dst cos key: %s, ret: %s",
                            srcKey, dstKey, moveFileRet);
            LOG.debug(debugMsg);
        }

        /*
         * CopyFileRequest request = new CopyFileRequest(this.bucket, srcKey, dstKey); String
         * copyFileRet = callCosSdkWithRetry(request); JSONObject copyFileJson = new
         * JSONObject(copyFileRet); if (copyFileJson.getInt("code") != 0) { String errMsg =
         * String.format("del object faild, src cos key: %s, dst cos key: %s, ret: %s", srcKey,
         * dstKey, copyFileRet); LOG.error(errMsg); handleException(new Exception(errMsg), srcKey);
         * } else { String debugMsg = String.format(
         * "del object SUCCESS, src cos key: %s, dst cos key: %s, ret: %s", srcKey, dstKey,
         * copyFileRet); LOG.debug(debugMsg); delete(srcKey); }
         */
    }

    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        LOG.debug("copy srcKey:{}, dstKey:{}", srcKey, dstKey);
        CopyFileRequest request = new CopyFileRequest(this.bucket, srcKey, dstKey);
        String copyFileRet = callCosSdkWithRetry(request);
        JSONObject copyFileJson = new JSONObject(copyFileRet);
        if (copyFileJson.getInt("code") != 0) {
            String errMsg =
                    String.format("copy object faild, src cos key: %s, dst cos key: %s, ret: %s",
                            srcKey, dstKey, copyFileRet);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), srcKey);
        } else {
            String debugMsg =
                    String.format("copy object SUCCESS, src cos key: %s, dst cos key: %s, ret: %s",
                            srcKey, dstKey, copyFileRet);
            LOG.debug(debugMsg);
        }
    }

    // TODO(chengwu)
    @Override
    public void purge(String prefix) throws IOException {
        throw new IOException("purge Not supported");
    }

    // TODO(chengwu)
    @Override
    public void dump() throws IOException {
        throw new IOException("dump Not supported");
    }

    // process Exception and print detail
    private void handleException(Exception e, String key) throws IOException {
        String cosPath = "cosn://" + bucket + key;
        String exceptInfo = String.format("%s : %s", cosPath, e.toString());
        throw new IOException(exceptInfo);
    }

    @Override
    public boolean retrieveBlock(String key, long byteRangeStart, long blockSize,
            String localBlockPath) throws IOException {
        GetFileLocalRequest request = new GetFileLocalRequest(this.bucket, key, localBlockPath);
        request.setRangeStart(byteRangeStart);
        request.setRangeEnd(byteRangeStart + blockSize - 1);
        String getFileLocalRet = callCosSdkWithRetry(request);
        JSONObject getFileLocalJson = new JSONObject(getFileLocalRet);
        if (getFileLocalJson.getInt("code") != 0) {
            String errMsg =
                    String.format("get file local failed, key: %s, ret: %s", key, getFileLocalRet);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return false;
        } else {
            return true;
        }
    }

    @Override
    public long getFileLength(String key) throws IOException {
        LOG.debug("getFile Length, cos key: {}", key);
        StatFileRequest request = new StatFileRequest(this.bucket, key);
        String statRetStr = "";
        statRetStr = callCosSdkWithRetry(request);
        JSONObject statRetJson = new JSONObject(statRetStr);
        if (statRetJson.getInt("code") != 0) {
            String errMsg = String.format("stat object failed, key: %s, ret: %s", key, statRetStr);
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        } else {
            try {
                return statRetJson.getJSONObject("data").getLong("filelen");
            } catch (JSONException e) {
                String errMsg =
                        String.format("stat object return is illegal json str, key: %s, ret: %s",
                                key, statRetStr);
                LOG.error(errMsg);
                handleException(new Exception(errMsg), key);
            }
        }
        return 0;
    }

    // 判断一个返回码是否可重试,true可重试，false不可重试
    private boolean IsRetryAbleCode(int code) {
        // TODO(rabbitliu) 不断细化
        switch (code) {
            case -29102: /* download */
            case -20002: /* download */
            case -29109:
            case -29083:
            case -46020:
            case -46022:
            case -50133:
            case -50134:
            case -50135:
            case -2: /* network_excepiton */
            case -3: /* server_exception */
            case -4: /* unknown_exception */
            case -66: /* ERROR_PROXY_NETWORK */
            case -97: /* auth_failed */
            case -98: /* auth_failed */
            case -181: /* ERROR_CMD_COS_ERROR */
            case -152: /* ERR_CMD_COS_TIMEOUT */
            case -5997: /* ERROR_CGI_NETWORK */
            case -5989: /* ERROR_CGI_UPLOAD_FAIL */
                return true;
            default:
                return false;
        }
    }

    private <T> String callCosSdkWithRetry(T request) {
        String sdkMethod = "";
        String callSdkRet = "";
        for (int i = 0; i < MAX_RETRY_TIME; ++i) {
            if (request instanceof UploadFileRequest) {
                callSdkRet = this.cosClient.uploadFile((UploadFileRequest) request);
                sdkMethod = "uploadFile";
            } else if (request instanceof CreateFolderRequest) {
                callSdkRet = this.cosClient.createFolder((CreateFolderRequest) request);
                sdkMethod = "createFolder";
            } else if (request instanceof StatFileRequest) {
                callSdkRet = this.cosClient.statFile((StatFileRequest) request);
                sdkMethod = "statFile";
            } else if (request instanceof StatFolderRequest) {
                callSdkRet = this.cosClient.statFolder((StatFolderRequest) request);
                sdkMethod = "statFolder";
            } else if (request instanceof DelFolderRequest) {
                callSdkRet = this.cosClient.delFolder((DelFolderRequest) request);
                sdkMethod = "delFolder";
            } else if (request instanceof DelFileRequest) {
                callSdkRet = this.cosClient.delFile((DelFileRequest) request);
                sdkMethod = "delFile";
            } else if (request instanceof CopyFileRequest) {
                callSdkRet = this.cosClient.copyFile((CopyFileRequest) request);
                sdkMethod = "copyFile";
            } else if (request instanceof GetFileLocalRequest) {
                callSdkRet = this.cosClient.getFileLocal((GetFileLocalRequest) request);
                sdkMethod = "getFileLocal";
            } else if (request instanceof ListFolderRequest) {
                callSdkRet = this.cosClient.listFolder((ListFolderRequest) request);
                sdkMethod = "listFolder";
            } else {
                LOG.error("No such method for request: " + request.toString());
                return "{\"code\": -5964, \"message\": \"No such method\"}";
            }
            JSONObject callSdkJson = new JSONObject(callSdkRet);
            int code = callSdkJson.getInt("code");
            // 成功或者不可重试的错误，直接返回
            if (code == 0 || !IsRetryAbleCode(code)) {
                return callSdkRet;
            }
            String errMsg = "Call cos sdk failure, call method : " + sdkMethod + "request: "
                    + request + ", ret: " + callSdkRet + ", retry_time:" + i;
            LOG.info(errMsg);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return callSdkRet;
    }
}

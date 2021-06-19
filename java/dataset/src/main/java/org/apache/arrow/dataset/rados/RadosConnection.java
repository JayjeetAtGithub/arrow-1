package org.apache.arrow.dataset.rados;

public class RadosConnection extends Connection {
    public RadosConnection(String path_to_config, String data_pool, String user_name, String cluster_name, String cls_name) {
        super(createNative(path_to_config, data_pool, user_name, cluster_name, cls_name));
    }

    public static long createNative(String path_to_config, String data_pool, String user_name, String cluster_name, String cls_name) {
        return JniWrapper.get().createConnection(path_to_config, data_pool, user_name, cluster_name, cls_name);
    }
}
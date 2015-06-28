(ns hdfs-block-reader.core
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [java.io BufferedWriter OutputStreamWriter]))

(defn -main [& args]
  (System/setProperty "HADOOP_USER_NAME" "root")
  (let [pt (Path. "hdfs://localhost:9000/target_file.txt")
        cfg (Configuration.)
        _ (.addResource cfg (Path. "file://home/root/core-site.xml"))
        fs (FileSystem/get cfg)
        br (BufferedWriter. (OutputStreamWriter. (.create fs pt true)))]
    (.write br "Hello Onyx world!")
    (.close br)))

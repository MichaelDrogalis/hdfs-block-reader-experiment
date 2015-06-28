(ns hdfs-block-reader.writer
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [java.io BufferedWriter OutputStreamWriter]))

(defn -main [& args]
  (System/setProperty "HADOOP_USER_NAME" "root")
  (let [pt (Path. "hdfs://0.0.0.0:9000/foo.txt")
        cfg (Configuration.)
        _ (.addResource cfg (Path. "file:///home/core-site.xml"))
        fs (FileSystem/get cfg)
        br (BufferedWriter. (OutputStreamWriter. (.create fs pt true)))]
    (doseq [k (range 10000000)]
      (.write br (str "Hello Onyx world! " k)))
    (.close br)))

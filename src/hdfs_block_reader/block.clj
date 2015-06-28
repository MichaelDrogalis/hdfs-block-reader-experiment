(ns hdfs-block-reader.block
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [java.io BufferedWriter OutputStreamWriter InputStreamReader]
           [org.apache.hadoop.fs FSDataInputStream]))

(defn -main [& args]
  (System/setProperty "HADOOP_USER_NAME" "root")
  (let [pt (Path. "hdfs://0.0.0.0:9000/foo.txt")
        cfg (Configuration.)
        _ (.addResource cfg (Path. "file:///home/core-site.xml"))
        fs (FileSystem/get cfg)
        status (.getFileStatus fs pt)
        blocks (.getFileBlockLocations fs status 0 (.getLen status))]
    (doseq [b blocks]
      (let [fdis (FSDataInputStream. (.open fs pt))
            length (.getLength b)
            arr (byte-array length)]
        (.seek fdis (.getOffset b))
        (.read fdis 0 arr 0 length)
        (prn "===")
        (prn fdis)
        (prn arr)
        (prn "---")))))
(ns hdfs-block-reader.block
  (:import [org.apache.hadoop.fs Path]
           [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.conf Configuration]
           [java.io BufferedWriter OutputStreamWriter]))

(defn -main [& args]
  (System/setProperty "HADOOP_USER_NAME" "root")
  (let [pt (Path. "hdfs://0.0.0.0:9000/target_file.txt")
        cfg (Configuration.)
        _ (.addResource cfg (Path. "file://home/root/core-site.xml"))
        fs (FileSystem/get cfg)
        status (.getFileStatus fs pt)
        blocks (.getFileBlockLocations fs status 0 (.getLen status))]
    (doseq [b block]
      (prn n))))



;; Path dataset = new Path (fs.getHomeDirectory (), <path-to-file>);
;; FileStatus datasetFile = fs.getFileStatus (dataset);

;; BlockLocation myBlocks [] = fs.getFileBlockLocations (datasetFile,0,datasetFile.getLen ());
;; for (BlockLocation b : myBlocks) {
;;                                   System.out.println ("Length "+b.getLength ());
;;                                   for (String host : b.getHosts ()) {
;;                                                                      System.out.println ("host "+host);
;;                                                                      }}



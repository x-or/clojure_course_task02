(ns clojure-course-task02.core
  (:gen-class))

(def group-size 20)
(def max-workers-count 8)
(def directory-pool (ref '()))
(def matched-files (atom '()))
(def max-live-workers-sleep-ms 1)
(def few-live-workers-sleep-ms 1)

(defn enqueue-directory [directory]
  (dosync (alter directory-pool conj directory)))
 
(defn pop-directories [n]
  (dosync
    (let [directories (take n (ensure directory-pool))]
      (alter directory-pool #(drop n %))
      directories)))

(defn collect-matched-file [file-name]
  (swap! matched-files conj file-name))
 
(defn directory-worker [directories pattern]
  (doseq [directory directories]
    (let [contents (.listFiles directory)]
      (->> contents
           (filter #(.isDirectory %))
           (map enqueue-directory) 
           dorun)
      (->> contents
           (filter #(.isFile %))
           (map #(.getName %))
           (filter #(re-matches pattern %))
           (map collect-matched-file) 
           dorun))))

(defn yield-directory-group [pattern]
  (when-let [dirs (seq (pop-directories group-size))]
    (future (directory-worker dirs pattern))))

(defn find-files-loop [pattern]
  (loop [workers '()]
    (let [last-directory-pool @directory-pool
          live-workers (filter (comp not realized?) workers)
          live-workers-count (count live-workers)]
      (cond
        (>= live-workers-count max-workers-count) (do (Thread/sleep max-live-workers-sleep-ms) (recur live-workers))
        (seq last-directory-pool) (recur (conj live-workers (yield-directory-group pattern)))
        ((comp not zero?) live-workers-count) (do (Thread/sleep few-live-workers-sleep-ms) (recur live-workers))
        :else
          (do
            (assert (empty? last-directory-pool)
            (assert (empty? live-workers))))))))

(defn find-files [file-name path]
  (enqueue-directory (clojure.java.io/file path))
  (find-files-loop (re-pattern file-name))
  @matched-files)

(defn usage []
  (println "Usage: $ run.sh file_name path [-benchmarking]"))

(defn -main [& args]
  (let [[file-name path benchmarking?] args]
    (cond
      (or (nil? file-name) (nil? path)) 
      (usage)
      (nil? benchmarking?)
      (do
        (println "Searching for " file-name " in " path "...")
        (dorun (map println (find-files file-name path)))
        (shutdown-agents)
        (println "*** done ***"))
      :else
      (do 
        (print "file-seq algoritm: ")
        (time (let [re (re-pattern file-name)] 
          (->> (file-seq (clojure.java.io/file path))
               (map #(.getName %))
               (filter #(re-matches re %))
               (count))))
        (print "multi-core algoritm: ")
        (time (count (find-files file-name path)))
        (shutdown-agents)))))

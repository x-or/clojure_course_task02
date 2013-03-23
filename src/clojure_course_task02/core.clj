(ns clojure-course-task02.core
  (:gen-class))

(def max-workers-count 10)
(def live-workers-count (ref 0))
(def directory-pool (ref '()))
(def matched-files (atom '()))

(defn enqueue-directory [directory]
  (dosync (alter directory-pool conj directory)))
 
(defn pop-directory []
  (dosync
    (let [first-directory (first (ensure directory-pool))]
      (alter directory-pool rest)
      first-directory)))

(defn collect-matched-file [file-name]
  (swap! matched-files conj file-name))
 
(defn directory-worker [directory pattern]
  (try
    (let [contents (.listFiles directory)]
      (dorun (->> contents
                  (filter #(.isDirectory %))
                  (map enqueue-directory)))
      (dorun (->> contents
                  (filter #(.isFile %))
                  (map #(.getName %))
                  (filter #(re-matches pattern %))
                  (map collect-matched-file))))
  (finally 
    (dosync (alter live-workers-count dec)))))

(defn working-directory []
  (dosync
    (when-let [directory (pop-directory)]
      (alter live-workers-count inc)
      directory)))

(defn yield-some-directory [pattern]
  (when-let [wd (working-directory)]
    (future (directory-worker wd pattern))))

;;; bit safer version
(defn find-files-loop [pattern]
  (loop [workers '()]
    (let [last-directory-pool @directory-pool
          live-workers (filter (comp not realized?) workers)
          live-workers-count (count live-workers)]
      (cond
        (>= live-workers-count max-workers-count) (do (Thread/sleep 50) (recur live-workers))
        (seq last-directory-pool) (recur (conj live-workers (yield-some-directory pattern)))
        ((comp not zero?) live-workers-count) (do (Thread/sleep 10) (recur live-workers))
        :else
          (do
            (assert (empty? last-directory-pool)
            (assert (empty? live-workers))))))))

;;; potentially non-safe version
(defn find-files-loop' [pattern]
  (loop []
    (let [[last-directory-pool last-live-workers-count] (dosync [(ensure directory-pool) (ensure live-workers-count)])]
      (assert (>= last-live-workers-count 0))
      (cond
        (>= last-live-workers-count max-workers-count) (do (Thread/sleep 50) (recur))
        (seq last-directory-pool) (do (yield-some-directory pattern) (recur))
        ((comp not zero?) last-live-workers-count) (do (Thread/sleep 10) (recur))
        :else 
          (do
            (assert (empty? last-directory-pool)
            (assert (zero? last-live-workers-count))))))))

(defn find-files [file-name path]
  (enqueue-directory (clojure.java.io/file path))
  (find-files-loop (re-pattern file-name))
  @matched-files)

(defn usage []
  (println "Usage: $ run.sh file_name path"))

(defn -main [file-name path]
  (if (or (nil? file-name)
          (nil? path))
    (usage)
    (do
      (println "Searching for " file-name " in " path "...")
      (dorun (map println (find-files file-name path)))
      (shutdown-agents) ; устраняет продолжительный вис в конце
      (println "*** done ***"))))

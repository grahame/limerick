(ns limerick.core)
(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io])

(defn go [data_dir]  
  (defn in-data [fname]
    (str data_dir "/" fname))
  (defn open-csv [fname]
    (println fname) 
    (with-open [in-file (io/reader (in-data fname))]
      (let [lines (csv/read-csv in-file)]
        (let [header (first lines)]
          (doseq [line (rest lines)]
            (zipmap header line))))))
  (open-csv "agency.txt")
  (open-csv "calendar.txt")
  (open-csv "calendar_dates.txt")
  (open-csv "routes.txt")
  (open-csv "stop_times.txt")
  (open-csv "stops.txt")
  (open-csv "trips.txt"))

(defn -main [data_dir] 
  (println data_dir)
  (go data_dir)
  )

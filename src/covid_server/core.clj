(ns covid-server.core
  (:require [clojure.core.memoize :as memo]
            [clojure.java.io :as io]
            [compojure.handler :as handler]
            [compojure.route :as route])
  (:use compojure.core
        [incanter.core :as i]
        [incanter.io :refer [read-dataset]]
        [muuntaja.middleware :as mw]
        [ring.middleware.content-type :only (wrap-content-type)]
        [ring.middleware.file :only (wrap-file)]
        [ring.middleware.file-info :only (wrap-file-info)]
        [ring.middleware.stacktrace :only (wrap-stacktrace)]
        [ring.util.response :only (redirect)]
        ring.adapter.jetty))

;; quick look
(defn ql ([d] (ql d 3)) ([d n] ($ (range n) :all d)))

(def populations (read-dataset "resources/data/country-populations.csv" :header true))

;; "data-directories: csse-daily-report, csse-daily-report-us, csse-time-series-confirmed-global"
(defn read-csse-daily-report []
  (read-dataset "resources/data/csse-daily-report.csv" :header true))
(defn read-csse-daily-report-us []
  (read-dataset "resources/data/csse-daily-report-us.csv" :header true))
(defn read-csse-time-series-confirmed-global []
  (read-dataset "resources/data/csse-time-series-confirmed-global.csv" :header true))

(defn confirmed-by-state [_]
  (let [csse-daily-report (read-csse-daily-report)]
    (->> csse-daily-report
         (i/$where {:Province_State {:ne nil}})
         (i/$where {:Province_State {:ne "Recovered"}}) ;; data error (?)
         (i/$rollup :sum :Confirmed :Province_State)
         (i/$order :Confirmed :desc)
         (i/rename-cols {:Confirmed :confirmed-sum})
         (i/$join [:Province_State :Province_State] csse-daily-report)
         (i/$ [:confirmed-sum :Province_State :Country_Region])
         to-vect)))

(def m-confirmed-by-state (memo/lu #(confirmed-by-state %) :lu/threshold 1))

(defn confirmed-by-country [_]
  (->> (read-csse-daily-report)
       (i/$rollup :sum :Confirmed :Country_Region)
       (i/$order :Confirmed :desc)
       to-vect))

(def m-confirmed-by-country (memo/lu #(confirmed-by-country %) :lu/threshold 1))

;; per 100,000
(defn incidence-by-country [data]
  (let [clean-data (i/$where {:Incidence_Rate {:ne nil}} data)
        confirmeds (i/$rollup :sum :Confirmed :Country_Region data)
        ;; countries with only one entry can use their incidence_rate directly
        single-entry-countries (->> clean-data (i/$where {:Province_State {:eq nil}})
                                    ;; make datasets' cols match
                                    (i/$ [:Country_Region :Incidence_Rate :Confirmed]))
        ;; "multi-entry" countries are divided by region, so need to be rolled up and then calculated
        multi-entry-countries (->> clean-data (i/$rollup :count :Province_State :Country_Region)
                                   (i/$where {:Province_State {:ne 1}})
                                   (i/$join [:Country_Region :Country_Region] confirmeds)
                                   (i/$join [:Country_Region :Country_Region] populations)
                                   ;; calculate incidence rate, per 100,000
                                   (add-derived-column :Incidence_Rate [:Confirmed :Population]
                                                       (fn [c p] (->> (/ c p) float (* 100000))))
                                   ;; make datasets' cols match
                                   (i/$ [:Country_Region :Incidence_Rate :Confirmed]))]
    (->> (i/conj-rows single-entry-countries multi-entry-countries)
         (i/$order :Incidence_Rate :desc))))

(defn confirmed-by-us-county [_]
  (->> (read-csse-daily-report)
       (i/rename-cols {:Admin2 :County_Name})
       (i/$where {:FIPS {:ne nil}})
       (i/$where {:County_Name {:ne "Unassigned"}})
       (i/$ [:Confirmed :County_Name :Province_State :Country_Region :FIPS])
       (i/$order :Confirmed :desc)
       to-vect))

(def m-confirmed-by-us-county (memo/lu #(confirmed-by-us-county %) :lu/threshold 1))

(defn confirmed-by-us-county-fips [_]
  (letfn [(left-pad-zeros-fips [d] (i/transform-col d :FIPS #(format "%05d" %)))]
    (->> (read-csse-daily-report)
         (i/$where {:Admin2 {:ne nil}})
         (i/$order :Confirmed :desc)
         (i/$ [:FIPS :Confirmed])
         left-pad-zeros-fips
         ;; TODO refactor to-vect -> to-map
         to-vect
         (reduce #(assoc %1 (first %2) (second %2)) {}))))

(def m-confirmed-by-us-county-fips (memo/lu #(confirmed-by-us-county-fips %) :lu/threshold 1))

(defn global-deaths [_]
  (let [csse-daily-report (read-csse-daily-report)]
    {:deaths-by-country (->> csse-daily-report
                             (i/$rollup :sum :Deaths :Country_Region)
                             (i/$order :Deaths :desc)
                             to-vect)
     :total-deaths (reduce + (i/$ :Deaths csse-daily-report))}))

(def m-global-deaths (memo/lu #(global-deaths %) :lu/threshold 1))

(defn global-recovered [_]
  (let [csse-daily-report (read-csse-daily-report)]
    {:recovered-by-country (->> csse-daily-report (i/$where {:Recovered {:ne 0}})
                                (i/$rollup :sum :Recovered :Country_Region)
                                (i/$order :Recovered :desc)
                                to-vect)
     :total-recovered (reduce + (i/$ :Recovered csse-daily-report))}))

(def m-global-recovered (memo/lu #(global-recovered %) :lu/threshold 1))

(defn time-series-confirmed-global [_]
  (let [;; note that the time series headers are "Province/State" instead of "Province_State" - not a typo
        data-date-columns-only (i/$ [:not :Province/State :Country/Region :Lat :Long] (read-csse-time-series-confirmed-global))
        dates (i/col-names data-date-columns-only)
        column-totals (->> data-date-columns-only
                           matrix
                           (reduce i/plus))]
    (map vector dates column-totals)))

(def m-time-series-confirmed-global (memo/lu #(time-series-confirmed-global %) :lu/threshold 1))

(defn total-confirmed [_]
  (reduce + (i/$ :Confirmed (read-csse-daily-report))))

(def m-total-confirmed (memo/lu #(total-confirmed %) :lu/threshold 1))

(defn us-states-deaths-recovered [_]
  (->> (read-csse-daily-report-us)
       (i/$ [:Province_State :Deaths :Recovered])
       (i/$order :Deaths :desc)
       (to-vect)))

(def m-us-states-deaths-recovered (memo/lu #(us-states-deaths-recovered %) :lu/threshold 1))

(defn us-states-hospitalized [_]
  (let [data-us-without-nil (i/$where {:People_Hospitalized {:ne nil}} (read-csse-daily-report-us))]
    (->> data-us-without-nil
         (i/$ [:Province_State :People_Hospitalized])
         (i/$order :People_Hospitalized :desc)
         (to-vect))))

(def m-us-states-hospitalized (memo/lu #(us-states-hospitalized %) :lu/threshold 1))

(defn us-states-tested [_]
  (let [data-us-without-nil (i/$where {:People_Tested {:ne nil}} (read-csse-daily-report-us))]
    {:tested-by-state (->> data-us-without-nil
                           (i/$ [:Province_State :People_Tested])
                           (i/$order :People_Tested :desc)
                           (to-vect))
     :total-tested (reduce + (i/$ :People_Tested data-us-without-nil))}))

(def m-us-states-tested (memo/lu #(us-states-tested %) :lu/threshold 1))

(defn dir->newest-file [dir]
  (->> (io/file dir)
       .listFiles
       (filter #(not (.isDirectory %)))
       (filter #(not= (.getName %) ".DS_Store"))
       (sort-by #(.lastModified %))
       (reverse)
       first))

(defn get-timestamp-daily []
  (.lastModified (io/file "resources/data/csse-daily-report.csv")))

(defn get-timestamp-daily-us []
  (.lastModified (io/file "resources/data/csse-daily-report-us.csv")))

(defn get-timestamp-time-series []
  (.lastModified (io/file "resources/data/csse-time-series-confirmed-global.csv")))

(defn last-updated []
  (-> (dir->newest-file "resources/data")
      .lastModified
      java.util.Date.
      .toInstant))

(defroutes site-routes
  (GET "/" [] "")
  (GET "/confirmed-by-state" [] (str (m-confirmed-by-state (get-timestamp-daily))))
  (GET "/confirmed-by-country" [] (str (m-confirmed-by-country (get-timestamp-daily))))
  (GET "/confirmed-by-us-county" [] (str (m-confirmed-by-us-county (get-timestamp-daily))))
  (GET "/confirmed-by-us-county-fips" [] (str (m-confirmed-by-us-county-fips (get-timestamp-daily))))
  (GET "/global-deaths" [] (str (m-global-deaths (get-timestamp-daily))))
  ;; (GET "/global-recovered" [] (str (m-global-recovered get-timestamp-daily)))
  (GET "/last-updated" [] (str (last-updated)))
  (GET "/time-series-confirmed-global" [] {:body (m-time-series-confirmed-global (get-timestamp-time-series))})
  (GET "/total-confirmed" [] (str (m-total-confirmed (get-timestamp-daily))))
  (GET "/us-states-deaths-recovered" [] (str (m-us-states-deaths-recovered (get-timestamp-daily-us))))
  ;; (GET "/us-states-hospitalized" [] (str (m-us-states-hospitalized (get-timestamp-daily-us))))
  ;; (GET "/us-states-tested" [] (str (m-us-states-tested (get-timestamp-daily-us))))
  (GET "/all" [] (let [timestamp-daily (get-timestamp-daily)
                       timestamp-daily-us (get-timestamp-daily-us)
                       timestamp-time-series (get-timestamp-time-series)]
                   {:body {:confirmed-by-state (m-confirmed-by-state timestamp-daily)
                           :confirmed-by-country (m-confirmed-by-country timestamp-daily)
                           :confirmed-by-us-county (m-confirmed-by-us-county timestamp-daily)
                           :confirmed-by-us-county-fips (m-confirmed-by-us-county-fips timestamp-daily)
                           :global-deaths (m-global-deaths timestamp-daily)
                           ;; :global-recovered (m-global-recovered timestamp-daily)
                           :last-updated (last-updated)
                           :time-series-confirmed-global (m-time-series-confirmed-global timestamp-time-series)
                           :total-confirmed (m-total-confirmed timestamp-daily)
                           :us-states-deaths-recovered (m-us-states-deaths-recovered timestamp-daily-us)
                           ;; :us-states-hospitalized (m-us-states-hospitalized timestamp-daily-us)
                           ;; :us-states-tested (m-us-states-tested timestamp-daily-us)
                           }}))
  (route/not-found "Page not found"))

(def api
  (-> (handler/site site-routes)
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

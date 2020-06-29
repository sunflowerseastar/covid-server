(ns covid-server.core
  (:require [clojure.java.io :as io]
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

(defn confirmed-by-province [data]
  (->> data (i/$where {:Province_State {:ne nil}})
       (i/$where {:Province_State {:ne "Recovered"}}) ;; data error (?)
       (i/$rollup :sum :Confirmed :Province_State)
       (i/$order :Confirmed :desc)
       (i/rename-cols {:Confirmed :confirmed-sum})
       (i/$join [:Province_State :Province_State] data)
       (i/$ [:confirmed-sum :Province_State :Country_Region])
       to-vect))

(defn confirmed-by-country [data]
  (->> data (i/$rollup :sum :Confirmed :Country_Region)
       (i/$order :Confirmed :desc)
       to-vect))

(defn confirmed-by-us-county [data]
  (->> data (i/$where {:Admin2 {:ne nil}})
       (i/$rollup :sum :Confirmed :Admin2)
       (i/$order :Confirmed :desc)
       (i/rename-cols {:Confirmed :confirmed-sum})
       (i/$join [:Admin2 :Admin2] data)
       (i/$ [:confirmed-sum :Admin2 :Province_State :Country_Region])
       to-vect))

(defn global-deaths [data]
  {:deaths-by-country (->> data (i/$rollup :sum :Deaths :Country_Region)
                           (i/$order :Deaths :desc)
                           to-vect)
   :total-deaths (reduce + (i/$ :Deaths data))})

(defn global-recovered [data]
  {:recovered-by-country (->> data (i/$where {:Recovered {:ne 0}})
                              (i/$rollup :sum :Recovered :Country_Region)
                              (i/$order :Recovered :desc)
                              to-vect)
   :total-recovered (reduce + (i/$ :Recovered data))})

(defn time-series-confirmed-global [data]
  (let [data-date-columns-only (i/$ [:not :Province/State :Country/Region :Lat :Long] data)
        dates (i/col-names data-date-columns-only)
        column-totals (->> data-date-columns-only
                           matrix
                           (reduce i/plus))]
    (map vector dates column-totals)))

(defn total-confirmed [data]
  (reduce + (i/$ :Confirmed data)))

(defn us-states-deaths-recovered [data-us]
  (->> data-us
       (i/$ [:Province_State :Deaths :Recovered])
       (i/$order :Deaths :desc)
       (to-vect)))

(defn us-states-hospitalized [data-us]
  (let [data-us-without-nil (i/$where {:People_Hospitalized {:ne nil}} data-us)]
    (->> data-us-without-nil
         (i/$ [:Province_State :People_Hospitalized])
         (i/$order :People_Hospitalized :desc)
         (to-vect))))

(defn us-states-tested [data-us]
  (let [data-us-without-nil (i/$where {:People_Tested {:ne nil}} data-us)]
    {:tested-by-state (->> data-us-without-nil
                           (i/$ [:Province_State :People_Tested])
                           (i/$order :People_Tested :desc)
                           (to-vect))
     :total-tested (reduce + (i/$ :People_Tested data-us-without-nil))}))

;; "data-directories: csse-daily-report, csse-daily-report-us, csse-time-series-confirmed-global"
(defn read-csse-daily-report []
  (read-dataset "resources/data/csse-daily-report.csv" :header true))
(defn read-csse-daily-report-us []
  (read-dataset "resources/data/csse-daily-report-us.csv" :header true))
(defn read-csse-time-series-confirmed-global []
  (read-dataset "resources/data/csse-time-series-confirmed-global.csv" :header true))

(defroutes site-routes
  (GET "/" [] "")
  (GET "/confirmed-by-province" [] (str (confirmed-by-province (read-csse-daily-report))))
  (GET "/confirmed-by-country" [] (str (confirmed-by-country (read-csse-daily-report))))
  (GET "/confirmed-by-us-county" [] (str (confirmed-by-us-county (read-csse-daily-report))))
  (GET "/global-deaths" [] (str (global-deaths (read-csse-daily-report))))
  (GET "/global-recovered" [] (str (global-recovered (read-csse-daily-report))))
  (GET "/time-series-confirmed-global" [] {:body (time-series-confirmed-global (read-csse-time-series-confirmed-global))})
  (GET "/total-confirmed" [] (-> (read-csse-daily-report)
                                 total-confirmed
                                 str))
  (GET "/us-states-deaths-recovered" [] (str (us-states-deaths-recovered (read-csse-daily-report-us))))
  (GET "/us-states-hospitalized" [] (str (us-states-hospitalized (read-csse-daily-report-us))))
  (GET "/us-states-tested" [] (str (us-states-tested (read-csse-daily-report-us))))
  (GET "/all" [] (let [csse-daily-report (read-csse-daily-report)
                       csse-daily-report-us (read-csse-daily-report-us)
                       csse-time-series-confirmed-global (read-csse-time-series-confirmed-global)]
                   {:body {:confirmed-by-province (confirmed-by-province csse-daily-report)
                           :confirmed-by-country (confirmed-by-country csse-daily-report)
                           :confirmed-by-us-county (confirmed-by-us-county csse-daily-report)
                           :global-deaths (global-deaths csse-daily-report)
                           :global-recovered (global-recovered csse-daily-report)
                           :time-series-confirmed-global (time-series-confirmed-global csse-time-series-confirmed-global)
                           :total-confirmed (total-confirmed csse-daily-report)
                           :us-states-deaths-recovered (us-states-deaths-recovered csse-daily-report-us)
                           :us-states-hospitalized (us-states-hospitalized csse-daily-report-us)
                           :us-states-tested (us-states-tested csse-daily-report-us)}}))
  (route/not-found "Page not found"))

(def api
  (-> (handler/site site-routes)
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

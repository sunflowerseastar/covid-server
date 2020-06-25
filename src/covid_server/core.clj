(ns covid-server.core
  (:require [compojure.route :as route]
            [compojure.handler :as handler])
  (:use compojure.core
        ring.adapter.jetty
        [ring.middleware.content-type :only (wrap-content-type)]
        [ring.middleware.file :only (wrap-file)]
        [ring.middleware.file-info :only (wrap-file-info)]
        [ring.middleware.stacktrace :only (wrap-stacktrace)]
        [ring.util.response :only (redirect)]
        [incanter.core :as i]
        [incanter.io :refer [read-dataset]]
        [muuntaja.middleware :as mw]))

(def csse-daily-report (read-dataset "06-08-2020.csv" :header true))
(def csse-daily-report-us (read-dataset "states_06-18-2020.csv" :header true))
(def csse-time-series-confirmed-global (read-dataset "time_series_covid19_confirmed_global.csv" :header true))

(defn confirmed-by-province [data]
  (->> data (i/$where {:Province_State {:ne nil}})
       (i/$where {:Province_State {:ne "Recovered"}}) ;; data error (?)
       (i/$rollup :sum :Confirmed :Province_State)
       (i/$order :Confirmed :desc)
       (i/rename-cols {:Confirmed :confirmed-sum})
       (i/$join [:Province_State :Province_State] data)
       (i/$ [:confirmed-sum :Province_State :Country_Region])
       to-vect))

(defn confirmed-by-region [data]
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
  {:deaths-by-region (->> data (i/$rollup :sum :Deaths :Country_Region)
                          (i/$order :Deaths :desc)
                          to-vect)
   :total-deaths (reduce + (i/$ :Deaths data))})

(defn global-recovered [data]
  {:recovered-by-region (->> data (i/$where {:Recovered {:ne 0}})
                             (i/$rollup :sum :Recovered :Country_Region)
                             (i/$order :Recovered :desc)
                             to-vect)
   :total-recovered (reduce + (i/$ :Recovered data))})

(defn time-series-confirmed-global [data]
  (let [data-date-columns-only (i/$ [:not :Province/State :Country/Region :Lat :Long] data)
        dates (i/col-names data-date-columns-only)
        column-totals (->> data-date-columns-only
                           to-matrix
                           (reduce i/plus))]
    (map vector dates column-totals)))

(defn total-confirmed [data]
  (reduce + (i/$ :Confirmed data)))

(defn us-states-deaths-recovered [data-us]
  (->> data-us
       (i/$ [:Province_State :Deaths :Recovered])
       (i/$order :Deaths :desc)
       (to-vect)))

(defn us-states-tested [data-us]
  (let [data-us-without-nil (i/$where {:People_Tested {:ne nil}} data-us)]
    {:tested-by-state (->> data-us-without-nil
                         (i/$ [:Province_State :People_Tested])
                         (i/$order :People_Tested :desc)
                         (to-vect))
     :total-tested (reduce + (i/$ :People_Tested data-us-without-nil))}))

(defroutes site-routes
  (GET "/" [] "")
  (GET "/confirmed-by-province" [] (str (confirmed-by-province csse-daily-report)))
  (GET "/confirmed-by-region" [] (str (confirmed-by-region csse-daily-report)))
  (GET "/confirmed-by-us-county" [] (str (confirmed-by-us-county csse-daily-report)))
  (GET "/global-deaths" [] (str (global-deaths csse-daily-report)))
  (GET "/global-recovered" [] (str (global-recovered csse-daily-report)))
  (GET "/time-series-confirmed-global" [] {:body (time-series-confirmed-global csse-time-series-confirmed-global)})
  (GET "/total-confirmed" [] (str (total-confirmed csse-daily-report)))
  (GET "/us-states-deaths-recovered" [] (str (us-states-deaths-recovered csse-daily-report-us)))
  (GET "/us-states-tested" [] (str (us-states-tested csse-daily-report-us)))
  (GET "/all" [] {:body {:confirmed-by-province (confirmed-by-province csse-daily-report)
                         :confirmed-by-region (confirmed-by-region csse-daily-report)
                         :confirmed-by-us-county (confirmed-by-us-county csse-daily-report)
                         :global-deaths (global-deaths csse-daily-report)
                         :global-recovered (global-recovered csse-daily-report)
                         :time-series-confirmed-global (time-series-confirmed-global csse-time-series-confirmed-global)
                         :total-confirmed (total-confirmed csse-daily-report)
                         :us-states-deaths-recovered (us-states-deaths-recovered csse-daily-report-us)
                         :us-states-tested (us-states-tested csse-daily-report-us)}})
  (route/not-found "Page not found"))

(def api
  (-> (handler/site site-routes)
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

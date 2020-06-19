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

(defn confirmed-by-region [data]
  (->> data (i/$rollup :sum :Confirmed :Country_Region)
       (i/$order :Confirmed :desc)
       to-vect))

(defn global-deaths [data]
  {:deaths-by-region (->> data (i/$rollup :sum :Deaths :Country_Region)
                          (i/$order :Deaths :desc)
                          to-vect)
   :total-deaths (reduce + (i/$ :Deaths data))})

(defn total-confirmed [data]
  (reduce + (i/$ :Confirmed data)))

(defn us-state-level-deaths-recovered [data-us]
  (->> data-us
       (i/$ [:Province_State :Deaths :Recovered])
       (i/$order :Deaths :desc)
       (to-vect)))

(defroutes site-routes
  (GET "/" [] "")
  (GET "/confirmed-by-region" [] (str (confirmed-by-region csse-daily-report)))
  (GET "/global-deaths" [] (str (global-deaths csse-daily-report)))
  (GET "/total-confirmed" [] (str (total-confirmed csse-daily-report)))
  (GET "/us-state-level-deaths-recovered" [] (str (us-state-level-deaths-recovered csse-daily-report-us)))
  (GET "/all" [] {:body {:confirmed-by-region (confirmed-by-region csse-daily-report)
                         :global-deaths (global-deaths csse-daily-report)
                         :total-confirmed (total-confirmed csse-daily-report)
                         :us-state-level-deaths-recovered (us-state-level-deaths-recovered csse-daily-report-us)}})
  (route/not-found "Page not found"))

(def api
  (-> (handler/site site-routes)
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

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

(def csse-daily-reports (read-dataset "06-08-2020.csv" :header true))

(defn total-confirmed []
  (reduce + (i/$ :Confirmed csse-daily-reports)))

(defn global-deaths []
  (let [total-deaths (reduce + (i/$ :Deaths csse-daily-reports))
        deaths-by-region (->> (i/$rollup :sum :Deaths :Country_Region csse-daily-reports)
                              (i/$order :Deaths :desc)
                              to-vect)]
    {:deaths-by-region deaths-by-region
     :total-deaths total-deaths}))

(def total-confirmed-memo (memoize total-confirmed))

(defn confirmed-by-region []
  (->> (i/$rollup :sum :Confirmed :Country_Region csse-daily-reports)
       (i/$order :Confirmed :desc)
       to-vect))

(defroutes site-routes
  (GET "/" [] "")
  (GET "/global-deaths" [] (str (global-deaths)))
  (GET "/us-state-level-deaths-recovered" [] (str (us-state-level-deaths-recovered)))
  (GET "/total-confirmed" [] (str (total-confirmed-memo)))
  (GET "/confirmed-by-region" [] (str (confirmed-by-region)))
  (GET "/all" [] {:body {:total-confirmed (total-confirmed-memo)
                         :global-deaths (global-deaths)
                         :confirmed-by-region (confirmed-by-region)}})
  (route/resources "/")
  (route/not-found "Page not found"))

(def api
  (-> (handler/site site-routes)
      (wrap-file "resources")
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

(ns covid-ring-compojure.web
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

(def covid-data (read-dataset "06-08-2020.csv" :header true))

(defn total-confirmed []
  (reduce + (i/$ :Confirmed covid-data)))

(def total-confirmed-memo (memoize total-confirmed))

(defroutes site-routes
  (GET "/" [] (redirect "/data/census-race.json"))
  (GET "/total-confirmed" [] (str (total-confirmed-memo)))
  (route/resources "/")
  (route/not-found "Page not found"))

(def app
  (-> (handler/site site-routes)
      (wrap-file "resources")
      (wrap-file-info)
      (mw/wrap-format)
      (wrap-content-type)))

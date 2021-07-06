(defproject covid-server "0.2.0"
  :description "Data pipeline + API for covid-dashboard"
  :url "https://covid-dashboard.sunflowerseastar.com"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [ring "1.8.1"]
                 [ring-cors "0.1.13"]
                 [compojure "1.6.1"]
                 [incanter "1.9.3"]
                 [tupelo "0.9.201"]
                 [criterium "0.4.5"]
                 [metosin/muuntaja "0.6.7"]]
  :plugins [[lein-ring "0.12.5"]]
  :ring {:handler covid-server.core/api}
  :repl-options {:init-ns covid-server.core})

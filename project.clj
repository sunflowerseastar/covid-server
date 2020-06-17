(defproject covid-ring-compojure "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [ring "1.8.1"]
                 [compojure "1.6.1"]
                 [incanter "1.9.3"]
                 [tupelo "0.9.201"]
                 [metosin/muuntaja "0.6.7"]]
  :plugins [[lein-ring "0.12.5"]]
  :ring {:handler covid-ring-compojure.web/app}
  :repl-options {:init-ns covid-ring-compojure.core})

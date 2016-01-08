(defproject setup "0.1.0-SNAPSHOT"
  :description "Generate input data for training models"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [me.raynes/fs "1.4.6"]]
  :main setup.core
  :jvm-opts ["-Xmx2g", "-server"])

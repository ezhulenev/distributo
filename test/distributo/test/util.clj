(ns distributo.test.util
  (:use clojure.test)
  (:require [distributo.util :refer :all]))

(deftest same-key-transition
  (let [a {:a 1}
        b {:a 2}
        expected {:a {:from 1 :to 2}}]
    (testing "Updating key"
      (is (= expected (transition a b))))))

(deftest new-key-transition
  (let [a {:a 1}
        b {:a 1 :b 1}
        expected {:b {:to 1}}]
    (testing "Adding new key"
      (is (= expected (transition a b))))))

(deftest removed-key-transition
  (let [a {:a 1 :b 1}
        b {:a 1}
        expected {:b {:from 1}}]
    (testing "Removing key"
      (is (= expected (transition a b))))))

(deftest update-and-add-transition
  (let [a {:a 1 :b 1}
        b {:a 2 :c 2}
        expected {:a {:from 1 :to 2}
                  :b {:from 1}
                  :c {:to 2}}]
    (testing "Updating and adding keys"
      (is (= expected (transition a b))))))
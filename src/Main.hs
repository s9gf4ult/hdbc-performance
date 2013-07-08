{-# LANGUAGE 
  DeriveDataTypeable 
, OverloadedStrings  
  #-}

module Main where

import Criterion.Main (Benchmark, bench, defaultMain, nfIO, bgroup)

import Database.HDBC
import Database.HDBC.PostgreSQL

import Data.Time.Calendar (Day, fromGregorian)
import Data.Time.LocalTime (TimeOfDay(TimeOfDay), LocalTime(LocalTime))

import qualified Data.Text.Lazy as TL

import Control.Monad (replicateM, forM, liftM)
import Debug.Trace

quickQuery' con query params = do
    s <- prepare con query
    execute s params
    res <- fetchAllRows s
    finish s
    return res

main :: IO ()
main = do
  -- Connect
  connPostgreSQL <- connectPostgreSQL "host=localhost dbname=hdbc-test user=hdbc password=qwerfdsa"

  -- Setup
  setupInsert connPostgreSQL
  setupSelect connPostgreSQL 10000

  -- Benchmark
  defaultMain
    [ benchBackend "postgresql" connPostgreSQL
    ]

  -- Teardown
  teardownInsert connPostgreSQL
  teardownSelect connPostgreSQL

  -- Disconnect
  disconnect connPostgreSQL

benchBackend :: Connection conn => String -> conn -> Benchmark
benchBackend backend conn = bgroup backend
  [ benchInsert conn 1000
  , benchSelectInt conn 10000
  -- , benchSelectInt32 conn 10000
  , benchSelectDouble conn 10000
  , benchSelectString conn 10000
  , benchSelectDate conn 10000
  , benchSelectTime conn 10000
  ]

--------------------
setupInsert :: Connection conn => conn -> IO ()
setupInsert conn = do 
  runRaw conn "drop table if exists testInsert"
  runRaw conn "CREATE TABLE testInsert (v1 INTEGER, v2 FLOAT, v3 CHAR(4))"

benchInsert :: Connection conn => conn -> Int -> Benchmark
benchInsert conn n = bench "Insert" $ nfIO $ do
  withTransaction conn $ forM [1 .. n] $ \x ->
    run conn "INSERT INTO testInsert (v1, v2, v3) VALUES (?, ?, ?)"
      [ SqlInt32 (fromIntegral x)
      , SqlDouble (fromIntegral x)
      , SqlText (TL.pack $ show x)
      ]
  withTransaction conn $ run conn "DELETE FROM testInsert" []

teardownInsert :: Connection conn => conn -> IO ()
teardownInsert conn = do
  run conn
    "DROP TABLE testInsert" []

--------------------
setupSelect :: Connection conn => conn -> Int -> IO ()
setupSelect conn n = do
  runRaw conn "drop table if exists testSelect"
  runRaw conn "CREATE TABLE testSelect (v1 INTEGER, v2 FLOAT, v3 CHAR(4), v4 DATE, v5 TIMESTAMP)" 
  withTransaction conn 
      $ runMany conn "INSERT INTO testSelect (v1, v2, v3, v4, v5) VALUES (?, ?, ?, ?, ?)"
      $ replicate n [ SqlInt32 1
                    , SqlDouble 1.0
                    , SqlText "test"
                    , SqlLocalDate testDay
                    , SqlLocalTime testTime
                    ]

testDay :: Day
testDay = fromGregorian 2011 11 20

testTime :: LocalTime
testTime = LocalTime testDay (TimeOfDay 3 14 15.926536) -- 5897932)

benchSelect :: Connection conn => conn -> Int -> Benchmark
benchSelect conn n = bench "Select" $ nfIO
           $ withTransaction conn $ do
                quickQuery' conn "SELECT * FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
                return ()

benchSelectInt :: Connection conn => conn -> Int -> Benchmark
benchSelectInt conn n = bench "SelectInt" $ nfIO $ do
  vss <- quickQuery' conn "SELECT v1 FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
  if ((sum . map (\[v] -> fromSql v :: Int)) vss == n)
    then return ()
    else error "benchSelectInt: Unexpected sum!"

benchSelectDouble :: Connection conn => conn -> Int -> Benchmark
benchSelectDouble conn n = bench "SelectDouble" $ nfIO $ do
  vss <- quickQuery' conn "SELECT v2 FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
  if ((sum . map (\[v] -> fromSql v :: Double)) vss == fromIntegral n)
    then return ()
    else error "benchSelectDouble: Unexpected sum!"

benchSelectString :: Connection conn => conn -> Int -> Benchmark
benchSelectString conn n = bench "SelectString" $ nfIO $ do
  vss <- quickQuery' conn "SELECT v3 FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
  if all (\[v] -> (fromSql v :: String) == "test") vss
    then return ()
    else error "benchSelectString: Unexpected String!"

benchSelectDate :: Connection conn => conn -> Int -> Benchmark
benchSelectDate conn n = bench "SelectDate" $ nfIO $ do
  vss <- quickQuery' conn "SELECT v4 FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
  if all (\[v] -> (fromSql v :: Day) == testDay) vss
    then return ()
    else error "benchSelectDate: Unexpected Date!"

benchSelectTime :: Connection conn => conn -> Int -> Benchmark
benchSelectTime conn n = bench "SelectTime" $ nfIO $ do
  vss <- quickQuery' conn "SELECT v5 FROM testSelect LIMIT ?" [SqlInt32 (fromIntegral n)]
  if all (\[v] -> (fromSql v :: LocalTime) == testTime) vss
    then return ()
    else error "benchSelectTime: Unexpected Time!"

teardownSelect :: Connection conn => conn -> IO ()
teardownSelect conn = withTransaction conn $ 
                        runRaw conn "DROP TABLE testSelect"

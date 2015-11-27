module Main where

import Text.DelimiterSeparated
import System.IO
import Control.Monad

main = do
  txt <- readFile "data.csv"
  putStrLn txt
  case recordsFromDelimiterSeparated csv txt of
    Left es -> forM_ es putStrLn
    Right recs -> do
      putStrLn $ show recs
      writeFile "data-out-csv.csv" $ recordsToDelimiterSeparated csv recs
      writeFile "data-out-csv.tsv" $ recordsToDelimiterSeparated tsv recs

  txt <- readFile "data.tsv"
  -- putStrLn txt
  case recordsFromDelimiterSeparated tsv txt of
    Left es -> forM_ es putStrLn
    Right recs -> do
      putStrLn $ show recs
      writeFile "data-out-tsv.csv" $ recordsToDelimiterSeparated csv recs


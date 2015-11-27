-------------------------------------------------------------------------------------------
-- Input/output of delimiter separated strings
-------------------------------------------------------------------------------------------

{-|
Module      : Text.DelimiterSeparated
Description : Library for manipulating delimiter separated records.
Copyright   : (c) Atze Dijkstra, 2015
License     : BSD3
Maintainer  : atzedijkstra@gmail.com
Stability   : experimental
Portability : POSIX

The library provides parsing/unparsing of 'Records' as well as interpreting those records to a datatype of your choice,
via 'toRecords' and 'fromRecords' using class 'DelimSepRecord', where each individual field can be interpreted via class 'DelimSepField'.

The following example demonstrates the basic parsing/unparsing:

> module Main where
> 
> import Text.DelimiterSeparated
> import System.IO
> import Control.Monad
> 
> main = do
>   txt <- readFile "data.csv"
>   putStrLn txt
>   case recordsFromDelimiterSeparated csv txt of
>     Left es -> forM_ es putStrLn
>     Right recs -> do
>       putStrLn $ show recs
>       writeFile "data-out-csv.csv" $ recordsToDelimiterSeparated csv recs
>       writeFile "data-out-csv.tsv" $ recordsToDelimiterSeparated tsv recs
> 
>   txt <- readFile "data.tsv"
>   -- putStrLn txt
>   case recordsFromDelimiterSeparated tsv txt of
>     Left es -> forM_ es putStrLn
>     Right recs -> do
>       putStrLn $ show recs
>       writeFile "data-out-tsv.csv" $ recordsToDelimiterSeparated csv recs

-}

module Text.DelimiterSeparated
  ( -- * Types
    Field(..)
  , Record
  , Records
  , DelimiterStyle(..)
  , csv, tsv
  
  -- * Construction
  , emptyField
  
  -- * Encoding, decoding
  , recordsToDelimiterSeparated
  , recordsFromDelimiterSeparated
  
  -- * Checks, fixes
  , Check(..)
  , checkAll
  , recordsCheck
  
  , Fix(..)
  , recordsFix
  
  -- * Construction
  , recordsFromHeaderStr
  , recordsAddHeaderStr
  
  -- * Manipulation
  , recordsPartitionRows
  , recordsPartitionColsBasedOnHeader
  , recordsSpan
  , recordsSplitHeader
  
  -- * Conversion
  , recordsToStrings
  , recordsFromStrings
  
  -- * Overloaded conversion
  , DelimSepField(..)
  , DelimSepRecord(..)
  
  , toRecords
  , toRecordsWithHeader
  , toRecordsWithHeaderStr
  
  , fromRecords
  )
  where

-------------------------------------------------------------------------------------------
import UU.Parsing
import UU.Parsing.CharParser
import UHC.Util.ParseUtils
import UHC.Util.Utils
import Data.List
-------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------
-- Types
-------------------------------------------------------------------------------------------

-- | Field
data Field
  = Field
      { fldStr      :: String       -- base case
      }
  deriving (Eq)

-- | Empty field
emptyField :: Field
emptyField = Field ""

instance Show Field where
  show = showField tsv

-- | Show field, depending on delimiter style
showField :: DelimiterStyle -> Field -> String
showField (CSV {}) (Field s) = "\"" ++ s ++ "\""
showField (TSV {}) (Field s) =         s

-- | Record is sequence of 'Field's (representation may change in future)
type Record = [Field]

-- | Records is sequence of 'Record's (representation may change in future)
type Records = [Record]

-- | Style of delimitation
data DelimiterStyle
  = CSV
      { styleFieldDelimChars        :: [Char]       -- field delimiters, first one used when unparsing
      , styleRecordDelimChars       :: [Char]       -- record delimiters, first one used when unparsing
      }
  | TSV

-- | Predefined delimiter style for comma field separated, newline/return record separated
csv :: DelimiterStyle
csv = CSV "," "\n\r"

-- | Predefined delimiter style for tab field separated, newline/return record separated
tsv :: DelimiterStyle
tsv = TSV

-------------------------------------------------------------------------------------------
-- Parsing, encoding, decoding
-------------------------------------------------------------------------------------------

type P p = PlainParser Char p

-- | Parsing of records, given a delimiterstyle
pRecords :: DelimiterStyle -> P Records
pRecords style
  = concat <$> pList1Sep_ng pNL pRecordsNonEmpty -- <* (pNL `opt` '?')
  where pField = case style of
          TSV -> Field <$> pList_ng (pExcept (minBound, maxBound, '?') "\t\n\r")
          CSV {styleFieldDelimChars=fs, styleRecordDelimChars=ls} ->
              Field <$>
                (   pDQ *> pList_ng pQChar <* pDQ
                <|> pList_ng pChar
                )
            where pQChar
                    =   pExcept (minBound, maxBound, '?') "\""
                    <|> pDQ <* pDQ
                  pChar
                    =   pExcept (minBound, maxBound, '?') ("\"" ++ fs ++ ls)
        pRecord = pList1Sep_ng pSep pField
        pRecordsNonEmpty = chk <$> pRecord
          where chk []         = []
                chk [Field ""] = []
                chk fs         = [fs]

        pNL = case style of
          TSV -> pAnySym "\n\r"
          CSV {styleRecordDelimChars=ls} -> pAnySym ls
        
        pDQ = pSym '"'
        pSep = case style of
          TSV -> pAnySym "\t"
          CSV {styleFieldDelimChars=fs} -> pAnySym fs

-- | Encode internal representation in external delimiter separated format
recordsToDelimiterSeparated :: DelimiterStyle -> Records -> String
recordsToDelimiterSeparated style recs
  = (mk ls $ map (mk fs . map (showField style)) recs) ++ "\n"
  where (fs,ls) = case style of
          TSV -> ('\t', '\n')
          CSV {styleFieldDelimChars=(fs:_), styleRecordDelimChars=(ls:_)} -> (fs,ls)
        mk sep = concat . intersperse [sep]

-- | Decode internal representation from external delimiter separated format, possible failing with error messages
recordsFromDelimiterSeparated :: DelimiterStyle -> String -> Either [String] Records
recordsFromDelimiterSeparated style str
  | null errs = Right res
  | otherwise = Left $ map show errs
  where (res,errs) = parseToResMsgs (pRecords style) str

-------------------------------------------------------------------------------------------
-- Construction
-------------------------------------------------------------------------------------------

-- | Convert a String representation of a header to actual record
recordsFromHeaderStr :: String -> Records
recordsFromHeaderStr s = recordsFromStrings [words s]

-- | Add a header described by string holding whitespaced separated labels
recordsAddHeaderStr :: String -> Records -> Records
recordsAddHeaderStr s = (recordsFromHeaderStr s ++)

-------------------------------------------------------------------------------------------
-- Manipulation
-------------------------------------------------------------------------------------------

-- | Lift a predicate to a Record
liftRecPred :: ([String] -> Bool) -> (Record -> Bool)
liftRecPred pred = pred . map fldStr

-- | Do something with records, taking into account header
recordsDoHeader
  :: Bool                           -- ^ first rec is header?
  -> (Records -> Records -> res)    -- ^ do it, given header (if any) and records
  -> Records
  -> res
recordsDoHeader fstIsHdr mk recs
  | fstIsHdr  = mk [hd] tl
  | otherwise = mk []   recs
  where (hd:tl) = recs

-- | Partition record rows.
-- Fst of tuple holds the possible header, if indicated it is present.
-- Snd of tuple holds records failing the predicate.
-- Assumes >0 records
recordsPartitionRows :: Bool -> ([String] -> Bool) -> Records -> (Records, Records)
recordsPartitionRows fstIsHdr pred recs
  = recordsDoHeader fstIsHdr mk recs
  where mk hd rs = (hd++y,n)
          where (y,n) = partition (liftRecPred pred) rs

-- | Partition record columns, fst of tuple holds the obligatory header upon wich partitioning is done
-- Assumes header and records all have same nr of fields
recordsPartitionColsBasedOnHeader :: (String -> Bool) -> Records -> (Records, Records)
recordsPartitionColsBasedOnHeader pred hr@(hdr:_)
  = let spl = split hdr in unzip $ map spl hr
  where split (h:t) | pred (fldStr h) = let tspl = split t in \(rh:rt) -> let (r1,r2) = tspl rt in (rh:r1,    r2)
                    | otherwise       = let tspl = split t in \(rh:rt) -> let (r1,r2) = tspl rt in (   r1, rh:r2)
        split []                      =                       \[]      ->                          (   [],    [])

-- | Partition records, fst of tuple holds the possible header, if indicated it is present.
-- Assumes >0 records
recordsSpan :: Bool -> ([String] -> Bool) -> Records -> (Records, Records)
recordsSpan fstIsHdr pred recs
  = recordsDoHeader fstIsHdr mk recs
  where mk hd rs = (hd++y,n)
          where (y,n) = span (liftRecPred pred) rs

-- | Split of header, assuming there is one
recordsSplitHeader :: Records -> (Record, Records)
recordsSplitHeader (h:t) = (h, t)


-------------------------------------------------------------------------------------------
-- Conversion
-------------------------------------------------------------------------------------------

-- | Get all fields as strings
recordsToStrings :: Records -> [[String]]
recordsToStrings = map (map fldStr)

-- | Lift strings as Records
recordsFromStrings :: [[String]] -> Records
recordsFromStrings = map (map Field)

-------------------------------------------------------------------------------------------
-- Check(s) & fixes
-------------------------------------------------------------------------------------------

-- | Which checks are to be done by 'recordsCheck'
data Check
  = Check_DupHdrNms             -- ^ check for duplicate header names
  | Check_EqualSizedRecs        -- ^ check for equal sized records (ignoring possible header)
  | Check_AtLeast1Rec           -- ^ check for at least 1 record (ignoring possible header)
  | Check_EqualSizedHdrRecs     -- ^ check for equal sized header and records
  | Check_NoRecsLargerThanHdr   -- ^ check for records not larger than header
  | Check_NoRecsSmallerThanHdr  -- ^ check for records not smaller than header
  deriving (Eq,Enum,Bounded)

-- | All checks
checkAll :: [Check]
checkAll = [minBound .. maxBound]

-- | Check records, possibly yielding errors
recordsCheck :: Bool -> [Check] -> Records -> Maybe String
recordsCheck fstIsHdr chks recs
  --- | null recs    = Just $ "no records nor header"

  | (Check_AtLeast1Rec `elem` chks || checksHdrSizeAndRecs) && 
    not (has1Rec recs)
                 = Just $ "not at least 1 record" ++ (if fstIsHdr then " and header" else "")

  | Check_EqualSizedRecs `elem` chks && 
    length tlSzs > 1
                 = Just $ "records have varying sizes: " ++ show tlSzs

  | checksHdrSizeAndRecs && fstIsHdr &&
    not (null cmp_tlSzs)
                 = Just $ "header size=" ++ show hdLen ++ " and records sizes=" ++ show cmp_tlSzs ++ " differ"

  | otherwise    = Nothing

  where has1Rec (_:_:_) | fstIsHdr      = True
        has1Rec (_:_  ) | fstIsHdr      = False
                        | otherwise     = True
        has1Rec _                       = False
        ~(~[rhd],rtl)                   = recordsDoHeader fstIsHdr (,) recs
        dupnms                          = concat $ map head $ filter (\l -> length l > 1) $ groupSortOn id $ map fldStr rhd
        tlNrAndLen                      = zipWith (\i r -> (i, length r)) [1::Int ..] rtl
        hdLen                           = length rhd
        tlSzs@(~(hd_tlSzs@(~(hd_tlSz,_)):_))
                                        = [ (l, map fst nl) | nl@((_,l):_) <- groupSortOn snd tlNrAndLen ]
        checksGT                        = Check_NoRecsLargerThanHdr `elem` chks
        checksLT                        = Check_NoRecsSmallerThanHdr `elem` chks
        checksHdrSizeAndRecs            = checksGT || checksLT
        cmpSz | checksGT && checksLT    = (/=)
              | checksGT                = (>)
              | checksLT                = (<)
              | otherwise               = \_ _ -> False
        cmp_tlSzs                       = filter (\(sz,_) -> cmpSz sz hdLen) tlSzs

-- | Which fixes are to be done by 'recordsCheck'
data Fix
  = Fix_Pad                 -- ^ pad
  | Fix_PadToHdrLen         -- ^ in combi with pad, pad to header len
  deriving (Eq,Enum,Bounded)

-- | Fix sizes of records by padding to max size
recordsFix :: Bool -> [Fix] -> Records -> Records
recordsFix fstIsHdr fxs recs
  = map fix recs
  where ~(~[rhd],rtl) = recordsDoHeader fstIsHdr (,) recs
        maxl | Fix_PadToHdrLen `elem` fxs && fstIsHdr = length rhd
             | otherwise                              = maximum $ map length rtl
        fix  | Fix_Pad `elem` fxs = \r -> r ++ take (maxl - length r) p
             | otherwise          = id
             where p = repeat emptyField

-------------------------------------------------------------------------------------------
-- Additional conversion/interpretation of field, i.e. show/read (why not use it like that?)
-------------------------------------------------------------------------------------------

-- | Conversion to/from Field, i.e. kinda show/read
class DelimSepField x where
  toDelimSepField :: x -> Field
  fromDelimSepField :: Field -> x

instance DelimSepField Field where
  toDelimSepField = id
  fromDelimSepField = id

instance {-# OVERLAPPABLE #-} DelimSepField x => DelimSepField [x] where
  toDelimSepField = toDelimSepField . unwords . map (fromDelimSepField . toDelimSepField)
  fromDelimSepField = map (fromDelimSepField . toDelimSepField) . words . fromDelimSepField

instance DelimSepField String where
  toDelimSepField = Field
  fromDelimSepField = fldStr

instance DelimSepField Integer where
  toDelimSepField = Field . show
  fromDelimSepField (Field x) = read x

instance DelimSepField Int where
  toDelimSepField = Field . show
  fromDelimSepField (Field x) = read x

instance DelimSepField Double where
  toDelimSepField = Field . show
  fromDelimSepField (Field x) = read x

-- | Conversion to/from Record
class DelimSepRecord x where
  toDelimSepRecord :: x -> Record
  fromDelimSepRecord :: Record -> x

instance {-# OVERLAPPABLE #-} DelimSepRecord Record where
  toDelimSepRecord = id
  fromDelimSepRecord = id

-- | Convert to records
toRecords :: DelimSepRecord x => [x] -> Records
toRecords = map toDelimSepRecord

-- | Convert to records, with a header described by string holding whitespaced separated labels
toRecordsWithHeader :: DelimSepRecord x => [Record] -> [x] -> Records
toRecordsWithHeader h = (h++) . toRecords

-- | Convert to records, with a header described by string holding whitespaced separated labels
toRecordsWithHeaderStr :: DelimSepRecord x => String -> [x] -> Records
toRecordsWithHeaderStr s = toRecordsWithHeader (recordsFromHeaderStr s)

-- | Convert from records
fromRecords :: DelimSepRecord x => Records -> [x]
fromRecords = map fromDelimSepRecord

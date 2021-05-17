import qualified Data.ByteString.Lazy as B
import qualified Data.List.Split as LS
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Aeson as A
import qualified Data.Maybe as DMaybe

jsonFile1 = "political_signal_groups_sorted_2.json"

--------- common helper funcs ---------------
makeMapping:: M.Map String [String] -> M.Map String Int
makeMapping nJSON = M.fromList $ map (\(a,b)-> (b,a)) (zip [1..] (M.keys nJSON))
makeNewJSON:: M.Map String [String] -> M.Map String [String]
makeNewJSON namesMap = 
    M.fromList [ (oK, map getSubredditName oV ) | (oK,oV) <- M.toList namesMap ]
getSubredditName:: String -> String 
getSubredditName k = L.intercalate "_" $ L.take (length splits - 2) splits
    where
        splits = LS.splitOn "_" k
getMetricsColoum:: String -> String 
getMetricsColoum key = splits !! (length splits - 2)
    where
        splits = LS.splitOn "_" key
getTypeOfData:: String -> String 
getTypeOfData key = splits !! (length splits - 1)
    where
        splits = LS.splitOn "_" key
----------------------------------------------

transformPolicatalGroups:: IO ()
transformPolicatalGroups = do
    let file = B.readFile jsonFile1
    jsonParsed <- fmap A.decode file :: IO (Maybe (M.Map String [String]))
    let finalJSON = fmap makeNewJSON jsonParsed
    let keyMapping = fmap makeMapping finalJSON
    A.encodeFile "political_signal_groups_sorted_corrected.json" finalJSON
    A.encodeFile "group_index_mapping.json" keyMapping


generateSQL1:: IO ([String])
generateSQL1 = do
    let file = B.readFile jsonFile1
    jsonParsed <- fmap A.decode file :: IO (Maybe (M.Map String [String]))
    let finalJSON = makeNewJSON  (DMaybe.fromJust jsonParsed)
    let keyMapping =  makeMapping finalJSON
    return [ 
        genQ keyMapping (a,b) 
        | (a,b) <- M.toList finalJSON
        ]


getFrequencyOfSubreddit:: IO([(String,Int)])
getFrequencyOfSubreddit = do
    let file = B.readFile jsonFile1
    jsonParsed <- fmap A.decode file :: IO (Maybe (M.Map String [String]))
    let finalJSON = makeNewJSON  (DMaybe.fromJust jsonParsed)
    let allSubreddits = foldl concat1 [] (M.toList finalJSON)
    let freq = L.map (\x -> (head x, length x)) . L.group . L.sort $ allSubreddits
    return $ L.filter (\(sub,fr) -> fr /= 4) freq
    where
        concat1:: [String] -> (String, [String]) -> [String]
        concat1 subArr (k, arr) = subArr ++ arr

generateSQL:: IO ()
generateSQL = do
    let file = B.readFile jsonFile1
    jsonParsed <- fmap A.decode file :: IO (Maybe (M.Map String [String]))
    let finalJSON = makeNewJSON  (DMaybe.fromJust jsonParsed)
    let keyMapping =  makeMapping finalJSON
    A.encodeFile "all.sql.json" [ genQ keyMapping (a,b) | (a,b) <- M.toList finalJSON ]
    -- where
genQ:: M.Map String Int -> (String,[String]) -> String
genQ keyM (groupType, arr) = 
    "update " <> tableName (getTypeOfData groupType)  <> " set " <> 
    metricName (getMetricsColoum groupType)  <> " = " <> index  <> " " <>
    "where subreddit = ANY(ARRAY" <>
    map replaceQ (show arr) <>
    ");" 
    where
        index :: String
        index = show $ getIndexFromKey keyM groupType
        tableName :: String -> String
        tableName "p" = "predictions_post_set"
        tableName "c" = "predictions_comment_set"
        metricName :: String -> String
        metricName "avg" = "avg_sub_type"
        metricName "n" = "n_sub_type"
        getIndexFromKey :: M.Map String Int -> String -> Int
        getIndexFromKey keyM grouping = DMaybe.fromJust $ M.lookup grouping keyM
        replaceQ:: Char -> Char
        replaceQ '"' = '\''
        replaceQ c = c

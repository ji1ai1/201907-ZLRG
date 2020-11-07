DROP TABLE IF EXISTS TEST_DATA_JD;
CREATE TABLE TEST_DATA_JD AS
SELECT 
    用戶標識, 甲.職位標識, 記錄標識, 職0, 職0_1, 職0_2, 職0_3, 職0_4, 職0_5, 職0_6, 職0_7, 職0_8, 職1, 職2, 職1_1, 職2_1
FROM TEST_X 甲
LEFT OUTER JOIN
(
    SELECT 
        職位標識
        , 1.0 * SUM(認可) AS 職0, AVG(投遞) AS 職1, AVG(認可) AS 職2
        , SUM(投遞) / (1 + COUNT(*)) AS 職1_1, SUM(認可) / (1 + COUNT(*)) AS 職2_1
    FROM TRAIN_X
    GROUP BY 職位標識
) 乙
ON 甲.職位標識 == 乙.職位標識
LEFT OUTER JOIN
(
    SELECT 職位標題, AVG(認可) AS 職0_1, 1.0 * SUM(認可) AS 職0_5 FROM TRAIN_X GROUP BY 職位標題
) 丙一
ON 甲.職位標題 == 丙一.職位標題
LEFT OUTER JOIN
(
    SELECT 職位子類, AVG(認可) AS 職0_2, 1.0 * SUM(認可) AS 職0_6 FROM TRAIN_X GROUP BY 職位子類
) 丙二
ON 甲.職位子類 == 丙二.職位子類
LEFT OUTER JOIN
(
    SELECT 職位城市, AVG(認可) AS 職0_3, 1.0 * SUM(認可) AS 職0_7 FROM TRAIN_X GROUP BY 職位城市
) 丙三
ON 甲.職位城市 == 丙三.職位城市
LEFT OUTER JOIN
(
    SELECT 職位描述, AVG(認可) AS 職0_4, 1.0 * SUM(認可) AS 職0_8 FROM TRAIN_X GROUP BY 職位描述
) 丙四
ON 甲.職位描述 == 丙四.職位描述
;


DROP TABLE IF EXISTS TRAIN_DATA_JD_IN;
CREATE TABLE TRAIN_DATA_JD_IN AS
SELECT
    甲.用戶標識, 職位標識, 記錄標識, 欄0, 瀏覽, 投遞, 認可, 職位標題, 職位子類, 職位城市, 職位描述
FROM TRAIN_X 甲
INNER JOIN
(
    SELECT 用戶標識, ROW_NUMBER() OVER(ORDER BY MD5(CONCAT(用戶標識, "JD"))) % 7 AS 欄0 FROM TRAIN_X GROUP BY 用戶標識
) 乙
ON 甲.用戶標識 == 乙.用戶標識;


DROP TABLE IF EXISTS TRAIN_DATA_JD_OUT;
CREATE TABLE TRAIN_DATA_JD_OUT
(
    職位標識 STRING
    , 欄0 BIGINT
    , 職0 DOUBLE
    , 職0_1 DOUBLE
    , 職0_2 DOUBLE
    , 職0_3 DOUBLE
    , 職0_4 DOUBLE
    , 職0_5 DOUBLE
    , 職0_6 DOUBLE
    , 職0_7 DOUBLE
    , 職0_8 DOUBLE
    , 職1 DOUBLE
    , 職2 DOUBLE
    , 職1_1 DOUBLE
    , 職2_1 DOUBLE
);

                                                
--@resource_reference{"RgExtract.jar"}
jar -resources RgExtract.jar -classpath ./RgExtract.jar jiangli.rg.rg_extract.RgExtractTrainJd TRAIN_DATA_JD_IN TRAIN_DATA_JD_OUT 7;


DROP TABLE IF EXISTS TRAIN_DATA_JD;
CREATE TABLE TRAIN_DATA_JD AS
SELECT
    用戶標識, 甲.職位標識, 記錄標識, 職0, 職0_1, 職0_2, 職0_3, 職0_4, 職0_5, 職0_6, 職0_7, 職0_8, 職1, 職2, 職1_1, 職2_1
FROM TRAIN_DATA_JD_IN 甲
LEFT OUTER JOIN TRAIN_DATA_JD_OUT 乙
ON 甲.職位標識 == 乙.職位標識 AND 甲.欄0 == 乙.欄0;




DROP TABLE IF EXISTS TRE_X;
CREATE TABLE TRE_X AS
SELECT *
FROM
(
    SELECT *, 1.0 * ROW_NUMBER() OVER(PARTITION BY 用戶標識 ORDER BY 記錄標識) AS 序號, 1.0 * ROW_NUMBER() OVER(PARTITION BY 用戶標識 ORDER BY 記錄標識 DESC) AS 逆序號 FROM TRAIN_X
    UNION ALL
    SELECT *, 1.0 * ROW_NUMBER() OVER(PARTITION BY 用戶標識 ORDER BY 記錄標識) AS 序號, 1.0 * ROW_NUMBER() OVER(PARTITION BY 用戶標識 ORDER BY 記錄標識 DESC) AS 逆序號 FROM TEST_X
) 甲;


DROP TABLE IF EXISTS TRAIN_X_JD;
CREATE TABLE TRAIN_X_JD AS
SELECT 
    甲.*
    , 職0, 職0_1, 職0_2, 職0_3, 職0_4, 職0_5, 職0_6, 職0_7, 職0_8, 職1, 職2, 職1_1, 職2_1, 職3, 職4, 職5, 職6, 職7, 職8, 職9, 職10, 職11, 職12, 職13, 職14, 職15, 職16, 職17, 職18, 職19, 職20, 職21, 職22
    , 職甲0, 職甲1, 職甲2, 職甲3, 職甲4, 職甲5, 職甲6, 職甲7, 職甲8, 職甲9, 職甲10, 職甲11, 職甲12, 職甲13, 職甲14, 職甲15, 職甲16, 職甲17, 職甲18, 職甲19, 職甲20, 職甲256, 職甲257
    , 職乙0, 職乙1, 職乙2, 職乙3, 職乙4, 職乙5, 職乙6, 職乙7, 職乙8, 職乙9, 職乙10, 職乙11, 職乙12, 職乙13, 職乙14, 職乙15, 職乙16, 職乙17, 職乙18, 職乙19, 職乙20, 職乙256, 職乙257
    , 職丙0, 職丙1, 職丙2, 職丙3, 職丙4, 職丙5, 職丙6, 職丙7, 職丙8, 職丙9, 職丙10, 職丙11, 職丙12, 職丙13, 職丙14, 職丙15, 職丙16, 職丙17, 職丙18, 職丙19, 職丙20, 職丙256, 職丙257
    , 職丁0, 職丁1, 職丁2, 職丁3, 職丁4, 職丁5, 職丁6, 職丁7, 職丁8, 職丁9, 職丁10, 職丁11, 職丁12, 職丁13, 職丁14, 職丁15, 職丁16, 職丁17, 職丁18, 職丁19, 職丁20, 職丁256, 職丁257
    , 職戊256, 職戊257
FROM TRAIN_X 甲
LEFT OUTER JOIN TRAIN_DATA_JD 乙
ON 甲.記錄標識 == 乙.記錄標識
LEFT OUTER JOIN
(
    SELECT 
        職位標識, 1.0 * COUNT(*) AS 職3, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職4
        , MIN(用戶期望最低薪水) AS 職5, MAX(用戶期望最低薪水) AS 職6, MEDIAN(用戶期望最低薪水) AS 職7
        , MIN(用戶學歷值) AS 職8, MAX(用戶學歷值) AS 職9, MEDIAN(用戶學歷值) AS 職10
        , MIN(用戶工作年限) AS 職11, MAX(用戶工作年限) AS 職12, MEDIAN(用戶工作年限) AS 職13
        , MIN(用戶年齡) AS 職14, MAX(用戶年齡) AS 職15, MEDIAN(用戶年齡) AS 職16
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職17, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職18
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職19, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職20
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職21, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職22
    FROM TRE_X
    GROUP BY 職位標識
) 丙一
ON 甲.職位標識 == 丙一.職位標識
LEFT OUTER JOIN
(
    SELECT 
        職位標題, 1.0 * COUNT(*) AS 職甲0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職甲1
        , MIN(用戶期望最低薪水) AS 職甲2, MAX(用戶期望最低薪水) AS 職甲3, MEDIAN(用戶期望最低薪水) AS 職甲4
        , MIN(用戶學歷值) AS 職甲5, MAX(用戶學歷值) AS 職甲6, MEDIAN(用戶學歷值) AS 職甲7
        , MIN(用戶工作年限) AS 職甲8, MAX(用戶工作年限) AS 職甲9, MEDIAN(用戶工作年限) AS 職甲10
        , AVG(職位需求人數) AS 職甲11, AVG(職位最低薪水) AS 職甲12, AVG(職位最低學歷值) AS 職甲13, AVG(職位工作年限) AS 職甲14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職甲15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職甲16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職甲17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職甲18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職甲19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職甲20
     FROM TRE_X GROUP BY 職位標題
) 丙二
ON 甲.職位標題 == 丙二.職位標題
LEFT OUTER JOIN
(
    SELECT 
        職位城市, 1.0 * COUNT(*) AS 職乙0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職乙1
        , MIN(用戶期望最低薪水) AS 職乙2, MAX(用戶期望最低薪水) AS 職乙3, MEDIAN(用戶期望最低薪水) AS 職乙4
        , MIN(用戶學歷值) AS 職乙5, MAX(用戶學歷值) AS 職乙6, MEDIAN(用戶學歷值) AS 職乙7
        , MIN(用戶工作年限) AS 職乙8, MAX(用戶工作年限) AS 職乙9, MEDIAN(用戶工作年限) AS 職乙10
        , AVG(職位需求人數) AS 職乙11, AVG(職位最低薪水) AS 職乙12, AVG(職位最低學歷值) AS 職乙13, AVG(職位工作年限) AS 職乙14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職乙15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職乙16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職乙17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職乙18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職乙19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職乙20
    FROM TRE_X GROUP BY 職位城市
) 丙三
ON 甲.職位城市 == 丙三.職位城市
LEFT OUTER JOIN
(
    SELECT 
        職位子類, 1.0 * COUNT(*) AS 職丙0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職丙1
        , MIN(用戶期望最低薪水) AS 職丙2, MAX(用戶期望最低薪水) AS 職丙3, MEDIAN(用戶期望最低薪水) AS 職丙4
        , MIN(用戶學歷值) AS 職丙5, MAX(用戶學歷值) AS 職丙6, MEDIAN(用戶學歷值) AS 職丙7
        , MIN(用戶工作年限) AS 職丙8, MAX(用戶工作年限) AS 職丙9, MEDIAN(用戶工作年限) AS 職丙10
        , AVG(職位需求人數) AS 職丙11, AVG(職位最低薪水) AS 職丙12, AVG(職位最低學歷值) AS 職丙13, AVG(職位工作年限) AS 職丙14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職丙15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職丙16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職丙17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職丙18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職丙19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職丙20
    FROM TRE_X GROUP BY 職位子類
) 丙四
ON 甲.職位子類 == 丙四.職位子類
LEFT OUTER JOIN
(
    SELECT 
        職位描述, 1.0 * COUNT(*) AS 職丁0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職丁1
        , MIN(用戶期望最低薪水) AS 職丁2, MAX(用戶期望最低薪水) AS 職丁3, AVG(用戶期望最低薪水) AS 職丁4
        , MIN(用戶學歷值) AS 職丁5, MAX(用戶學歷值) AS 職丁6, AVG(用戶學歷值) AS 職丁7
        , MIN(用戶工作年限) AS 職丁8, MAX(用戶工作年限) AS 職丁9, AVG(用戶工作年限) AS 職丁10
        , AVG(職位需求人數) AS 職丁11, AVG(職位最低薪水) AS 職丁12, AVG(職位最低學歷值) AS 職丁13, AVG(職位工作年限) AS 職丁14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職丁15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職丁16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職丁17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職丁18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職丁19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職丁20
    FROM TRE_X GROUP BY 職位描述
) 丙五
ON 甲.職位描述 == 丙五.職位描述
LEFT OUTER JOIN
(
    SELECT 職位標題, 1.0 * COUNT(*) AS 職甲256, AVG(職位需求人數) AS 職甲257 FROM JD_X GROUP BY 職位標題
) 丁一
ON 甲.職位標題 == 丁一.職位標題
LEFT OUTER JOIN
(
    SELECT 職位城市, 1.0 * COUNT(*) AS 職乙256, AVG(職位需求人數) AS 職乙257 FROM JD_X GROUP BY 職位城市
) 丁二
ON 甲.職位城市 == 丁二.職位城市
LEFT OUTER JOIN
(
    SELECT 職位子類, 1.0 * COUNT(*) AS 職丙256, AVG(職位需求人數) AS 職丙257 FROM JD_X GROUP BY 職位子類
) 丁三
ON 甲.職位子類 == 丁三.職位子類
LEFT OUTER JOIN
(
    SELECT 職位描述, 1.0 * COUNT(*) AS 職丁256, AVG(職位需求人數) AS 職丁257 FROM JD_X GROUP BY 職位描述
) 丁四
ON 甲.職位描述 == 丁四.職位描述
LEFT OUTER JOIN
(
    SELECT 職位關鍵詞, 1.0 * COUNT(*) AS 職戊256, AVG(職位需求人數) AS 職戊257 FROM JD_X GROUP BY 職位關鍵詞
) 丁五
ON 甲.職位關鍵詞 == 丁五.職位關鍵詞
;


DROP TABLE IF EXISTS TEST_X_JD;
CREATE TABLE TEST_X_JD AS
SELECT 
    甲.*
    , 職0, 職0_1, 職0_2, 職0_3, 職0_4, 職0_5, 職0_6, 職0_7, 職0_8, 職1, 職2, 職1_1, 職2_1, 職3, 職4, 職5, 職6, 職7, 職8, 職9, 職10, 職11, 職12, 職13, 職14, 職15, 職16, 職17, 職18, 職19, 職20, 職21, 職22
    , 職甲0, 職甲1, 職甲2, 職甲3, 職甲4, 職甲5, 職甲6, 職甲7, 職甲8, 職甲9, 職甲10, 職甲11, 職甲12, 職甲13, 職甲14, 職甲15, 職甲16, 職甲17, 職甲18, 職甲19, 職甲20, 職甲256, 職甲257
    , 職乙0, 職乙1, 職乙2, 職乙3, 職乙4, 職乙5, 職乙6, 職乙7, 職乙8, 職乙9, 職乙10, 職乙11, 職乙12, 職乙13, 職乙14, 職乙15, 職乙16, 職乙17, 職乙18, 職乙19, 職乙20, 職乙256, 職乙257
    , 職丙0, 職丙1, 職丙2, 職丙3, 職丙4, 職丙5, 職丙6, 職丙7, 職丙8, 職丙9, 職丙10, 職丙11, 職丙12, 職丙13, 職丙14, 職丙15, 職丙16, 職丙17, 職丙18, 職丙19, 職丙20, 職丙256, 職丙257
    , 職丁0, 職丁1, 職丁2, 職丁3, 職丁4, 職丁5, 職丁6, 職丁7, 職丁8, 職丁9, 職丁10, 職丁11, 職丁12, 職丁13, 職丁14, 職丁15, 職丁16, 職丁17, 職丁18, 職丁19, 職丁20, 職丁256, 職丁257
    , 職戊256, 職戊257
FROM TEST_X 甲
LEFT OUTER JOIN TEST_DATA_JD 乙
ON 甲.記錄標識 == 乙.記錄標識
LEFT OUTER JOIN
(
    SELECT 
        職位標識, 1.0 * COUNT(*) AS 職3, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職4
        , MIN(用戶期望最低薪水) AS 職5, MAX(用戶期望最低薪水) AS 職6, MEDIAN(用戶期望最低薪水) AS 職7
        , MIN(用戶學歷值) AS 職8, MAX(用戶學歷值) AS 職9, MEDIAN(用戶學歷值) AS 職10
        , MIN(用戶工作年限) AS 職11, MAX(用戶工作年限) AS 職12, MEDIAN(用戶工作年限) AS 職13
        , MIN(用戶年齡) AS 職14, MAX(用戶年齡) AS 職15, MEDIAN(用戶年齡) AS 職16
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職17, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職18
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職19, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職20
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職21, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職22
    FROM TRE_X
    GROUP BY 職位標識
) 丙一
ON 甲.職位標識 == 丙一.職位標識
LEFT OUTER JOIN
(
    SELECT 
        職位標題, 1.0 * COUNT(*) AS 職甲0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職甲1
        , MIN(用戶期望最低薪水) AS 職甲2, MAX(用戶期望最低薪水) AS 職甲3, MEDIAN(用戶期望最低薪水) AS 職甲4
        , MIN(用戶學歷值) AS 職甲5, MAX(用戶學歷值) AS 職甲6, MEDIAN(用戶學歷值) AS 職甲7
        , MIN(用戶工作年限) AS 職甲8, MAX(用戶工作年限) AS 職甲9, MEDIAN(用戶工作年限) AS 職甲10
        , AVG(職位需求人數) AS 職甲11, AVG(職位最低薪水) AS 職甲12, AVG(職位最低學歷值) AS 職甲13, AVG(職位工作年限) AS 職甲14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職甲15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職甲16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職甲17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職甲18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職甲19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職甲20
     FROM TRE_X GROUP BY 職位標題
) 丙二
ON 甲.職位標題 == 丙二.職位標題
LEFT OUTER JOIN
(
    SELECT 
        職位城市, 1.0 * COUNT(*) AS 職乙0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職乙1
        , MIN(用戶期望最低薪水) AS 職乙2, MAX(用戶期望最低薪水) AS 職乙3, MEDIAN(用戶期望最低薪水) AS 職乙4
        , MIN(用戶學歷值) AS 職乙5, MAX(用戶學歷值) AS 職乙6, MEDIAN(用戶學歷值) AS 職乙7
        , MIN(用戶工作年限) AS 職乙8, MAX(用戶工作年限) AS 職乙9, MEDIAN(用戶工作年限) AS 職乙10
        , AVG(職位需求人數) AS 職乙11, AVG(職位最低薪水) AS 職乙12, AVG(職位最低學歷值) AS 職乙13, AVG(職位工作年限) AS 職乙14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職乙15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職乙16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職乙17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職乙18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職乙19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職乙20
    FROM TRE_X GROUP BY 職位城市
) 丙三
ON 甲.職位城市 == 丙三.職位城市
LEFT OUTER JOIN
(
    SELECT 
        職位子類, 1.0 * COUNT(*) AS 職丙0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職丙1
        , MIN(用戶期望最低薪水) AS 職丙2, MAX(用戶期望最低薪水) AS 職丙3, MEDIAN(用戶期望最低薪水) AS 職丙4
        , MIN(用戶學歷值) AS 職丙5, MAX(用戶學歷值) AS 職丙6, MEDIAN(用戶學歷值) AS 職丙7
        , MIN(用戶工作年限) AS 職丙8, MAX(用戶工作年限) AS 職丙9, MEDIAN(用戶工作年限) AS 職丙10
        , AVG(職位需求人數) AS 職丙11, AVG(職位最低薪水) AS 職丙12, AVG(職位最低學歷值) AS 職丙13, AVG(職位工作年限) AS 職丙14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職丙15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職丙16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職丙17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職丙18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職丙19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職丙20
    FROM TRE_X GROUP BY 職位子類
) 丙四
ON 甲.職位子類 == 丙四.職位子類
LEFT OUTER JOIN
(
    SELECT 
        職位描述, 1.0 * COUNT(*) AS 職丁0, 1.0 * COUNT(DISTINCT(用戶標識)) AS 職丁1
        , MIN(用戶期望最低薪水) AS 職丁2, MAX(用戶期望最低薪水) AS 職丁3, AVG(用戶期望最低薪水) AS 職丁4
        , MIN(用戶學歷值) AS 職丁5, MAX(用戶學歷值) AS 職丁6, AVG(用戶學歷值) AS 職丁7
        , MIN(用戶工作年限) AS 職丁8, MAX(用戶工作年限) AS 職丁9, AVG(用戶工作年限) AS 職丁10
        , AVG(職位需求人數) AS 職丁11, AVG(職位最低薪水) AS 職丁12, AVG(職位最低學歷值) AS 職丁13, AVG(職位工作年限) AS 職丁14
        , 1.0 * COUNT(DISTINCT(用戶期望城市)) AS 職丁15, 1.0 * COUNT(DISTINCT(用戶期望行業)) AS 職丁16
        , 1.0 * COUNT(DISTINCT(用戶期望職類)) AS 職丁17, 1.0 * COUNT(DISTINCT(用戶城市)) AS 職丁18
        , 1.0 * COUNT(DISTINCT(用戶當前行業)) AS 職丁19, 1.0 * COUNT(DISTINCT(用戶當前職類)) AS 職丁20
    FROM TRE_X GROUP BY 職位描述
) 丙五
ON 甲.職位描述 == 丙五.職位描述
LEFT OUTER JOIN
(
    SELECT 職位標題, 1.0 * COUNT(*) AS 職甲256, AVG(職位需求人數) AS 職甲257 FROM JD_X GROUP BY 職位標題
) 丁一
ON 甲.職位標題 == 丁一.職位標題
LEFT OUTER JOIN
(
    SELECT 職位城市, 1.0 * COUNT(*) AS 職乙256, AVG(職位需求人數) AS 職乙257 FROM JD_X GROUP BY 職位城市
) 丁二
ON 甲.職位城市 == 丁二.職位城市
LEFT OUTER JOIN
(
    SELECT 職位子類, 1.0 * COUNT(*) AS 職丙256, AVG(職位需求人數) AS 職丙257 FROM JD_X GROUP BY 職位子類
) 丁三
ON 甲.職位子類 == 丁三.職位子類
LEFT OUTER JOIN
(
    SELECT 職位描述, 1.0 * COUNT(*) AS 職丁256, AVG(職位需求人數) AS 職丁257 FROM JD_X GROUP BY 職位描述
) 丁四
ON 甲.職位描述 == 丁四.職位描述
LEFT OUTER JOIN
(
    SELECT 職位關鍵詞, 1.0 * COUNT(*) AS 職戊256, AVG(職位需求人數) AS 職戊257 FROM JD_X GROUP BY 職位關鍵詞
) 丁五
ON 甲.職位關鍵詞 == 丁五.職位關鍵詞
;


DROP TABLE IF EXISTS TEST_DATA;
CREATE TABLE TEST_DATA
(
    用戶標識 STRING, 職位標識 STRING, 記錄標識 BIGINT, 瀏覽 BIGINT, 投遞 BIGINT, 認可 BIGINT
    , 用戶期望最低薪水 DOUBLE, 用戶期望最高薪水 DOUBLE, 用戶當前最低薪水 DOUBLE, 用戶當前最高薪水 DOUBLE, 用戶學歷值 DOUBLE, 用戶年齡 DOUBLE, 用戶工作年限 DOUBLE, 用戶工作年限差 DOUBLE 
    , 職位需求人數 DOUBLE, 職位最低薪水 DOUBLE, 職位最高薪水 DOUBLE, 職位出差否 DOUBLE, 職位工作年限 DOUBLE, 職位最低學歷值 DOUBLE, 職位開始日序 DOUBLE, 職位終止日序 DOUBLE
    , 特0 DOUBLE, 特1 DOUBLE, 特2 DOUBLE, 特3 DOUBLE, 特4 DOUBLE, 特5 DOUBLE, 特6 DOUBLE, 特7 DOUBLE , 特8 DOUBLE, 特9 DOUBLE, 特10 DOUBLE, 特11 DOUBLE
    , 特12 DOUBLE, 特13 DOUBLE, 特14 DOUBLE, 特15 DOUBLE , 特16 DOUBLE, 特17 DOUBLE, 特18 DOUBLE, 特19 DOUBLE, 特19_1 DOUBLE, 特20 DOUBLE, 特20_1 DOUBLE
    , 特21 DOUBLE, 特21_1 DOUBLE, 特22 DOUBLE, 特22_1 DOUBLE, 特23_1 DOUBLE, 特23_2 DOUBLE, 特23_3 DOUBLE, 特23_4 DOUBLE, 特23_5 DOUBLE, 特23_6 DOUBLE
    , 特23_7 DOUBLE, 特23_8 DOUBLE, 特23_9 DOUBLE, 特23_10 DOUBLE, 特24_1 DOUBLE, 特24_2 DOUBLE, 特24_3 DOUBLE, 特24_4 DOUBLE, 特24_5 DOUBLE, 特24_6 DOUBLE
    , 特25_1 DOUBLE, 特25_2 DOUBLE, 特25_3 DOUBLE, 特25_4 DOUBLE, 特28 DOUBLE, 特29 DOUBLE, 特30 DOUBLE, 特31 DOUBLE , 特32 DOUBLE, 特33 DOUBLE, 特34 DOUBLE, 特35 DOUBLE
    , 特36 DOUBLE, 特37 DOUBLE, 特38 DOUBLE, 特39 DOUBLE, 特40 DOUBLE, 特41_1 DOUBLE, 特41_2 DOUBLE, 特41_3 DOUBLE, 特41_4 DOUBLE, 特41_5 DOUBLE, 特41_6 DOUBLE, 特41_7 DOUBLE
    , 特42_1 DOUBLE, 特42_2 DOUBLE, 特42_3 DOUBLE, 特42_4 DOUBLE, 特42_5 DOUBLE, 特42_6 DOUBLE, 特42_7 DOUBLE, 特43_1 DOUBLE, 特43_2 DOUBLE, 特43_3 DOUBLE, 特43_4 DOUBLE
    , 特43_5 DOUBLE, 特43_6 DOUBLE, 特43_7 DOUBLE, 特44_1 DOUBLE, 特44_2 DOUBLE, 特44_3 DOUBLE, 特44_4 DOUBLE, 特44_5 DOUBLE, 特44_6 DOUBLE, 特44_7 DOUBLE
    , 特57 DOUBLE, 特58 DOUBLE, 特59 DOUBLE, 特60 DOUBLE, 特61 DOUBLE, 特62 DOUBLE, 特63 DOUBLE
    , 特64 DOUBLE, 特65 DOUBLE, 特66 DOUBLE, 特67 DOUBLE, 特68 DOUBLE, 特69 DOUBLE, 特70 DOUBLE, 特71 DOUBLE, 特72 DOUBLE, 特73 DOUBLE, 特74 DOUBLE, 特75 DOUBLE
    , 特76 DOUBLE, 特77 DOUBLE, 特78 DOUBLE, 特79 DOUBLE, 特80 DOUBLE, 特81 DOUBLE, 特82 DOUBLE, 特83 DOUBLE, 特84 DOUBLE, 特85 DOUBLE, 特86 DOUBLE, 特87 DOUBLE
    , 特88 DOUBLE, 特89 DOUBLE, 特90 DOUBLE, 特91 DOUBLE, 特92 DOUBLE, 特93 DOUBLE, 特94 DOUBLE, 特95 DOUBLE, 特96 DOUBLE, 特97 DOUBLE, 特98 DOUBLE, 特99 DOUBLE
    , 特100 DOUBLE, 特101 DOUBLE, 特102 DOUBLE, 特103 DOUBLE, 特104 DOUBLE, 特105 DOUBLE, 特106 DOUBLE, 特107 DOUBLE, 特108 DOUBLE, 特109 DOUBLE, 特110 DOUBLE, 特111 DOUBLE
    , 特112 DOUBLE, 特113 DOUBLE, 特114 DOUBLE, 特115 DOUBLE, 特116 DOUBLE, 特117 DOUBLE, 特118 DOUBLE, 特119 DOUBLE, 特120 DOUBLE, 特121 DOUBLE, 特122 DOUBLE, 特123 DOUBLE
    , 特124 DOUBLE, 特125 DOUBLE, 特126 DOUBLE, 特127 DOUBLE, 特128 DOUBLE, 特129 DOUBLE, 特130 DOUBLE, 特131 DOUBLE, 特132 DOUBLE, 特133 DOUBLE, 特134 DOUBLE, 特135 DOUBLE
    , 特136 DOUBLE, 特137 DOUBLE, 特138 DOUBLE, 特139 DOUBLE, 特140 DOUBLE, 特141 DOUBLE, 特142 DOUBLE, 特143 DOUBLE, 特144 DOUBLE, 特145 DOUBLE, 特146 DOUBLE, 特147 DOUBLE
    , 特148 DOUBLE, 特149 DOUBLE, 特150 DOUBLE, 特151 DOUBLE, 特152 DOUBLE, 特153 DOUBLE, 特154 DOUBLE, 特155 DOUBLE, 特156 DOUBLE, 特157 DOUBLE, 特158 DOUBLE, 特159 DOUBLE
    , 特160 DOUBLE, 特161 DOUBLE, 特162 DOUBLE, 特163 DOUBLE, 特164 DOUBLE, 特165 DOUBLE, 特166 DOUBLE, 特167 DOUBLE, 特168 DOUBLE, 特169 DOUBLE, 特170 DOUBLE, 特171 DOUBLE
    , 特172 DOUBLE, 特173 DOUBLE, 特174 DOUBLE, 特175 DOUBLE, 特176 DOUBLE, 特177 DOUBLE, 特178 DOUBLE, 特179 DOUBLE, 特180 DOUBLE, 特181 DOUBLE, 特182 DOUBLE, 特183 DOUBLE
    , 特184 DOUBLE, 特185 DOUBLE, 特186 DOUBLE, 特187 DOUBLE, 特188 DOUBLE, 特189 DOUBLE, 特190 DOUBLE, 特191 DOUBLE, 特192 DOUBLE, 特193 DOUBLE, 特194 DOUBLE, 特195 DOUBLE
    , 特196 DOUBLE, 特197 DOUBLE, 特198 DOUBLE, 特199 DOUBLE, 特200 DOUBLE, 特201 DOUBLE, 特202 DOUBLE, 特203 DOUBLE, 特204 DOUBLE, 特205 DOUBLE, 特206 DOUBLE, 特207 DOUBLE
    , 特208 DOUBLE, 特209 DOUBLE, 特210 DOUBLE, 特211 DOUBLE, 特212 DOUBLE, 特213 DOUBLE, 特214 DOUBLE, 特215 DOUBLE, 特216 DOUBLE, 特217 DOUBLE, 特218 DOUBLE, 特219 DOUBLE
    , 特220 DOUBLE, 特221 DOUBLE, 特222 DOUBLE, 特223 DOUBLE, 特224 DOUBLE, 特225 DOUBLE, 特226 DOUBLE, 特227 DOUBLE, 特228 DOUBLE, 特229 DOUBLE, 特230 DOUBLE, 特231 DOUBLE
    , 特232 DOUBLE, 特233 DOUBLE, 特234 DOUBLE, 特235 DOUBLE, 特236 DOUBLE, 特237 DOUBLE, 特238 DOUBLE, 特239 DOUBLE, 特240 DOUBLE, 特241 DOUBLE, 特242 DOUBLE, 特243 DOUBLE
    , 特244 DOUBLE, 特245 DOUBLE, 特246 DOUBLE, 特247 DOUBLE, 特248 DOUBLE, 特249 DOUBLE, 特250 DOUBLE, 特251 DOUBLE, 特252 DOUBLE, 特253 DOUBLE, 特254 DOUBLE, 特255 DOUBLE
    , 特256 DOUBLE, 特257 DOUBLE, 特258 DOUBLE, 特259 DOUBLE, 特260 DOUBLE, 特261 DOUBLE, 特262 DOUBLE, 特263 DOUBLE, 特264 DOUBLE, 特265 DOUBLE, 特266 DOUBLE, 特267 DOUBLE
    , 特268 DOUBLE
);


DROP TABLE IF EXISTS TRAIN_DATA;
CREATE TABLE TRAIN_DATA
(
    用戶標識 STRING, 職位標識 STRING, 記錄標識 BIGINT, 瀏覽 BIGINT, 投遞 BIGINT, 認可 BIGINT
    , 用戶期望最低薪水 DOUBLE, 用戶期望最高薪水 DOUBLE, 用戶當前最低薪水 DOUBLE, 用戶當前最高薪水 DOUBLE, 用戶學歷值 DOUBLE, 用戶年齡 DOUBLE, 用戶工作年限 DOUBLE, 用戶工作年限差 DOUBLE 
    , 職位需求人數 DOUBLE, 職位最低薪水 DOUBLE, 職位最高薪水 DOUBLE, 職位出差否 DOUBLE, 職位工作年限 DOUBLE, 職位最低學歷值 DOUBLE, 職位開始日序 DOUBLE, 職位終止日序 DOUBLE
    , 特0 DOUBLE, 特1 DOUBLE, 特2 DOUBLE, 特3 DOUBLE, 特4 DOUBLE, 特5 DOUBLE, 特6 DOUBLE, 特7 DOUBLE , 特8 DOUBLE, 特9 DOUBLE, 特10 DOUBLE, 特11 DOUBLE
    , 特12 DOUBLE, 特13 DOUBLE, 特14 DOUBLE, 特15 DOUBLE , 特16 DOUBLE, 特17 DOUBLE, 特18 DOUBLE, 特19 DOUBLE, 特19_1 DOUBLE, 特20 DOUBLE, 特20_1 DOUBLE
    , 特21 DOUBLE, 特21_1 DOUBLE, 特22 DOUBLE, 特22_1 DOUBLE, 特23_1 DOUBLE, 特23_2 DOUBLE, 特23_3 DOUBLE, 特23_4 DOUBLE, 特23_5 DOUBLE, 特23_6 DOUBLE
    , 特23_7 DOUBLE, 特23_8 DOUBLE, 特23_9 DOUBLE, 特23_10 DOUBLE, 特24_1 DOUBLE, 特24_2 DOUBLE, 特24_3 DOUBLE, 特24_4 DOUBLE, 特24_5 DOUBLE, 特24_6 DOUBLE
    , 特25_1 DOUBLE, 特25_2 DOUBLE, 特25_3 DOUBLE, 特25_4 DOUBLE, 特28 DOUBLE, 特29 DOUBLE, 特30 DOUBLE, 特31 DOUBLE , 特32 DOUBLE, 特33 DOUBLE, 特34 DOUBLE, 特35 DOUBLE
    , 特36 DOUBLE, 特37 DOUBLE, 特38 DOUBLE, 特39 DOUBLE, 特40 DOUBLE, 特41_1 DOUBLE, 特41_2 DOUBLE, 特41_3 DOUBLE, 特41_4 DOUBLE, 特41_5 DOUBLE, 特41_6 DOUBLE, 特41_7 DOUBLE
    , 特42_1 DOUBLE, 特42_2 DOUBLE, 特42_3 DOUBLE, 特42_4 DOUBLE, 特42_5 DOUBLE, 特42_6 DOUBLE, 特42_7 DOUBLE, 特43_1 DOUBLE, 特43_2 DOUBLE, 特43_3 DOUBLE, 特43_4 DOUBLE
    , 特43_5 DOUBLE, 特43_6 DOUBLE, 特43_7 DOUBLE, 特44_1 DOUBLE, 特44_2 DOUBLE, 特44_3 DOUBLE, 特44_4 DOUBLE, 特44_5 DOUBLE, 特44_6 DOUBLE, 特44_7 DOUBLE
    , 特57 DOUBLE, 特58 DOUBLE, 特59 DOUBLE, 特60 DOUBLE, 特61 DOUBLE, 特62 DOUBLE, 特63 DOUBLE
    , 特64 DOUBLE, 特65 DOUBLE, 特66 DOUBLE, 特67 DOUBLE, 特68 DOUBLE, 特69 DOUBLE, 特70 DOUBLE, 特71 DOUBLE, 特72 DOUBLE, 特73 DOUBLE, 特74 DOUBLE, 特75 DOUBLE
    , 特76 DOUBLE, 特77 DOUBLE, 特78 DOUBLE, 特79 DOUBLE, 特80 DOUBLE, 特81 DOUBLE, 特82 DOUBLE, 特83 DOUBLE, 特84 DOUBLE, 特85 DOUBLE, 特86 DOUBLE, 特87 DOUBLE
    , 特88 DOUBLE, 特89 DOUBLE, 特90 DOUBLE, 特91 DOUBLE, 特92 DOUBLE, 特93 DOUBLE, 特94 DOUBLE, 特95 DOUBLE, 特96 DOUBLE, 特97 DOUBLE, 特98 DOUBLE, 特99 DOUBLE
    , 特100 DOUBLE, 特101 DOUBLE, 特102 DOUBLE, 特103 DOUBLE, 特104 DOUBLE, 特105 DOUBLE, 特106 DOUBLE, 特107 DOUBLE, 特108 DOUBLE, 特109 DOUBLE, 特110 DOUBLE, 特111 DOUBLE
    , 特112 DOUBLE, 特113 DOUBLE, 特114 DOUBLE, 特115 DOUBLE, 特116 DOUBLE, 特117 DOUBLE, 特118 DOUBLE, 特119 DOUBLE, 特120 DOUBLE, 特121 DOUBLE, 特122 DOUBLE, 特123 DOUBLE
    , 特124 DOUBLE, 特125 DOUBLE, 特126 DOUBLE, 特127 DOUBLE, 特128 DOUBLE, 特129 DOUBLE, 特130 DOUBLE, 特131 DOUBLE, 特132 DOUBLE, 特133 DOUBLE, 特134 DOUBLE, 特135 DOUBLE
    , 特136 DOUBLE, 特137 DOUBLE, 特138 DOUBLE, 特139 DOUBLE, 特140 DOUBLE, 特141 DOUBLE, 特142 DOUBLE, 特143 DOUBLE, 特144 DOUBLE, 特145 DOUBLE, 特146 DOUBLE, 特147 DOUBLE
    , 特148 DOUBLE, 特149 DOUBLE, 特150 DOUBLE, 特151 DOUBLE, 特152 DOUBLE, 特153 DOUBLE, 特154 DOUBLE, 特155 DOUBLE, 特156 DOUBLE, 特157 DOUBLE, 特158 DOUBLE, 特159 DOUBLE
    , 特160 DOUBLE, 特161 DOUBLE, 特162 DOUBLE, 特163 DOUBLE, 特164 DOUBLE, 特165 DOUBLE, 特166 DOUBLE, 特167 DOUBLE, 特168 DOUBLE, 特169 DOUBLE, 特170 DOUBLE, 特171 DOUBLE
    , 特172 DOUBLE, 特173 DOUBLE, 特174 DOUBLE, 特175 DOUBLE, 特176 DOUBLE, 特177 DOUBLE, 特178 DOUBLE, 特179 DOUBLE, 特180 DOUBLE, 特181 DOUBLE, 特182 DOUBLE, 特183 DOUBLE
    , 特184 DOUBLE, 特185 DOUBLE, 特186 DOUBLE, 特187 DOUBLE, 特188 DOUBLE, 特189 DOUBLE, 特190 DOUBLE, 特191 DOUBLE, 特192 DOUBLE, 特193 DOUBLE, 特194 DOUBLE, 特195 DOUBLE
    , 特196 DOUBLE, 特197 DOUBLE, 特198 DOUBLE, 特199 DOUBLE, 特200 DOUBLE, 特201 DOUBLE, 特202 DOUBLE, 特203 DOUBLE, 特204 DOUBLE, 特205 DOUBLE, 特206 DOUBLE, 特207 DOUBLE
    , 特208 DOUBLE, 特209 DOUBLE, 特210 DOUBLE, 特211 DOUBLE, 特212 DOUBLE, 特213 DOUBLE, 特214 DOUBLE, 特215 DOUBLE, 特216 DOUBLE, 特217 DOUBLE, 特218 DOUBLE, 特219 DOUBLE
    , 特220 DOUBLE, 特221 DOUBLE, 特222 DOUBLE, 特223 DOUBLE, 特224 DOUBLE, 特225 DOUBLE, 特226 DOUBLE, 特227 DOUBLE, 特228 DOUBLE, 特229 DOUBLE, 特230 DOUBLE, 特231 DOUBLE
    , 特232 DOUBLE, 特233 DOUBLE, 特234 DOUBLE, 特235 DOUBLE, 特236 DOUBLE, 特237 DOUBLE, 特238 DOUBLE, 特239 DOUBLE, 特240 DOUBLE, 特241 DOUBLE, 特242 DOUBLE, 特243 DOUBLE
    , 特244 DOUBLE, 特245 DOUBLE, 特246 DOUBLE, 特247 DOUBLE, 特248 DOUBLE, 特249 DOUBLE, 特250 DOUBLE, 特251 DOUBLE, 特252 DOUBLE, 特253 DOUBLE, 特254 DOUBLE, 特255 DOUBLE
    , 特256 DOUBLE, 特257 DOUBLE, 特258 DOUBLE, 特259 DOUBLE, 特260 DOUBLE, 特261 DOUBLE, 特262 DOUBLE, 特263 DOUBLE, 特264 DOUBLE, 特265 DOUBLE, 特266 DOUBLE, 特267 DOUBLE
    , 特268 DOUBLE
);

--@resource_reference{"RgExtract.jar"}
jar -resources RgExtract.jar -classpath ./RgExtract.jar jiangli.rg.rg_extract.RgExtractUser TRAIN_X_JD TRAIN_DATA;

--@resource_reference{"RgExtract.jar"}
jar -resources RgExtract.jar -classpath ./RgExtract.jar jiangli.rg.rg_extract.RgExtractUser TEST_X_JD TEST_DATA;


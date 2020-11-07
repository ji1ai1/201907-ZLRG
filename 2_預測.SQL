DROP TABLE IF EXISTS PREDICTION;
CREATE TABLE PREDICTION AS 
SELECT
    用戶標識, 職位標識
    , POW(SUM(POW(瀏覽打分, 2)), 0.125) * POW(SUM(POW(投遞打分, 2)), 0.125) * POW(SUM(POW(認可打分, 2)), 0.25) AS 打分
FROM
(
    SELECT 
        甲二.用戶標識
        , 甲二.職位標識
        , 甲二.記錄標識
        , ABS(1 - 乙二.結果甲 - 乙二.結果分甲) AS 瀏覽打分
        , ABS(1 - 丙二.結果乙 - 丙二.結果分乙) AS 投遞打分
        , ABS(1 - 丁二.結果丙 - 丁二.結果分丙) AS 認可打分
    FROM TEST_X 甲二
    LEFT OUTER JOIN A_PREDICTION 乙二
    ON 甲二.記錄標識 == 乙二.記錄標識
    LEFT OUTER JOIN B_PREDICTION 丙二
    ON 甲二.記錄標識 == 丙二.記錄標識
    LEFT OUTER JOIN C_PREDICTION 丁二
    ON 甲二.記錄標識 == 丁二.記錄標識
) 甲
GROUP BY 用戶標識, 職位標識;


DROP TABLE IF EXISTS ZHAOPIN_ROUND2_SUBMIT;
CREATE TABLE ZHAOPIN_ROUND2_SUBMIT AS
SELECT 用戶標識 AS USER_ID, 職位標識 AS JD_NO, ROW_NUMBER() OVER(ORDER BY -打分, 用戶標識) AS INDEX_EVALUATE
FROM PREDICTION;

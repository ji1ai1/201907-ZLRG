#R version 3.6.1 (2019-07-05) -- "Action of the Toes"
#R package data.table 1.12.2
#輸入：
#	zhaopin_round1_train_20190716/table3_action
#	zhaopin_round1_user_exposure_A_20190723
#輸出：
#	result.csv
#
library(data.table)

Training = fread("zhaopin_round1_train_20190716/table3_action", encoding = "UTF-8", fill = T)
Testing = fread("zhaopin_round1_user_exposure_A_20190723", encoding = "UTF-8", fill = T)

Prediction = merge(Testing, Training[, .(satisfied_ratio = mean(satisfied)), .(jd_no)], by = "jd_no", all.x = T)
Prediction[is.na(Prediction)] = 0
Prediction = Prediction[, .(user_id, jd_no, score = satisfied_ratio)]
Prediction = Prediction[order(-score)]

write.csv(Prediction[, .(user_id, jd_no)], "result.csv", row.names = F, quote = F)

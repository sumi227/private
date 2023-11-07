# dt='20230213'
# bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql

# dt='20230331'
# bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql

# for dt in `seq 20230401 1 20230403` 
# do
#     bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
#     # bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     # bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     # bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# for dt in `seq 20230204 1 20230207` 
# do
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

# dt='20230331'
# bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
# bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
# bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql

# for dt in `seq 20230401 1 20230403` 
# do
#     # bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
#     bee --hivevar dt=${dt} -f 2_mobile_payment_transport_session_daily.sql
#     bee --hivevar dt=${dt} -f 3_mobile_payment_session_daily.sql
#     bee --hivevar dt=${dt} -f 4_mobile_payment_mapping_daily.sql
# done

for dt in `seq 20230915 1 20230930` 
do
    bee --hivevar dt=${dt} -f 1_mobile_pay_host_daily.sql
done

for dt in `seq 20230901 1 20230930` 
do
    bee --hivevar dt=${dt} -f 1_mobile_pay_shop_daily.sql
done
